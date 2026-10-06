package cluster

import (
	"context"
	"strconv"
	"testing"
	"time"

	"pushupes/internal/data"
	"pushupes/internal/storage"
)

// ---- PlanLeaderRebalance: the ring anchor, as a pure function --------------

func rebalanceTable(nodes ...string) *Table {
	tbl := NewTable(8, 2)
	for _, id := range nodes {
		// tests model ONLINE members by default: the ring anchors on the
		// self-announced client addr (see OnlinePeerIDs).
		tbl.Peers[id] = Peer{ID: id, ClientAddr: "http://" + id + ":8591"}
	}
	tbl.Slots = PlanSlots(nodes, tbl.SlotCount, 2)
	return tbl
}

func slotSet(t *testing.T, moves []LeaderMove) map[int32]LeaderMove {
	t.Helper()
	out := map[int32]LeaderMove{}
	for _, m := range moves {
		if _, dup := out[m.Slot]; dup {
			t.Fatalf("duplicate slot %d in plan", m.Slot)
		}
		out[m.Slot] = m
	}
	return out
}

func TestPlanLeaderRebalanceRingAnchored(t *testing.T) {
	// Ring layout: nothing to move — the plan is empty on a fresh table.
	tbl := rebalanceTable("node-1", "node-2", "node-3")
	if moves := PlanLeaderRebalance(tbl); len(moves) != 0 {
		t.Fatalf("a ring-exact table must plan no moves, got %v", moves)
	}

	// node-1 dies: the failover command (OpLeaveNode) drops it from the
	// directory and hands its slots to the surviving replicas. With node-1
	// gone the ring is the SURVIVORS' ring: slots 1 and 7 (node-2's under
	// the old ring) now expect node-3 — the plan balances leadership across
	// the nodes that are actually up, not "give node-1's slots back" (it is
	// not a member to give anything to).
	apply := func(c *Command) {
		if err := tbl.Apply(c); err != nil {
			t.Fatalf("apply %s: %v", c.Op, err)
		}
	}
	apply(&Command{Op: OpLeaveNode, NodeID: "node-1"})
	moves := PlanLeaderRebalance(tbl)
	if len(moves) != 2 {
		t.Fatalf("the survivors' ring must plan exactly the slots 1 and 7 (to node-3), got %v", moves)
	}
	for _, m := range moves {
		if m.To != "node-3" || m.Slot != 1 && m.Slot != 7 {
			t.Fatalf("unexpected survivor-ring move %+v", m)
		}
	}

	// node-1 returns and replan re-admits it: the ring is its own again and
	// its slots, still led by the failover nodes, must be handed back. This
	// is the case the rebalancer exists for. The join seed carries no
	// client addr (the -peers shape); re-entering the ring requires the
	// re-registration first.
	apply(&Command{Op: OpJoinNode, Peer: &Peer{ID: "node-1"}})
	apply(&Command{Op: OpRegister, Peer: &Peer{ID: "node-1", AdminAddr: "http://127.0.0.1:8091", ClientAddr: "http://127.0.0.1:8591"}})
	apply(&Command{Op: OpReplanSlots})
	moves = PlanLeaderRebalance(tbl)
	if len(moves) != 3 {
		t.Fatalf("expected one move per ring slot of node-1 (0,3,6), got %v", moves)
	}
	bySlot := slotSet(t, moves)
	for s, m := range bySlot {
		if m.To != "node-1" {
			t.Errorf("slot %d: rebalance must hand leadership BACK to the ring leader node-1, got %+v", s, m)
		}
		if m.From == "" || m.From == "node-1" {
			t.Errorf("slot %d: bogus source %q", s, m.From)
		}
	}
	// Every move is an in-set hand-over: the target already holds a copy.
	for _, m := range moves {
		p := tbl.Slots[m.Slot]
		if !replicaListHas(p.Replicas, m.To) {
			t.Fatalf("slot %d: rebalance targets %s which is NOT a replica — that would be a full data migration", m.Slot, m.To)
		}
	}

	// Executing the plan (leader moves onto replica-set members) must drain
	// it: the table converges to the ring in one round.
	var slots []int32
	for _, m := range moves {
		slots = append(slots, m.Slot)
	}
	apply(&Command{Op: OpLeaderMove, Slots: slots, NewLeader: "node-1"})
	if left := PlanLeaderRebalance(tbl); len(left) != 0 {
		t.Fatalf("applying the plan must reach the ring layout, leftovers: %v", left)
	}
}

func TestPlanLeaderRebalanceIgnoresWhatItMustNotTouch(t *testing.T) {
	tbl := rebalanceTable("node-1", "node-2", "node-3")

	// Slot 0: mid-migration (a human started it) — even though its leader
	// then deviates from the ring, a non-stable slot is never a plan entry.
	// (Set the placement by hand: Apply(OpLeaderMove) commits a migration
	// and returns the slot to stable, which is its own behaviour to keep.)
	if err := tbl.Apply(&Command{Op: OpSlotState, Slots: []int32{0}, State: SlotMigratingOut, MigratingTo: "node-2"}); err != nil {
		t.Fatal(err)
	}
	tbl.Slots[0].Leader = "node-2"

	// Slot 2: leader unassigned (everything else is down) is not a
	// deviation this function fixes — the operator (or the full plan) owns it.
	tbl.Slots[2].Leader = ""

	for _, m := range PlanLeaderRebalance(tbl) {
		if m.Slot == 0 || m.Slot == 2 {
			t.Fatalf("plan touches a slot it must not: %+v", m)
		}
	}

	// Single node: nothing to rebalance between.
	single := NewTable(4, 1)
	single.Peers["node-1"] = Peer{ID: "node-1"}
	single.Slots = PlanSlots([]string{"node-1"}, 4, 1)
	if moves := PlanLeaderRebalance(single); moves != nil {
		t.Fatalf("single-node plan: %v", moves)
	}
}

func TestPlanLeaderRebalanceSkipsOfflinePeers(t *testing.T) {
	// The ring anchors on ONLINE members only: a peer in the directory that
	// has not self-announced its client-plane address (ClientAddr empty —
	// the -peers seed carries just the peer port) is not a node clients can
	// reach, so it neither takes part in the ring nor may leadership move
	// onto it. Here node-3 is offline in that sense: the plan treats the
	// cluster as a 2-node ring.
	tbl := rebalanceTable("node-1", "node-2", "node-3")
	tbl.Peers["node-3"] = Peer{ID: "node-3"} // no ClientAddr: offline
	// Everything piled on node-1 (an outage's worst shape): with node-3
	// offline the online ring is node-1/node-2, so the plan must shed the
	// ODD slots to node-2 and never touch node-3 — neither as a target nor
	// through a 3-node ring that would give slots 2,5 to node-3.
	for _, p := range tbl.Slots {
		p.Leader = "node-1"
	}
	moves := PlanLeaderRebalance(tbl)
	for _, m := range moves {
		if m.To == "node-3" {
			t.Fatalf("leadership must not move onto an offline peer: %+v", m)
		}
		want := []string{"node-1", "node-2"}[int(m.Slot)%2]
		if m.To != want {
			t.Fatalf("slot %d: online-ring leader should be %q, got %+v", m.Slot, want, m)
		}
	}
	// odd slots whose replica set genuinely holds node-2 must be planned
	if len(moves) == 0 {
		t.Fatal("an unbalanced layout over the ONLINE ring must plan moves")
	}
	// once node-3 re-registers, the 3-node ring is authoritative again and
	// the plan hands node-3's own slots back to it
	tbl.Peers["node-3"] = Peer{ID: "node-3", ClientAddr: "http://node-3:8591"}
	var back int
	for _, m := range PlanLeaderRebalance(tbl) {
		if m.To == "node-3" {
			back++
		}
	}
	if back == 0 {
		t.Fatal("a re-registered node must re-enter the ring")
	}
}

func TestPlanLeaderRebalanceSkipsNodeWithoutTheData(t *testing.T) {
	tbl := rebalanceTable("node-1", "node-2", "node-3")
	// node-1 left and has NOT been re-admitted: every ring slot of node-1
	// is led elsewhere but node-1 holds no copy. A leader hand-back would
	// have to move the data — that is a migration decision, not a
	// rebalance one, so the plan must be empty until replan tops the sets.
	if err := tbl.Apply(&Command{Op: OpLeaveNode, NodeID: "node-1"}); err != nil {
		t.Fatal(err)
	}
	for _, p := range tbl.Slots {
		p.Replicas = []string{} // failover removed node-1; keep the sets honest
	}
	if moves := PlanLeaderRebalance(tbl); len(moves) != 0 {
		t.Fatalf("no node without a replica copy may be a rebalance target: %v", moves)
	}
}

// ---- rebalanceRound: the controller-loop gates ------------------------------

// tableWithLeaders plans the ring over the joined peers and moves the given
// slots onto leaderOf.
func tableWithLeaders(t *testing.T, e *Engine, leaderOf map[int32]string) {
	t.Helper()
	applyCmd(t, e, &Command{Op: OpPlanSlots})
	for s, who := range leaderOf {
		applyCmd(t, e, &Command{Op: OpSlotAddReplica, Slots: []int32{s}, NodeID: who})
		applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{s}, NewLeader: who})
	}
}

// layoutSettled is production code (table.go); these tests drive the
// rebalancer gates that consume it.

func TestLayoutSettled(t *testing.T) {
	tbl := rebalanceTable("node-1", "node-2", "node-3")
	if !layoutSettled(tbl) {
		t.Fatal("a fresh ring is settled")
	}
	if err := tbl.Apply(&Command{Op: OpSlotState, Slots: []int32{4}, State: SlotMigratingOut, MigratingTo: "node-1"}); err != nil {
		t.Fatal(err)
	}
	if layoutSettled(tbl) {
		t.Fatal("one migrating slot unsettles the whole round")
	}
}

func TestRebalanceRoundSkipsUnregisteredTarget(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	// The post-failover survivors' ring: slot 1 would move from node-2 to
	// node-3, but nothing listens on node-3's address (the test's fake
	// addresses): the gate must skip it without churning the placement.
	tableWithLeaders(t, e, map[int32]string{0: "node-2"})
	before := e.TableSnapshot()
	if len(PlanLeaderRebalance(before)) == 0 {
		t.Fatal("setup: the table must deviate from its ring or this proves nothing")
	}
	if done := e.rebalanceRound(context.Background(), 8); done != 0 {
		t.Fatalf("no move may execute against an unreachable target")
	}
	after := e.TableSnapshot()
	for s, p := range after.Slots {
		if beforeLeader := before.Slots[s].Leader; p.Leader != beforeLeader || p.State != before.Slots[s].State {
			t.Fatalf("gated round disturbed slot %d: %+v", s, p)
		}
	}
}

// ---- End-to-end: failover → rejoin → rebalance hands the ring leader back --

// TestRebalanceHandBackEndToEnd drives the real production shape on loopback
// with two engines and real stores: node-1 dies (OpLeaveNode: node-2 takes
// its ring slots), comes back (join + register + replan re-admits it as a
// FOLLOWER), and the rebalancer hands node-1 its ring leadership back — only
// once node-1's replica copy is equivalent, with the per-round batch limit
// respected, the segment snapshot skipped for the equivalent copies, and the
// acknowledged data intact on both sides afterwards.
func TestRebalanceHandBackEndToEnd(t *testing.T) {
	oldStore, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer oldStore.Close()
	old := NewEngine(nil, oldStore, "node-1", nil) // node-1: the ring leader, died, came back

	newStore, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer newStore.Close()
	neu := NewEngine(nil, newStore, "node-2", nil) // node-2: leads node-1's slots now
	neu.node = newTestRaftNode(t, neu)

	oldAddr := newPeerHarness(t, old)
	newAddr := newPeerHarness(t, neu)

	// Healthy ring on both tables (3 nodes / 8 slots: node-1 leads 0,3,6),
	// with the real addresses registered so every peer-plane probe answers.
	for _, e := range []*Engine{old, neu} {
		// The production join shape: the controller's directory sync carries
		// the PeerAddr from the raft config; registration patches the
		// self-announced admin/client addrs in place. node-3 gets an address
		// nobody listens on, so its liveness gate genuinely fires.
		applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{ID: "node-1", PeerAddr: oldAddr}})
		applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{ID: "node-2", PeerAddr: newAddr}})
		applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{ID: "node-3", PeerAddr: "127.0.0.1:9"}})
		applyCmd(t, e, &Command{Op: OpRegister, Peer: &Peer{ID: "node-1", AdminAddr: oldAddr, ClientAddr: oldAddr}})
		applyCmd(t, e, &Command{Op: OpRegister, Peer: &Peer{ID: "node-2", AdminAddr: newAddr, ClientAddr: newAddr}})
		// node-3 announces a client addr NOBODY listens on: it counts as
		// online (so it enters the ring), and the liveness gate fires for
		// real when a round tries to move leadership onto it. An
		// UNregistered peer would not even be planned — see
		// TestPlanLeaderRebalanceSkipsOfflinePeers.
		applyCmd(t, e, &Command{Op: OpRegister, Peer: &Peer{ID: "node-3", AdminAddr: "127.0.0.1:9", ClientAddr: "127.0.0.1:9"}})
		// The hand-back needs each slot to carry a replica on another node,
		// which is what the ring layout over three members gives; the
		// fixture states it instead of relying on a startup default.
		pinReplicas(t, e, 2)
		applyCmd(t, e, &Command{Op: OpPlanSlots})
	}

	// node-1 dies: the failover sweep's exact damage (OpLeaveNode drops it
	// from the directory and from every replica set, moving leadership to
	// replicas[0]) — node-2 now leads slots 0, 3, 6 as well as its own.
	// While node-1 is out of the directory the ring is the SURVIVORS' ring
	// (N=2): the plan wants node-2 to shed slots 1 and 7 to node-3 — a
	// balanced layout during the outage, which must wait for node-3 to be
	// reachable (its announced address goes nowhere here, so the liveness
	// gate must keep the round from acting).
	for _, e := range []*Engine{old, neu} {
		applyCmd(t, e, &Command{Op: OpLeaveNode, NodeID: "node-1"})
	}
	midFail := PlanLeaderRebalance(neu.TableSnapshot())
	if len(midFail) != 2 {
		t.Fatalf("with node-1 gone the ring rebalances over the survivors (want slots 1,7 to node-3), plan: %v", midFail)
	}
	for _, m := range midFail {
		if m.To != "node-3" || m.From != "node-2" {
			t.Fatalf("unexpected survivor-ring move %+v", m)
		}
	}
	if done := neu.rebalanceRound(context.Background(), 8); done != 0 {
		t.Fatalf("node-3 answers no ping: the round must not move leadership to it")
	}

	// Acknowledged writes for slot 0 land on the current leader node-2.
	slot0 := int32(0)
	agg := ""
	for i := 0; i < 4096 && agg == ""; i++ {
		id := "hand-back-" + strconv.Itoa(i)
		if newStore.SlotOf(id) == slot0 {
			agg = id
		}
	}
	if agg == "" {
		t.Fatal("test setup: no aggregate routes to slot 0")
	}
	for v := uint32(1); v <= 5; v++ {
		rec := &data.EventRecord{
			AggregateID: agg, Version: v,
			CommandID: "hb-" + strconv.FormatUint(uint64(v), 10),
			Events:    []data.Event{{Type: "T", Body: []byte("payload")}},
		}
		if _, err := newStore.Append(rec); err != nil {
			t.Fatal(err)
		}
	}

	// node-1 rejoins: join + register + replan. Replan tops slot 0's set
	// (short: [node-2]) back to the ring pair, making node-1 a FOLLOWER —
	// leadership is not moved. The rebalancer now has candidates {0,3,6},
	// but node-1's copy of slot 0 is five records behind: the equivalence
	// gate must hold that slot back. Slots 3 and 6 are EMPTY everywhere, so
	// they are equivalent already — and the batch limit (1) must stop the
	// round after the first one of them.
	for _, e := range []*Engine{old, neu} {
		applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{ID: "node-1", PeerAddr: oldAddr}})
		applyCmd(t, e, &Command{Op: OpRegister, Peer: &Peer{ID: "node-1", AdminAddr: oldAddr, ClientAddr: oldAddr}})
		applyCmd(t, e, &Command{Op: OpReplanSlots})
	}
	if p, _ := neu.TableSnapshot().Slots[slot0]; p.Leader != "node-2" || !replicaListHas(p.Replicas, "node-1") {
		t.Fatalf("test setup: slot 0 after rejoin %+v", p)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if done := neu.rebalanceRound(ctx, 1); done != 1 {
		t.Fatalf("batch limit: one empty-equivalent move, then the round stops, got %d", done)
	}
	if p, _ := neu.TableSnapshot().Slots[slot0]; p.Leader != "node-2" {
		t.Fatalf("slot 0 moved before its replica caught up: %+v", p)
	}
	if p, _ := neu.TableSnapshot().Slots[6]; p.Leader != "node-2" {
		t.Fatalf("slot 6 moved past the batch limit: %+v", p)
	}

	// node-1's fetch loop catches the replica copy up: same records, same
	// seqs, via the ordinary replication landing path.
	for seq := uint64(1); seq <= 5; seq++ {
		_, next, payload, err := newStore.ReadSlotBytes(slot0, seq, seq+1, 1<<20)
		if err != nil || next != seq+1 || len(payload) == 0 {
			t.Fatalf("read slot 0 seq %d: next=%d len=%d err=%v", seq, next, len(payload), err)
		}
		if err := old.HandleReplicate(slot0, seq, payload); err != nil {
			t.Fatal(err)
		}
	}

	// The final round: slot 0 is equivalent now, slot 6 still waits — both
	// hand back, and slot 0's migration must take the fast path (the
	// equivalence skip, the fence's ensureTargetConsistent seeing matching
	// digests: no segment is re-shipped for a copy that is already there).
	if done := neu.rebalanceRound(ctx, 8); done != 2 {
		t.Fatalf("want slot 0 back (caught up) + slot 6 (queued last round), got %d moves", done)
	}
	for _, s := range []int32{0, 3, 6} {
		p, _ := neu.TableSnapshot().Slots[s]
		if p.Leader != "node-1" || p.State != SlotStable || p.Epoch < 2 {
			t.Fatalf("slot %d not handed back to the ring leader: %+v", s, p)
		}
	}
	if oldStore.LastSeqOf(slot0) != 5 || newStore.LastSeqOf(slot0) != 5 {
		t.Fatalf("data must survive the hand-back on both sides: old=%d new=%d",
			oldStore.LastSeqOf(slot0), newStore.LastSeqOf(slot0))
	}
	if left := PlanLeaderRebalance(neu.TableSnapshot()); len(left) != 0 {
		t.Fatalf("after the hand-backs the ring is reached; leftovers: %v", left)
	}
}

// TestSlotCopyStatusSeparatesBehindFromDiverged pins the distinction the
// rebalance gate rests on — and getting it wrong is what stranded slots off
// the ring for good.
//
// A target whose LEO is below the source's is merely BEHIND: its fetch is
// still catching up, and the round must wait for it (that is the cheap
// hand-back the ring layout is built on). A target that reports the source's
// LEO with a DIFFERENT directory is DIVERGED: its aggregate directory stopped
// adopting versions at a gap, so the records above the gap sit in its WAL
// unreadable and no amount of fetching repairs them. Skipping that one every
// round is a permanent, silent refusal to rebalance the slot — the round has
// to hand it to the migration path so the copy gets rebuilt instead.
func TestSlotCopyStatusSeparatesBehindFromDiverged(t *testing.T) {
	src, srcStore := newTestEngine(t, "node-1")
	dst, dstStore := newTestEngine(t, "node-2")
	srcAddr := newPeerHarness(t, src)
	dstAddr := newPeerHarness(t, dst)
	ctx := context.Background()

	aggFor := func(st *storage.Store, slot int32, prefix string) string {
		for i := 0; i < 1_000_000; i++ {
			id := prefix + strconv.Itoa(i)
			if st.SlotOf(id) == slot {
				return id
			}
		}
		t.Fatalf("no aggregate routes to slot %d", slot)
		return ""
	}
	appendRange := func(st *storage.Store, agg string, from, to int) {
		for v := from; v <= to; v++ {
			rec := &data.EventRecord{
				AggregateID: agg, Version: uint32(v),
				CommandID: agg + "-" + strconv.Itoa(v),
				Events:    []data.Event{{Type: "T", Body: []byte("p")}},
			}
			if _, err := st.Append(rec); err != nil {
				t.Fatalf("append %s v%d: %v", agg, v, err)
			}
		}
	}

	// --- slot 0: behind, then equivalent ------------------------------------
	slot0 := int32(0)
	agg0 := aggFor(srcStore, slot0, "behind-")
	appendRange(srcStore, agg0, 1, 5)
	appendRange(dstStore, agg0, 1, 2)

	eq, behind, err := src.slotCopyStatus(ctx, slot0, srcAddr, dstAddr)
	if err != nil {
		t.Fatalf("slot 0 behind: %v", err)
	}
	if eq || !behind {
		t.Fatalf("a target 3 records behind must read as (equivalent=false, behind=true), got (%v,%v)", eq, behind)
	}

	appendRange(dstStore, agg0, 3, 5) // caught up: same records, same directory
	eq, behind, err = src.slotCopyStatus(ctx, slot0, srcAddr, dstAddr)
	if err != nil {
		t.Fatalf("slot 0 equivalent: %v", err)
	}
	if !eq || behind {
		t.Fatalf("a caught-up copy must read as (equivalent=true, behind=false), got (%v,%v)", eq, behind)
	}

	// --- slot 1: same LEO, different directory ------------------------------
	slot1 := int32(1)
	agg1 := aggFor(srcStore, slot1, "div-")
	appendRange(srcStore, agg1, 1, 5)

	// The target reaches the SAME LEO (5) through two aggregates instead of
	// one: the sums differ only in the aggregate count, which is exactly the
	// "high LEO, short stream" shape the digest exists to catch.
	aggA := aggFor(dstStore, slot1, "divA-")
	aggB := aggFor(dstStore, slot1, "divB-")
	if aggA == aggB {
		t.Fatal("test setup: need two distinct aggregates routing to slot 1")
	}
	appendRange(dstStore, aggA, 1, 3)
	appendRange(dstStore, aggB, 1, 2)

	if got := dstStore.LastSeqOf(slot1); got != srcStore.LastSeqOf(slot1) {
		t.Fatalf("test setup: LEOs must match to model divergence (target %d, source %d)",
			got, srcStore.LastSeqOf(slot1))
	}
	eq, behind, err = src.slotCopyStatus(ctx, slot1, srcAddr, dstAddr)
	if err != nil {
		t.Fatalf("slot 1 diverged: %v", err)
	}
	if eq || behind {
		t.Fatalf("same LEO with a different directory must read as (equivalent=false, behind=false) "+
			"so the caller rebuilds it, got (%v,%v)", eq, behind)
	}
}

// ---- Surplus-seat reclaim: the drop side of a re-layout ---------------------

// TestSurplusSeatForReplan pins the choice: only a seat the ring plan does not
// prescribe may go, the leader never may (a slot without a writer cannot accept
// appends), and an oversized set whose extras are all in the plan offers nobody
// (the re-layout keeps adding, the reclaim only takes away what the ring does
// not want).
func TestSurplusSeatForReplan(t *testing.T) {
	plan := []string{"node-2", "node-3"}

	// Over the factor, one extra seat outside the plan: that one goes.
	p := &Placement{Leader: "node-2", Replicas: []string{"node-2", "node-3", "node-1"}, State: SlotStable}
	if got := surplusSeatForReplan(p, 2, plan); got != "node-1" {
		t.Fatalf("surplus seat = %q, want node-1", got)
	}

	// The leader is never eligible, even when the plan does not hold it.
	p = &Placement{Leader: "node-1", Replicas: []string{"node-1", "node-2", "node-3"}, State: SlotStable}
	if got := surplusSeatForReplan(p, 2, plan); got != "" {
		t.Fatalf("the leader must never be reclaimed, got %q", got)
	}

	// At or under the factor there is nothing to reclaim.
	p = &Placement{Leader: "node-2", Replicas: []string{"node-2", "node-3"}, State: SlotStable}
	if got := surplusSeatForReplan(p, 2, plan); got != "" {
		t.Fatalf("a set at the factor must not offer a seat, got %q", got)
	}

	// A migration owns its placement: never a candidate.
	p = &Placement{Leader: "node-2", Replicas: []string{"node-2", "node-3", "node-1"}, State: SlotMigratingOut}
	if got := surplusSeatForReplan(p, 2, plan); got != "" {
		t.Fatalf("a non-stable slot must not offer a seat, got %q", got)
	}

	// Deterministic when several extras are outside the plan: the sorted first.
	p = &Placement{Leader: "node-2", Replicas: []string{"node-2", "node-4", "node-1", "node-3"}, State: SlotStable}
	if got := surplusSeatForReplan(p, 2, plan); got != "node-1" {
		t.Fatalf("surplus seat = %q, want the sorted first of the extras (node-1)", got)
	}
}

// TestReclaimRoundDropsSurplusSeat drives the reclaim against real stores and a
// real peer plane, on the two shapes that matter:
//
//   - slot 0: the seats that STAY are in sync, so the surplus seat is redundant
//     and comes off;
//   - slot 2: a seat that stays is behind (the re-layout's fresh seat has not
//     caught up), so the surplus seat is still the slot's second home for
//     acknowledged records and stays until the copy catches up.
func TestReclaimRoundDropsSurplusSeat(t *testing.T) {
	ids := []string{"node-1", "node-2", "node-3"}
	engines := map[string]*Engine{}
	stores := map[string]*storage.Store{}
	addrs := map[string]string{}
	for _, id := range ids {
		st, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
		if err != nil {
			t.Fatalf("open store %s: %v", id, err)
		}
		t.Cleanup(func() { st.Close() })
		engines[id], stores[id], addrs[id] = NewEngine(nil, st, id, nil), st, ""
	}
	ctr := engines["node-1"]
	ctr.node = newTestRaftNode(t, ctr) // the reclaim submits through the controller only
	for _, id := range ids {
		addrs[id] = newPeerHarness(t, engines[id])
	}

	// The production shape of the directory: every member with its real
	// peer-plane address (the digest probes dial it).
	for _, id := range ids {
		e := engines[id]
		for _, p := range ids {
			applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{ID: p, PeerAddr: addrs[p]}})
			applyCmd(t, e, &Command{Op: OpRegister, Peer: &Peer{ID: p, AdminAddr: addrs[p], ClientAddr: addrs[p]}})
		}
		applyCmd(t, e, &Command{Op: OpConfig, Replicas: 2})
		applyCmd(t, e, &Command{Op: OpPlanSlots})
	}
	appendOne := func(st *storage.Store, agg string) {
		rec := makeRecord(agg, 1, agg+"-1")
		if _, err := st.Append(rec); err != nil {
			t.Fatalf("append %s: %v", agg, err)
		}
	}

	// Ring pairs at factor 2: slot 0 -> [node-1, node-2] (leader node-1),
	// slot 2 -> [node-3, node-1] (leader node-3).
	if p := ctr.TableSnapshot().Slots[0]; p.Leader != "node-1" || p.Replicas[1] != "node-2" {
		t.Fatalf("slot 0's ring pair: %+v", p)
	}
	if p := ctr.TableSnapshot().Slots[2]; p.Leader != "node-3" || p.Replicas[1] != "node-1" {
		t.Fatalf("slot 2's ring pair: %+v", p)
	}

	// slot 0: the leader and its pair are in sync; the re-layout's extra seat
	// (node-3) is the surplus.
	agg0 := aggInSlot(t, ctr, 0)
	appendOne(stores["node-1"], agg0)
	appendOne(stores["node-2"], agg0)
	applyCmd(t, ctr, &Command{Op: OpSlotAddReplica, Slots: []int32{0}, NodeID: "node-3"})

	// slot 2: the pair's other seat (node-1, the one that stays) is empty while
	// the leader holds a record, so it is behind — the surplus (node-2) must stay.
	agg2 := aggInSlot(t, ctr, 2)
	appendOne(stores["node-3"], agg2)
	applyCmd(t, ctr, &Command{Op: OpSlotAddReplica, Slots: []int32{2}, NodeID: "node-2"})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if done := ctr.reclaimRound(ctx, 8); done != 1 {
		t.Fatalf("one surplus is redundant and one is not: want 1 reclaim, got %d", done)
	}
	if p := ctr.TableSnapshot().Slots[0]; replicaListHas(p.Replicas, "node-3") {
		t.Fatalf("slot 0's redundant surplus seat must be reclaimed: %v", p.Replicas)
	}
	if p := ctr.TableSnapshot().Slots[0]; p.Leader != "node-1" || len(p.Replicas) != 2 {
		t.Fatalf("slot 0 after the reclaim: %+v", p)
	}
	if p := ctr.TableSnapshot().Slots[2]; !replicaListHas(p.Replicas, "node-2") {
		t.Fatalf("slot 2's surplus must stay while a kept seat is behind: %v", p.Replicas)
	}

	// The fresh seat catches up (the ordinary fetch loop does this): the surplus
	// is redundant now and the next round takes it off.
	appendOne(stores["node-1"], agg2)
	if done := ctr.reclaimRound(ctx, 8); done != 1 {
		t.Fatalf("the caught-up pair makes the surplus redundant: want 1 reclaim, got %d", done)
	}
	if p := ctr.TableSnapshot().Slots[2]; replicaListHas(p.Replicas, "node-2") {
		t.Fatalf("slot 2's surplus seat must be reclaimed once its replacement is in sync: %v", p.Replicas)
	}
	// Nothing left to reclaim.
	if done := ctr.reclaimRound(ctx, 8); done != 0 {
		t.Fatalf("a settled layout must not reclaim anything, got %d", done)
	}
}
