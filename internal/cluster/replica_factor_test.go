package cluster

import (
	"testing"
)

// populatePeers seeds a table's member directory. OpReplanSlots plans over
// Table.PeerIDs(), so a test that drives the replan must register the members.
func populatePeers(t *testing.T, tbl *Table, ids []string) {
	t.Helper()
	for _, id := range ids {
		addr := "http://127.0.0.1:1"
		tbl.Peers[id] = Peer{ID: id, PeerAddr: "127.0.0.1:1", AdminAddr: addr, ClientAddr: addr}
	}
}

// TestReplicaFactorRaisesExistingTable pins the mechanism the controller drives
// once the derived factor grows: lift Table.Replicas (OpConfig) and only then
// top every slot's replica set up to it (OpReplanSlots). A table whose factor
// went up without the replan would sit short forever — the shortfall check is
// what makes the controller issue the replan at all.
func TestReplicaFactorRaisesExistingTable(t *testing.T) {
	nodes := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	tbl := NewTable(16, 2)
	populatePeers(t, tbl, nodes)
	tbl.Slots = PlanSlots(nodes, tbl.SlotCount, tbl.Replicas)
	for s, p := range tbl.Slots {
		if len(p.Replicas) != 2 {
			t.Fatalf("setup: slot %d has %d replicas, want 2", s, len(p.Replicas))
		}
	}
	if tableReplicaShortfall(tbl) {
		t.Fatalf("setup: a factor-2 table must not report a shortfall at factor 2")
	}

	// The controller's reconcile step: raise the table's factor to the
	// configured (member-clamped) value.
	if err := tbl.Apply(&Command{Op: OpConfig, Replicas: 3}); err != nil {
		t.Fatalf("apply config: %v", err)
	}
	if tbl.Replicas != 3 {
		t.Fatalf("table factor = %d, want 3", tbl.Replicas)
	}
	if !tableReplicaShortfall(tbl) {
		t.Fatalf("a factor-3 table with 2-replica slots must report a shortfall")
	}

	if err := tbl.Apply(&Command{Op: OpReplanSlots}); err != nil {
		t.Fatalf("apply replan: %v", err)
	}
	for s, p := range tbl.Slots {
		if len(p.Replicas) != 3 {
			t.Fatalf("slot %d has %d replicas after the replan, want 3: %v", s, len(p.Replicas), p.Replicas)
		}
	}
	if tableReplicaShortfall(tbl) {
		t.Fatalf("shortfall must be gone after the replan")
	}
}

// TestMemberRemovalReDerivesReplicaSets is the regression for the shrink path.
// After members leave, the derived factor drops (7 members -> 4 copies, 6 -> 3
// on the way, 5 -> 3), and the sets the departed members were dropped from are
// stale: some hold SURPLUS seats (the ring no longer counts two of their four),
// others hold too few, and either way the ring's expected leader may not be
// among them at all. The re-layout must put that expected leader back INTO the
// replica set — the precondition for the rebalancer to hand the slot's
// leadership back, because it never hands leadership to a node that is not a
// replica — and it must do so without dropping a seat: the copies a stale set
// holds are the ones that have been replicating the slot, and the failure path
// picks a slot's new leader from the front of that list. The surplus comes off
// later, against real copies (reclaimRound, TestReclaimRoundDropsSurplusSeat).
func TestMemberRemovalReDerivesReplicaSets(t *testing.T) {
	members := []string{"node-1", "node-2", "node-3", "node-4", "node-5", "node-6", "node-7"}
	tbl := NewTable(1680, ReplicaCountForMembers(len(members)))
	populatePeers(t, tbl, members)
	tbl.Slots = PlanSlots(members, tbl.SlotCount, tbl.Replicas)
	if tbl.Replicas != 4 {
		t.Fatalf("setup: seven members must plan four copies, got %d", tbl.Replicas)
	}
	seatsBefore := map[int32][]string{}
	for s, p := range tbl.Slots {
		seatsBefore[s] = append([]string(nil), p.Replicas...)
	}

	// Two members leave: leave_node drops them from the directory and from
	// every replica set (the controller's failover path).
	for _, id := range []string{"node-6", "node-7"} {
		if err := tbl.Apply(&Command{Op: OpLeaveNode, NodeID: id}); err != nil {
			t.Fatalf("leave %s: %v", id, err)
		}
	}
	// The controller re-derives the factor from the member count first.
	if err := tbl.Apply(&Command{Op: OpConfig, Replicas: ReplicaCountForMembers(len(tbl.Peers))}); err != nil {
		t.Fatalf("apply config: %v", err)
	}
	if tbl.Replicas != 3 {
		t.Fatalf("five members must want three copies, got %d", tbl.Replicas)
	}
	if !tbl.layoutStale() {
		t.Fatalf("a removal leaves the replica sets stale (surplus seats, or seats lost with the departed member)")
	}

	// The state the report describes: slots whose ring leader is not a replica,
	// which no one but a re-seat can fix.
	online := tbl.OnlinePeerIDs()
	stranded := 0
	for s, p := range tbl.Slots {
		if want := online[int(s)%len(online)]; !replicaListHas(p.Replicas, want) {
			stranded++
		}
	}
	if stranded == 0 {
		t.Fatalf("fixture: a two-member removal must leave slots off the ring")
	}

	if err := tbl.Apply(&Command{Op: OpReplanSlots}); err != nil {
		t.Fatalf("apply replan: %v", err)
	}
	nodes := tbl.PeerIDs()
	if len(nodes) != 5 {
		t.Fatalf("member directory after the removals: %v", nodes)
	}
	ring := func(s int32) []string {
		out := make([]string, 0, tbl.Replicas)
		for i := 0; i < tbl.Replicas; i++ {
			out = append(out, nodes[(int(s)+i)%len(nodes)])
		}
		return out
	}

	seatsAfter, surplus := 0, 0
	for s, p := range tbl.Slots {
		// The ring's seats are all present — every slot's expected leader is a
		// replica now, which is what the rebalancer's target gate needs.
		for _, want := range ring(s) {
			if !replicaListHas(p.Replicas, want) {
				t.Fatalf("slot %d after the re-layout: replicas %v, want the five-member ring %v", s, p.Replicas, ring(s))
			}
		}
		// No seat was dropped on the way: a member that is still in the
		// directory keeps its seat until its replacement is caught up.
		for _, had := range seatsBefore[s] {
			if had == "node-6" || had == "node-7" {
				continue
			}
			if !replicaListHas(p.Replicas, had) {
				t.Fatalf("slot %d dropped the seat %s it was already replicating on", s, had)
			}
		}
		if len(p.Replicas) > tbl.Replicas {
			if drop := surplusSeatForReplan(p, tbl.Replicas, ring(s)); drop != "" {
				surplus++
			} else {
				// The only extra seat is the slot's own leader: the reclaim may
				// not take a slot's writer away, so this one waits for a
				// hand-over to move leadership off it first.
				for _, r := range p.Replicas {
					if r != p.Leader && !replicaListHas(ring(s), r) {
						t.Fatalf("slot %d is over the factor (%v) with the non-leader extra seat %s, but offers it not", s, p.Replicas, r)
					}
				}
			}
		}
		seatsAfter += len(p.Replicas)
	}
	if surplus == 0 {
		t.Fatalf("fixture: the removal must leave surplus seats to reclaim")
	}
	if seatsAfter < int(tbl.SlotCount)*tbl.Replicas {
		t.Fatalf("the re-layout must never take the cluster below the factor: %d seats for %d slots", seatsAfter, tbl.SlotCount)
	}

	// The ring is now reachable for every slot: each planned hand-over targets a
	// replica (a target outside the set would be a full data migration, which
	// the rebalancer refuses to start).
	moves := PlanLeaderRebalance(tbl)
	if len(moves) == 0 {
		t.Fatalf("the failover layout must still deviate from the ring after a removal")
	}
	for _, m := range moves {
		if p := tbl.Slots[m.Slot]; !replicaListHas(p.Replicas, m.To) {
			t.Fatalf("slot %d: rebalance targets %s, which is not a replica — the slot stays off the ring", m.Slot, m.To)
		}
	}
	// Executing the plan lands on the ring, and a settled table plans no further
	// replan (the controller would otherwise write a Raft entry every round).
	byTarget := map[string][]int32{}
	for _, m := range moves {
		byTarget[m.To] = append(byTarget[m.To], m.Slot)
	}
	for to, slots := range byTarget {
		if err := tbl.Apply(&Command{Op: OpLeaderMove, Slots: slots, NewLeader: to}); err != nil {
			t.Fatalf("leader move to %s: %v", to, err)
		}
	}
	if left := PlanLeaderRebalance(tbl); len(left) != 0 {
		t.Fatalf("every slot must reach the ring after the hand-overs, leftovers: %d", len(left))
	}
	if probe := tbl.Clone(); probe.applyReplanSlots() {
		t.Fatal("a settled table must not plan a further replan")
	}
}

// TestReplicaCountFollowsFaultTolerance pins the high tier's derivation: one
// more copy than the failures the Raft group survives. These numbers are the
// specification for ReplicaPolicyHigh (the other tiers fix the factor at 1/2,
// clamped to the member count — see TestReplicaCountForPolicy):
//
//	1 voter  -> 0+1 = 1
//	3 voters -> 1+1 = 2
//	5 voters -> 2+1 = 3
//	7 voters -> 3+1 = 4
//
// Even counts use the same floor((N-1)/2) arithmetic (2->1, 4->2, 6->3): a
// 4-voter group survives one failure exactly like a 3-voter group.
func TestReplicaCountFollowsFaultTolerance(t *testing.T) {
	cases := []struct{ members, want int }{
		{0, 1}, // no members enumerated yet: never below one copy
		{1, 1},
		{2, 1},
		{3, 2},
		{4, 2},
		{5, 3},
		{6, 3},
		{7, 4},
		{8, 4},
		{9, 5},
	}
	for _, c := range cases {
		got := ReplicaCountForMembers(c.members)
		if got != c.want {
			t.Errorf("ReplicaCountForMembers(%d) = %d, want %d", c.members, got, c.want)
		}
		if c.members > 0 && got > c.members {
			t.Errorf("%d members must never plan more replicas (%d) than members", c.members, got)
		}
	}
}

// TestEvenMembershipDoesNotRelayReplicaSets pins the operator's rule: the
// replica-set re-layout is triggered by the ODD member counts only, because the
// derived copy count only moves there (3 members -> 2 copies, 5 -> 3, 7 -> 4).
// Adding a 4th or a 6th member must therefore leave the table exactly as it
// was — the new member holds no seat and no slot until the next odd count is
// reached — and adding the 5th (or 7th) must re-lay it.
func TestEvenMembershipDoesNotRelayReplicaSets(t *testing.T) {
	nodes := []string{"node-1", "node-2", "node-3"}
	tbl := NewTable(16, ReplicaCountForMembers(len(nodes)))
	populatePeers(t, tbl, nodes)
	tbl.Slots = PlanSlots(nodes, tbl.SlotCount, tbl.Replicas)

	before := map[int32][]string{}
	for s, p := range tbl.Slots {
		before[s] = append([]string(nil), p.Replicas...)
	}

	// The 4th member: the derived count stays 2 (4 voters survive one failure,
	// exactly like 3), so the controller's OpConfig writes the same value and
	// the replan must not touch a single replica set.
	populatePeers(t, tbl, []string{"node-4"})
	if want := ReplicaCountForMembers(4); want != 2 {
		t.Fatalf("ReplicaCountForMembers(4) = %d, want 2", want)
	}
	applyBoth := func(factor int) {
		if err := tbl.Apply(&Command{Op: OpConfig, Replicas: factor}); err != nil {
			t.Fatalf("apply config %d: %v", factor, err)
		}
		if err := tbl.Apply(&Command{Op: OpReplanSlots}); err != nil {
			t.Fatalf("apply replan: %v", err)
		}
	}
	applyBoth(ReplicaCountForMembers(4))
	// A steady table is also a table with no replan to make: probing the
	// convergence on a clone (what the controller does every round) must find
	// nothing to change, or the new member's arrival would cost a Raft entry
	// per reconcile round for as long as it stays a member.
	if probe := tbl.Clone(); probe.applyReplanSlots() {
		t.Fatal("a 4th member must leave the layout alone: the round must not plan a replan")
	}
	for s, p := range tbl.Slots {
		if len(p.Replicas) != len(before[s]) {
			t.Fatalf("a 4th member changed slot %d's replica set: %v -> %v", s, before[s], p.Replicas)
		}
		for i, r := range before[s] {
			if p.Replicas[i] != r {
				t.Fatalf("a 4th member re-laid slot %d: %v -> %v", s, before[s], p.Replicas)
			}
		}
		if replicaListHas(p.Replicas, "node-4") {
			t.Fatalf("a 4th member must not enter a replica set yet: slot %d %v", s, p.Replicas)
		}
	}

	// The 5th member moves the derived count to 3, and now both new members
	// enter the layout (the ring window over five members).
	populatePeers(t, tbl, []string{"node-5"})
	if want := ReplicaCountForMembers(5); want != 3 {
		t.Fatalf("ReplicaCountForMembers(5) = %d, want 3", want)
	}
	applyBoth(ReplicaCountForMembers(5))
	seats := map[string]int{}
	for s, p := range tbl.Slots {
		// Adding the 5th member moves the factor to 3 and re-lays the ring: the
		// slot covers the five-member ring window (and may still carry an extra
		// seat the re-layout kept, until reclaimRound takes it off).
		if len(p.Replicas) < 3 {
			t.Fatalf("after a 5th member slot %d has %d replicas, want at least 3: %v", s, len(p.Replicas), p.Replicas)
		}
		nodes := tbl.PeerIDs()
		for i := 0; i < 3; i++ {
			if on := nodes[(int(s)+i)%len(nodes)]; !replicaListHas(p.Replicas, on) {
				t.Fatalf("after a 5th member slot %d does not hold the ring's seat %s: %v", s, on, p.Replicas)
			}
		}
		for _, r := range p.Replicas {
			seats[r]++
		}
	}
	for _, id := range []string{"node-4", "node-5"} {
		if seats[id] == 0 {
			t.Fatalf("the 5th member must pull %s into the layout: %v", id, seats)
		}
	}
}

// TestReplicaFactorGrowsWithMembership is the regression for the scaling
// report: a cluster that grows must re-derive the factor and re-lay its slots,
// so the members that just joined end up carrying copies. It drives the
// controller's exact sequence (OpConfig from the member count, then
// OpReplanSlots) across 1 -> 3 -> 5 -> 7 members and checks the whole table
// each time.
func TestReplicaFactorGrowsWithMembership(t *testing.T) {
	ids := []string{"node-1"}
	tbl := NewTable(16, ReplicaCountForMembers(len(ids)))
	populatePeers(t, tbl, ids)
	tbl.Slots = PlanSlots(ids, tbl.SlotCount, tbl.Replicas)

	steps := []struct {
		nodes []string
		want  int
	}{
		{[]string{"node-1", "node-2", "node-3"}, 2},
		{[]string{"node-1", "node-2", "node-3", "node-4", "node-5"}, 3},
		{[]string{"node-1", "node-2", "node-3", "node-4", "node-5", "node-6", "node-7"}, 4},
	}
	for _, s := range steps {
		populatePeers(t, tbl, s.nodes) // the members that are in the cluster now
		want := ReplicaCountForMembers(len(s.nodes))
		if want != s.want {
			t.Fatalf("ReplicaCountForMembers(%d) = %d, want %d", len(s.nodes), want, s.want)
		}
		if err := tbl.Apply(&Command{Op: OpConfig, Replicas: want}); err != nil {
			t.Fatalf("apply config: %v", err)
		}
		if err := tbl.Apply(&Command{Op: OpReplanSlots}); err != nil {
			t.Fatalf("apply replan: %v", err)
		}
		seats := map[string]int{}
		for slot, p := range tbl.Slots {
			// The re-layout ADDS the ring's seats and never drops one (the
			// surplus comes off later, against real copies — reclaimRound), so
			// a slot holds at least the factor and always covers the ring
			// window of the member set it was re-laid on.
			if len(p.Replicas) < want {
				t.Fatalf("%d members: slot %d has %d replicas, want at least %d: %v", len(s.nodes), slot, len(p.Replicas), want, p.Replicas)
			}
			nodes := tbl.PeerIDs()
			for i := 0; i < want; i++ {
				if on := nodes[(int(slot)+i)%len(nodes)]; !replicaListHas(p.Replicas, on) {
					t.Fatalf("%d members: slot %d does not hold the ring's seat %s: %v", len(s.nodes), slot, on, p.Replicas)
				}
			}
			for _, r := range p.Replicas {
				seats[r]++
			}
		}
		if tableReplicaShortfall(tbl) {
			t.Fatalf("shortfall after replanning %d members", len(s.nodes))
		}
		// The newest member must hold copies: deriving the count from the
		// membership is exactly what is supposed to make that happen.
		newest := s.nodes[len(s.nodes)-1]
		if seats[newest] == 0 {
			t.Fatalf("%d members: the newest member %s holds no replica seat: %v", len(s.nodes), newest, seats)
		}
	}
}
