// Copyright 2026 berkaroad
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

// TestFactorOneGrowthReSeatsTheRing is the regression for the single-copy
// blind spot: at factor 1 every replica set holds exactly one seat, so the
// size test ("set size != factor") can never fire and a slot whose copy sits
// on a node the ring does not expect there is never re-seated. The rebalancer
// only hands leadership to an EXISTING replica, so such a slot can never be
// moved back to the ring — a cluster planned while it held fewer members stays
// exactly where that first plan put it, forever (observed: a low-policy
// cluster bootstrapped as one node and grown to two and three never moves a
// slot off node-1).
//
// The re-seat is armed by the ring-coverage half of layoutStale, which is the
// only thing that can arm it when the factor cannot move.
func TestFactorOneGrowthReSeatsTheRing(t *testing.T) {
	// The bootstrap shape: one member, low policy, one copy per slot. The
	// first plan is a full plan, so every slot lands on node-1.
	tbl := NewTableWithPolicy(16, ReplicaPolicyLow)
	populatePeers(t, tbl, []string{"node-1"})
	if err := tbl.Apply(&Command{Op: OpPlanSlots}); err != nil {
		t.Fatalf("plan slots: %v", err)
	}
	for s, p := range tbl.Slots {
		if p.Leader != "node-1" || len(p.Replicas) != 1 {
			t.Fatalf("bootstrap slot %d: %+v, want one copy on node-1", s, p)
		}
	}

	// node-2 joins. The controller's plan step re-derives the factor (low
	// holds it at 1 whatever the membership) and probes the replan: with the
	// ring now spanning two members, half the slots' expected leader is
	// node-2 — which holds no seat at all.
	populatePeers(t, tbl, []string{"node-2"})
	if want := ReplicaCountForPolicy(tbl.Policy, len(tbl.Peers)); want != 1 {
		t.Fatalf("low over two members must stay at one copy, got %d", want)
	}
	if tbl.Replicas != 1 {
		t.Fatalf("setup: factor = %d, want 1", tbl.Replicas)
	}
	if !tbl.layoutStale() {
		t.Fatal("a member joined a single-copy cluster and half the slots' ring leader holds no seat: the layout must read stale, " +
			"or the re-seat never happens and the ring leader is never a replica for the rebalancer to target")
	}
	if err := tbl.Apply(&Command{Op: OpReplanSlots}); err != nil {
		t.Fatalf("replan: %v", err)
	}

	// Every deviating slot is now hand-over-able: its ring leader is a
	// replica (the whole point of the re-seat — the rebalancer refuses a
	// target that is not one, because that would be a full data migration).
	nodes := tbl.PeerIDs()
	moves := PlanLeaderRebalance(tbl)
	if len(moves) == 0 {
		t.Fatal("after the re-seat the table must still deviate from the ring and plan hand-overs")
	}
	for _, m := range moves {
		want := nodes[int(m.Slot)%len(nodes)]
		if m.To != want {
			t.Fatalf("slot %d: hand-over target %s, ring leader is %s", m.Slot, m.To, want)
		}
		if p := tbl.Slots[m.Slot]; !replicaListHas(p.Replicas, m.To) {
			t.Fatalf("slot %d: rebalance targets %s, which is not a replica — the slot would stay off the ring: %v",
				m.Slot, m.To, p.Replicas)
		}
	}

	// Executing the plan lands on the ring; the seat each handed-over slot
	// inherited from its former leader is then the surplus the reclaim takes
	// off. Both steps are table-side here (the data plane has its own test,
	// TestFactorOneGrowthConvergesEndToEnd).
	byTarget := map[string][]int32{}
	for _, m := range moves {
		byTarget[m.To] = append(byTarget[m.To], m.Slot)
	}
	for to, slots := range byTarget {
		if err := tbl.Apply(&Command{Op: OpLeaderMove, Slots: slots, NewLeader: to}); err != nil {
			t.Fatalf("leader move to %s: %v", to, err)
		}
	}
	planned := PlanSlots(nodes, tbl.SlotCount, tbl.Replicas)
	for s, p := range tbl.Slots {
		if drop := surplusSeatForReplan(p, tbl.Replicas, planned[s].Replicas); drop != "" {
			if err := tbl.Apply(&Command{Op: OpSlotRemoveReplica, Slots: []int32{s}, NodeID: drop}); err != nil {
				t.Fatalf("reclaim %s from slot %d: %v", drop, s, err)
			}
		}
	}

	// Settled: one copy per slot, on the ring, spread over both members.
	seats := map[string]int{}
	for s, p := range tbl.Slots {
		if want := nodes[int(s)%len(nodes)]; p.Leader != want {
			t.Fatalf("slot %d: leader %s, want the ring leader %s", s, p.Leader, want)
		}
		if len(p.Replicas) != 1 || p.Replicas[0] != p.Leader {
			t.Fatalf("slot %d: replicas %v, want the single copy on the ring leader", s, p.Replicas)
		}
		seats[p.Leader]++
	}
	for _, id := range nodes {
		if seats[id] == 0 {
			t.Fatalf("member %s leads nothing after the convergence: %v", id, seats)
		}
	}
	if left := PlanLeaderRebalance(tbl); len(left) != 0 {
		t.Fatalf("the ring is reached; leftovers: %v", left)
	}
	// A settled table is also a table with no replan to make: probing the
	// convergence (what the controller does every round) must find nothing,
	// or the steady state would cost a Raft entry per round.
	if probe := tbl.Clone(); probe.applyReplanSlots() {
		t.Fatal("a settled single-copy table must not plan a further replan")
	}
}

// TestFactorOneShrinkRestoresLeaderlessSlots is the regression for the other
// single-copy blind spot: removing the member that held a slot's ONLY copy
// leaves the slot without a writer (failoverLeader has no other replica to hand
// it to), and nothing assigns one afterwards — PlanLeaderRebalance skips an
// empty leader by contract and the full plan only runs on an empty table. The
// re-layout adopts the ring's expected leader, which is the seat it seats for
// the same slot anyway.
func TestFactorOneShrinkRestoresLeaderlessSlots(t *testing.T) {
	tbl := NewTableWithPolicy(8, ReplicaPolicyLow)
	populatePeers(t, tbl, []string{"node-1", "node-2", "node-3"})
	if err := tbl.Apply(&Command{Op: OpPlanSlots}); err != nil {
		t.Fatalf("plan slots: %v", err)
	}
	// node-3 leads slots 2 and 5 under the three-member ring.
	if p := tbl.Slots[2]; p.Leader != "node-3" || p.Epoch != 1 {
		t.Fatalf("setup: slot 2 %+v, want node-3 leading at epoch 1", p)
	}

	// node-3 leaves: it is dropped from the directory and from every replica
	// set, and the slots it led lose their only copy with it.
	if err := tbl.Apply(&Command{Op: OpLeaveNode, NodeID: "node-3"}); err != nil {
		t.Fatalf("leave node-3: %v", err)
	}
	leaderless := 0
	for s, p := range tbl.Slots {
		if p.Leader == "" {
			leaderless++
			if s != 2 && s != 5 {
				t.Fatalf("slot %d lost its leader, only node-3's slots may: %+v", s, p)
			}
		}
	}
	if leaderless != 2 {
		t.Fatalf("want the two slots node-3 led left without a writer, got %d", leaderless)
	}
	epochAfterLeave := tbl.Slots[2].Epoch
	// The rebalancer cannot repair them: a leader-less slot is not a deviation
	// it fixes, so the repair has to happen in the re-layout.
	if moves := PlanLeaderRebalance(tbl); len(moves) != 0 {
		t.Fatalf("an empty leader is not the rebalancer's to fix, got %v", moves)
	}

	// The controller's probe must find work to do, or the replan never lands.
	if probe := tbl.Clone(); !probe.applyReplanSlots() {
		t.Fatal("a table with a slot left without a writer must arm the replan")
	}
	if err := tbl.Apply(&Command{Op: OpReplanSlots}); err != nil {
		t.Fatalf("replan: %v", err)
	}

	nodes := tbl.PeerIDs()
	if len(nodes) != 2 {
		t.Fatalf("member directory after the removal: %v", nodes)
	}

	// The two slots left without a writer adopt the ring's expected leader
	// (the ring over the two remaining members), at the epoch bump the leader
	// change takes, with their single copy on that node.
	if p := tbl.Slots[2]; p.Leader != "node-1" || len(p.Replicas) != 1 || p.Replicas[0] != "node-1" {
		t.Fatalf("slot 2 must adopt the ring leader node-1 with its single copy: %+v", p)
	}
	if got := tbl.Slots[2].Epoch; got != epochAfterLeave+1 {
		t.Fatalf("the adopted leader must bump the epoch: %d, want %d (leave_node already bumped to %d)",
			got, epochAfterLeave+1, epochAfterLeave)
	}
	if p := tbl.Slots[5]; p.Leader != "node-2" || len(p.Replicas) != 1 || p.Replicas[0] != "node-2" {
		t.Fatalf("slot 5 must adopt the ring leader node-2 with its single copy: %+v", p)
	}

	// The rest of the shrink is the rebalancer's half: the slots that still
	// lead keep doing so (the re-layout never moves a leader), but their ring
	// leader is a replica now, so the hand-over can actually happen.
	moves := PlanLeaderRebalance(tbl)
	if len(moves) == 0 {
		t.Fatal("the two-member ring deviates from the three-member layout and must plan hand-overs")
	}
	for _, m := range moves {
		if p := tbl.Slots[m.Slot]; !replicaListHas(p.Replicas, m.To) {
			t.Fatalf("slot %d: hand-over target %s is not a replica — the slot would stay off the ring: %v", m.Slot, m.To, p.Replicas)
		}
	}
	byTarget := map[string][]int32{}
	for _, m := range moves {
		byTarget[m.To] = append(byTarget[m.To], m.Slot)
	}
	for to, slots := range byTarget {
		if err := tbl.Apply(&Command{Op: OpLeaderMove, Slots: slots, NewLeader: to}); err != nil {
			t.Fatalf("leader move to %s: %v", to, err)
		}
	}
	planned := PlanSlots(nodes, tbl.SlotCount, tbl.Replicas)
	for s, p := range tbl.Slots {
		if drop := surplusSeatForReplan(p, tbl.Replicas, planned[s].Replicas); drop != "" {
			if err := tbl.Apply(&Command{Op: OpSlotRemoveReplica, Slots: []int32{s}, NodeID: drop}); err != nil {
				t.Fatalf("reclaim %s from slot %d: %v", drop, s, err)
			}
		}
	}

	// Settled on the two-member ring: a writer on every slot, one copy each.
	for s, p := range tbl.Slots {
		if p.Leader == "" {
			t.Fatalf("slot %d is still without a writer: %+v", s, p)
		}
		if want := nodes[int(s)%len(nodes)]; p.Leader != want {
			t.Fatalf("slot %d: leader %s, want the ring leader %s", s, p.Leader, want)
		}
		if len(p.Replicas) != 1 || p.Replicas[0] != p.Leader {
			t.Fatalf("slot %d: replicas %v, want the single copy on %s", s, p.Replicas, p.Leader)
		}
	}
	if left := PlanLeaderRebalance(tbl); len(left) != 0 {
		t.Fatalf("the ring is reached; leftovers: %v", left)
	}
	if probe := tbl.Clone(); probe.applyReplanSlots() {
		t.Fatal("a settled single-copy table must not plan a further replan")
	}
}

// TestLayoutStaleIgnoresMembersBeyondThePlan pins the guard on the zero-seat
// judgement: with more members than the slot count can spread over, the ring
// plan gives some of them no seat at all, and a settled table must not read
// stale (and re-plan) forever because of members the re-layout could not seat
// anyway.
func TestLayoutStaleIgnoresMembersBeyondThePlan(t *testing.T) {
	tbl := NewTableWithPolicy(4, ReplicaPolicyLow)
	populatePeers(t, tbl, []string{"node-1", "node-2", "node-3", "node-4", "node-5", "node-6"})
	if err := tbl.Apply(&Command{Op: OpPlanSlots}); err != nil {
		t.Fatalf("plan slots: %v", err)
	}
	// Four slots cover four of the six members; node-5 and node-6 are outside
	// every window.
	seated := map[string]bool{}
	for _, p := range tbl.Slots {
		seated[p.Leader] = true
	}
	for _, id := range []string{"node-5", "node-6"} {
		if seated[id] {
			t.Fatalf("setup: %s must be outside the plan's windows: %v", id, seated)
		}
	}
	if tbl.layoutStale() {
		t.Fatal("a settled table must not read stale over members the ring plan gives no seat to")
	}
	if probe := tbl.Clone(); probe.applyReplanSlots() {
		t.Fatal("the round must not plan a replan for members the plan cannot seat")
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

// TestEvenMemberCountSeatsTheNewMemberAndHoldsTheFactor pins the rule after the
// trigger change: the derived copy count still only moves on the ODD member
// counts (4 voters survive one failure exactly like 3, so the 4th member leaves
// the factor at 2), but the member itself is seated like any other — the layout
// has left it out, which arms the re-layout (layoutStale's second shape). It
// used to carry no seat and no slot until the next odd count, with its slots'
// ring leader unable to become a replica.
func TestEvenMemberCountSeatsTheNewMemberAndHoldsTheFactor(t *testing.T) {
	nodes := []string{"node-1", "node-2", "node-3"}
	tbl := NewTable(16, ReplicaCountForMembers(len(nodes)))
	populatePeers(t, tbl, nodes)
	tbl.Slots = PlanSlots(nodes, tbl.SlotCount, tbl.Replicas)

	// The 4th member: the derived count stays 2 (4 voters survive one failure,
	// exactly like 3), so the controller's OpConfig writes the same value — and
	// the new member holds no seat, which must arm the replan.
	populatePeers(t, tbl, []string{"node-4"})
	if want := ReplicaCountForMembers(4); want != 2 {
		t.Fatalf("ReplicaCountForMembers(4) = %d, want 2", want)
	}
	if err := tbl.Apply(&Command{Op: OpConfig, Replicas: ReplicaCountForMembers(4)}); err != nil {
		t.Fatalf("apply config: %v", err)
	}
	if tbl.Replicas != 2 {
		t.Fatalf("the 4th member must not raise the factor, got %d", tbl.Replicas)
	}
	if probe := tbl.Clone(); !probe.applyReplanSlots() {
		t.Fatal("the 4th member holds no seat: the round must re-plan, or it never enters the layout")
	}
	if err := tbl.Apply(&Command{Op: OpReplanSlots}); err != nil {
		t.Fatalf("apply replan: %v", err)
	}

	// Every slot covers the four-member ring window now, and no seat was
	// dropped on the way (the surplus comes off with reclaimRound once the
	// copies that stay are in sync).
	ring := tbl.PeerIDs()
	seats := map[string]int{}
	for s, p := range tbl.Slots {
		if len(p.Replicas) < 2 {
			t.Fatalf("after the replan slot %d has %d replicas, want at least 2: %v", s, len(p.Replicas), p.Replicas)
		}
		for i := 0; i < 2; i++ {
			if on := ring[(int(s)+i)%len(ring)]; !replicaListHas(p.Replicas, on) {
				t.Fatalf("slot %d does not hold the ring's seat %s: %v", s, on, p.Replicas)
			}
		}
		for _, r := range p.Replicas {
			seats[r]++
		}
	}
	if seats["node-4"] == 0 {
		t.Fatalf("the 4th member must pull its share of the layout: %v", seats)
	}
	// Hand-over-able now: the ring's expected leader is a replica of every
	// deviating slot (that gate is what the new member could never pass).
	for _, m := range PlanLeaderRebalance(tbl) {
		if p := tbl.Slots[m.Slot]; !replicaListHas(p.Replicas, m.To) {
			t.Fatalf("slot %d: hand-over target %s is not a replica: %v", m.Slot, m.To, p.Replicas)
		}
	}
	// A converged table is a table with no replan to make: probing the round
	// (what the controller does every reconcile) must find nothing, or the new
	// member's arrival would cost a Raft entry per round for good.
	if probe := tbl.Clone(); probe.applyReplanSlots() {
		t.Fatal("a table whose sets already cover the ring must not plan a further replan")
	}

	// The 5th member moves the derived count to 3, and now both new members
	// enter the layout (the ring window over five members).
	populatePeers(t, tbl, []string{"node-5"})
	if want := ReplicaCountForMembers(5); want != 3 {
		t.Fatalf("ReplicaCountForMembers(5) = %d, want 3", want)
	}
	if err := tbl.Apply(&Command{Op: OpConfig, Replicas: ReplicaCountForMembers(5)}); err != nil {
		t.Fatalf("apply config: %v", err)
	}
	if err := tbl.Apply(&Command{Op: OpReplanSlots}); err != nil {
		t.Fatalf("apply replan: %v", err)
	}
	seats = map[string]int{}
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

// TestMediumGrowthSeatsTheNewMember is the regression for the membership change
// that does not move the factor: medium holds two copies whatever the member
// count, so a 3rd member used to be left with no seat and no slot for good (the
// ring's expected leader for the slots it should lead was never a replica, and
// the rebalancer only hands leadership to an existing replica). The layout
// reads stale because the member holds no seat anywhere, the re-seat makes it a
// follower of the slots the ring gives it, and the ordinary fetch + rebalance +
// reclaim spread the leadership the rest of the way.
func TestMediumGrowthSeatsTheNewMember(t *testing.T) {
	tbl := NewTableWithPolicy(16, ReplicaPolicyMedium)
	populatePeers(t, tbl, []string{"node-1", "node-2"})
	tbl.Replicas = ReplicaCountForPolicy(tbl.Policy, len(tbl.Peers))
	if err := tbl.Apply(&Command{Op: OpPlanSlots}); err != nil {
		t.Fatalf("plan slots: %v", err)
	}
	// Two members at two copies: every slot lives on both.
	for s, p := range tbl.Slots {
		if len(p.Replicas) != 2 {
			t.Fatalf("bootstrap slot %d: %v, want two copies", s, p)
		}
	}

	// node-3 joins: the factor stays 2, so the size test sees nothing — and
	// node-3 holds no seat at all.
	populatePeers(t, tbl, []string{"node-3"})
	want := ReplicaCountForPolicy(tbl.Policy, len(tbl.Peers))
	if want != 2 {
		t.Fatalf("medium over three members must stay at two copies, got %d", want)
	}
	if err := tbl.Apply(&Command{Op: OpConfig, Replicas: want}); err != nil {
		t.Fatalf("apply config: %v", err)
	}
	if !tbl.layoutStale() {
		t.Fatal("node-3 holds no seat in any slot: the layout must read stale, or the member stays out of the layout for good")
	}
	if err := tbl.Apply(&Command{Op: OpReplanSlots}); err != nil {
		t.Fatalf("apply replan: %v", err)
	}

	// node-3 now holds the ring's seats, and every slot's ring leader is a
	// replica — the hand-over gate it could never pass before.
	nodes := tbl.PeerIDs()
	seats := map[string]int{}
	for s, p := range tbl.Slots {
		for i := 0; i < 2; i++ {
			if on := nodes[(int(s)+i)%len(nodes)]; !replicaListHas(p.Replicas, on) {
				t.Fatalf("slot %d does not hold the ring's seat %s: %v", s, on, p.Replicas)
			}
		}
		for _, r := range p.Replicas {
			seats[r]++
		}
	}
	if seats["node-3"] == 0 {
		t.Fatalf("the 3rd member must pull its share of the layout: %v", seats)
	}
	moves := PlanLeaderRebalance(tbl)
	if len(moves) == 0 {
		t.Fatal("the three-member ring deviates from the two-member layout and must plan hand-overs")
	}
	for _, m := range moves {
		if p := tbl.Slots[m.Slot]; !replicaListHas(p.Replicas, m.To) {
			t.Fatalf("slot %d: hand-over target %s is not a replica: %v", m.Slot, m.To, p.Replicas)
		}
	}

	// Executing the plan and reclaiming the inherited seats converges on the
	// three-member ring (the data plane's own test is the factor-1 one; this
	// asserts the table-side outcome).
	byTarget := map[string][]int32{}
	for _, m := range moves {
		byTarget[m.To] = append(byTarget[m.To], m.Slot)
	}
	for to, slots := range byTarget {
		if err := tbl.Apply(&Command{Op: OpLeaderMove, Slots: slots, NewLeader: to}); err != nil {
			t.Fatalf("leader move to %s: %v", to, err)
		}
	}
	planned := PlanSlots(nodes, tbl.SlotCount, tbl.Replicas)
	for s, p := range tbl.Slots {
		if drop := surplusSeatForReplan(p, tbl.Replicas, planned[s].Replicas); drop != "" {
			if err := tbl.Apply(&Command{Op: OpSlotRemoveReplica, Slots: []int32{s}, NodeID: drop}); err != nil {
				t.Fatalf("reclaim %s from slot %d: %v", drop, s, err)
			}
		}
	}
	leaders := map[string]int{}
	for s, p := range tbl.Slots {
		if got := nodes[int(s)%len(nodes)]; p.Leader != got {
			t.Fatalf("slot %d: leader %s, want the ring leader %s", s, p.Leader, got)
		}
		if len(p.Replicas) != 2 {
			t.Fatalf("slot %d: replicas %v, want two seats", s, p.Replicas)
		}
		leaders[p.Leader]++
	}
	for _, id := range nodes {
		if leaders[id] == 0 {
			t.Fatalf("member %s leads nothing after the convergence: %v", id, leaders)
		}
	}
	if left := PlanLeaderRebalance(tbl); len(left) != 0 {
		t.Fatalf("the ring is reached; leftovers: %v", left)
	}
	if probe := tbl.Clone(); probe.applyReplanSlots() {
		t.Fatal("a settled table must not plan a further replan")
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
