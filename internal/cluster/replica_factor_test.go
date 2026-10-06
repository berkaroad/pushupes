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

// TestReplicaCountFollowsFaultTolerance pins the derivation: one more copy than
// the failures the Raft group survives. It is not a knob, so these numbers are
// the specification:
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
		if len(p.Replicas) != 3 {
			t.Fatalf("after a 5th member slot %d has %d replicas, want 3: %v", s, len(p.Replicas), p.Replicas)
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
			if len(p.Replicas) != want {
				t.Fatalf("%d members: slot %d has %d replicas, want %d: %v", len(s.nodes), slot, len(p.Replicas), want, p.Replicas)
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
