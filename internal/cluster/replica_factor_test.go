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

// TestReplicaFactorRaisesExistingTable is the regression for "-replication-factor
// is silently ignored": a table planned with two replicas per slot must, once
// the configured factor is raised, first lift Table.Replicas and then top every
// slot's replica set up to the new factor. That is exactly the sequence the
// controller now drives (OpConfig then OpReplanSlots).
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

// TestReplanRespectsAFactorAboveTheMemberCount pins the guard that keeps the
// controller from looping: PlanSlots clamps a factor larger than the cluster to
// the member count, so the table's factor must be clamped the same way.
func TestReplanRespectsAFactorAboveTheMemberCount(t *testing.T) {
	nodes := []string{"node-1", "node-2", "node-3"}
	tbl := NewTable(8, 2)
	populatePeers(t, tbl, nodes)
	tbl.Slots = PlanSlots(nodes, tbl.SlotCount, tbl.Replicas)

	// Effective factor for 3 members is 3, not the configured 5.
	if err := tbl.Apply(&Command{Op: OpConfig, Replicas: 3}); err != nil {
		t.Fatalf("apply config: %v", err)
	}
	if err := tbl.Apply(&Command{Op: OpReplanSlots}); err != nil {
		t.Fatalf("apply replan: %v", err)
	}
	if tableReplicaShortfall(tbl) {
		t.Fatalf("shortfall must be reachable when the factor is clamped to the member count")
	}
}

func TestEffectiveReplicationFactorClampsToMembers(t *testing.T) {
	e := &Engine{}
	cases := []struct {
		configured, members, want int
	}{
		{3, 5, 3},
		{3, 2, 2}, // more replicas than members: clamp
		{1, 5, 1},
		{0, 5, 1}, // never below 1
		{-4, 5, 1},
		{5, 5, 5},
		{2, 0, 2}, // no members enumerated yet: keep the configured value
	}
	for _, c := range cases {
		e.SetReplicationFactor(c.configured)
		if got := e.effectiveReplicationFactor(c.members); got != c.want {
			t.Errorf("configured=%d members=%d: got %d, want %d", c.configured, c.members, got, c.want)
		}
	}
}
