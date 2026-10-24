package cluster

// The failover pin (OpMarkDown NewLeaders) picks the FRESHEST live replica —
// but until now it threw the freshness evidence away: the command carried no
// per-slot LEO, so a slot whose leadership fell back to the table rule
// (backupLeaderFor: replica-set order, LEO-agnostic) could be seated on a
// behind copy with no commit floor to hold it, and even a correct pin left no
// bound for later promotions. The probe already knows the best LEO; it must
// travel with the verdict and become the slot's floor on every node that
// applies the command (monotone: an existing, higher floor never shrinks).

import "testing"

func TestMarkDownPinCarriesFloor(t *testing.T) {
	tbl := NewTable(8, 3)
	for _, id := range []string{"node-1", "node-2", "node-3", "node-4"} {
		tbl.Peers[id] = Peer{Role: RoleLearner, ID: id, ClientAddr: "http://" + id + ":8591"}
	}
	tbl.Slots[0] = &Placement{Leader: "node-2", Replicas: []string{"node-2", "node-3", "node-4"}, Epoch: 1, State: SlotStable}
	tbl.Slots[1] = &Placement{Leader: "node-2", Replicas: []string{"node-2", "node-3", "node-4"}, Epoch: 1, State: SlotStable}
	tbl.Slots[2] = &Placement{Leader: "node-2", Replicas: []string{"node-2", "node-3", "node-4"}, Epoch: 1, State: SlotStable}
	tbl.CommitLEO[2] = 99 // a fence from an earlier live handover

	if err := tbl.Apply(&Command{
		Op: OpMarkDown, NodeID: "node-2",
		NewLeaders:   map[int32]string{0: "node-4"},
		NewLeaderLEO: map[int32]uint64{0: 30, 1: 40, 2: 5},
	}); err != nil {
		t.Fatal(err)
	}

	// Slot 0: pinned to the pick — floor is the probe's best evidence.
	if got := tbl.CommitLEO[0]; got != 30 {
		t.Fatalf("slot 0 floor %d, want 30 (pin carries the freshness evidence)", got)
	}
	// Slot 1: no pick — the table rule seats node-3, but the evidence must
	// still bound every later promotion.
	if got := tbl.Slots[1].Leader; got != "node-3" {
		t.Fatalf("slot 1 leader %q, want the table rule's node-3", got)
	}
	if got := tbl.CommitLEO[1]; got != 40 {
		t.Fatalf("slot 1 floor %d, want 40 (recorded even for table-rule fallback)", got)
	}
	// Slot 2: monotone — a lower reading never shrinks an existing floor.
	if got := tbl.CommitLEO[2]; got != 99 {
		t.Fatalf("slot 2 floor %d, want the existing 99 to survive", got)
	}

	// And the floors survive the snapshot round trip (v8 codec).
	b := tbl.EncodeTableBinary()
	got, err := DecodeTableBinary(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.CommitLEO[0] != 30 || got.CommitLEO[1] != 40 || got.CommitLEO[2] != 99 {
		t.Fatalf("pin floors lost through a snapshot: %v", got.CommitLEO)
	}
}
