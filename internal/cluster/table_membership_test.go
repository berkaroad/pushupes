package cluster

import "testing"

// A leave_node must purge the removed node from EVERY placement field. The
// migration label used to survive it, which left a slot in migrating_out
// pointing at a node the directory no longer held: the slot never returns to
// stable (so the rebalancer yields to a migration that will never commit) and
// the placement names a node the snapshot dictionary has no entry for.
func TestLeaveNodeClearsMigratingTarget(t *testing.T) {
	tbl := NewTable(2, 2)
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		tbl.Peers[id] = Peer{ID: id, PeerAddr: "127.0.0.1:1"}
	}
	tbl.Slots[0] = &Placement{Leader: "node-1", Replicas: []string{"node-1", "node-3"}, Epoch: 1, State: SlotStable}
	tbl.Slots[1] = &Placement{Leader: "node-2", Replicas: []string{"node-2", "node-3"}, Epoch: 1, State: SlotMigratingOut, MigratingTo: "node-3"}

	if err := tbl.Apply(&Command{Op: OpLeaveNode, NodeID: "node-3"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := tbl.Peers["node-3"]; ok {
		t.Fatal("leave_node must delete the directory entry")
	}
	p := tbl.Slots[1]
	if p.MigratingTo != "" {
		t.Fatalf("slot 1 still migrates to the removed node: %q", p.MigratingTo)
	}
	if p.State != SlotStable {
		t.Fatalf("slot 1 left in state %q: a migration whose target is gone must be abandoned", p.State)
	}
	for s, p := range tbl.Slots {
		if replicaListHas(p.Replicas, "node-3") {
			t.Fatalf("slot %d still seats the removed node: %v", s, p.Replicas)
		}
	}
	if _, err := DecodeTableBinary(tbl.EncodeTableBinary()); err != nil {
		t.Fatalf("the table after leave_node must still snapshot cleanly: %v", err)
	}
}

// A migration staged before a leave_node and committed after it must not put
// the removed node back in a slot: leader_move and slot_add_replica refuse a
// node that is not in the peer directory. (This is how a real cluster ended up
// with 238 slots led by an already-removed node.)
func TestPlacementMutationsRefuseNonMember(t *testing.T) {
	tbl := NewTable(2, 2)
	for _, id := range []string{"node-1", "node-2"} {
		tbl.Peers[id] = Peer{ID: id}
	}
	tbl.Slots[0] = &Placement{Leader: "node-1", Replicas: []string{"node-1", "node-2"}, Epoch: 1, State: SlotStable}

	if err := tbl.Apply(&Command{Op: OpLeaderMove, Slots: []int32{0}, NewLeader: "node-9"}); err == nil {
		t.Fatal("leader_move must refuse a node outside the directory")
	}
	if err := tbl.Apply(&Command{Op: OpSlotAddReplica, Slots: []int32{0}, NodeID: "node-9"}); err == nil {
		t.Fatal("slot_add_replica must refuse a node outside the directory")
	}
	if got := tbl.Slots[0].Leader; got != "node-1" {
		t.Fatalf("a refused leader_move must leave the placement alone, got leader %q", got)
	}
	if len(tbl.Slots[0].Replicas) != 2 {
		t.Fatalf("a refused admission must leave the replica set alone: %v", tbl.Slots[0].Replicas)
	}
}
