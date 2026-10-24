package cluster

// The commit floor (Table.CommitLEO) is what makes awaitCommitFloor hold a
// freshly promoted leader that has not caught up to the tail the handover
// confirmed. It lives in the replicated table — and until now the snapshot
// codec NEVER PERSISTED it: a raft snapshot or a learner layout mirror taken
// over a table with floors restores a table WITHOUT them, so every node that
// restarts through a snapshot silently loses the only bound that says "the
// system has acknowledged at least this far". The live-cluster forensics read
// "all 1024 floors are zero" on a decoded mirror — that reading is this bug,
// not (only) floorless handovers.

import (
	"reflect"
	"testing"
)

func TestTableSnapshotKeepsCommitFloors(t *testing.T) {
	tbl := NewTable(8, 2)
	for _, id := range []string{"node-1", "node-2"} {
		tbl.Peers[id] = Peer{Role: RoleLearner, ID: id, ClientAddr: "http://" + id + ":8591"}
	}
	tbl.Slots[0] = &Placement{Leader: "node-1", Replicas: []string{"node-1", "node-2"}, Epoch: 1, State: SlotStable}
	tbl.CommitLEO[0] = 57
	tbl.CommitLEO[3] = 12345

	b := tbl.EncodeTableBinary()
	got, err := DecodeTableBinary(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.CommitLEO[0] != 57 || got.CommitLEO[3] != 12345 {
		t.Fatalf("commit floors lost through a snapshot: slot0=%d slot3=%d (want 57/12345)",
			got.CommitLEO[0], got.CommitLEO[3])
	}
	if len(got.CommitLEO) != 2 {
		t.Fatalf("floors grew or shrank: %v", got.CommitLEO)
	}
	// And full-map parity, the shape TestTableSnapshotRestore enforces for
	// every other replicated field.
	if !reflect.DeepEqual(tbl.CommitLEO, got.CommitLEO) {
		t.Fatalf("floors diverged through the round trip: %+v vs %+v", tbl.CommitLEO, got.CommitLEO)
	}
}
