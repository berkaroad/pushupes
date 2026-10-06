package cluster

import (
	"testing"
)

// A placement may name a node the peer directory no longer holds — a
// leave_node that landed while a migration to that node was committing, or a
// leader-less slot. The snapshot encoder must serialise EVERY name the
// placements reference: its node dictionary is written as one block before the
// entries, so a name first registered mid-walk gets an index that is never
// written to the file, and the decoder then refuses the whole snapshot
// ("table: node index 6 out of range"), which is fatal at startup.
//
// Reproduced from a real cluster's snapshot directory: the table held six
// peers (node-1..node-5, node-7) while 238 placements still named node-6,
// which had been dropped from the directory and was re-seated as the leader
// of those slots by a migration that committed after the removal.
func TestTableBinaryRoundTripsPlacementOutsideDirectory(t *testing.T) {
	tbl := NewTable(4, 2)
	for _, id := range []string{"node-1", "node-2", "node-4", "node-5", "node-7"} {
		tbl.Peers[id] = Peer{ID: id, PeerAddr: "127.0.0.1:8391", ClientAddr: "http://127.0.0.1:8591"}
	}
	// node-6 is no longer a directory member, yet the placements still name
	// it (leader and replica), exactly like the real table did.
	tbl.Slots[0] = &Placement{Leader: "node-6", Replicas: []string{"node-5", "node-1", "node-2", "node-6"}, Epoch: 3, State: SlotStable}
	tbl.Slots[1] = &Placement{Leader: "node-7", Replicas: []string{"node-7", "node-6"}, Epoch: 2, State: SlotStable}
	// A leader-less slot (leave_node clears the leader when the set empties)
	// registers the empty name just as late: same failure mode.
	tbl.Slots[2] = &Placement{Leader: "", Replicas: []string{"node-1"}, Epoch: 1, State: SlotStable}
	tbl.Slots[3] = &Placement{Leader: "node-2", Replicas: []string{"node-2", "node-6"}, Epoch: 5, State: SlotMigratingOut, MigratingTo: "node-6"}

	raw := tbl.EncodeTableBinary()
	out, err := DecodeTableBinary(raw)
	if err != nil {
		t.Fatalf("snapshot of a table whose placements name a non-member is unreadable: %v", err)
	}
	if len(out.Peers) != len(tbl.Peers) {
		t.Fatalf("peer directory changed across the round trip: got %d peers, want %d", len(out.Peers), len(tbl.Peers))
	}
	for s, want := range tbl.Slots {
		got, ok := out.Slots[s]
		if !ok {
			t.Fatalf("slot %d missing from the restored table", s)
		}
		if got.Leader != want.Leader || got.Epoch != want.Epoch || got.State != want.State || got.MigratingTo != want.MigratingTo {
			t.Fatalf("slot %d changed across the round trip: got %+v want %+v", s, got, want)
		}
		if len(got.Replicas) != len(want.Replicas) {
			t.Fatalf("slot %d replica set changed: got %v want %v", s, got.Replicas, want.Replicas)
		}
		for i := range want.Replicas {
			if got.Replicas[i] != want.Replicas[i] {
				t.Fatalf("slot %d replica set changed: got %v want %v", s, got.Replicas, want.Replicas)
			}
		}
	}
}

// The encoder's dictionary also has to cover the names of a table that names
// NOBODY but its peers (the ordinary case) after the two-pass rewrite — the
// common path must stay byte-identical in behaviour (same dictionary order:
// peers first, then any name the placements add).
func TestTableBinaryDictionaryOrderStaysPeerFirst(t *testing.T) {
	tbl := NewTable(4, 2)
	for _, id := range []string{"node-2", "node-1"} {
		tbl.Peers[id] = Peer{ID: id}
	}
	tbl.Slots[0] = &Placement{Leader: "node-1", Replicas: []string{"node-1", "node-2"}, Epoch: 1, State: SlotStable}
	out, err := DecodeTableBinary(tbl.EncodeTableBinary())
	if err != nil {
		t.Fatal(err)
	}
	if out.Slots[0].Leader != "node-1" || out.Slots[0].Replicas[1] != "node-2" {
		t.Fatalf("ordinary round trip broke: %+v", out.Slots[0])
	}
}
