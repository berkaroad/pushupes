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
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"io"
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
		tbl.Peers[id] = Peer{Role: RoleLearner, ID: id, PeerAddr: "127.0.0.1:8391", ClientAddr: "http://127.0.0.1:8591"}
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

// The cluster generation (Table.Topology) rides the snapshot header: a node
// that joins from one has to learn the value its copies will be fenced
// against, and the fence runs before any entry of the next round applies.
func TestSnapshotCarriesTheClusterGeneration(t *testing.T) {
	tbl := NewTable(4, 2)
	tbl.Topology = "gen-a"
	tbl.Peers["node-2"] = Peer{Role: RoleLearner, ID: "node-2"}
	tbl.Slots[0] = &Placement{Leader: "node-2", Replicas: []string{"node-2"}, Epoch: 1, State: SlotStable}
	out, err := DecodeTableBinary(tbl.EncodeTableBinary())
	if err != nil {
		t.Fatal(err)
	}
	if out.Topology != "gen-a" {
		t.Fatalf("generation across the round trip = %q, want gen-a", out.Topology)
	}
	if out.Slots[0].Leader != "node-2" || len(out.Peers) != 1 {
		t.Fatalf("the generation field displaced the rest of the file: %+v", out)
	}
}

// A snapshot written BEFORE the generation field must stay readable: the new
// field is one len-prefixed string between the policy tier and the directory,
// so a v6 blob is a v7 blob minus that string. Reading it back as v6 also
// pins the two version gates (role, topology) that are easy to break by
// bumping tableVer.
func TestSnapshotFromBeforeTheGenerationFieldStillRestores(t *testing.T) {
	tbl := NewTable(4, 2)
	tbl.Topology = "gen-a"
	tbl.Peers["node-2"] = Peer{Role: RoleLearner, ID: "node-2", PeerAddr: "127.0.0.1:8392"}
	tbl.Slots[1] = &Placement{Leader: "node-2", Replicas: []string{"node-2"}, Epoch: 2, State: SlotStable}

	raw := tbl.EncodeTableBinary()
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	// Header: magic(4) ver(1) slotCount(4) replicas(4) then the policy string.
	off := 13
	plen, n := binary.Uvarint(plain[off:])
	if n <= 0 {
		t.Fatal("policy length unreadable")
	}
	off += n + int(plen)
	tlen, n := binary.Uvarint(plain[off:])
	if n <= 0 || string(plain[off+n:off+n+int(tlen)]) != "gen-a" {
		t.Fatalf("generation not where the format says: %d bytes at %d", tlen, off)
	}
	old := append([]byte{}, plain[:off]...)
	old = append(old, plain[off+n+int(tlen):]...) // drop the generation string
	old[4] = tableVerRole                         // ...and say v6
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(old); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	back, err := DecodeTableBinary(buf.Bytes())
	if err != nil {
		t.Fatalf("a pre-generation snapshot no longer decodes: %v", err)
	}
	if back.Topology != "" {
		t.Fatalf("v6 blob claimed a generation %q", back.Topology)
	}
	if back.Peers["node-2"].Role != RoleLearner || back.Peers["node-2"].PeerAddr != "127.0.0.1:8392" {
		t.Fatalf("v6 directory entry misread: %+v", back.Peers["node-2"])
	}
	if p := back.Slots[1]; p == nil || p.Leader != "node-2" || p.Epoch != 2 {
		t.Fatalf("v6 placement misread: %+v", p)
	}
}

// The encoder's dictionary also has to cover the names of a table that names
// NOBODY but its peers (the ordinary case) after the two-pass rewrite — the
// common path must stay byte-identical in behaviour (same dictionary order:
// peers first, then any name the placements add).
func TestTableBinaryDictionaryOrderStaysPeerFirst(t *testing.T) {
	tbl := NewTable(4, 2)
	for _, id := range []string{"node-2", "node-1"} {
		tbl.Peers[id] = Peer{Role: RoleLearner, ID: id}
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
