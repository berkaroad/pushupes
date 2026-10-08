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
	"fmt"
	"testing"
)

// TestStorageGaugeSumsLeaderCopies drives the cluster storage gauge across two
// nodes: the slots this node leads are measured locally, the ones a peer leads
// are asked for over the peer plane, and each slot is counted exactly once —
// its LEADER's copy. The peer holds a copy of the other node's slot too, so a
// gauge that summed "a copy somewhere" (or every replica) would come out too
// high and the assertion below catches it.
func TestStorageGaugeSumsLeaderCopies(t *testing.T) {
	self, selfStore := newTestEngine(t, "node-1")
	peer, peerStore := newTestEngine(t, "node-2")
	peerAddr := newPeerHarness(t, peer)

	// Six records in a slot this node leads, three in one the peer leads, and
	// two more in the peer's COPY of the first slot (data the gauge must not
	// count twice).
	selfAgg := aggInSlot(t, self, 0)
	for v := 1; v <= 6; v++ {
		if _, err := selfStore.Append(makeRecord(selfAgg, uint32(v), fmt.Sprintf("self-%d", v))); err != nil {
			t.Fatalf("append self: %v", err)
		}
	}
	peerAgg := aggInSlot(t, peer, 1)
	for v := 1; v <= 3; v++ {
		if _, err := peerStore.Append(makeRecord(peerAgg, uint32(v), fmt.Sprintf("peer-%d", v))); err != nil {
			t.Fatalf("append peer: %v", err)
		}
	}
	// ... and the mirror image: this node's own copy of the PEER's slot. A gauge
	// that summed "a copy of every slot on whoever holds one" would count both
	// of these; the sum below must not move.
	sharedAgg := aggInSlot(t, peer, 0)
	for v := 1; v <= 2; v++ {
		if _, err := peerStore.Append(makeRecord(sharedAgg, uint32(v), fmt.Sprintf("shared-%d", v))); err != nil {
			t.Fatalf("append shared: %v", err)
		}
	}
	mirrorAgg := aggInSlot(t, self, 1)
	for v := 1; v <= 4; v++ {
		if _, err := selfStore.Append(makeRecord(mirrorAgg, uint32(v), fmt.Sprintf("mirror-%d", v))); err != nil {
			t.Fatalf("append mirror: %v", err)
		}
	}

	// Before any sample the gauge is empty (not "0 bytes of storage").
	if bytes, complete := self.ClusterStorageBytes(); bytes != 0 || complete {
		t.Fatalf("unsampled gauge = %d/%v, want 0/false", bytes, complete)
	}

	// Table: slot 0 led by this node, slot 1 by the peer; both replicate the
	// other's slot.
	self.tableMu.Lock()
	self.table.Peers = map[string]Peer{
		"node-1": {ID: "node-1", PeerAddr: "127.0.0.1:1"},
		"node-2": {ID: "node-2", PeerAddr: peerAddr},
	}
	self.table.Slots[0] = &Placement{Leader: "node-1", Replicas: []string{"node-1", "node-2"}, Epoch: 1, State: SlotStable}
	self.table.Slots[1] = &Placement{Leader: "node-2", Replicas: []string{"node-2", "node-1"}, Epoch: 1, State: SlotStable}
	self.tableMu.Unlock()

	self.refreshStorage()
	local := uint64(self.SlotSize(0))
	remote := uint64(peer.SlotSize(1))
	if local == 0 || remote == 0 {
		t.Fatalf("setup produced no bytes (local=%d remote=%d)", local, remote)
	}
	bytes, complete := self.ClusterStorageBytes()
	if !complete {
		t.Fatal("every slot leader answered; the sample must be complete")
	}
	if bytes != local+remote {
		t.Fatalf("gauge %d, want the two leader copies %d+%d=%d (the peer's copy of slot 0 must not count)",
			bytes, local, remote, local+remote)
	}

	// A leader that cannot be reached leaves the sample partial instead of
	// failing it: the bytes that were collected still show up.
	self.tableMu.Lock()
	self.table.Peers["node-2"] = Peer{ID: "node-2", PeerAddr: "127.0.0.1:1"}
	self.tableMu.Unlock()
	self.refreshStorage()
	bytes, complete = self.ClusterStorageBytes()
	if complete {
		t.Fatal("a slot leader did not answer; the sample must be marked incomplete")
	}
	if bytes != local {
		t.Fatalf("partial gauge %d, want the local leader copy only (%d)", bytes, local)
	}
}
