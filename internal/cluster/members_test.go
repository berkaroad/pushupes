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
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// testListenerAddr reserves a loopback address and returns it as host:port. The
// listener is closed at cleanup: the address is only used as a dial target for
// a replicator whose peer never exists, so the port must be free to bind.
func testListenerAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// TestEngineAddRemoveMember exercises the engine-level runtime membership API:
// a follower refuses (with the controller named), the leader commits, and every
// node converges on the new configuration.
func TestEngineAddRemoveMember(t *testing.T) {
	c := newTestRaftCluster(t, []string{"node-1", "node-2", "node-3"}, map[string]Applier{})
	leader := c.waitLeader(15 * time.Second)
	le := c.nodes[leader]

	// A follower refuses and names the controller. The engine is what the admin
	// API talks to, so the guard is exercised through it.
	leEng, _ := newTestEngine(t, leader)
	leEng.node = le

	var follower *Node
	for _, id := range c.ids {
		if id != leader {
			follower = c.nodes[id]
			break
		}
	}
	fEng, _ := newTestEngine(t, follower.cfg.NodeID)
	fEng.node = follower

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := fEng.AddMember(ctx, "node-4", "127.0.0.1:9999"); err == nil {
		t.Fatal("AddMember on a follower must be refused")
	} else if _, ok := err.(*NotControllerError); !ok {
		t.Fatalf("AddMember on a follower returned %T (%v), want *NotControllerError", err, err)
	}

	// The new node's address must be a real listener so the replicator can dial
	// it (the node itself is not run here: the change must still commit — the
	// cluster keeps a quorum without it).
	added := testListenerAddr(t)
	if err := leEng.AddMember(ctx, "node-4", added); err != nil {
		t.Fatalf("AddMember: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range c.ids {
			if len(c.nodes[id].Members()) != 4 {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, id := range c.ids {
		if got := len(c.nodes[id].Members()); got != 4 {
			t.Fatalf("%s membership = %d, want 4", id, got)
		}
	}

	// Idempotent re-add.
	if err := leEng.AddMember(ctx, "node-4", added); err != nil {
		t.Fatalf("repeat AddMember must be a no-op: %v", err)
	}

	// Remove it again.
	if err := leEng.RemoveMember(ctx, "node-4"); err != nil {
		t.Fatalf("RemoveMember: %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range c.ids {
			seen := false
			for _, m := range c.nodes[id].Members() {
				if m.ID == "node-4" {
					seen = true
				}
			}
			if seen {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, id := range c.ids {
		for _, m := range c.nodes[id].Members() {
			if m.ID == "node-4" {
				t.Fatalf("%s still has node-4 as a member: %v", id, c.nodes[id].Members())
			}
		}
	}
}

// TestAddMemberRejectsBadInput pins the operator mistakes that must be refused
// with a clear error rather than a half-applied change.
func TestAddMemberRejectsBadInput(t *testing.T) {
	c := newTestRaftCluster(t, []string{"node-1"}, map[string]Applier{})
	leader := c.waitLeader(10 * time.Second)
	leEng, _ := newTestEngine(t, leader)
	leEng.node = c.nodes[leader]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := leEng.AddMember(ctx, "", "127.0.0.1:1"); err == nil {
		t.Fatal("AddMember without an id must be refused")
	}
	if err := leEng.AddMember(ctx, "node-9", ""); err == nil {
		t.Fatal("AddMember without a peer address must be refused")
	}
	// An address that already belongs to a member under another id is a
	// misconfigured joining node, not a new member.
	if err := leEng.AddMember(ctx, "node-9", c.peerOf(leader).PeerAddr); err == nil {
		t.Fatal("AddMember reusing a member's address must be refused")
	}
	// A single-voter cluster cannot remove its last member.
	if err := leEng.RemoveMember(ctx, leader); err == nil {
		t.Fatal("removing the last voter must be refused")
	}
}

// TestRemoveMemberOnlyOffline pins the offline-only rule of the removal
// surface: a member the peer directory still marks reachable is refused with
// ErrOnlineMember before the raft change is even submitted, and the same
// member becomes removable once the controller's mark_down verdict reaches
// the table. This is the guard the console mirrors on the card button — the
// backend is authoritative because the console's status snapshot is polled.
func TestRemoveMemberOnlyOffline(t *testing.T) {
	c := newTestRaftCluster(t, []string{"node-1", "node-2", "node-3"}, map[string]Applier{})
	leader := c.waitLeader(15 * time.Second)
	leEng, _ := newTestEngine(t, leader)
	leEng.node = c.nodes[leader]
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// A voter outside the three: added through the ordinary membership path
	// (a real listener so the change commits without it).
	added := testListenerAddr(t)
	if err := leEng.AddMember(ctx, "node-4", added); err != nil {
		t.Fatalf("AddMember: %v", err)
	}
	// The directory learns it with a full registration (join sets all three
	// plane addresses): online under Peer.Offline, so not removable.
	join(t, leEng, "node-4", added)
	err := leEng.RemoveMember(ctx, "node-4")
	if !errors.Is(err, ErrOnlineMember) {
		t.Fatalf("RemoveMember on an online member = %v, want ErrOnlineMember", err)
	}

	// The controller's liveness verdict flips it offline (the mark_down path
	// keeps the directory entry and its addresses — the same state a dead
	// node is left in). The guard now lets the removal through.
	applyCmd(t, leEng, &Command{Op: OpMarkDown, NodeID: "node-4"})
	if err := leEng.RemoveMember(ctx, "node-4"); err != nil {
		t.Fatalf("RemoveMember on an offline member: %v", err)
	}
	for _, id := range c.ids {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) && c.nodes[id].IsMember("node-4") {
			time.Sleep(20 * time.Millisecond)
		}
		if c.nodes[id].IsMember("node-4") {
			t.Fatalf("%s still has node-4 as a member after removal", id)
		}
	}
}
