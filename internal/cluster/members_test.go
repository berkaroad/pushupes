package cluster

import (
	"context"
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
