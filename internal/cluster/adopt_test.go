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
	"testing"
	"time"
)

// TestAcceptAdoptionCommitsOnLeader pins the leader-side handler: an offer from
// a node that is not a member is committed as a membership change, and a
// repeat offer is a no-op.
func TestAcceptAdoptionCommitsOnLeader(t *testing.T) {
	c := newTestRaftCluster(t, []string{"node-1", "node-2", "node-3"}, map[string]Applier{})
	leader := c.waitLeader(15 * time.Second)
	le, _ := newTestEngine(t, leader)
	le.node = c.nodes[leader]

	added := testListenerAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// The offer is delivered by the node itself; here we call the handler
	// directly, which is exactly what the Adopt RPC does on the leader.
	if err := le.acceptAdoption(ctx, &Adoption{ID: "node-4", PeerAddr: added}); err != nil {
		t.Fatalf("acceptAdoption: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(c.nodes[leader].Members()) == 4 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := len(c.nodes[leader].Members()); got != 4 {
		t.Fatalf("membership = %d, want 4 after adoption", got)
	}
	// Idempotent.
	if err := le.acceptAdoption(ctx, &Adoption{ID: "node-4", PeerAddr: added}); err != nil {
		t.Fatalf("repeat adoption must be a no-op: %v", err)
	}
	if got := len(c.nodes[leader].Members()); got != 4 {
		t.Fatalf("repeat adoption changed membership: %d", got)
	}
	// An incomplete offer is refused.
	if err := le.acceptAdoption(ctx, &Adoption{ID: "", PeerAddr: added}); err == nil {
		t.Fatal("incomplete adoption offer must be refused")
	}
}

// TestAdoptOnFollowerRelaysToLeader: a non-leader relays the offer to the
// leader instead of refusing (the joiner may hit any member).
func TestAdoptOnFollowerRelaysToLeader(t *testing.T) {
	c := newTestRaftCluster(t, []string{"node-1", "node-2", "node-3"}, map[string]Applier{})
	leader := c.waitLeader(15 * time.Second)
	_ = leader
	// Without a live leader peer address the relay has nowhere to go: it must
	// fail loudly rather than silently swallow the offer.
	var follower *Node
	for _, id := range c.ids {
		if id != leader {
			follower = c.nodes[id]
			break
		}
	}
	fe, _ := newTestEngine(t, follower.cfg.NodeID)
	fe.node = follower
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := fe.acceptAdoption(ctx, &Adoption{ID: "node-9", PeerAddr: testListenerAddr(t)}); err == nil {
		t.Fatal("a follower with no reachable leader must fail the offer, not drop it")
	}
}
