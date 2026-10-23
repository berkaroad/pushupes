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
	if err := fEng.AddMember(ctx, "node-4", "127.0.0.1:9999", RoleVoter); err == nil {
		t.Fatal("AddMember on a follower must be refused")
	} else if _, ok := err.(*NotControllerError); !ok {
		t.Fatalf("AddMember on a follower returned %T (%v), want *NotControllerError", err, err)
	}

	// The new node's address must be a real listener so the replicator can dial
	// it (the node itself is not run here: the change must still commit — the
	// cluster keeps a quorum without it).
	added := testListenerAddr(t)
	if err := leEng.AddMember(ctx, "node-4", added, RoleVoter); err != nil {
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
	if err := leEng.AddMember(ctx, "node-4", added, RoleVoter); err != nil {
		t.Fatalf("repeat AddMember must be a no-op: %v", err)
	}

	// Remove it again (a slot-less node drains trivially and is gone in one
	// call — the drain pipeline only parks a DELETE that still has work).
	status, err := leEng.RemoveMember(ctx, "node-4")
	if err != nil || status != "removed" {
		t.Fatalf("RemoveMember = (%q, %v), want removed", status, err)
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

	if err := leEng.AddMember(ctx, "", "127.0.0.1:1", RoleVoter); err == nil {
		t.Fatal("AddMember without an id must be refused")
	}
	if err := leEng.AddMember(ctx, "node-9", "", RoleVoter); err == nil {
		t.Fatal("AddMember without a peer address must be refused")
	}
	// An address that already belongs to a member under another id is a
	// misconfigured joining node, not a new member.
	if err := leEng.AddMember(ctx, "node-9", c.peerOf(leader).PeerAddr, RoleVoter); err == nil {
		t.Fatal("AddMember reusing a member's address must be refused")
	}
	// A single-voter cluster cannot remove its last member.
	if _, err := leEng.RemoveMember(ctx, leader); err == nil {
		t.Fatal("removing the last voter must be refused")
	}
}

// TestRemoveMemberDrainsThenRemoves pins the DELETE state machine: a node
// that still leads slots is never evicted from under those writes — the
// first call marks the drain (with the removal intent riding the same
// table entry) and answers "draining"; the handovers keep serving clients
// while they run; the moment the node is empty, finishPendingRemovals —
// the drain rounds of the rebalancer, not a second operator call — commits
// the removal. The online/offline question is gone from the whole path.
func TestRemoveMemberDrainsThenRemoves(t *testing.T) {
	e := newLeaderEngine(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Two data-plane learners so the ring has somewhere to move TO.
	for _, pair := range [][2]string{{"node-4", "127.0.0.1:9904"}, {"node-5", "127.0.0.1:9905"}} {
		id, addr := pair[0], pair[1]
		if err := e.AddMember(ctx, id, addr, RoleLearner); err != nil {
			t.Fatalf("AddMember %s: %v", id, err)
		}
		join(t, e, id, addr)
	}
	// Slot 0 led by node-4: this is the work the drain must move out.
	tableWithLeaders(t, e, map[int32]string{0: "node-4"})

	status, err := e.RemoveMember(ctx, "node-4")
	if err != nil || status != "draining" {
		t.Fatalf("RemoveMember(slots led) = (%q, %v), want draining", status, err)
	}
	p := e.TableSnapshot().Peers["node-4"]
	if !p.Draining || !p.PendingRemove {
		t.Fatalf("DELETE must mark drain+remove intent: %+v", p)
	}
	// A re-issued DELETE is a progress report, not a restarted pipeline.
	if status, err = e.RemoveMember(ctx, "node-4"); err != nil || status != "draining" {
		t.Fatalf("repeat RemoveMember = (%q, %v), want draining", status, err)
	}

	// The handovers commit (what the rebalancer's drain rounds drive in
	// production): EVERY slot the node led moves to the other data node —
	// the drain only counts as done when the whole ledger is empty.
	var led []int32
	for sl, pl := range e.TableSnapshot().Slots {
		if pl.Leader == "node-4" {
			led = append(led, sl)
		}
	}
	if len(led) == 0 {
		t.Fatal("setup: node-4 led nothing")
	}
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: led, NewLeader: "node-5"})

	// The removal now completes WITHOUT another DELETE: the drain round
	// that finds the node empty executes the pending intent itself.
	e.finishPendingRemovals(ctx)
	if _, ok := e.TableSnapshot().Peers["node-4"]; ok {
		t.Fatal("drained node with pending removal intent still in the directory")
	}
	if _, ok := e.TableSnapshot().Peers["node-5"]; !ok {
		t.Fatal("the handover target vanished too")
	}
	if status, err := e.RemoveMember(ctx, "node-4"); err != nil || status != "removed" {
		t.Fatalf("RemoveMember(gone node) = (%q, %v), want removed", status, err)
	}
}

// TestReAdmissionWithdrawsDrain pins the cancel half of the scale-down at
// the REAL entry point (AddMember, not a raw table apply): re-admitting a
// draining learner must commit and clear both marks — an idempotent no-op
// there would leave the node parked in a drain no operator asked to
// continue. The announced addresses survive the withdrawal.
func TestReAdmissionWithdrawsDrain(t *testing.T) {
	e := newLeaderEngine(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	addr := "127.0.0.1:9906"
	if err := e.AddMember(ctx, "node-6", addr, RoleLearner); err != nil {
		t.Fatal(err)
	}
	join(t, e, "node-6", addr)
	tableWithLeaders(t, e, map[int32]string{0: "node-6"})
	if _, err := e.RemoveMember(ctx, "node-6"); err != nil {
		t.Fatal(err)
	}
	p := e.TableSnapshot().Peers["node-6"]
	if !p.Draining || !p.PendingRemove {
		t.Fatalf("setup: DELETE must have marked the drain: %+v", p)
	}
	// The operator changes their mind: the ordinary admission call IS the
	// withdrawal.
	if err := e.AddMember(ctx, "node-6", addr, RoleLearner); err != nil {
		t.Fatalf("re-admission (withdrawal) failed: %v", err)
	}
	p = e.TableSnapshot().Peers["node-6"]
	if p.Draining || p.PendingRemove {
		t.Fatalf("re-admission must withdraw drain+removal: %+v", p)
	}
	if p.ClientAddr == "" {
		t.Fatalf("the withdrawal must not drop the node's announced addresses: %+v", p)
	}
	// And a clean re-admission stays the no-op register_test pins.
	before := commitIndexOf(t, e.node)
	if err := e.AddMember(ctx, "node-6", addr, RoleLearner); err != nil {
		t.Fatal(err)
	}
	if after := commitIndexOf(t, e.node); after != before {
		t.Fatalf("clean re-admission must not commit: %d -> %d", before, after)
	}
}

// TestSelfAdmissionNeverWithdrawsDrain pins the split: a node offering
// itself (the adoption path a reboot walks) leaves the operator's drain
// and removal marks standing — only the admin-plane admission withdraws.
func TestSelfAdmissionNeverWithdrawsDrain(t *testing.T) {
	e := newLeaderEngine(t)
	ctx := context.Background()
	addr := "127.0.0.1:9907"
	if err := e.AddMember(ctx, "node-7", addr, RoleLearner); err != nil {
		t.Fatal(err)
	}
	join(t, e, "node-7", addr)
	tableWithLeaders(t, e, map[int32]string{0: "node-7"})
	if _, err := e.RemoveMember(ctx, "node-7"); err != nil {
		t.Fatal(err)
	}
	// The node reboots and offers itself: marks stand.
	if err := e.AdmitSelf(ctx, "node-7", addr, RoleLearner); err != nil {
		t.Fatal(err)
	}
	p := e.TableSnapshot().Peers["node-7"]
	if !p.Draining || !p.PendingRemove {
		t.Fatalf("self-admission must not withdraw the operator's removal: %+v", p)
	}
	// The operator's admission does.
	if err := e.AddMember(ctx, "node-7", addr, RoleLearner); err != nil {
		t.Fatal(err)
	}
	if p := e.TableSnapshot().Peers["node-7"]; p.Draining || p.PendingRemove {
		t.Fatalf("admin re-admission must withdraw: %+v", p)
	}
}
