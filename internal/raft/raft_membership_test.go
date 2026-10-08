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

package raft

import (
	"fmt"
	"testing"
	"time"
)

// addNode extends the test cluster with a node that is NOT in the original
// configuration: it starts on the cluster's own voters as a seed (exactly the
// way a runtime-joined node starts) and is then added through the consensus
// layer's AddVoter.
func (c *testCluster) addNode(id string) {
	c.t.Helper()
	tn := newTestNet(c.t)
	c.nodes[id] = &testNode{id: id, net: tn}
	c.order = append(c.order, id)
	c.seedIDs[id] = true
	c.start(id, false)
}

// TestAddVoterAtRuntime is the runtime-join path end to end: a node that the
// original configuration does not contain is added as a voter with AddVoter,
// the change replicates to every member, and the cluster keeps committing
// afterwards — which is the property that matters (a membership change that
// leaves the group unable to commit is worse than no change at all).
func TestAddVoterAtRuntime(t *testing.T) {
	c := startCluster(t, 3)
	defer c.stopAll()
	leader := c.leader(3 * time.Second)

	c.addNode("node-4")
	// A joining node starts with the cluster's voters as its seed list.
	joining := c.nodes["node-4"].node
	if joining.IsMember("node-4") {
		t.Fatal("a joining node must not consider itself a member before it is added")
	}
	if len(joining.Members()) != 3 {
		t.Fatalf("joining node's seed = %v, want the 3 original voters", joining.Members())
	}

	if err := c.nodes[leader].node.AddMember("node-4", c.nodes["node-4"].net.addr, 10*time.Second); err != nil {
		t.Fatalf("AddVoter: %v", err)
	}

	// Every node must converge on the new membership. The joining node is the
	// one that matters most: it derives the membership from what the leader
	// replicated to it, so "node-1 knows about node-4" says nothing about
	// whether node-4 knows about node-1.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, id := range c.order {
			if !c.nodes[id].node.IsMember("node-4") {
				all = false
				break
			}
		}
		if all {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, id := range c.order {
		n := c.nodes[id].node
		if !n.IsMember("node-4") {
			t.Fatalf("%s did not learn the new membership: %v", id, n.Voters())
		}
		if len(n.Voters()) != 4 {
			t.Fatalf("%s membership = %v, want 4 voters", id, n.Voters())
		}
	}
	// ...and specifically that the joining node did not come up believing it is
	// the whole cluster: a voter set of one is a quorum of one, which is how a
	// stale node elects itself and acknowledges writes nobody else has.
	for _, id := range c.order {
		if id == "node-4" {
			continue
		}
		if !c.nodes["node-4"].node.IsMember(id) {
			t.Fatalf("the joining node does not know member %s: %v", id, c.nodes["node-4"].node.Voters())
		}
	}

	// The cluster must still commit with the new configuration (this is where
	// a change that broke quorum arithmetic would show up).
	leader = c.leader(3 * time.Second)
	if _, err := c.apply(leader, "set after-add 1"); err != nil {
		t.Fatalf("apply after add: %v", err)
	}
	c.waitApplied(c.nodes[leader].node.LastIndex(), 5*time.Second, "")
}

// TestAddVoterRefusesOnFollower pins the "controller only" rule and its
// diagnostics: the change is never forwarded, the follower names the leader.
func TestAddVoterRefusesOnFollower(t *testing.T) {
	c := startCluster(t, 3)
	defer c.stopAll()
	leader := c.leader(3 * time.Second)

	var follower string
	for _, id := range c.order {
		if id != leader {
			follower = id
			break
		}
	}
	c.addNode("node-4")
	err := c.nodes[follower].node.AddMember("node-4", c.nodes["node-4"].net.addr, 2*time.Second)
	if err == nil {
		t.Fatal("AddVoter on a follower must be refused")
	}
	if !isNotLeader(err) {
		t.Fatalf("AddVoter on a follower returned %v, want ErrNotLeader", err)
	}
}

// TestRemoveVoterAtRuntime drops a member and checks that the survivors keep
// committing: a 3-voter cluster becomes 2, and quorum follows the new
// configuration (2, not 3), which is exactly what a stale voter set would get
// wrong — the cluster would stall waiting for a quorum it can no longer reach.
func TestRemoveVoterAtRuntime(t *testing.T) {
	c := startCluster(t, 3)
	defer c.stopAll()
	leader := c.leader(3 * time.Second)

	var victim string
	for _, id := range c.order {
		if id != leader {
			victim = id
			break
		}
	}
	if err := c.nodes[leader].node.RemoveMember(victim, 10*time.Second); err != nil {
		t.Fatalf("RemoveVoter: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	converged := false
	for time.Now().Before(deadline) {
		all := true
		for _, id := range c.order {
			if c.nodes[id].node == nil {
				continue
			}
			if c.nodes[id].node.IsMember(victim) {
				all = false
				break
			}
		}
		if all {
			converged = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !converged {
		for _, id := range c.order {
			if c.nodes[id].node == nil {
				continue
			}
			t.Logf("%s: applied=%d commit=%d last=%d voters=%v learners=%v",
				id, c.nodes[id].node.AppliedIndex(), c.nodes[id].node.CommitIndex(),
				c.nodes[id].node.LastIndex(), c.nodes[id].node.Voters(), c.nodes[id].node.learners)
		}
	}
	for _, id := range c.order {
		n := c.nodes[id].node
		if n == nil {
			continue
		}
		if n.IsMember(victim) {
			t.Fatalf("%s still counts %s as a voter: %v", id, victim, n.Voters())
		}
	}

	// The two survivors must be able to commit on their own.
	c.stop(victim)
	leader = c.leader(3 * time.Second)
	if _, err := c.apply(leader, "set after-remove 1"); err != nil {
		t.Fatalf("apply after remove: %v", err)
	}
	c.waitApplied(c.nodes[leader].node.LastIndex(), 5*time.Second, victim)
}

// TestRemoveLastVoterIsRefused keeps a cluster from being configured out of
// existence.
func TestRemoveLastVoterIsRefused(t *testing.T) {
	c := startCluster(t, 1)
	defer c.stopAll()
	leader := c.leader(3 * time.Second)
	err := c.nodes[leader].node.RemoveMember(leader, 2*time.Second)
	if err == nil {
		t.Fatal("removing the only voter must be refused")
	}
}

// TestAddVoterIsIdempotent: adding a current member is a no-op, so an operator
// (or a retrying join loop) cannot corrupt the configuration by repeating it.
func TestAddVoterIsIdempotent(t *testing.T) {
	c := startCluster(t, 3)
	defer c.stopAll()
	leader := c.leader(3 * time.Second)
	var member string
	for _, id := range c.order {
		if id != leader {
			member = id
			break
		}
	}
	before := len(c.nodes[leader].node.Voters())
	if err := c.nodes[leader].node.AddMember(member, c.nodes[member].net.addr, 5*time.Second); err != nil {
		t.Fatalf("re-adding a member must be a no-op: %v", err)
	}
	if after := len(c.nodes[leader].node.Voters()); after != before {
		t.Fatalf("membership grew on a repeat add: %d -> %d", before, after)
	}
}

func isNotLeader(err error) bool {
	for err != nil {
		if err == ErrNotLeader {
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// waitVoters blocks until every node in the cluster reports n voters (node-4
// included), and fails the test with each node's view when they do not.
func waitVoters(t *testing.T, c *testCluster, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, id := range c.order {
			if c.nodes[id].node == nil {
				continue
			}
			if len(c.nodes[id].node.Voters()) != n {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, id := range c.order {
		if c.nodes[id].node != nil {
			t.Logf("%s: voters=%v", id, c.nodes[id].node.Voters())
		}
	}
	t.Fatalf("not every node reported %d voters within 10s", n)
}

// TestBootstrapPutsConfigurationInTheLog pins where the initial configuration
// has to live. Writing it only to the local conf record is enough for the nodes
// that were configured at the start and for nobody else: a node added at
// runtime has to be able to derive the membership from what it is given (the
// leader's log, or a snapshot of it).
func TestBootstrapPutsConfigurationInTheLog(t *testing.T) {
	c := startCluster(t, 3)
	defer c.stopAll()
	c.leader(3 * time.Second)

	var first Entry
	for i, id := range c.order {
		e, ok := c.nodes[id].node.log.Entry(1)
		if !ok {
			t.Fatalf("%s has no entry at index 1: %v", id, c.nodes[id].node.Voters())
		}
		if e.Kind != KindConf {
			t.Fatalf("%s: entry 1 is kind %d, want the bootstrap configuration (KindConf=%d)", id, e.Kind, KindConf)
		}
		cc, err := decodeConfChange(e.Data)
		if err != nil {
			t.Fatalf("%s: decode the bootstrap entry: %v", id, err)
		}
		if cc.op != confSet || len(cc.voters) != 3 {
			t.Fatalf("%s: bootstrap entry = op %d with %d voters, want a set of 3", id, cc.op, len(cc.voters))
		}
		// Every node has to write the identical entry, or their logs do not
		// match from the first entry on.
		if i == 0 {
			first = e
			continue
		}
		if e.Term != first.Term || string(e.Data) != string(first.Data) {
			t.Fatalf("%s wrote a different bootstrap entry than %s", id, c.order[0])
		}
	}
	if first.Term == 0 {
		t.Fatal("the bootstrap entry was written at term 0; it must be term 1 history, not an entry a leader could have committed on its own")
	}
}

// TestAddVoterLearnsConfigurationFromSnapshot exercises the catch-up path that
// does not carry the log: the leader has already compacted the entries that
// reported the configuration, so the joining node's first catch-up is a
// snapshot. Whatever the snapshot does not carry, the node never learns.
func TestAddVoterLearnsConfigurationFromSnapshot(t *testing.T) {
	// SnapshotThreshold 1 makes every apply snapshot, so the leader's log is
	// compacted within a few writes.
	c := startClusterCfg(t, 3, func(cfg *Config) { cfg.SnapshotThreshold = 1 })
	defer c.stopAll()
	leader := c.leader(3 * time.Second)
	for i := 0; i < 8; i++ {
		if _, err := c.apply(leader, fmt.Sprintf("set s%d v", i)); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c.nodes[leader].node.log.FirstIndex() > 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if first := c.nodes[leader].node.log.FirstIndex(); first <= 1 {
		t.Fatalf("the leader's log is not compacted (first index %d): this test would exercise the log path, not the snapshot path", first)
	}

	c.addNode("node-4")
	if err := c.nodes[leader].node.AddMember("node-4", c.nodes["node-4"].net.addr, 10*time.Second); err != nil {
		t.Fatalf("AddVoter: %v", err)
	}
	waitVoters(t, c, 4)
}

// TestJoinedNodeCannotElectItselfAlone is the failure a wrong membership view
// causes: a node that believes it is the whole cluster has a quorum of one, so
// it elects itself — on a log that can be arbitrarily far behind — and starts
// acknowledging writes nobody else holds. With the real configuration (one
// voter of four) no election can be won.
func TestJoinedNodeCannotElectItselfAlone(t *testing.T) {
	c := startCluster(t, 3)
	defer c.stopAll()
	leader := c.leader(3 * time.Second)
	c.addNode("node-4")
	if err := c.nodes[leader].node.AddMember("node-4", c.nodes["node-4"].net.addr, 10*time.Second); err != nil {
		t.Fatalf("AddVoter: %v", err)
	}
	waitVoters(t, c, 4)

	// Take the other three away: what is left is 1 of 4 voters, which is not
	// a quorum.
	for _, id := range c.order {
		if id != "node-4" {
			c.stop(id)
		}
	}
	// Give it several election timeouts to try.
	time.Sleep(time.Second)
	n := c.nodes["node-4"].node
	if n.IsLeader() {
		t.Fatalf("one voter of four elected itself: voters=%v", n.Voters())
	}
	if _, err := n.Apply([]byte("set ghost 1"), 300*time.Millisecond); err == nil {
		t.Fatalf("a node without a quorum accepted a write (voters=%v)", n.Voters())
	}
}

// ---- tiny test hooks (kept out of the production path) ----

func (n *Node) mu() {}
