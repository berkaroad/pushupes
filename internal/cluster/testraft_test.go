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
	"testing"
	"time"

	"github.com/berkaroad/pushupes/internal/raft"
)

// newTestRaftNode stands up an in-process single-voter consensus node so tests
// can exercise the real submit -> FSM -> table path. It runs the production
// transport (a real 127.0.0.1 listener) rather than an in-memory shortcut.
func newTestRaftNode(t *testing.T, applier Applier) *Node {
	t.Helper()
	c := newTestRaftCluster(t, []string{"node-1"}, map[string]Applier{"node-1": applier})
	leader := c.waitLeader(5 * time.Second)
	return c.nodes[leader]
}

// newFollowerRaftNode stands up a three-voter consensus cluster and returns a
// Node that is a FOLLOWER, plus the leader's node id. The "controller-only
// command lands on a follower" path is guarded by Node.IsLeader(), so it can
// only be exercised against a real election.
func newFollowerRaftNode(t *testing.T) (*Node, string) {
	return newFollowerRaftNodeWithID(t)
}

// newFollowerRaftNodeWithID is the same harness under its older name.
func newFollowerRaftNodeWithID(t *testing.T) (*Node, string) {
	t.Helper()
	ids := []string{"node-1", "node-2", "node-3"}
	appliers := map[string]Applier{}
	for _, id := range ids {
		appliers[id] = &Engine{}
	}
	c := newTestRaftCluster(t, ids, appliers)
	leader := c.waitLeader(15 * time.Second)
	for _, id := range ids {
		if id != leader {
			return c.nodes[id], leader
		}
	}
	t.Fatal("unreachable: every node claims to be the leader")
	return nil, ""
}

// testRaftCluster is a small real-TCP consensus cluster for tests that need
// more than one voter. The transport address is deliberately not the node id:
// ids are what the slot table and peer directory are keyed by, so the two must
// stay distinguishable.
type testRaftCluster struct {
	t     *testing.T
	ids   []string
	peers []Peer
	muxes map[string]*peerMux
	nodes map[string]*Node
}

func newTestRaftCluster(t *testing.T, ids []string, appliers map[string]Applier) *testRaftCluster {
	t.Helper()
	c := &testRaftCluster{
		t:     t,
		ids:   ids,
		muxes: map[string]*peerMux{},
		nodes: map[string]*Node{},
	}
	for _, id := range ids {
		mux, err := newPeerMux("127.0.0.1:0", nil)
		if err != nil {
			t.Fatalf("peer mux %s: %v", id, err)
		}
		c.muxes[id] = mux
		c.peers = append(c.peers, Peer{
			ID:         id,
			PeerAddr:   mux.Addr(),
			AdminAddr:  "http://" + mux.Addr(),
			ClientAddr: "http://" + mux.Addr(),
		})
	}
	voters := make([]raft.Voter, 0, len(c.peers))
	for _, p := range c.peers {
		voters = append(voters, raft.Voter{ID: p.ID, Addr: p.PeerAddr})
	}
	for _, id := range ids {
		ap := appliers[id]
		if ap == nil {
			ap = &Engine{}
		}
		fsm := &FSM{applier: ap}
		rn, err := raft.NewNode(raft.Config{
			NodeID:           id,
			DataDir:          t.TempDir(),
			Voters:           voters,
			ApplyTimeout:     5 * time.Second,
			HeartbeatTimeout: 40 * time.Millisecond,
			ElectionTimeout:  120 * time.Millisecond,
		}, fsm, c.muxes[id])
		if err != nil {
			t.Fatalf("raft node %s: %v", id, err)
		}
		self := c.peerOf(id)
		c.nodes[id] = &Node{
			cfg: Config{
				NodeID:       id,
				PeerAddr:     self.PeerAddr,
				AdminAddr:    self.AdminAddr,
				ClientAddr:   self.ClientAddr,
				ApplyTimeout: 5 * time.Second,
				Peers:        c.peers,
			},
			raft: rn,
			fsm:  fsm,
			mux:  c.muxes[id],
		}
	}
	t.Cleanup(c.stopAll)
	return c
}

func (c *testRaftCluster) peerOf(id string) Peer {
	for _, p := range c.peers {
		if p.ID == id {
			return p
		}
	}
	return Peer{ID: id}
}

func (c *testRaftCluster) waitLeader(within time.Duration) string {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		leader := ""
		agree := true
		for _, id := range c.ids {
			got := c.nodes[id].LeaderID()
			if got == "" {
				agree = false
				break
			}
			if leader == "" {
				leader = got
			} else if got != leader {
				agree = false
				break
			}
		}
		if agree && leader != "" {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("in-process raft never settled on a leader")
	return ""
}

func (c *testRaftCluster) stopAll() {
	for _, id := range c.ids {
		if n := c.nodes[id]; n != nil {
			n.Close()
		}
	}
}
