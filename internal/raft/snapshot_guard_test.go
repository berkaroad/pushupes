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

// An InstallSnapshot that is not NEWER than what the receiver already holds
// must be refused. Accepting one rolls the receiver backward: the snapshot's
// voter set replaces the live configuration (a member added at runtime
// silently disappears from it), the log is reset to the snapshot's index (the
// entries above it are dropped, possibly committed ones) and the FSM is
// restored to an older state — while applied/commit only ever move forward, so
// the node ends up holding a state machine that no configuration and no log
// supports. The protocol refuses such a message for the same reason: rolling a
// node backward behind its own commit/applied cursors is never recoverable.
func TestInstallSnapshotRefusesOutOfDate(t *testing.T) {
	c := startClusterCfg(t, 3, func(cfg *Config) {
		cfg.SnapshotThreshold = 1 // snapshot early so the local snapshot index is non-zero
	})
	defer c.stopAll()
	leader := c.leader(5 * time.Second)
	for i := 0; i < 5; i++ {
		if _, err := c.apply(leader, fmt.Sprintf("set k%d v%d", i, i)); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	var follower *testNode
	for _, id := range c.order {
		if id != leader {
			follower = c.nodes[id]
			break
		}
	}
	waitLastIndex(t, follower, 7*time.Second)

	_, snapIndex, _ := follower.node.log.Snapshot()
	commit := statIndex(follower, "commit_index")
	if snapIndex == 0 || commit == 0 {
		t.Fatalf("setup: follower has snapshot index %d and commit index %d", snapIndex, commit)
	}
	beforeVoters := len(follower.node.Voters())
	beforeLast := follower.node.log.LastIndex()

	// An out-of-date snapshot: the index is at (or below) what this node
	// already has, and it carries an older, smaller configuration.
	stale := installSnapshot{
		Term:      follower.node.Term(),
		LeaderID:  leader,
		LastIndex: snapIndex,
		LastTerm:  1,
		Voters:    []Voter{{ID: "node-1", Addr: "127.0.0.1:1"}, {ID: "node-9", Addr: "127.0.0.1:9"}},
		Data:      []byte("k0=stale\n"),
	}
	if resp := install(t, follower, stale); resp.Success {
		t.Fatalf("an out-of-date snapshot (index %d <= local snapshot index %d) was accepted: "+
			"voters %v (was %d), log last index %d (was %d)", snapIndex, snapIndex,
			follower.node.Voters(), beforeVoters, follower.node.log.LastIndex(), beforeLast)
	}
	// And one below the commit point even without a local snapshot to compare
	// against: accepting it would drop committed entries.
	belowCommit := stale
	belowCommit.LastIndex = 1
	if resp := install(t, follower, belowCommit); resp.Success {
		t.Fatalf("a snapshot at index 1 (commit index %d) was accepted", commit)
	}
	if got := len(follower.node.Voters()); got != beforeVoters {
		t.Fatalf("configuration changed by a refused snapshot: %d voters, was %d", got, beforeVoters)
	}
	if got := follower.node.log.LastIndex(); got < beforeLast {
		t.Fatalf("log rolled back by a refused snapshot: last index %d, was %d", got, beforeLast)
	}
	if v, ok := follower.fsm.get("k0"); !ok || v == "stale" {
		t.Fatalf("FSM state rolled back by a refused snapshot: k0=%q ok=%v", v, ok)
	}

	// The legitimate case must still work: a snapshot ahead of everything the
	// node holds is adopted, configuration included.
	fresh := installSnapshot{
		Term:      follower.node.Term(),
		LeaderID:  leader,
		LastIndex: follower.node.log.LastIndex() + 10,
		LastTerm:  follower.node.Term(),
		Voters:    c.voters,
		Data:      []byte("k0=fresh\n"),
	}
	if resp := install(t, follower, fresh); !resp.Success {
		t.Fatalf("a newer snapshot was refused: %+v", resp)
	}
	if v, _ := follower.fsm.get("k0"); v != "fresh" {
		t.Fatalf("newer snapshot not restored: k0=%q", v)
	}
}

// The commit-point guard, on its own: a node that has never snapshotted
// (index 0) still refuses a snapshot below what it has already committed —
// adopting it would drop committed entries and restore an older FSM state.
func TestInstallSnapshotRefusesBelowCommitPoint(t *testing.T) {
	c := startCluster(t, 3)
	defer c.stopAll()
	leader := c.leader(5 * time.Second)
	for i := 0; i < 5; i++ {
		if _, err := c.apply(leader, fmt.Sprintf("set k%d v%d", i, i)); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	var follower *testNode
	for _, id := range c.order {
		if id != leader {
			follower = c.nodes[id]
			break
		}
	}
	waitLastIndex(t, follower, 7*time.Second)
	if si, _, _ := follower.node.log.Snapshot(); si != 0 {
		t.Fatalf("setup: expected no snapshot, got index %d", si)
	}
	stale := installSnapshot{
		Term:      follower.node.Term(),
		LeaderID:  leader,
		LastIndex: 1,
		LastTerm:  1,
		Voters:    []Voter{{ID: "node-1", Addr: "127.0.0.1:1"}, {ID: "node-9", Addr: "127.0.0.1:9"}},
		Data:      []byte("k0=stale\n"),
	}
	if resp := install(t, follower, stale); resp.Success {
		t.Fatalf("a snapshot below the commit point (%d) was accepted", statIndex(follower, "commit_index"))
	}
	if v, _ := follower.fsm.get("k0"); v == "stale" {
		t.Fatal("FSM state rolled back by a refused snapshot")
	}
}

func install(t *testing.T, to *testNode, msg installSnapshot) installSnapshotResp {
	t.Helper()
	typ, payload, err := to.node.handleRPC(msgInstallSnapshot, encodeInstallSnapshot(msg))
	if err != nil {
		t.Fatalf("install snapshot rpc: %v", err)
	}
	if typ != msgInstallSnapshotResp {
		t.Fatalf("install snapshot: response type %d", typ)
	}
	resp, err := decodeInstallSnapshotResp(payload)
	if err != nil {
		t.Fatalf("decode install response: %v", err)
	}
	return resp
}

func waitLastIndex(t *testing.T, n *testNode, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		// Stats reads the published view (no lock on the loop's fields).
		if last, commit := statIndex(n, "last_log_index"), statIndex(n, "commit_index"); last >= 7 && commit >= 7 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("follower never caught up: last index %d, commit %d",
		statIndex(n, "last_log_index"), statIndex(n, "commit_index"))
}

func statIndex(n *testNode, key string) uint64 {
	switch v := n.node.Stats()[key].(type) {
	case uint64:
		return v
	case int:
		return uint64(v)
	}
	return 0
}
