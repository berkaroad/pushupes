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

// TestSnapshotUnderLowThresholdKeepsProgress is the regression for the bug
// where a snapshot trimmed the log from the wrong end: the baseline advanced
// but the in-memory entries below it stayed, so the applied cursor ran past the
// log and every later Apply waited out its timeout. A tight threshold keeps a
// snapshot firing in the middle of a continuous write stream.
func TestSnapshotUnderLowThresholdKeepsProgress(t *testing.T) {
	c := startClusterCfg(t, 1, func(cfg *Config) {
		cfg.SnapshotThreshold = 4
		cfg.SnapshotInterval = 20 * time.Millisecond
	})
	defer c.stopAll()
	id := c.leader(3 * time.Second)
	for i := 0; i < 80; i++ {
		if _, err := c.apply(id, fmt.Sprintf("set k%d v%d", i, i)); err != nil {
			snapIdx, _, _ := c.nodes[id].node.log.Snapshot()
			v := c.nodes[id].node.readView()
			t.Fatalf("apply %d stalled: %v (state=%s commit=%d applied=%d last=%d snap=%d first=%d)",
				i, err, v.state, v.commit, v.applied, v.lastIndex, snapIdx, c.nodes[id].node.log.FirstIndex())
		}
	}
	snapIdx, _, _ := c.nodes[id].node.log.Snapshot()
	if snapIdx == 0 {
		// The snapshot file lands from the snapshot goroutine through the
		// event queue; the write path answers on the append, so poll for it
		// instead of assuming the fsync waits had serialised it already.
		deadline := time.Now().Add(5 * time.Second)
		for snapIdx == 0 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
			snapIdx, _, _ = c.nodes[id].node.log.Snapshot()
		}
	}
	if snapIdx == 0 {
		t.Fatalf("no snapshot was taken at threshold 4")
	}
	if v, ok := c.nodes[id].fsm.get("k79"); !ok || v != "v79" {
		t.Fatalf("last command missing from the fsm: %q %v", v, ok)
	}
}

// TestAckWaitsLeaderFlushAndRidesQuorumWrites pins both halves of the raft
// ack promise. The LEADER answer must not return before its own fsync round
// covers the record — the parked answer kicks that round, so the wait is fsync
// time, not the window. The FOLLOWER side stays answer-on-write: the follower
// that acked must still be holding the record unflushed at the moment the
// client's ack comes back (the one-second window keeps its own fsync outside
// the measurement). Together that is the promise: a whole-cluster power loss
// cannot take back an acknowledged command, and no follower disk gates the
// round trip.
func TestAckWaitsLeaderFlushAndRidesQuorumWrites(t *testing.T) {
	c := startClusterCfg(t, 3, func(cfg *Config) {
		// The one-second window must stay far below the election floor: the
		// vote-persist path still waits for durability, and a grant that
		// stalls the loop past an election timeout unseats the cluster.
		cfg.FlushInterval = time.Second
		cfg.HeartbeatTimeout = 300 * time.Millisecond
		cfg.ElectionTimeout = 2 * time.Second
	})
	defer c.stopAll()
	id := c.leader(20 * time.Second)

	// Drain every group-commit buffer first and hold a beat longer than one
	// window, so establishment records (bootstraps, no-ops) can no more
	// interfere with the measurement than cross it.
	for _, nid := range c.order {
		if err := c.nodes[nid].node.log.wal.Flush(); err != nil {
			t.Fatalf("drain %s: %v", nid, err)
		}
	}
	time.Sleep(1200 * time.Millisecond)

	flushes := map[string]uint64{}
	bufs := map[string]int{}
	durables := map[string]uint64{}
	for _, nid := range c.order {
		w := c.nodes[nid].node.log.wal
		flushes[nid], bufs[nid], durables[nid] = w.flushCount(), w.buffered(), w.durableNow()
	}

	if _, err := c.apply(id, "set k1 v1"); err != nil {
		t.Fatalf("apply: %v", err)
	}

	lw := c.nodes[id].node.log.wal
	if got := lw.flushCount(); got <= flushes[id] {
		t.Fatalf("leader acknowledged with no fsync round run (%d -> %d): the ack does not wait on local durability", flushes[id], got)
	}
	if got := lw.durableNow(); got <= durables[id] {
		t.Fatalf("leader durable watermark unmoved at ack (%d -> %d)", durables[id], got)
	}
	held := false
	for _, nid := range c.order {
		if nid == id {
			continue
		}
		w := c.nodes[nid].node.log.wal
		// A follower's reply is causally after its append but never after its
		// fsync; the ack returning inside the one-second window with the
		// record still in its buffer proves the follower answered on the write.
		if w.buffered() >= bufs[nid]+1 && w.flushCount() == flushes[nid] {
			held = true
			break
		}
	}
	if !held {
		t.Fatal("no follower was caught holding the acked record unflushed — either the quorum answer did not ride the write, or a window crossed the measurement")
	}
	if v, ok := c.nodes[id].fsm.get("k1"); !ok || v != "v1" {
		t.Fatalf("acked command not applied on the leader: %q %v", v, ok)
	}

	// The command then reaches every follower's state machine (commit rides
	// the next heartbeat's leaderCommit) — past the flush window on purpose;
	// follower fsync is background cadence, never an ack gate.
	deadline := time.Now().Add(5 * time.Second)
	for {
		n := 0
		for _, nid := range c.order {
			if nid == id {
				continue
			}
			if v, ok := c.nodes[nid].fsm.get("k1"); ok && v == "v1" {
				n++
			}
		}
		if n == 2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("acked command missing from the followers' fsm: %d/2 applied", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
