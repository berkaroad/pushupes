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
	"path/filepath"
	"testing"
	"time"

	"pushupes/internal/raft"
)

// TestRestartRestoresSnapshotWithNonMemberPlacement is the end-to-end
// regression for the reported fatal:
//
//	level=fatal msg="pushupes exited with error" error="raft node: raft:
//	restore snapshot: table: node index 6 out of range"
//
// A real cluster's node restarts from its own snapshot directory and dies
// there. The state it died on is reproduced here: a table whose placements name
// a node the peer directory no longer holds (a member removed while a migration
// to it was in flight — the old code let that commit, re-seating the node).
// Such a table is injected directly because the membership guards now refuse to
// create it (TestPlacementMutationsRefuseNonMember): what the test pins is that
// a node holding it on disk can still START.
//
// It drives the production path end to end: raft.Node + the cluster FSM write a
// real snapshot file, the node is closed, and a fresh engine + raft node are
// opened on the same data directory. Before the fix the second NewNode failed
// with the error above.
func TestRestartRestoresSnapshotWithNonMemberPlacement(t *testing.T) {
	dir := t.TempDir()
	eng, _ := newTestEngine(t, "node-1")

	// A directory of five members plus a placement that names a sixth node
	// the directory does not hold (in the real breakage: node-6, the leader
	// of 238 slots).
	for _, id := range []string{"node-1", "node-2", "node-3", "node-4", "node-5"} {
		applyCmd(t, eng, &Command{Op: OpJoinNode, Peer: &Peer{ID: id, PeerAddr: "127.0.0.1:1", AdminAddr: "http://127.0.0.1:1", ClientAddr: "http://127.0.0.1:1"}})
	}
	applyCmd(t, eng, &Command{Op: OpConfig, Replicas: 3})
	applyCmd(t, eng, &Command{Op: OpPlanSlots})
	eng.tableMu.Lock()
	eng.table.Slots[19] = &Placement{Leader: "node-6", Replicas: []string{"node-5", "node-1", "node-2", "node-6"}, Epoch: 3, State: SlotStable}
	eng.table.Slots[24] = &Placement{Leader: "node-4", Replicas: []string{"node-4", "node-6"}, Epoch: 2, State: SlotStable}
	eng.tableMu.Unlock()
	want := eng.TableSnapshot()

	n1 := startRaftOn(t, dir, eng, 1, 5*time.Second)
	// Drive entries so the node snapshots (threshold 1) and commits.
	for i := 0; i < 4; i++ {
		if _, err := n1.raft.Apply((&Command{Op: OpMarkUp, NodeID: "node-1"}).Encode(), 5*time.Second); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	waitForSnapshot(t, dir, 10*time.Second)
	if err := n1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Restart: this is where the reported fatal happened.
	eng2, _ := newTestEngine(t, "node-1")
	n2 := startRaftOn(t, dir, eng2, 1, 5*time.Second)
	defer n2.Close()

	got := eng2.TableSnapshot()
	if len(got.Peers) != len(want.Peers) {
		t.Fatalf("restored directory has %d peers, want %d", len(got.Peers), len(want.Peers))
	}
	for s, wp := range want.Slots {
		gp, ok := got.Slots[s]
		if !ok {
			t.Fatalf("slot %d missing after restore", s)
		}
		if gp.Leader != wp.Leader || gp.Epoch != wp.Epoch || len(gp.Replicas) != len(wp.Replicas) {
			t.Fatalf("slot %d restored as %+v, want %+v", s, gp, wp)
		}
	}
}

// startRaftOn opens a raft node for one engine on dir, waiting until it is the
// leader so the caller can commit through it.
func startRaftOn(t *testing.T, dir string, eng *Engine, threshold uint64, within time.Duration) *Node {
	t.Helper()
	mux, err := newPeerMux("127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("peer mux: %v", err)
	}
	rn, err := raft.NewNode(raft.Config{
		NodeID:            "node-1",
		DataDir:           dir,
		Voters:            []raft.Voter{{ID: "node-1", Addr: mux.Addr()}},
		ApplyTimeout:      5 * time.Second,
		SnapshotThreshold: threshold,
		SnapshotInterval:  time.Hour, // the threshold drives the snapshot here
		HeartbeatTimeout:  40 * time.Millisecond,
		ElectionTimeout:   120 * time.Millisecond,
	}, &FSM{applier: eng}, mux)
	if err != nil {
		// The reported failure surfaces exactly here.
		t.Fatalf("raft node on %s: %v", dir, err)
	}
	n := &Node{cfg: Config{NodeID: "node-1", PeerAddr: mux.Addr(), Peers: nil}, raft: rn, fsm: &FSM{applier: eng}, mux: mux}
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if rn.IsLeader() {
			return n
		}
		time.Sleep(10 * time.Millisecond)
	}
	n.Close()
	t.Fatal("in-process raft node never became leader")
	return nil
}

func waitForSnapshot(t *testing.T, dir string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if m, _ := filepath.Glob(filepath.Join(dir, "snapshot", "*.snap")); len(m) > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no snapshot file was written")
}
