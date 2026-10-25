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
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/berkaroad/pushupes/internal/storage"
)

// discardLogger is what NewNode expects: production always passes one (it logs
// its startup timings through it), so tests that go through the public
// constructor have to as well.
func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// The data-plane feed end to end: a REAL Raft group's committed metadata
// reaching a REAL data-plane node over the real gRPC plane.
//
// The unit tests around this cover each half against fakes (layoutfeed_test.go
// drives the hub with a fixed slice, learnernode_test.go drives the client with
// synthetic frames). What they cannot cover is the seam between them: the
// resume point surviving the wire, the leader's LayoutSlice answering a real
// log, and the learner's table converging on the leader's — the property the
// whole role split rests on. Run that here, on the same in-process harness the
// consensus tests use (real TCP transport, real election).
func TestDataPlaneNodeFollowsTheControlPlane(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	ctrlStore, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatalf("open control store: %v", err)
	}
	defer ctrlStore.Close()
	ctrl := NewEngine(nil, ctrlStore, "node-1", nil)

	c := newTestRaftCluster(t, []string{"node-1"}, map[string]Applier{"node-1": ctrl})
	leader := c.waitLeader(15 * time.Second)
	ctrlNode := c.nodes[leader]
	ctrl.SetNode(ctrlNode)
	ctrl.Start(ctx)
	ctrlPeer := ctrlNode.Cfg().PeerAddr
	// The production shape is ONE peer port carrying both consensus frames and
	// the gRPC peer plane (peer_mux demuxes by first byte), and the data-plane
	// node's registration goes through that gRPC plane.
	ctrlSrv := ctrl.ServePeer(c.muxes[leader].GRPCListener())
	defer ctrlSrv.Stop()

	// The data-plane node: a real process shape, minus the Raft group. Its own
	// peer plane serves PeerService (peer_mux demuxes by first byte), so the
	// control plane can probe it exactly as it probes a voter.
	learnerDir := t.TempDir()
	lStore, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatalf("open learner store: %v", err)
	}
	defer lStore.Close()
	lEng := NewEngine(nil, lStore, "node-2", nil)

	lPeer := freeLoopbackAddr(t)
	lCfg := Config{
		NodeID:     "node-2",
		PeerAddr:   lPeer,
		AdminAddr:  "http://" + lPeer,
		ClientAddr: "http://" + lPeer,
		DataDir:    learnerDir,
		Role:       RoleLearner,
		Peers:      []Peer{{ID: "node-1", PeerAddr: ctrlPeer}},
	}
	lNode, lLis, err := NewNode(lCfg, lEng, discardLogger())
	if err != nil {
		t.Fatalf("data-plane node: %v", err)
	}
	defer lNode.Close()
	lEng.SetNode(lNode)
	peerSrv := lEng.ServePeer(lLis)
	defer peerSrv.Stop()
	go lEng.NewRegisterAnnouncer().Run(ctx)

	// Admission is the operator's directory command; the node's own
	// announcement of its admin/client addresses follows (the layout pool only
	// takes members clients can route to).
	if err := ctrl.AdmitSelf(ctx, "node-2", lPeer, RoleLearner); err != nil {
		t.Fatalf("admit data-plane node: %v", err)
	}
	waitFor(t, 20*time.Second, func() bool {
		p, ok := ctrl.TableSnapshot().Peers["node-2"]
		return ok && p.ClientAddr != "" && p.Role == RoleLearner
	}, "the data-plane node to announce itself")
	if ctrlNode.IsMember("node-2") {
		t.Fatal("a learner joined the Raft configuration; it must never vote")
	}

	// The plan lands (the pool settles one round first) and the feed has to
	// bring the learner the same metadata the controller wrote.
	waitFor(t, 30*time.Second, func() bool {
		return len(ctrl.TableSnapshot().Slots) == 8
	}, "the layout to be planned")
	waitFor(t, 30*time.Second, func() bool {
		return tablesMatch(ctrl.TableSnapshot(), lEng.TableSnapshot())
	}, "the data-plane node to mirror the layout")

	// Incremental delivery, not just the initial catch-up: a later directory
	// change has to arrive without a restart.
	if err := ctrl.AdmitSelf(ctx, "node-3", "127.0.0.1:1", RoleLearner); err != nil {
		t.Fatalf("admit the second data-plane node: %v", err)
	}
	waitFor(t, 30*time.Second, func() bool {
		_, ok := lEng.TableSnapshot().Peers["node-3"]
		return ok
	}, "the second admission to reach the data-plane node")

	// The learner's view of the consensus it follows is filled from the feed.
	if got := lNode.LeaderID(); got != leader {
		t.Fatalf("learner's leader = %q, want %q", got, leader)
	}
	if got, want := lNode.TopologyID(), ctrlNode.TopologyID(); got != want {
		t.Fatalf("learner's generation = %q, want the control plane's %q", got, want)
	}
	if lNode.LeaderPeerAddr() == "" {
		t.Fatal("learner has no leader address to reconnect to")
	}

	// Restart: the resume point and the state it indexes are both on disk, so
	// the node comes back at the index it reached instead of from nothing.
	appliedBefore := statUint(t, lNode, "applied-index")
	lNode.Close()
	for _, f := range []string{layoutFeedFile, layoutStateFile} {
		if _, err := os.Stat(filepath.Join(learnerDir, f)); err != nil {
			t.Fatalf("%s missing after shutdown: %v", f, err)
		}
	}
	restarted, rLis, err := NewNode(lCfg, lEng, discardLogger())
	if err != nil {
		t.Fatalf("restart the data-plane node: %v", err)
	}
	defer restarted.Close()
	rSrv := lEng.ServePeer(rLis)
	defer rSrv.Stop()
	lEng.SetNode(restarted)

	waitFor(t, 30*time.Second, func() bool {
		return tablesMatch(ctrl.TableSnapshot(), lEng.TableSnapshot())
	}, "the restarted data-plane node to mirror the layout again")
	if got := statUint(t, restarted, "applied-index"); got < appliedBefore {
		t.Fatalf("applied-index went backwards across the restart: %d -> %d", appliedBefore, got)
	}
}

// tablesMatch reports whether the data-plane node's replicated metadata equals
// the control plane's — member for member and slot for slot. That equality is
// the claim of the whole design: the two nodes ran no shared code path, one
// took the commands from a log and the other from a socket.
func tablesMatch(a, b *Table) bool { return tableDigest(a) == tableDigest(b) }

// tableDigest renders the replicated state canonically (sorted, so map order
// never decides).
func tableDigest(t *Table) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "topology %s\n", t.Topology)
	ids := make([]string, 0, len(t.Peers))
	for id := range t.Peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		p := t.Peers[id]
		fmt.Fprintf(&sb, "peer %s role=%s addr=%s down=%v\n", id, p.Role, HostPort(p.PeerAddr), p.Down)
	}
	slots := make([]int, 0, len(t.Slots))
	for s := range t.Slots {
		slots = append(slots, int(s))
	}
	sort.Ints(slots)
	for _, s := range slots {
		p := t.Slots[int32(s)]
		reps := append([]string(nil), p.Replicas...)
		fmt.Fprintf(&sb, "slot %d leader=%s replicas=%v epoch=%d state=%s migrating=%q\n",
			s, p.Leader, reps, p.Epoch, p.State, p.MigratingTo)
	}
	fmt.Fprintf(&sb, "replicas=%d policy=%s plan=%v\n", t.Replicas, t.Policy, t.PlanMembers)
	return sb.String()
}

func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func statUint(t *testing.T, n consensus, key string) uint64 {
	t.Helper()
	switch v := n.Stats()[key].(type) {
	case uint64:
		return v
	case int:
		return uint64(v)
	default:
		t.Fatalf("Stats()[%q] = %v (%T), want a number", key, v, v)
		return 0
	}
}
