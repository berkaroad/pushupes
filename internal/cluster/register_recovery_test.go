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
	"net"
	"testing"

	"github.com/berkaroad/pushupes/internal/config"
)

// TestRegistrationMarksARecoveredPeerUp pins the recovery fast path: the
// announcement a restarted node sends on its own startup (RegisterAnnouncer) is
// what brings it back into the layout. Without it the controller waits for its
// liveness probe, whose cached connection is still in the backoff the outage
// built up — measured on a real cluster: 45-60s after the node was serving again.
func TestRegistrationMarksARecoveredPeerUp(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	e.node = newTestRaftNode(t, e)
	addr := newPeerHarness(t, e) // a peer plane that answers, like a recovered node
	join(t, e, "node-2", addr)
	applyCmd(t, e, &Command{Op: OpPlanSlots})
	applyCmd(t, e, &Command{Op: OpMarkDown, NodeID: "node-2"})
	if !e.TableSnapshot().Peers["node-2"].Down {
		t.Fatal("setup: node-2 must start out marked down")
	}
	// A connection the controller dialed during the outage: it is what carries
	// the stale backoff, so it must be dropped on recovery.
	if _, err := e.peers.conn(addr); err != nil {
		t.Fatalf("setup: cache a peer conn: %v", err)
	}
	e.failMu.Lock()
	e.failStreak["node-2"] = config.LivenessFailThreshold + 2
	e.failMu.Unlock()

	reg := &Registration{ID: "node-2", AdminAddr: addr, ClientAddr: addr, Role: RoleLearner}
	if err := e.acceptRegistration(reg); err != nil {
		t.Fatalf("accept registration: %v", err)
	}

	if p := e.TableSnapshot().Peers["node-2"]; p.Down {
		t.Fatal("a registration from a marked-down peer that answers on its peer port must mark it up")
	}
	if e.failStreak["node-2"] != 0 {
		t.Fatalf("the announcement must clear accumulated probe strikes, got %d", e.failStreak["node-2"])
	}
	if _, cached := e.peers.conns[addr]; cached {
		t.Fatal("the cached connection must be dropped, or the next probe keeps reading the old backoff")
	}
}

// TestRegistrationKeepsAnUnreachablePeerDown: the announcement proves the node is
// running, not that we can reach it. Marking up a node we cannot dial would be
// undone three probes later, flapping leadership on every announcement — so the
// reachability dial stays the authority on "can this node serve".
func TestRegistrationKeepsAnUnreachablePeerDown(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	e.node = newTestRaftNode(t, e)
	join(t, e, "node-3", deadPeerAddr("node-3"))
	applyCmd(t, e, &Command{Op: OpPlanSlots})
	applyCmd(t, e, &Command{Op: OpMarkDown, NodeID: "node-3"})
	e.failMu.Lock()
	e.failStreak["node-3"] = config.LivenessFailThreshold + 2
	e.failMu.Unlock()

	reg := &Registration{ID: "node-3", AdminAddr: deadPeerAddr("node-3"), ClientAddr: deadPeerAddr("node-3"), Role: RoleLearner}
	if err := e.acceptRegistration(reg); err != nil {
		t.Fatalf("accept registration: %v", err)
	}
	if !e.TableSnapshot().Peers["node-3"].Down {
		t.Fatal("a peer we cannot reach must stay down, whatever it announces")
	}
	if e.failStreak["node-3"] != 0 {
		t.Fatalf("the announcement is still evidence the process ran: strikes should reset, got %d", e.failStreak["node-3"])
	}
}

// TestLivenessSweepDropsAStaleConnection: recovery that sends no announcement
// (a partition heal, a paused process) still has to be seen by the probe. When
// the peer port answers while the RPC fails, the stale half is our cached
// connection, so the sweep drops it and the next round dials fresh.
func TestLivenessSweepDropsAStaleConnection(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	e.node = newTestRaftNode(t, e)
	// A listener that accepts and then does nothing: the port is open (what a
	// recovered node's port looks like) but no PeerService answers behind it.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	addr := ln.Addr().String()
	join(t, e, "node-2", addr)
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	if _, err := e.peers.conn(addr); err != nil {
		t.Fatalf("setup: cache a peer conn: %v", err)
	}
	if _, cached := e.peers.conns[addr]; !cached {
		t.Fatal("setup: the connection must be cached before the sweep")
	}
	// Out of the start-up grace window: this is the running-cluster path.
	e.SetStartupGrace(0)

	e.livenessSweep(e.TableSnapshot())

	if _, cached := e.peers.conns[addr]; cached {
		t.Fatal("a listening port with a failing RPC means the cached connection is stale: the sweep must drop it")
	}
	if e.TableSnapshot().Peers["node-2"].Down {
		t.Fatal("one failed probe is not a verdict: the peer must not be down yet")
	}
}
