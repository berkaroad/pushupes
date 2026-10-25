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
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// deadPeerAddr is a loopback address nothing listens on: a probe there fails
// (and the connect is refused), which is exactly what the sweep sees for a data
// node that has not bound its port yet.
func deadPeerAddr(id string) string { return "127.0.0.1:1" }

// startupGraceEngine builds a control node engine whose replicated directory
// already names two learner peers, with nothing listening on either peer
// address — the state a control node wakes up in the middle of a whole-cluster
// restart, before the data nodes have bound their ports.
func startupGraceEngine(t *testing.T) *Engine {
	t.Helper()
	e, _ := newTestEngine(t, "node-1")
	e.node = newTestRaftNode(t, e)
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		join(t, e, id, deadPeerAddr(id))
	}
	applyCmd(t, e, &Command{Op: OpPlanSlots})
	return e
}

// TestStartupGraceHoldsVerdictsForDataNodesThatHaveNotStarted is the fix for
// the whole-cluster restart: while the grace window is open, a registered data
// node that has not answered a probe gets NO verdict. Rendering one is what
// marked every lagging data node down within seconds and piled all 1024 slots
// onto the first node that answered (measured: 3.1s after a staged restart,
// then ~34 minutes of serial hand-backs).
//
// Rolling the guard back turns this red: tests 2 and 3 run the same sweep with
// the grace inert and mark the very same peers down.
func TestStartupGraceHoldsVerdictsForDataNodesThatHaveNotStarted(t *testing.T) {
	e := startupGraceEngine(t)
	if e.startupGrace != DefaultStartupGrace {
		t.Fatalf("default grace = %s, want %s", e.startupGrace, DefaultStartupGrace)
	}
	e.livenessSweep(e.TableSnapshot())
	tbl := e.TableSnapshot()
	for _, id := range []string{"node-2", "node-3"} {
		if tbl.Peers[id].Down {
			t.Fatalf("%s: a data node that has not started yet must not be marked down inside the grace window", id)
		}
		if e.failStreak[id] != 0 {
			t.Fatalf("%s: the grace must not accumulate probe strikes (streak %d)", id, e.failStreak[id])
		}
	}
	// The rebalancer stays off the layout for the same window; a drain outranks it.
	if e.rebalanceMayRun(tbl, false) {
		t.Fatal("the rebalancer must not start hand-overs while the grace window waits for data nodes")
	}
	if !e.rebalanceMayRun(tbl, true) {
		t.Fatal("a drain is operator intent and must run inside the grace window")
	}
}

// TestStartupGraceExpiresIntoTheOrdinaryVerdict: the window is a bound, not a
// promise. A data node still silent when it runs out is judged exactly as before
// — which caps the failover delay at the grace.
func TestStartupGraceExpiresIntoTheOrdinaryVerdict(t *testing.T) {
	e := startupGraceEngine(t)
	e.startedAt = time.Now().Add(-DefaultStartupGrace - time.Second)
	e.livenessSweep(e.TableSnapshot())
	tbl := e.TableSnapshot()
	for _, id := range []string{"node-2", "node-3"} {
		if !tbl.Peers[id].Down {
			t.Fatalf("%s: once the grace is over the ordinary verdict must render", id)
		}
	}
	if !e.rebalanceMayRun(tbl, false) {
		t.Fatal("the rebalancer must run once the grace is over")
	}
}

// TestStartupGraceOffRendersTheVerdictAtOnce: -startup-grace 0 is the previous
// behaviour, verdict from the first round.
func TestStartupGraceOffRendersTheVerdictAtOnce(t *testing.T) {
	e := startupGraceEngine(t)
	e.SetStartupGrace(0)
	e.livenessSweep(e.TableSnapshot())
	if !e.TableSnapshot().Peers["node-2"].Down {
		t.Fatal("with the grace off a silent peer must be marked down on the first sweep")
	}
}

// TestStartupGraceCoversTheDataPlaneOnly: a control-plane (voter) peer hosts no
// slots, so its absence never moves leadership — no reason to hold its verdict.
func TestStartupGraceCoversTheDataPlaneOnly(t *testing.T) {
	e := startupGraceEngine(t)
	applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{
		ID: "node-9", Role: RoleVoter, PeerAddr: deadPeerAddr("node-9"), ClientAddr: deadPeerAddr("node-9"),
	}})
	e.livenessSweep(e.TableSnapshot())
	tbl := e.TableSnapshot()
	if !tbl.Peers["node-9"].Down {
		t.Fatal("a voter is outside the grace: its verdict renders at once")
	}
	if tbl.Peers["node-2"].Down {
		t.Fatal("data nodes keep their verdict held in the same sweep")
	}
}

// TestStartupGraceRecordsAProbeAnswer: the window closes per node as soon as the
// node answers, so a node that WAS up and then stopped is judged immediately.
func TestStartupGraceRecordsAProbeAnswer(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	answered := newPeerHarness(t, e) // a real PeerService that answers Ping
	join(t, e, "node-2", answered)
	join(t, e, "node-3", deadPeerAddr("node-3"))
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	e.livenessSweep(e.TableSnapshot())
	if !e.startupSeen["node-2"] {
		t.Fatal("an answered probe must be recorded: it is what ends the wait for that node")
	}
	awaiting := e.startupAwaiting(e.TableSnapshot(), time.Now())
	if awaiting["node-2"] {
		t.Fatal("a node that answered is no longer awaited")
	}
	if !awaiting["node-3"] {
		t.Fatal("a node that has not answered stays awaited")
	}
}

// TestStartupGraceDoesNotCoverANodeThatAnsweredAndThenStopped: the grace covers
// "never started", never "started and stopped".
func TestStartupGraceDoesNotCoverANodeThatAnsweredAndThenStopped(t *testing.T) {
	e := startupGraceEngine(t)
	e.startupSeen["node-2"] = true // what a successful probe leaves behind (see the test above)
	e.livenessSweep(e.TableSnapshot())
	tbl := e.TableSnapshot()
	if !tbl.Peers["node-2"].Down {
		t.Fatal("a node that answered and then stopped must be marked down at once")
	}
	if tbl.Peers["node-3"].Down {
		t.Fatal("a node that never answered keeps its verdict held")
	}
}

// TestStartupGraceReportsItsEndOnce: the window's end is logged once, and the
// two reasons are distinguishable — every node answered, or the grace ran out
// with some still silent.
func TestStartupGraceReportsItsEndOnce(t *testing.T) {
	newLogged := func(t *testing.T) (*Engine, *bytes.Buffer) {
		t.Helper()
		e, _ := newTestEngine(t, "node-1")
		var buf bytes.Buffer
		e.logger = slog.New(slog.NewJSONHandler(&buf, nil))
		e.node = newTestRaftNode(t, e)
		for _, id := range []string{"node-1", "node-2", "node-3"} {
			join(t, e, id, deadPeerAddr(id))
		}
		applyCmd(t, e, &Command{Op: OpPlanSlots})
		return e, &buf
	}

	t.Run("every node answered", func(t *testing.T) {
		e, buf := newLogged(t)
		e.livenessSweep(e.TableSnapshot()) // the window holds node-2 and node-3
		e.startupSeen["node-2"], e.startupSeen["node-3"] = true, true
		e.livenessSweep(e.TableSnapshot())
		if n := strings.Count(buf.String(), "every registered data node has answered"); n != 1 {
			t.Fatalf("want exactly one close line, got %d:\n%s", n, buf.String())
		}
	})

	t.Run("grace ran out", func(t *testing.T) {
		e, buf := newLogged(t)
		e.livenessSweep(e.TableSnapshot())
		e.startedAt = time.Now().Add(-DefaultStartupGrace - time.Second)
		e.livenessSweep(e.TableSnapshot())
		if msg := buf.String(); !strings.Contains(msg, "is over with 2 data node(s) still not answering") {
			t.Fatalf("the expiry must be reported with the nodes still silent:\n%s", msg)
		}
	})
}

// TestStartupGracePoolIsCapturedOnce: the window covers the data nodes the
// directory knew at start-up. A node that joins later is a runtime change and
// is judged like any other — the reason the whole feature is scoped to the
// control node's first start.
func TestStartupGracePoolIsCapturedOnce(t *testing.T) {
	e := startupGraceEngine(t)
	e.livenessSweep(e.TableSnapshot()) // captures the pool: node-2, node-3
	applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{
		ID: "node-4", Role: RoleLearner, PeerAddr: deadPeerAddr("node-4"), ClientAddr: deadPeerAddr("node-4"),
	}})
	tbl := e.TableSnapshot()
	if e.startupAwaiting(tbl, time.Now())["node-4"] {
		t.Fatal("a node that joined after start-up is not part of the grace window")
	}
	e.livenessSweep(tbl)
	if !e.TableSnapshot().Peers["node-4"].Down {
		t.Fatal("a node that joined after start-up is judged at once")
	}
}
