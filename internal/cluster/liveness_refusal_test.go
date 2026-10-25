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
)

// TestRefusedPortNeedsTheConfirmWindow: a refused peer port is the strongest
// liveness signal there is, but a process that just restarted refuses too while
// it binds its port — so one refused probe must not be a verdict. The node is
// judged only once the port has refused for the whole window, and from then on
// the clock (not the strike count) keeps the pace.
func TestRefusedPortNeedsTheConfirmWindow(t *testing.T) {
	e := startupGraceEngine(t) // node-2/node-3 dead, slots planned
	e.SetStartupGrace(0)       // running-cluster path, no start-up window

	e.livenessSweep(e.TableSnapshot())
	if e.TableSnapshot().Peers["node-2"].Down {
		t.Fatal("a peer whose port refuses once may be a node that is still binding it: no verdict on the first probe")
	}
	if _, seen := e.refusedSince["node-2"]; !seen {
		t.Fatal("the refusal clock must start on the first refused probe")
	}

	e.livenessSweep(e.TableSnapshot())
	if e.TableSnapshot().Peers["node-2"].Down {
		t.Fatal("two probes are still inside the confirmation window")
	}

	// The port has now refused for longer than the window: the process is gone.
	e.failMu.Lock()
	e.refusedSince["node-2"] = time.Now().Add(-refusedConfirmWindow - time.Second)
	e.failMu.Unlock()
	e.livenessSweep(e.TableSnapshot())
	if !e.TableSnapshot().Peers["node-2"].Down {
		t.Fatal("a port that has refused for the whole window must be judged dead")
	}
}

// TestRefusalClockClearsWhenThePeerAnswers: the clock is about continuous
// refusal. A peer that answers (or whose port accepts again) starts over, so a
// node that flaps back inside the window is never judged.
func TestRefusalClockClearsWhenThePeerAnswers(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	e.node = newTestRaftNode(t, e)
	addr := newPeerHarness(t, e) // something answers there
	join(t, e, "node-2", addr)
	applyCmd(t, e, &Command{Op: OpPlanSlots})
	e.SetStartupGrace(0)
	e.failMu.Lock()
	e.refusedSince["node-2"] = time.Now().Add(-refusedConfirmWindow - time.Second)
	e.failMu.Unlock()

	e.livenessSweep(e.TableSnapshot())

	if _, seen := e.refusedSince["node-2"]; seen {
		t.Fatal("an answered probe must clear the refusal clock")
	}
	if e.TableSnapshot().Peers["node-2"].Down {
		t.Fatal("a peer that answers must not be marked down")
	}
}

// TestBlackholedPeerStillRunsOnStrikes: a port that does not refuse either (a
// firewall dropping SYN, a hung host) is not the "nothing is listening" signal —
// it keeps the strike threshold instead of a wall-clock verdict, so a slow node
// is not declared dead by one dial timeout.
func TestBlackholedPeerStillRunsOnStrikes(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	e.node = newTestRaftNode(t, e)
	join(t, e, "node-2", "192.0.2.1:9391") // TEST-NET-1: connects hang, nothing answers
	applyCmd(t, e, &Command{Op: OpPlanSlots})
	e.SetStartupGrace(0)

	e.livenessSweep(e.TableSnapshot())

	if e.TableSnapshot().Peers["node-2"].Down {
		t.Fatal("one failed probe is not a verdict for a peer whose port neither answers nor refuses")
	}
	if _, seen := e.refusedSince["node-2"]; seen {
		t.Fatal("a black-holed port is not a refusal: the refusal clock must stay empty")
	}
}
