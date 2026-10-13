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
	"fmt"
	"strconv"
	"testing"
)

// commitIndexOf reads the Raft commit index a node reports in its stats
// (the very number the console shows as "commit" in the cluster overview).
func commitIndexOf(t *testing.T, n consensus) uint64 {
	t.Helper()
	raw, ok := n.Stats()["commit_index"]
	if !ok {
		t.Fatal("stats carry no commit_index")
	}
	v, err := strconv.ParseUint(fmt.Sprint(raw), 10, 64)
	if err != nil {
		t.Fatalf("commit_index %v: %v", raw, err)
	}
	return v
}

// newLeaderEngine stands up a real (in-process, single-voter) Raft leader
// with an Engine as its state machine, so registration commits travel the
// genuine submit -> FSM -> table path.
func newLeaderEngine(t *testing.T) *Engine {
	t.Helper()
	e, _ := newTestEngine(t, "node-1")
	e.node = newTestRaftNode(t, e)
	return e
}

// TestRegistrationUnchangedDoesNotCommit pins the idle-cluster invariant:
// the announcer re-sends the SAME addresses every 10s as a healing
// heartbeat, and that must not advance the Raft commit index — otherwise
// an untouched cluster shows a steadily creeping "commit" number.
func TestRegistrationUnchangedDoesNotCommit(t *testing.T) {
	e := newLeaderEngine(t)
	join(t, e, "node-1", "127.0.0.1:9491")
	join(t, e, "node-2", "127.0.0.1:9492")

	reg := &Registration{ID: "node-2", AdminAddr: "127.0.0.1:9492", ClientAddr: "127.0.0.1:9492", Role: RoleLearner}
	before := commitIndexOf(t, e.node)

	// Three heartbeat rounds' worth of the exact same announcement.
	for i := 0; i < 3; i++ {
		if err := e.acceptRegistration(reg); err != nil {
			t.Fatalf("idempotent re-registration %d: %v", i, err)
		}
	}
	if after := commitIndexOf(t, e.node); after != before {
		t.Fatalf("unchanged registration heartbeat advanced commit index: %d -> %d", before, after)
	}
}

// TestRegistrationChangeStillCommits keeps the fix conservative: a node
// that comes back with different ports (or a brand-new peer) must still be
// recorded through Raft.
func TestRegistrationChangeStillCommits(t *testing.T) {
	e := newLeaderEngine(t)
	join(t, e, "node-1", "127.0.0.1:9491")
	join(t, e, "node-2", "127.0.0.1:9492")

	before := commitIndexOf(t, e.node)
	if err := e.acceptRegistration(&Registration{
		ID: "node-2", AdminAddr: "127.0.0.1:9592", ClientAddr: "127.0.0.1:9792", Role: RoleLearner,
	}); err != nil {
		t.Fatalf("changed registration: %v", err)
	}
	if after := commitIndexOf(t, e.node); after <= before {
		t.Fatalf("a real address change must commit: %d -> %d", before, after)
	}
	p := e.TableSnapshot().Peers["node-2"]
	if p.AdminAddr != "127.0.0.1:9592" || p.ClientAddr != "127.0.0.1:9792" {
		t.Fatalf("changed addresses not applied: %+v", p)
	}

	// ...and once adopted, repeating it is a no-op again.
	stable := commitIndexOf(t, e.node)
	if err := e.acceptRegistration(&Registration{
		ID: "node-2", AdminAddr: "127.0.0.1:9592", ClientAddr: "127.0.0.1:9792", Role: RoleLearner,
	}); err != nil {
		t.Fatalf("re-registration after change: %v", err)
	}
	if after := commitIndexOf(t, e.node); after != stable {
		t.Fatalf("post-change heartbeat advanced commit index: %d -> %d", stable, after)
	}
}

// TestPeerAddrForNodeUsesDirectory pins the relay lookup order: a member
// admitted at runtime carries its peer address in the replicated directory
// but NOT in the static -peers list, and a follower relaying a registration
// must resolve it from the directory (before this, such a relay silently
// returned "" and stranded registration heartbeats until a restart).
func TestPeerAddrForNodeUsesDirectory(t *testing.T) {
	e := newLeaderEngine(t)
	join(t, e, "node-9", "127.0.0.1:9499")
	if got := e.peerAddrForNode("node-9"); got != "127.0.0.1:9499" {
		t.Fatalf("directory-only member resolved to %q, want its announced peer addr", got)
	}
	// Static fallback still answers for a configured peer the directory has
	// not seeded yet (a bootstrapped node that never registered).
	nd := e.node.(*Node)
	nd.cfg.Peers = append(nd.cfg.Peers, Peer{Role: RoleLearner, ID: "node-8", PeerAddr: "127.0.0.1:9498"})
	if got := e.peerAddrForNode("node-8"); got != "127.0.0.1:9498" {
		t.Fatalf("static-list-only peer resolved to %q, want the configured addr", got)
	}
	if got := e.peerAddrForNode("node-7"); got != "" {
		t.Fatalf("unknown peer resolved to %q, want no answer", got)
	}
}
