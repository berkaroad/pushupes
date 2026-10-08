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
	"testing"
	"time"
)

// A node that has not learned any configuration must not stand for election.
// Its voter set is empty until the leader's first entries or snapshot reach
// it; quorum(0) is 1, so it used to elect itself immediately — bumping the
// term (the real leader steps down) and writing a no-op at index 1 of its own
// log. A lower-term entry never overwrites an existing one, so the cluster's
// real index-1 configuration entry could then never reach it: it applied
// everything above that point with no configuration as the base, ended up
// with a partial voter set that did not contain itself, and refused to start
// on the next restart ("not in the recorded membership") — reproduced on a
// node whose data directory had been rebuilt while its id was still a member.
//
// This pins the isolated half of that: a seed node with no reachable peers
// stays put instead of campaigning, and writes nothing.
func TestSeedWithoutConfigurationDoesNotCampaign(t *testing.T) {
	tn := newTestNet(t)
	voters := []Voter{
		{ID: "node-1", Addr: "127.0.0.1:1"},
		{ID: "node-2", Addr: "127.0.0.1:2"},
		{ID: "node-3", Addr: "127.0.0.1:3"},
	}
	cfg := testConfig("node-4", t.TempDir(), voters, testLogger{t})
	// A runtime-joined node: it is not in the voters it was given (that list
	// is its seed), so it starts knowing no membership at all.
	cfg.Seed = true
	// Several election timeouts' worth of waiting, so the timings stay short —
	// but keep the pair valid (the config refuses an election timeout below
	// twice the heartbeat).
	cfg.HeartbeatTimeout = 30 * time.Millisecond
	cfg.ElectionTimeout = 60 * time.Millisecond
	n, err := NewNode(cfg, newTestFSM(), tn)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	t.Cleanup(func() { n.Close(); tn.ln.Close() })

	// Several election timeouts' worth of waiting: nothing may happen.
	time.Sleep(400 * time.Millisecond)

	if st := n.State(); st == Leader || st == Candidate {
		t.Fatalf("a node with no configuration became %s", st)
	}
	if last := n.log.LastIndex(); last != 0 {
		t.Fatalf("a node with no configuration wrote log entry %d (it elected itself and appended a no-op)", last)
	}
	if term := n.Term(); term > 1 {
		t.Fatalf("a node with no configuration inflated the term to %d", term)
	}
}

// The single-voter case must keep electing itself at once: quorum(1) is 1 and
// that is a legitimate cluster, not an unknown configuration.
func TestSingleVoterStillElectsItself(t *testing.T) {
	c := startCluster(t, 1)
	defer c.stopAll()
	if leader := c.leader(5 * time.Second); leader != "node-1" {
		t.Fatalf("single-voter cluster elected %q", leader)
	}
}
