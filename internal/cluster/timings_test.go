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
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/berkaroad/pushupes/internal/raft"
)

// TestClusterConfigCarriesRaftTimings pins the cluster side of the two knobs:
// zero means "the consensus default" (so the cluster's Config and the raft
// package can never drift apart on what an unset value is) and an explicit
// value is passed through untouched.
func TestClusterConfigCarriesRaftTimings(t *testing.T) {
	c := Config{}
	c.withDefaults()
	if c.HeartbeatTimeout != raft.DefaultHeartbeatTimeout || c.ElectionTimeout != raft.DefaultElectionTimeout {
		t.Fatalf("unset timings became %s/%s, want the raft defaults %s/%s",
			c.HeartbeatTimeout, c.ElectionTimeout, raft.DefaultHeartbeatTimeout, raft.DefaultElectionTimeout)
	}

	c = Config{HeartbeatTimeout: 250 * time.Millisecond, ElectionTimeout: 3 * time.Second}
	c.withDefaults()
	if c.HeartbeatTimeout != 250*time.Millisecond || c.ElectionTimeout != 3*time.Second {
		t.Fatalf("explicit timings were rewritten: %s/%s", c.HeartbeatTimeout, c.ElectionTimeout)
	}
}

// TestClusterRejectsATightElectionTimeout drives the values through the whole
// wiring (cluster.Config -> raft.Config -> validation), which is the path a
// node takes at startup: a pair that would vote the leader out on one late
// heartbeat must stop the node instead of being accepted silently.
func TestClusterRejectsATightElectionTimeout(t *testing.T) {
	logger := slog.Default()
	build := func(heartbeat, election time.Duration) (*Node, error) {
		n, listener, err := NewNode(Config{
			NodeID:           "node-1",
			PeerAddr:         "127.0.0.1:0",
			DataDir:          t.TempDir(),
			Peers:            []Peer{{ID: "node-1", PeerAddr: "127.0.0.1:1"}},
			HeartbeatTimeout: heartbeat,
			ElectionTimeout:  election,
		}, nil, logger)
		if listener != nil {
			_ = listener.Close()
		}
		if n != nil {
			t.Cleanup(func() { _ = n.Close() })
		}
		return n, err
	}

	if _, err := build(time.Second, time.Second); err == nil {
		t.Fatal("election timeout == heartbeat timeout was accepted")
	} else if !strings.Contains(err.Error(), "election timeout") {
		t.Fatalf("refusal does not name the knob: %v", err)
	}

	// Twice the heartbeat is the floor and must build, and the node reports
	// back the timings it took (that is what /admin/cluster/status serves).
	n, err := build(time.Second, 2*time.Second)
	if err != nil {
		t.Fatalf("election timeout at twice the heartbeat refused: %v", err)
	}
	st := n.Stats()
	if st["heartbeat_timeout"] != "1s" || st["election_timeout"] != "2s" {
		t.Fatalf("status reports heartbeat=%v election=%v, want 1s/2s", st["heartbeat_timeout"], st["election_timeout"])
	}
}
