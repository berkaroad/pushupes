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
	"errors"
	"fmt"
	"time"
)

// State is the node role.
type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
	Shutdown  State = "shutdown"
)

// Entry kinds carried inside a log entry.
//
// KindConf is a membership change (add_voter / remove_voter): it is the only
// kind that moves the voter set, is applied to the local configuration when it
// becomes committed, and — unlike a command — is never handed to the FSM. Its
// payload is a confChange.
const (
	KindCommand uint8 = 0
	KindNoop    uint8 = 1
	KindConf    uint8 = 2
)

// Voter is a cluster member: a stable id plus the peer-plane address it is
// reachable at (host:port).
type Voter struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// Entry is one replicated log entry.
type Entry struct {
	Index uint64
	Term  uint64
	Kind  uint8
	Data  []byte
}

// FSM is the replicated state machine. Apply must be deterministic and is
// called serially in index order.
type FSM interface {
	Apply(e Entry) any
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// Logger is the minimal logging surface the consensus layer needs.
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Debugf(string, ...any) {}
func (nopLogger) Infof(string, ...any)  {}
func (nopLogger) Warnf(string, ...any)  {}
func (nopLogger) Errorf(string, ...any) {}

// The consensus timings every node starts with. ElectionTimeout is how long a
// follower waits for a heartbeat before standing for election, so it has to stay
// several multiples of the heartbeat: a late heartbeat — a busy apply batch, a
// CPU-throttled container, a stalled event loop — must not look like a dead
// leader. Hosts where that happens raise both (-raft-election-timeout /
// -raft-heartbeat-timeout, PUSHUPES_RAFT_* on the node, RAFT_* in the cluster
// start script) instead of living with the churn.
const (
	DefaultHeartbeatTimeout = 100 * time.Millisecond
	DefaultElectionTimeout  = 500 * time.Millisecond
	// minElectionPerHeartbeat is the smallest ratio the two knobs may have:
	// below it one late heartbeat starts an election, which is the very churn
	// the caller raising these values is trying to stop.
	minElectionPerHeartbeat = 2
)

// Config configures one node. Zero-valued timing knobs fall back to defaults.
type Config struct {
	NodeID  string
	DataDir string
	// Voters is the configured membership — normally the -peers set of every
	// member. It is used as-is on a first start (written to the WAL as the
	// initial configuration). A node that already has a recorded
	// configuration keeps it: Voters then only has to contain this node, and
	// is remembered as the bootstrap seed so a grown cluster whose -peers
	// files are still the original three can still start (see Members).
	Voters []Voter
	// Seed marks Voters as a bootstrap seed list rather than an
	// authoritative membership. It is what a runtime-added member starts
	// with: such a node is not in the voters its -peers names, so it joins
	// through the admin endpoint and learns the real membership from the
	// leader.
	Seed bool

	ApplyTimeout      time.Duration
	SnapshotThreshold uint64
	SnapshotInterval  time.Duration
	TrailingLogs      uint64

	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
	FlushInterval    time.Duration
	SegmentBytes     int64

	Logger Logger
}

func (c *Config) withDefaults() error {
	if c.NodeID == "" {
		return errors.New("raft: node id is required")
	}
	if c.DataDir == "" {
		return errors.New("raft: data dir is required")
	}
	if len(c.Voters) == 0 {
		return errors.New("raft: at least one voter is required")
	}
	found := false
	for _, v := range c.Voters {
		if v.ID == "" || v.Addr == "" {
			return errors.New("raft: every voter needs an id and an address")
		}
		if v.ID == c.NodeID {
			found = true
		}
	}
	if !found && !c.Seed {
		return errors.New("raft: this node is not in the voter set")
	}
	if c.ApplyTimeout <= 0 {
		c.ApplyTimeout = 10 * time.Second
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = 1024
	}
	if c.SnapshotInterval <= 0 {
		c.SnapshotInterval = 30 * time.Second
	}
	if c.HeartbeatTimeout <= 0 {
		c.HeartbeatTimeout = DefaultHeartbeatTimeout
	}
	if c.ElectionTimeout <= 0 {
		c.ElectionTimeout = DefaultElectionTimeout
	}
	if c.ElectionTimeout < minElectionPerHeartbeat*c.HeartbeatTimeout {
		return fmt.Errorf("raft: election timeout %s is less than %dx the heartbeat timeout %s: one late heartbeat would start an election",
			c.ElectionTimeout, minElectionPerHeartbeat, c.HeartbeatTimeout)
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 200 * time.Microsecond
	}
	if c.SegmentBytes <= 0 {
		c.SegmentBytes = 64 << 20
	}
	if c.Logger == nil {
		c.Logger = nopLogger{}
	}
	return nil
}

// voterAddr resolves a member id to its peer address: the live membership
// first (runtime-added members are in it but not in the configured seed), then
// the configured list a node that has not learned the membership yet can still
// index.
func (c *Config) voterAddr(id string) string {
	for _, v := range c.Voters {
		if v.ID == id {
			return v.Addr
		}
	}
	return ""
}

// Errors returned by Node.
var (
	ErrNotLeader = errors.New("raft: not the leader")
	ErrClosed    = errors.New("raft: node is shut down")
	ErrTimeout   = errors.New("raft: timed out waiting for the leader")
	// ErrBadChange marks a membership change the caller has to fix — a missing
	// id or address, an address that already belongs to another member, or
	// removing the last voter. Callers map it onto their own bad-request
	// class instead of reporting it as a server failure.
	ErrBadChange = errors.New("raft: invalid membership change")
)

func quorum(n int) int { return n/2 + 1 }
