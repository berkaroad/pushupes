package raft

import (
	"errors"
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

// Config configures one node. Zero-valued timing knobs fall back to defaults.
type Config struct {
	NodeID  string
	DataDir string
	Voters  []Voter

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
	if !found {
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
		c.HeartbeatTimeout = 100 * time.Millisecond
	}
	if c.ElectionTimeout <= 0 {
		c.ElectionTimeout = 500 * time.Millisecond
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
)

func quorum(n int) int { return n/2 + 1 }
