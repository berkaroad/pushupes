// Package cluster provides the pushupes cluster: a Raft control plane that
// replicates the slot assignment table, a leader-follower data plane that
// replicates slot WALs by seq-based fetch, and hot slot migration.
//
// The split: consensus carries only metadata; event data
// travels over a dedicated replication protocol and never through the Raft log.
//
// The consensus layer is the in-repo simplified Raft (internal/raft), whose log
// and hard state live in a custom segmented WAL.
package cluster

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"pushupes/internal/raft"
)

// ErrNotLeader is returned when a command is submitted on a follower.
var ErrNotLeader = errors.New("not the cluster leader")

// Consensus WAL defaults: the group-commit window and the segment size.
const (
	DefaultRaftFlushInterval = 200 * time.Microsecond
	DefaultRaftSegmentBytes  = 64 << 20
)

// ReplicaCountForMembers is the replica count per slot, derived from the Raft
// cluster's own fault tolerance: a group of N voters survives floor((N-1)/2)
// failures, and every slot keeps exactly one more copy than that — so the data
// outlives the same failures the consensus group outlives:
//
//	1 voter  -> 0 + 1 = 1 replica
//	3 voters -> 1 + 1 = 2 replicas
//	5 voters -> 2 + 1 = 3
//	7 voters -> 3 + 1 = 4
//
// It is NOT an operator knob any more (-replication-factor is gone): the
// layout follows the membership, so a cluster grown at runtime re-derives it
// on the next controller round. Even member counts land on the same count as
// the odd count below them (4 voters -> 2, 6 -> 3), which is the same
// floor((N-1)/2) arithmetic.
func ReplicaCountForMembers(members int) int {
	if members < 1 {
		return 1
	}
	return (members-1)/2 + 1
}

// Applier is implemented by the engine: it applies replicated slot-table
// commands and can snapshot/restore that state.
type Applier interface {
	// ApplyCommand applies one encoded command and returns a result payload.
	ApplyCommand(cmd []byte) ([]byte, error)
	// SnapshotState serialises the entire replicated state (the slot table).
	SnapshotState() ([]byte, error)
	// RestoreState replaces the replicated state from a snapshot.
	RestoreState(data []byte) error
}

// Peer is a cluster member. AdminAddr serves inter-node replication and
// admin traffic only; ClientAddr is the client-facing data plane (event
// writes + queries) and the target of MOVED/ASK redirects.
type Peer struct {
	ID         string `json:"id"`
	PeerAddr   string `json:"peer_addr"`
	AdminAddr  string `json:"admin_addr"`
	ClientAddr string `json:"client_addr"`
	// Down is the controller's liveness verdict, replicated through Raft:
	// the peer failed its consecutive peer-plane probes. The directory entry
	// KEEPS its announced addresses (unlike leave_node, which deletes it),
	// so a failed node stays visible — flagged 离线 in the console — instead
	// of flickering between "removed" and "re-seeded without addresses".
	Down bool `json:"down,omitempty"`
}

// Offline is the cluster-wide "clients cannot reach this node" rule: no
// announced client address (not registered yet) or a live-marked-down peer.
// The ring layout, the rebalancer's targets and the console's 离线 tag all
// read this predicate — one definition, no drift.
func (p Peer) Offline() bool { return p.Down || p.ClientAddr == "" }

// Config configures one Raft node.
type Config struct {
	NodeID     string
	PeerAddr   string
	AdminAddr  string
	ClientAddr string
	DataDir    string
	Peers      []Peer
	// Seed marks Peers as a bootstrap seed rather than an authoritative
	// membership: a node that does not author the cluster's configuration
	// (a runtime joiner, or any configured node other than the one leading
	// -peers) is not a member of the cluster it is joining yet, and this tells
	// the consensus layer to start with the seed instead of refusing — and,
	// crucially, instead of writing a competing configuration of its own.
	Seed              bool
	ApplyTimeout      time.Duration
	SnapshotThreshold uint64
	SnapshotInterval  time.Duration
	TrailingLogs      uint64
	// FlushInterval is the consensus WAL's group-commit window (how long an
	// append waits to share one fsync with its neighbours).
	FlushInterval time.Duration
	// SegmentBytes is the consensus WAL's segment size.
	SegmentBytes int64
	// HeartbeatTimeout and ElectionTimeout are the consensus timings
	// (raft.DefaultHeartbeatTimeout / DefaultElectionTimeout when zero). They
	// are knobs because they are a RATIO: where the consensus event loop can be
	// delayed — a heavy apply batch, a CPU-throttled host — the election timeout
	// has to be raised, otherwise nodes keep voting each other out. The election
	// timeout must be at least twice the heartbeat.
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
}

func (c *Config) withDefaults() {
	if c.ApplyTimeout <= 0 {
		c.ApplyTimeout = 10 * time.Second
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = 1024
	}
	if c.SnapshotInterval <= 0 {
		c.SnapshotInterval = 30 * time.Second
	}
	if c.TrailingLogs == 0 {
		c.TrailingLogs = 256
	}
	if c.HeartbeatTimeout <= 0 {
		c.HeartbeatTimeout = raft.DefaultHeartbeatTimeout
	}
	if c.ElectionTimeout <= 0 {
		c.ElectionTimeout = raft.DefaultElectionTimeout
	}
}

// Node is a running Raft node owning the slot-table state machine.
type Node struct {
	cfg      Config
	raft     *raft.Node
	fsm      *FSM
	mux      *peerMux
	stopOnce sync.Once
	logger   *slog.Logger
}

// NewNode starts (or recovers) a Raft node. The second return value is the
// gRPC listener demuxed off the peer port (HTTP/2 preface 'P') — the caller
// hands it to Engine.ServePeer.
func NewNode(cfg Config, applier Applier, logger *slog.Logger) (*Node, net.Listener, error) {
	cfg.withDefaults()
	if cfg.NodeID == "" || cfg.PeerAddr == "" || cfg.DataDir == "" {
		return nil, nil, errors.New("cluster: node id, raft addr and data dir are required")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, nil, err
	}

	advertise, err := net.ResolveTCPAddr("tcp", HostPort(cfg.PeerAddr))
	if err != nil {
		return nil, nil, fmt.Errorf("cluster: resolve peer addr %q: %w", cfg.PeerAddr, err)
	}
	if advertise.IP == nil || advertise.IP.IsUnspecified() {
		return nil, nil, fmt.Errorf("cluster: peer addr %q is not advertisable", cfg.PeerAddr)
	}
	// The peer port carries BOTH consensus traffic and the inter-node
	// PeerService gRPC plane, demuxed by the first byte of each connection
	// ('P' = HTTP/2 preface goes to gRPC, everything else to the consensus
	// handshake magic 0x9E — see peer_mux.go).
	mux, err := newPeerMux(HostPort(cfg.PeerAddr), advertise)
	if err != nil {
		return nil, nil, err
	}

	fsm := &FSM{applier: applier, logger: logger}
	rn, err := raft.NewNode(raft.Config{
		NodeID:            cfg.NodeID,
		DataDir:           cfg.DataDir,
		Voters:            raftVoters(cfg),
		Seed:              cfg.Seed,
		HeartbeatTimeout:  cfg.HeartbeatTimeout,
		ElectionTimeout:   cfg.ElectionTimeout,
		ApplyTimeout:      cfg.ApplyTimeout,
		SnapshotThreshold: cfg.SnapshotThreshold,
		SnapshotInterval:  cfg.SnapshotInterval,
		TrailingLogs:      cfg.TrailingLogs,
		FlushInterval:     cfg.FlushInterval,
		SegmentBytes:      cfg.SegmentBytes,
		Logger:            raftLogAdapter{logger},
	}, fsm, mux)
	if err != nil {
		mux.Close()
		return nil, nil, err
	}

	// One line per start: the timings are a ratio, so an operator tuning them
	// needs to see what the node actually took (and the node log is where the
	// cluster's own start script looks).
	logger.Info("raft consensus timings",
		"heartbeat", cfg.HeartbeatTimeout,
		"election", cfg.ElectionTimeout,
		"ratio", float64(cfg.ElectionTimeout)/float64(cfg.HeartbeatTimeout))

	return &Node{cfg: cfg, raft: rn, fsm: fsm, mux: mux, logger: logger}, mux.GRPCListener(), nil
}

// raftVoters projects the configured peer directory onto the consensus
// layer's member set.
func raftVoters(cfg Config) []raft.Voter {
	out := make([]raft.Voter, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		out = append(out, raft.Voter{ID: p.ID, Addr: HostPort(p.PeerAddr)})
	}
	return out
}

// raftLogAdapter routes the consensus layer's log lines into the service logger.
type raftLogAdapter struct{ l *slog.Logger }

func (a raftLogAdapter) Debugf(f string, args ...any) { a.l.Debug(fmt.Sprintf(f, args...)) }
func (a raftLogAdapter) Infof(f string, args ...any)  { a.l.Info(fmt.Sprintf(f, args...)) }
func (a raftLogAdapter) Warnf(f string, args ...any)  { a.l.Warn(fmt.Sprintf(f, args...)) }
func (a raftLogAdapter) Errorf(f string, args ...any) { a.l.Error(fmt.Sprintf(f, args...)) }

// Apply submits a command through Raft and returns the applied result.
func (n *Node) Apply(cmd []byte) ([]byte, error) {
	res, err := n.raft.Apply(cmd, n.cfg.ApplyTimeout)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return nil, ErrNotLeader
		}
		return nil, err
	}
	r, ok := res.(*ApplyResult)
	if !ok {
		return nil, fmt.Errorf("cluster: unexpected raft apply result %T", res)
	}
	if r.Error != "" {
		return nil, errors.New(r.Error)
	}
	return r.Value, nil
}

// IsLeader reports whether this node holds the Raft leadership (controller).
func (n *Node) IsLeader() bool { return n.raft.IsLeader() }

// LeaderID returns the Raft leader (controller) node id — the id the slot
// table and the peer directory are keyed by.
func (n *Node) LeaderID() string { return n.raft.LeaderID() }

// LeaderPeerAddr returns the leader's peer-plane address ("" when unknown).
func (n *Node) LeaderPeerAddr() string { return n.raft.LeaderAddr() }

// WaitForLeader blocks until a leader is known.
func (n *Node) WaitForLeader(timeout time.Duration) (string, error) {
	return n.raft.WaitForLeader(timeout)
}

// Peers returns the current Raft cluster membership ids.
func (n *Node) Peers() []string {
	m := n.PeerAddrs()
	out := make([]string, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	return out
}

// PeerAddrs maps member id -> peer (consensus transport) address from the
// static voter set — the authoritative source of the only address every node
// learns statically about every other node.
func (n *Node) PeerAddrs() map[string]string {
	vs := n.raft.Voters()
	out := make(map[string]string, len(vs))
	for _, v := range vs {
		out[v.ID] = v.Addr
	}
	return out
}

// Stats exposes raft counters for the admin API. Both key spellings
// ("commit-index" and "commit_index") are provided because callers
// historically disagreed.
// Stats is the consensus section of the admin status payload. Besides what the
// raft layer reports, it carries the timings THIS process took: they are a
// ratio (the election timeout must stay several multiples of the heartbeat), so
// an operator who raised -raft-election-timeout has to be able to read back
// what the node is actually running with — next to the flag they passed, not
// buried in a startup log line.
func (n *Node) Stats() map[string]any {
	st := n.raft.Stats()
	heartbeat, election := n.cfg.HeartbeatTimeout, n.cfg.ElectionTimeout
	if heartbeat <= 0 {
		heartbeat = raft.DefaultHeartbeatTimeout
	}
	if election <= 0 {
		election = raft.DefaultElectionTimeout
	}
	st["heartbeat_timeout"] = heartbeat.String()
	st["election_timeout"] = election.String()
	return st
}

// IsMember reports whether id is in the current cluster membership.
func (n *Node) IsMember(id string) bool { return n.raft.IsMember(id) }

// Members returns the current membership (id + consensus address).
func (n *Node) Members() []raft.Voter { return n.raft.Members() }

// AddMember adds a node to the cluster membership at runtime (leader only).
func (n *Node) AddMember(id, addr string, timeout time.Duration) error {
	return n.raft.AddMember(id, addr, timeout)
}

// RemoveMember drops a node from the cluster membership at runtime (leader only).
func (n *Node) RemoveMember(id string, timeout time.Duration) error {
	return n.raft.RemoveMember(id, timeout)
}

// Voters reports whether id currently counts towards quorum.
func (n *Node) IsVoter(id string) bool { return n.raft.IsVoter(id) }

// Close shuts the node down.
func (n *Node) Close() error {
	var err error
	n.stopOnce.Do(func() {
		if cerr := n.raft.Close(); cerr != nil {
			err = cerr
		}
		if n.mux != nil {
			if merr := n.mux.Close(); merr != nil && err == nil {
				err = merr
			}
		}
	})
	return err
}

// NodePortOf parses the port of a host:port address.
func NodePortOf(addr string) (int, error) {
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(p)
}
