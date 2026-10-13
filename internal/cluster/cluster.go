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

	"github.com/berkaroad/pushupes/internal/raft"
)

// ErrNotLeader is returned when a command is submitted on a follower.
var ErrNotLeader = errors.New("not the cluster leader")

// Consensus WAL defaults: the group-commit window and the segment size.
const (
	DefaultRaftFlushInterval = 200 * time.Microsecond
	DefaultRaftSegmentBytes  = 64 << 20
)

// Applier is implemented by the engine: it applies replicated slot-table
// commands and can snapshot/restore that state.
type Applier interface {
	// ApplyCommand applies one encoded command and returns a result payload.
	ApplyCommand(cmd []byte) ([]byte, error)
	// SnapshotState serialises the entire replicated state (the slot table).
	SnapshotState() ([]byte, error)
	// RestoreState replaces the replicated state from a snapshot.
	RestoreState(data []byte) error
	// ResetState drops the replicated state back to "nothing has been
	// decided yet". A data-plane node is told to do this when the control
	// plane it follows cannot place the point it resume from (index 0 with no
	// image to install): replaying a log onto a state it does not belong to
	// would leave the two silently divergent, so the state goes first.
	ResetState() error
}

// Node roles (-role): who votes in the consensus group and who may host
// slot replicas. A node is exactly one of the two — the single flag, with no
// per-node third shape, is what keeps every deployment speaking one
// inter-node protocol (the dev cluster and the production cluster run the
// same code path on purpose):
//
//   - voter:   control-plane member — votes and is elected; hosts NO slots,
//     so its raft timings never share a machine with a data-plane load.
//   - learner: data-plane member — receives the replicated layout over the
//     control plane's distribution stream; never votes, never elects.
//
// The smallest deployment is one voter plus one learner. The role is never
// implied: -role is required, and admissions, adoptions and registrations all
// carry an explicit voter or learner.
const (
	RoleVoter   = "voter"
	RoleLearner = "learner"
)

// NormalizeRole validates a role name; a role must be stated (voter or
// learner), there is no default.
func NormalizeRole(role string) (string, error) {
	switch role {
	case RoleVoter, RoleLearner:
		return role, nil
	}
	return "", fmt.Errorf("cluster: role %q is not a valid role: -role must state voter (control plane) or learner (data plane); the smallest deployment is one control node plus one data node", role)
}

// Peer is a cluster member. AdminAddr serves inter-node replication and
// admin traffic only; ClientAddr is the client-facing data plane (event
// writes + queries) and the target of MOVED/ASK redirects.
type Peer struct {
	ID         string `json:"id"`
	PeerAddr   string `json:"peer_addr"`
	AdminAddr  string `json:"admin_addr"`
	ClientAddr string `json:"client_addr"`
	// Role is the node's self-announced place in the cluster (see the role
	// constants above): announced like the admin/client addresses — the node
	// knows its own -role flag, nobody else can answer for it.
	Role string `json:"role,omitempty"`
	// Down is the controller's liveness verdict, replicated through Raft:
	// the peer failed its consecutive peer-plane probes. The directory entry
	// KEEPS its announced addresses (unlike leave_node, which deletes it), so
	// a failed node stays visible — flagged 离线 in the console — instead of
	// flickering between "removed" and "re-seeded without addresses".
	Down bool `json:"down,omitempty"`
}

// Hosts reports whether a directory entry may carry slot replicas. Two
// conditions, both about the node having said what it is and where to reach
// it:
//
//   - it announced its role as learner (the data plane; a voter hosts nothing);
//   - it has announced a client address. An entry that has never registered is
//     a static -peers seed or a directory entry the controller derived from the
//     Raft configuration — a node clients cannot route to, and one the
//     controller would seat, hand leadership to and then watch stall. Planning
//     onto it also opened a real hole in a control/data split: on a cold start
//     every static peer was pool-eligible before its role announcement landed,
//     so a cluster of pure voters planned slots onto itself (observed live:
//     three voters each leading 840 slots on a fresh cluster, layout
//     unrecoverable without replaying history). Registration closes both,
//     because OpRegister carries the addresses and the role in one command —
//     the entry joins the pool on the plan after it.
//
// Note what is deliberately NOT a condition: Down. A failed node keeps its
// seats (its copies are what a failover hands leadership to); "can this node
// be a target right now" is a different question, answered by Offline.
func (p Peer) Hosts() bool { return p.Role == RoleLearner && p.ClientAddr != "" }

// PureControl reports whether the directory says this node is control-plane
// only — the one case where an unassigned slot must not be served locally
// (see Engine.controlOnlyRefusal). Unlike Hosts it does NOT treat "unknown" as
// not hosting: an entry with no role is a node that has not announced yet, and
// refusing its writes would turn "the plan is one round away" into an error.
func (p Peer) PureControl() bool { return p.Role == RoleVoter }

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
	// Role is this node's place in the cluster (RoleVoter or RoleLearner;
	// required, there is no default). It lands in the peer directory through
	// the registration announcement and decides whether the layout may place
	// slots here (see Peer.Hosts).
	Role    string
	DataDir string
	Peers   []Peer
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
	cfg Config
	// raft is the control-plane engine: a Raft group this node votes in.
	// learner is the data-plane engine: no group at all, the replicated
	// metadata arriving over the controller's feed (learnernode.go). Exactly
	// one of them is set — see NewNode.
	raft     *raft.Node
	learner  *learnerNode
	fsm      *FSM
	mux      *peerMux
	stopOnce sync.Once
	logger   *slog.Logger
	// topology identifies THIS node's consensus history generation: a random
	// id written once into the data dir and kept for the dir's lifetime. It
	// fences the owner provenance record (owner.go): a copy stamped under one
	// topology and found after the dir was rebuilt carries a different id,
	// so the node refuses to lead it rather than writing into a stale seq
	// space. It never needs to agree across nodes — every node compares its
	// own record against its own dir.
	topology string
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

	if cfg.Role == RoleLearner {
		// Data-plane node (DESIGN §12): it serves slot replicas and the
		// inter-node PeerService plane, and takes the replicated metadata from
		// the controller's feed. There is no Raft group, so there is no
		// consensus dir either — which is why the generation id it fences its
		// own copies with comes from the control plane rather than from a file
		// here (see learnernode.go).
		ln, err := newLearnerNode(cfg, applier, logger)
		if err != nil {
			mux.Close()
			return nil, nil, err
		}
		logger.Info("data-plane node: layout arrives from the control plane",
			"role", cfg.Role, "seeds", len(cfg.Peers))
		ln.start()
		return &Node{cfg: cfg, learner: ln, mux: mux, logger: logger}, mux.GRPCListener(), nil
	}

	topo, err := loadOrInitTopology(cfg.DataDir)
	if err != nil {
		mux.Close()
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

	return &Node{cfg: cfg, raft: rn, fsm: fsm, mux: mux, logger: logger, topology: topo}, mux.GRPCListener(), nil
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

// Apply submits a command through Raft and returns the applied result. A
// data-plane node never submits: it answers ErrNotLeader, the same answer a
// follower gives, so every leader-only path in the Engine takes its
// non-controller branch.
func (n *Node) Apply(cmd []byte) ([]byte, error) {
	if n.learner != nil {
		return nil, ErrNotLeader
	}
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
// A data-plane node is never the controller.
func (n *Node) IsLeader() bool {
	if n.learner != nil {
		return false
	}
	return n.raft.IsLeader()
}

// Cfg returns the node's configuration (addresses and join seeds) — the
// Engine reads its own plane addresses and the seed list through it, so a
// learner implementation can answer the same shape without a Raft group.
func (n *Node) Cfg() Config { return n.cfg }

// TopologyID is the consensus generation this node's replicated metadata
// belongs to (see owner.go). A voter reads its own dir — the dir IS its
// consensus history; a data-plane node took the id from the control plane it
// follows, because its dir holds slot data and no history of its own.
func (n *Node) TopologyID() string {
	if n.learner != nil {
		return n.learner.TopologyID()
	}
	return n.topology
}

// consensus is the cluster layer's seam to the replicated-metadata engine.
// raftNode (the *Node above) implements it with a Raft group; the learner
// implementation (role=learner, see learner.go) answers the same surface
// from the control plane's distribution stream — the Engine's behavior (a
// leader-only submit, directory sync, membership views) never branches on
// the difference: it only observes IsLeader()==false and reads the same
// replicated table its raft siblings apply.
type consensus interface {
	Apply(cmd []byte) ([]byte, error)
	IsLeader() bool
	LeaderID() string
	LeaderPeerAddr() string
	PeerAddrs() map[string]string
	Members() []raft.Voter
	IsMember(id string) bool
	Stats() map[string]any
	AddMember(id, addr string, timeout time.Duration) error
	RemoveMember(id string, timeout time.Duration) error
	Cfg() Config
	TopologyID() string
	WaitForLeader(timeout time.Duration) (string, error)
	// LayoutSlice answers a data-plane node asking for the replicated
	// metadata from a given point (raft.LayoutSlice): entries to continue
	// with, or an image to rebuild from. It is a control-plane primitive —
	// only a voter has a log to slice — so a learner answers an error and the
	// feed is served by whoever holds the controller (layoutfeed.go).
	LayoutSlice(from, fromTerm uint64, max int) (raft.LayoutSlice, error)
	Close() error
}

var _ consensus = (*Node)(nil)

// LeaderID returns the Raft leader (controller) node id — the id the slot
// table and the peer directory are keyed by.
func (n *Node) LeaderID() string {
	if n.learner != nil {
		return n.learner.LeaderID()
	}
	return n.raft.LeaderID()
}

// LeaderPeerAddr returns the leader's peer-plane address ("" when unknown).
func (n *Node) LeaderPeerAddr() string {
	if n.learner != nil {
		return n.learner.LeaderPeerAddr()
	}
	return n.raft.LeaderAddr()
}

// WaitForLeader blocks until a leader is known.
func (n *Node) WaitForLeader(timeout time.Duration) (string, error) {
	if n.learner != nil {
		return n.learner.WaitForLeader(timeout)
	}
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
	if n.learner != nil {
		return n.learner.PeerAddrs()
	}
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
	if n.learner != nil {
		return n.learner.Stats()
	}
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
func (n *Node) IsMember(id string) bool {
	if n.learner != nil {
		return n.learner.IsMember(id)
	}
	return n.raft.IsMember(id)
}

// Members returns the current membership (id + consensus address).
func (n *Node) Members() []raft.Voter {
	if n.learner != nil {
		return n.learner.Members()
	}
	return n.raft.Members()
}

// AddMember adds a node to the cluster membership at runtime (leader only).
func (n *Node) AddMember(id, addr string, timeout time.Duration) error {
	if n.learner != nil {
		return n.learner.AddMember(id, addr, timeout)
	}
	return n.raft.AddMember(id, addr, timeout)
}

// RemoveMember drops a node from the cluster membership at runtime (leader only).
func (n *Node) RemoveMember(id string, timeout time.Duration) error {
	if n.learner != nil {
		return n.learner.RemoveMember(id, timeout)
	}
	return n.raft.RemoveMember(id, timeout)
}

// Voters reports whether id currently counts towards quorum.
func (n *Node) IsVoter(id string) bool {
	if n.learner != nil {
		return n.learner.IsMember(id)
	}
	return n.raft.IsVoter(id)
}

// LayoutSlice serves the data-plane feed (layoutfeed.go). A learner has no log
// to slice and answers an error, which keeps a misdirected caller loud.
func (n *Node) LayoutSlice(from, fromTerm uint64, max int) (raft.LayoutSlice, error) {
	if n.learner != nil {
		return n.learner.LayoutSlice(from, fromTerm, max)
	}
	return n.raft.LayoutSlice(from, fromTerm, max)
}

// Close shuts the node down.
func (n *Node) Close() error {
	var err error
	n.stopOnce.Do(func() {
		if n.learner != nil {
			if cerr := n.learner.Close(); cerr != nil {
				err = cerr
			}
		} else if cerr := n.raft.Close(); cerr != nil {
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
