// Package cluster provides the pushupes cluster: a Raft control plane that
// replicates the slot assignment table, a leader-follower data plane that
// replicates slot WALs by seq-based fetch, and hot slot migration.
//
// The split: consensus carries only metadata; event data
// travels over a dedicated replication protocol and never through the Raft log.
package cluster

import (
	"errors"
	"fmt"
	hclog "github.com/hashicorp/go-hclog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/sirupsen/logrus"
)

// ErrNotLeader is returned when a command is submitted on a follower.
var ErrNotLeader = errors.New("not the cluster leader")

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
}

// Config configures one Raft node.
type Config struct {
	NodeID            string
	PeerAddr          string
	AdminAddr         string
	ClientAddr        string
	DataDir           string
	Bootstrap         bool
	Peers             []Peer
	ApplyTimeout      time.Duration
	SnapshotThreshold uint64
	SnapshotInterval  time.Duration
	TrailingLogs      uint64
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
}

// Node is a running Raft node owning the slot-table state machine.
type Node struct {
	cfg         Config
	raft        *raft.Raft
	fsm         *FSM
	transport   *raft.NetworkTransport
	logStore    *raftboltdb.BoltStore
	stableStore *raftboltdb.BoltStore
	joinStop    chan struct{}
	joinOnce    sync.Once
	stopOnce    sync.Once
	logger      *logrus.Entry
}

// NewNode starts (or joins) a Raft node. The second return value is
// the gRPC listener demuxed off the peer port (HTTP/2 preface 'P') —
// the caller hands it to Engine.ServePeer.
func NewNode(cfg Config, applier Applier, logger *logrus.Entry) (*Node, net.Listener, error) {
	cfg.withDefaults()
	if cfg.NodeID == "" || cfg.PeerAddr == "" || cfg.DataDir == "" {
		return nil, nil, errors.New("cluster: node id, raft addr and data dir are required")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, nil, err
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)
	rc.SnapshotThreshold = cfg.SnapshotThreshold
	rc.SnapshotInterval = cfg.SnapshotInterval
	rc.TrailingLogs = cfg.TrailingLogs

	advertise, err := net.ResolveTCPAddr("tcp", HostPort(cfg.PeerAddr))
	if err != nil {
		return nil, nil, fmt.Errorf("cluster: resolve peer addr %q: %w", cfg.PeerAddr, err)
	}
	if advertise.IP == nil || advertise.IP.IsUnspecified() {
		return nil, nil, fmt.Errorf("cluster: peer addr %q is not advertisable", cfg.PeerAddr)
	}
	// The peer port carries BOTH raft transport traffic and the
	// inter-node PeerService gRPC plane, demuxed by the first byte of
	// each connection ('P' = HTTP/2 preface goes to gRPC, the rest to
	// raft — see peer_mux.go).
	mux, err := newPeerMux(HostPort(cfg.PeerAddr), advertise)
	if err != nil {
		return nil, nil, err
	}
	transport := raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
		Stream:  mux,
		MaxPool: 5,
		Timeout: 10 * time.Second,
		Logger:  hclog.New(&hclog.LoggerOptions{Name: "raft-net", Output: os.Stderr, Level: hclog.Info}),
	})

	bolt, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return nil, nil, err
	}
	snapshots, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, nil, err
	}

	fsm := &FSM{applier: applier, logger: logger}

	hasState, err := raft.HasExistingState(bolt, bolt, snapshots)
	if err != nil {
		return nil, nil, err
	}
	if cfg.Bootstrap && !hasState {
		if err := raft.BootstrapCluster(rc, bolt, bolt, snapshots, transport,
			raftConfiguration(cfg.NodeID, cfg.Peers)); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			return nil, nil, fmt.Errorf("cluster: bootstrap: %w", err)
		}
	}

	r, err := raft.NewRaft(rc, fsm, bolt, bolt, snapshots, transport)
	if err != nil {
		return nil, nil, err
	}

	n := &Node{cfg: cfg, raft: r, fsm: fsm, transport: transport, logStore: bolt, stableStore: bolt, logger: logger}

	if !hasState && !cfg.Bootstrap {
		n.startAutoJoin()
	}
	return n, mux.GRPCListener(), nil
}

func raftConfiguration(self string, peers []Peer) raft.Configuration {
	servers := make([]raft.Server, 0, len(peers))
	for _, p := range peers {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(p.ID),
			Address: raft.ServerAddress(HostPort(p.PeerAddr)),
		})
	}
	if len(servers) == 0 {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(self),
			Address: raft.ServerAddress(self),
		})
	}
	return raft.Configuration{Servers: servers}
}

// startAutoJoin keeps trying to join configured peers after the first
// bootstrap node came up earlier than us.
func (n *Node) startAutoJoin() {
	n.joinOnce.Do(func() {
		n.joinStop = make(chan struct{})
		go func() {
			for {
				select {
				case <-n.joinStop:
					return
				case <-time.After(time.Second):
				}
				for _, p := range n.cfg.Peers {
					if p.ID == n.cfg.NodeID {
						continue
					}
					if err := n.Join(p.ID, HostPort(p.PeerAddr)); err == nil {
						n.logger.WithField("peer", p.ID).Info("Joined cluster")
						return
					}
				}
			}
		}()
	})
}

// Join adds this node to the cluster led by peer (the peer must already be
// in the configuration for us to reach a quorum decision).
func (n *Node) Join(peerID, peerPeerAddr string) error {
	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	for _, s := range configFuture.Configuration().Servers {
		if string(s.Address) == peerPeerAddr {
			return n.raft.AddVoter(raft.ServerID(n.cfg.NodeID), raft.ServerAddress(HostPort(n.cfg.PeerAddr)), 0, n.cfg.ApplyTimeout).Error()
		}
	}
	return fmt.Errorf("cluster: peer %s not in configuration yet", peerID)
}

// Apply submits a command through Raft and returns the applied result.
func (n *Node) Apply(cmd []byte) ([]byte, error) {
	fut := n.raft.Apply(cmd, n.cfg.ApplyTimeout)
	if err := fut.Error(); err != nil {
		return nil, err
	}
	res := fut.Response().(*ApplyResult)
	if res.Error != "" {
		return nil, errors.New(res.Error)
	}
	return res.Value, nil
}

// IsLeader reports whether this node holds the Raft leadership (controller).
func (n *Node) IsLeader() bool { return n.raft.State() == raft.Leader }

// LeaderID returns the Raft leader node id.
func (n *Node) LeaderID() string {
	id, _ := n.raft.LeaderWithID()
	return string(id)
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

// PeerAddrs maps member id -> peer (raft transport) address from the
// committed raft configuration — the authoritative source of the only
// address every node learns statically about every other node.
func (n *Node) PeerAddrs() map[string]string {
	fut := n.raft.GetConfiguration()
	if err := fut.Error(); err != nil {
		return nil
	}
	out := make(map[string]string, len(fut.Configuration().Servers))
	for _, s := range fut.Configuration().Servers {
		out[string(s.ID)] = string(s.Address)
	}
	return out
}

// Stats exposes raft counters for the admin API.
func (n *Node) Stats() map[string]any {
	st := n.raft.Stats()
	out := map[string]any{"state": st["state"], "leader": st["leader"]}
	// raft.Stats()["leader"] is empty on the leader itself and an address on
	// followers; LeaderWithID returns (address, ServerID) on every node — use
	// the ServerID so the UI shows a stable node id on all roles.
	if _, id := n.raft.LeaderWithID(); id != "" {
		out["leader"] = string(id)
	}
	if v, ok := st["applied_index"]; ok {
		out["applied_index"] = v
	}
	if v, ok := st["commit_index"]; ok {
		out["commit_index"] = v
	}
	if v, ok := st["last_index"]; ok {
		out["last_log_index"] = v
	}
	return out
}

// Close shuts the node down.
func (n *Node) Close() error {
	var err error
	n.stopOnce.Do(func() {
		if n.joinStop != nil {
			close(n.joinStop)
		}
		if serr := n.raft.Shutdown().Error(); serr != nil {
			err = serr
		}
		if n.transport != nil {
			if terr := n.transport.Close(); terr != nil && err == nil {
				err = terr
			}
		}
		// Raft never closes its stores; leave bolt open would hold the file
		// lock and block an in-process restart.
		if n.logStore != nil {
			if cerr := n.logStore.Close(); cerr != nil && err == nil {
				err = cerr
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
