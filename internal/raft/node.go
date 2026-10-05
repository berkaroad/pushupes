// node.go — the public face of the consensus layer.
//
// Node owns the FSM, the log, the transport and the single runLoop goroutine
// that mutates protocol state. Every other goroutine talks to it through
// events (channels) and reads status through an atomically published view, so
// there is no shared mutable protocol state to lock from the outside.
package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// view is the read-only status snapshot published by the run loop.
type view struct {
	state     State
	term      uint64
	leader    string
	commit    uint64
	applied   uint64
	lastIndex uint64
}

// Node is one consensus node.
type Node struct {
	cfg    Config
	fsm    FSM
	log    *raftLog
	tr     Transport
	logger Logger

	events     chan any
	applyCh    chan *applyReq
	durableSig chan struct{}
	durableSeq atomic.Uint64
	done       chan struct{}
	loopDone   chan struct{}
	closeOnce  sync.Once

	srv     *rpcServer
	clients map[string]*peerClient

	voters []Voter

	// ---- owned by the run loop ----
	state       State
	term        uint64
	vote        string
	leaderID    string
	commitIndex uint64
	applied     uint64
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	votes       map[string]bool
	replicators map[string]*replicator
	waiters     []applyWaiter
	barriers    []barrierWait
	pendingAcks []pendingAck
	durableLSN  uint64
	snapRunning bool

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	view atomic.Pointer[view]
}

// NewNode recovers (or initialises) a node and starts its run loop.
func NewNode(cfg Config, fsm FSM, tr Transport) (*Node, error) {
	if err := cfg.withDefaults(); err != nil {
		return nil, err
	}
	if fsm == nil {
		return nil, errors.New("raft: fsm is required")
	}
	if tr == nil {
		return nil, errors.New("raft: transport is required")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}

	lg, err := openLog(cfg.DataDir, WALOptions{SegmentBytes: cfg.SegmentBytes, FlushInterval: cfg.FlushInterval})
	if err != nil {
		return nil, err
	}

	n := &Node{
		cfg:         cfg,
		fsm:         fsm,
		log:         lg,
		tr:          tr,
		logger:      cfg.Logger,
		events:      make(chan any, 256),
		applyCh:     make(chan *applyReq, 256),
		durableSig:  make(chan struct{}, 1),
		done:        make(chan struct{}),
		loopDone:    make(chan struct{}),
		clients:     map[string]*peerClient{},
		voters:      nil,
		state:       Follower,
		nextIndex:   map[string]uint64{},
		matchIndex:  map[string]uint64{},
		replicators: map[string]*replicator{},
	}

	// Membership is written once and is immutable afterwards: a node that is
	// started with a different -peers set than the one it recorded refuses to
	// start rather than silently running with two different views.
	if lg.HasConf() {
		recorded := lg.Voters()
		if err := validateVoters(recorded, cfg.Voters); err != nil {
			lg.close()
			return nil, err
		}
		n.voters = recorded
	} else {
		if err := lg.setVoters(cfg.Voters); err != nil {
			lg.close()
			return nil, err
		}
		n.voters = append([]Voter(nil), cfg.Voters...)
	}

	// Restore the newest snapshot. Entries above it are applied later, as the
	// leader advances commitIndex — never eagerly, because they may be
	// uncommitted and get overwritten.
	si, _, _, data, ok, err := loadLatestSnapshot(cfg.DataDir)
	if err != nil {
		lg.close()
		return nil, err
	}
	if ok {
		if err := fsm.Restore(data); err != nil {
			lg.close()
			return nil, fmt.Errorf("raft: restore snapshot: %w", err)
		}
		n.applied = si
		n.commitIndex = si
	}

	hs := lg.HardState()
	n.term = hs.Term
	n.vote = hs.Vote

	for _, v := range n.voters {
		if v.ID == cfg.NodeID {
			continue
		}
		n.clients[v.ID] = newPeerClient(cfg.NodeID, v.ID, v.Addr, tr, cfg.Logger)
	}

	n.srv = newRPCServer(cfg.NodeID, tr, n.handleRPC, cfg.Logger)
	// Durability notifications drive deferred acknowledgements: the flusher
	// publishes the highest durable LSN and the loop releases every follower
	// ack and commit step that was waiting on it.
	n.log.wal.SetOnDurable(n.publishDurable)
	go n.srv.run()
	n.publishView()
	go n.run()
	return n, nil
}

// publishDurable records the highest durable LSN and nudges the run loop. It is
// called from the flusher goroutine and must never block: the atomic keeps the
// maximum, so a coalesced wake-up is always enough.
func (n *Node) publishDurable(lsn uint64) {
	for {
		cur := n.durableSeq.Load()
		if lsn <= cur {
			return
		}
		if n.durableSeq.CompareAndSwap(cur, lsn) {
			break
		}
	}
	select {
	case n.durableSig <- struct{}{}:
	default:
	}
}

func validateVoters(recorded, configured []Voter) error {
	if len(recorded) != len(configured) {
		return fmt.Errorf("raft: recorded membership has %d voters but %d were configured; refusing to start", len(recorded), len(configured))
	}
	have := map[string]string{}
	for _, v := range recorded {
		have[v.ID] = v.Addr
	}
	for _, v := range configured {
		if have[v.ID] != v.Addr {
			return fmt.Errorf("raft: configured voter %s=%s does not match the recorded %s=%s; refusing to start", v.ID, v.Addr, v.ID, have[v.ID])
		}
	}
	return nil
}

func (n *Node) publishView() {
	n.view.Store(&view{
		state:     n.state,
		term:      n.term,
		leader:    n.leaderID,
		commit:    n.commitIndex,
		applied:   n.applied,
		lastIndex: n.log.LastIndex(),
	})
}

func (n *Node) readView() view {
	if v := n.view.Load(); v != nil {
		return *v
	}
	return view{state: Follower}
}

func (n *Node) post(ev any) error {
	select {
	case n.events <- ev:
		return nil
	case <-n.done:
		return ErrClosed
	}
}

// Apply submits a command through the log and returns the FSM's result once it
// has been committed and applied locally.
//
// Requests go through their own channel so the run loop can drain a whole
// batch and hand it to the WAL as one group: concurrent callers then share a
// single fsync instead of paying one each.
func (n *Node) Apply(cmd []byte, timeout time.Duration) (any, error) {
	req := &applyReq{cmd: cmd, resp: make(chan applyResp, 1)}
	select {
	case n.applyCh <- req:
	case <-n.done:
		return nil, ErrClosed
	}
	if timeout <= 0 {
		timeout = n.cfg.ApplyTimeout
	}
	select {
	case r := <-req.resp:
		return r.value, r.err
	case <-time.After(timeout):
		return nil, ErrTimeout
	case <-n.done:
		return nil, ErrClosed
	}
}

// Barrier blocks until everything committed so far has been applied locally.
func (n *Node) Barrier(timeout time.Duration) error {
	req := &barrierReq{resp: make(chan error, 1)}
	if err := n.post(req); err != nil {
		return err
	}
	if timeout <= 0 {
		timeout = n.cfg.ApplyTimeout
	}
	select {
	case err := <-req.resp:
		return err
	case <-time.After(timeout):
		return ErrTimeout
	case <-n.done:
		return ErrClosed
	}
}

// State returns the current role.
func (n *Node) State() State { return n.readView().state }

// IsLeader reports whether this node holds leadership.
func (n *Node) IsLeader() bool { return n.State() == Leader }

// LeaderID returns the current leader's node id ("" when unknown).
func (n *Node) LeaderID() string { return n.readView().leader }

// LeaderAddr returns the current leader's peer address, or "" when unknown.
func (n *Node) LeaderAddr() string {
	id := n.LeaderID()
	if id == "" {
		return ""
	}
	if id == n.cfg.NodeID {
		return n.tr.Addr()
	}
	return n.cfg.voterAddr(id)
}

// Term returns the current term.
func (n *Node) Term() uint64 { return n.readView().term }

// CommitIndex returns the highest committed log index.
func (n *Node) CommitIndex() uint64 { return n.readView().commit }

// AppliedIndex returns the highest applied log index.
func (n *Node) AppliedIndex() uint64 { return n.readView().applied }

// LastIndex returns the last log index.
func (n *Node) LastIndex() uint64 { return n.readView().lastIndex }

// Voters returns the static voter set.
func (n *Node) Voters() []Voter { return append([]Voter(nil), n.voters...) }

// WaitForLeader blocks until a leader is known or the timeout elapses.
func (n *Node) WaitForLeader(timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		if id := n.LeaderID(); id != "" {
			return id, nil
		}
		if time.Now().After(deadline) {
			return "", ErrTimeout
		}
		select {
		case <-n.done:
			return "", ErrClosed
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// Stats exposes counters for the admin API. Both key spellings are provided
// because callers historically used "commit-index" and "commit_index".
func (n *Node) Stats() map[string]any {
	v := n.readView()
	return map[string]any{
		"state":          string(v.state),
		"leader":         v.leader,
		"term":           v.term,
		"applied-index":  v.applied,
		"applied_index":  v.applied,
		"commit-index":   v.commit,
		"commit_index":   v.commit,
		"last_log_index": v.lastIndex,
	}
}

// Close shuts the node down and releases the WAL lock.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		close(n.done)
		<-n.loopDone
		for _, c := range n.clients {
			c.close()
		}
		n.srv.close()
		if err := n.log.close(); err != nil {
			n.logger.Warnf("raft: closing wal: %v", err)
		}
	})
	return nil
}

func (n *Node) randomElectionTimeout() time.Duration {
	base := n.cfg.ElectionTimeout
	return base + time.Duration(rand.Int63n(int64(base)+1))
}
