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
	"sort"
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
	confCh     chan *confReq
	durableSig chan struct{}
	durableSeq atomic.Uint64
	done       chan struct{}
	loopDone   chan struct{}
	closeOnce  sync.Once

	srv     *rpcServer
	clients map[string]*peerClient
	// clientsMu guards the clients map: the run loop writes membership into it
	// while the replicator goroutines read it on every round.
	clientsMu sync.RWMutex

	// voters is the live membership: rebuilt at startup and replaced by every
	// committed conf change. The run loop is the only writer, and it writes
	// under votersMu because readers (Members/Voters/IsMember) take the same
	// lock and copy — an unlocked assignment races with them mid-copy.
	voters   []Voter
	votersMu sync.RWMutex

	// ---- owned by the run loop ----
	state       State
	term        uint64
	vote        string
	leaderID    string
	commitIndex uint64
	applied     uint64
	// noConfWarned records that the "no configuration learned yet, waiting"
	// warning has been printed, so a node waiting for the leader does not log
	// it once per election timeout.
	noConfWarned bool
	nextIndex    map[string]uint64
	matchIndex   map[string]uint64
	votes        map[string]bool
	replicators  map[string]*replicator
	waiters      []applyWaiter
	barriers     []barrierWait
	pendingAcks  []pendingAck
	durableLSN   uint64
	snapRunning  bool
	// snapWG tracks the goroutine writing a snapshot file into the data
	// directory, so Close does not return while one is still being placed.
	snapWG sync.WaitGroup

	// Membership-change state, owned by the run loop.
	confWaiters      map[uint64]confWaiter
	confNextDeadline time.Time
	confSeq          atomic.Uint64
	// pendingAdd is the learner waiting to be promoted: the change is held
	// back until the learner's log reaches pendingCommit, or the budget runs
	// out.
	pendingAdd    string
	pendingAddrs  map[string]string
	pendingCommit uint64
	closing       map[string]bool
	// confPending maps a member being removed to the index of the
	// configuration entry it must acknowledge before its replicator is
	// dropped; closingAcked records that it has, so one final round (which
	// carries the new commit index) can go out.
	confPending  map[string]uint64
	closingAcked map[string]bool
	// Learners are accepted members that are not voters yet: they receive the
	// log (so they catch up) but do not count towards quorum. A pending add
	// must be replicated to a learner before the voter entry is appended —
	// that is what makes "leader hops to the new node right after the add"
	// safe.
	learners  []Voter
	learnerMu sync.RWMutex

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
		cfg:          cfg,
		fsm:          fsm,
		log:          lg,
		tr:           tr,
		logger:       cfg.Logger,
		events:       make(chan any, 256),
		applyCh:      make(chan *applyReq, 256),
		confCh:       make(chan *confReq, 16),
		durableSig:   make(chan struct{}, 1),
		done:         make(chan struct{}),
		loopDone:     make(chan struct{}),
		clients:      map[string]*peerClient{},
		voters:       nil,
		state:        Follower,
		nextIndex:    map[string]uint64{},
		matchIndex:   map[string]uint64{},
		replicators:  map[string]*replicator{},
		confWaiters:  map[uint64]confWaiter{},
		pendingAddrs: map[string]string{},
		closing:      map[string]bool{},
		confPending:  map[string]uint64{},
		closingAcked: map[string]bool{},
	}

	// Membership is recovered from the log: the conf record is this node's own
	// copy of the configuration (rewritten whenever a change commits), and the
	// conf ENTRIES in the log are replayed on top of it. A node that has a
	// record keeps it even when -peers has changed — that is what lets a
	// cluster grown at runtime restart without editing every member's flags.
	//
	// The snapshot's voter set is the base for a node whose log starts above
	// the snapshot: it is the configuration that was in force when the
	// snapshot was taken, and its log no longer holds the entry that carried
	// it.
	//
	// A SEED start is the other case: the node is not a member of the cluster
	// its -peers names (it is being joined into it), so the configured list is
	// only kept as the seed a joiner announces itself with — never as a live
	// membership. Starting with it would make the node believe it is already a
	// member and skip the join entirely.
	switch {
	case lg.HasConf():
		n.publishVoters(lg.Voters())
	case !cfg.Seed:
		// A first start: this node is part of the initial configuration, and
		// that configuration has to reach the LOG, not just the conf record —
		// see bootstrapConfig.
		if err := n.bootstrapConfig(cfg.Voters); err != nil {
			lg.close()
			return nil, err
		}
		if err := lg.setVoters(cfg.Voters); err != nil {
			lg.close()
			return nil, err
		}
		n.publishVoters(append([]Voter(nil), cfg.Voters...))
	default:
		if sb := n.log.SnapshotVoters(); len(sb) > 0 {
			if err := lg.setVoters(sb); err != nil {
				lg.close()
				return nil, err
			}
			n.publishVoters(append([]Voter(nil), sb...))
			break
		}
		n.logger.Infof("raft: starting as a bootstrap seed: %d configured member(s) are the join target, not this node's membership", len(cfg.Voters))
	}
	if err := n.replayConfEntries(); err != nil {
		lg.close()
		return nil, err
	}
	if !n.hasVoter(cfg.NodeID) {
		if !cfg.Seed {
			lg.close()
			return nil, fmt.Errorf("raft: this node (%s) is not in the recorded membership; start it with -peers as a bootstrap seed and join it through the admin endpoint", cfg.NodeID)
		}
		n.logger.Infof("raft: %s is not a member yet; start the cluster and add it at runtime (id=%s)", cfg.NodeID, cfg.NodeID)
	}

	// Restore the newest snapshot. Entries above it are applied later, as the
	// leader advances commitIndex — never eagerly, because they may be
	// uncommitted and get overwritten.
	si, _, _, _, data, ok, err := loadLatestSnapshot(cfg.DataDir)
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

	n.rebuildClients(nil)

	n.srv = newRPCServer(cfg.NodeID, tr, n.handleRPC, n.acceptablePeer, cfg.Logger)
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

// bootstrapConfig writes the initial configuration into the LOG, and opens the
// log at term 1.
//
// The conf record alone is enough for the nodes that were configured at the
// start (each writes its own copy) and for nobody else. A member added at
// runtime catches up by replicating the leader's log or installing a snapshot;
// with no configuration entry in that log, the only conf entry it ever applies
// is the one that adds itself — it comes up believing it is the whole cluster,
// with a quorum of one, free to elect itself on a stale log.
//
// Every initially configured node writes the identical entry at index 1
// (canonicalVoters sorts by id, so the order -peers happened to arrive in does
// not matter), which keeps their logs matching from the first entry on. Term 1
// is where hashicorp/raft's BootstrapCluster starts for the same reason: the
// entry is history nobody had to campaign for, the first real election runs at
// term 2, and no leader can commit it by counting alone — the current-term
// no-op a fresh leader appends is what commits it.
func (n *Node) bootstrapConfig(voters []Voter) error {
	if n.log.LastIndex() != 0 {
		return fmt.Errorf("raft: refusing to write the initial configuration over a non-empty log (last index %d)", n.log.LastIndex())
	}
	vs := canonicalVoters(voters)
	e := Entry{Index: 1, Term: 1, Kind: KindConf, Data: encodeConfChange(confChange{op: confSet, voters: vs})}
	if err := n.log.appendEntries([]Entry{e}); err != nil {
		return err
	}
	return n.log.setHardState(hardState{Term: 1})
}

// canonicalVoters returns the voter set in a deterministic order, so every node
// that starts from the same -peers list writes the same bootstrap entry.
func canonicalVoters(vs []Voter) []Voter {
	out := append([]Voter(nil), vs...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// validateVoters is retained for the first-start path and for tests: it checks
// a configured list is usable as an initial membership.
func validateVoters(recorded, configured []Voter) error {
	if len(recorded) == 0 && len(configured) == 0 {
		return errors.New("raft: empty voter set")
	}
	have := map[string]string{}
	for _, v := range recorded {
		have[v.ID] = v.Addr
	}
	for _, v := range configured {
		if v.ID == "" || v.Addr == "" {
			return fmt.Errorf("raft: configured voter %q needs an id and an address", v.ID)
		}
	}
	return nil
}

// acceptablePeer reports whether a handshaked peer may speak the consensus
// protocol here. Members and accepted learners may; anybody else — a node that
// was removed and never learned it, or one that has not joined yet — is refused
// at the server, so it can neither cast a vote nor have its vote counted.
func (n *Node) acceptablePeer(id string) bool {
	if id == "" {
		return false
	}
	if n.IsMember(id) || n.IsLearner(id) {
		return true
	}
	// A node whose membership has not been learned yet (a fresh start) still
	// has to be able to receive the leader's messages, otherwise it could
	// never be added at a cluster it has not joined.
	return len(n.Members()) == 0
}

// selfAddr is this node's peer-plane address: the address the transport
// advertises (so a runtime-added member announces where it can be reached).
func (n *Node) selfAddr() string {
	if n.tr != nil {
		if a := n.tr.Addr(); a != "" {
			return a
		}
	}
	return n.cfg.voterAddr(n.cfg.NodeID)
}

// hasVoter reports whether id is in the current membership.
func (n *Node) hasVoter(id string) bool {
	for _, v := range n.voters {
		if v.ID == id {
			return true
		}
	}
	return false
}

// voterByID returns the member with this id.
func (n *Node) voterByID(id string) (Voter, bool) {
	for _, v := range n.voters {
		if v.ID == id {
			return v, true
		}
	}
	return Voter{}, false
}

// clients maps a peer id to its connection to that peer. The run loop owns
// membership and therefore writes it; the per-peer replicator goroutines and
// the vote path READ it on every round, so it is the one piece of transport
// state both sides touch — hence clientsMu.
func (n *Node) peerClientOf(id string) *peerClient {
	n.clientsMu.RLock()
	defer n.clientsMu.RUnlock()
	return n.clients[id]
}

// putPeerClient installs a peer's client, closing the one it replaces when the
// address changed (an address that stayed the same keeps its connections).
func (n *Node) putPeerClient(id, addr string, tr Transport) {
	n.clientsMu.Lock()
	defer n.clientsMu.Unlock()
	if c, ok := n.clients[id]; ok {
		if c.addr == addr {
			return
		}
		c.close()
	}
	n.clients[id] = newPeerClient(n.cfg.NodeID, id, addr, tr, n.cfg.Logger)
}

// rebuildClients brings the peer-client table in line with the membership
// (startup, and every committed conf change). closing lists clients already
// retired by an in-flight conf change; the rest are closed and replaced when a
// member's address moved.
func (n *Node) rebuildClients(closing map[string]bool) {
	for _, v := range n.voters {
		if v.ID == n.cfg.NodeID || closing[v.ID] {
			continue
		}
		n.putPeerClient(v.ID, v.Addr, n.tr)
	}
}

// replayConfEntries folds the conf entries present in the WAL into the voter
// set, so the in-memory membership is exactly what a replay of the same log
// would produce.
func (n *Node) replayConfEntries() error {
	for i := n.log.FirstIndex(); i <= n.log.LastIndex(); i++ {
		e, ok := n.log.Entry(i)
		if !ok || e.Kind != KindConf {
			continue
		}
		cc, err := decodeConfChange(e.Data)
		if err != nil {
			return err
		}
		n.applyConfLocally(cc, i)
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

// Term returns the current term.
func (n *Node) Term() uint64 { return n.readView().term }

// CommitIndex returns the highest committed log index.
func (n *Node) CommitIndex() uint64 { return n.readView().commit }

// AppliedIndex returns the highest applied log index.
func (n *Node) AppliedIndex() uint64 { return n.readView().applied }

// LastIndex returns the last log index.
func (n *Node) LastIndex() uint64 { return n.readView().lastIndex }

// Voters is the historical name for Members (the live voter set).
func (n *Node) Voters() []Voter { return n.Members() }

// IsMember reports whether id is in the current membership.
func (n *Node) IsMember(id string) bool {
	for _, v := range n.Voters() {
		if v.ID == id {
			return true
		}
	}
	return false
}

// IsVoter reports whether id currently counts towards quorum.
func (n *Node) IsVoter(id string) bool { return n.isVoter(id) }

// IsLearner reports whether id is an accepted non-voting member.
func (n *Node) IsLearner(id string) bool {
	n.learnerMu.RLock()
	defer n.learnerMu.RUnlock()
	for _, l := range n.learners {
		if l.ID == id {
			return true
		}
	}
	return false
}

// LeaderAddr returns the current leader's peer address, or "" when unknown.
func (n *Node) LeaderAddr() string {
	id := n.LeaderID()
	if id == "" {
		return ""
	}
	if id == n.cfg.NodeID {
		return n.tr.Addr()
	}
	if v, ok := n.voterByID(id); ok && v.Addr != "" {
		return v.Addr
	}
	return n.cfg.voterAddr(id)
}

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
		// A snapshot write in flight lands in the data directory: let it
		// finish before the caller is told the node is down (and before the
		// WAL is closed under it).
		n.snapWG.Wait()
		n.clientsMu.Lock()
		for _, c := range n.clients {
			c.close()
		}
		n.clientsMu.Unlock()
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
