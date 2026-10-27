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

// The data-plane node (role=learner): it hosts slot replicas and takes the
// replicated metadata from the control plane instead of voting on it
// (DESIGN §12).
//
// It runs no Raft group, holds no log and never appears in a quorum — so it
// cannot be a leader, cannot submit, and does not decide anything. What it
// does is consume the control plane's feed (layoutfeed.go): a subscription to
// the leader's WatchLayout stream, applied in index order into the same state
// machine the voters apply to. Everything downstream — routing, fetch,
// migration, the owner fence — is the ordinary Engine, which reads a table
// that happens to arrive over a socket rather than from a log.
//
// Two properties the design leans on:
//
//   - Resume is by INDEX, not by time: the node persists the last index it
//     applied, and on reconnect asks for what follows it. The leader either
//     continues from there (its log still covers the point) or answers with an
//     image; there is no "how far behind am I" heuristic.
//   - Rebuild is not partial: when the feed cannot place the node's applied
//     point, the state machine is replaced wholesale (image) or reset to empty
//     and replayed from the log's floor. A learner never patches its metadata
//     with a delta of unknown provenance.
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/berkaroad/pushupes/internal/config"
	pushupesv1 "github.com/berkaroad/pushupes/internal/grpcapi/pushupes/v1"
	"github.com/berkaroad/pushupes/internal/raft"
)

const (
	// layoutFeedFile remembers what this node applied and which cluster
	// generation it belongs to. It lives in the node's data dir (not in a
	// consensus dir — a learner has none): without it every restart would ask
	// for a full image, and the generation is the input the owner fence needs
	// to tell "my bytes, from this cluster" from "my bytes, from one that was
	// rebuilt underneath me".
	layoutFeedFile = "layout-feed.json"

	// layoutStateFile is the state machine's bytes at the index layoutFeedFile
	// records, kept as a CACHE of the derived state — a learner's table is a
	// function of the control plane's log, so the pair (index, bytes) is what
	// makes a restart resumable at all.
	//
	// It has to exist: an index without the state it indexes is not a resume
	// point. A learner that remembered only the index would come back with an
	// empty table and apply the NEXT entry onto it (observed live: a restarted
	// data node failed "mark_down: unknown peer node-4" in a loop, because the
	// command that admitted it lived below the index and had never been
	// replayed). The bytes are gzipped table data — a few KB — so the cache
	// costs one small write per flush.
	layoutStateFile = "layout-state.bin"
)

// layoutFeedState is the on-disk resume point: the index (in the JSON file)
// plus, in its own file, the state machine's bytes at that index.
type layoutFeedState struct {
	Topology     string `json:"topology,omitempty"`
	AppliedIndex uint64 `json:"applied_index"`
	AppliedTerm  uint64 `json:"applied_term"`
	Leader       string `json:"leader,omitempty"`

	// StateBytes is written to layoutStateFile, not into the JSON.
	StateBytes []byte `json:"-"`
}

// learnerNode is the consensus implementation for a data-plane node: same
// surface as the Raft-backed node, answered from the control plane's feed.
type learnerNode struct {
	cfg     Config
	applier Applier
	logger  *slog.Logger

	mu          sync.Mutex
	leaderID    string
	leaderPeer  string
	term        uint64
	commit      uint64
	applied     uint64
	appliedTerm uint64
	topology    string
	voters      []raft.Voter

	// flushed tracks the durable resume point so it is written on progress
	// rather than on every apply.
	flushedIndex uint64
	flushedAt    time.Time
	stop         chan struct{}
	done         chan struct{}
	stopOnce     sync.Once
}

func newLearnerNode(cfg Config, applier Applier, logger *slog.Logger) (*learnerNode, error) {
	l := &learnerNode{
		cfg:     cfg,
		applier: applier,
		logger:  logger,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	// Before the first frame the directory may be all this node has: the
	// configured seed peers are voters by construction (they are what the node
	// was told to follow), so Membership and PeerAddrs have an honest answer
	// from t=0 instead of an empty one that reads as "no cluster".
	for _, p := range cfg.Peers {
		l.voters = append(l.voters, raft.Voter{ID: p.ID, Addr: HostPort(p.PeerAddr)})
	}
	sort.Slice(l.voters, func(i, j int) bool { return l.voters[i].ID < l.voters[j].ID })
	if err := l.loadState(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *learnerNode) loadState() error {
	if l.cfg.DataDir == "" {
		return nil
	}
	raw, err := os.ReadFile(filepath.Join(l.cfg.DataDir, layoutFeedFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("cluster: read %s: %w", layoutFeedFile, err)
	}
	var st layoutFeedState
	if err := json.Unmarshal(raw, &st); err != nil {
		return fmt.Errorf("cluster: decode %s: %w", layoutFeedFile, err)
	}
	l.topology, l.applied, l.appliedTerm = st.Topology, st.AppliedIndex, st.AppliedTerm
	l.flushedIndex, l.flushedAt = st.AppliedIndex, time.Now()

	// The state cache is only good for the index it was written at. A missing
	// or mismatched file leaves the resume point unusable: the node then asks
	// to be rebuilt wholesale rather than continuing onto a table it cannot
	// account for.
	if st.AppliedIndex == 0 {
		return nil
	}
	statePath := filepath.Join(l.cfg.DataDir, layoutStateFile)
	cached, err := os.ReadFile(statePath)
	if err != nil || len(cached) == 0 {
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			l.warn("layout feed: read cached state: %v", err)
		}
		l.forgetResumePoint()
		return nil
	}
	if err := l.applier.RestoreState(cached); err != nil {
		l.warn("layout feed: cached state at index %d is unusable (%v); rebuilding from the control plane", st.AppliedIndex, err)
		l.forgetResumePoint()
		return nil
	}
	l.info("layout feed: resumed from cached state at index=%d term=%d", st.AppliedIndex, st.AppliedTerm)
	return nil
}

// forgetResumePoint drops the resume point and empties the state machine, in
// that order's only safe direction: (index 0, empty table) is a pair a rebuild
// can always start from, whereas an index whose state was thrown away is not a
// resume point at all.
func (l *learnerNode) forgetResumePoint() {
	l.applied, l.appliedTerm, l.flushedIndex = 0, 0, 0
	if err := l.applier.ResetState(); err != nil {
		l.warn("layout feed: reset state: %v", err)
	}
}

func (l *learnerNode) saveState(force bool) {
	l.mu.Lock()
	if !force && l.applied < l.flushedIndex+config.LayoutApplyFlush && time.Since(l.flushedAt) < 2*time.Second {
		l.mu.Unlock()
		return
	}
	st := layoutFeedState{
		Topology:     l.topology,
		AppliedIndex: l.applied,
		AppliedTerm:  l.appliedTerm,
		Leader:       l.leaderID,
	}
	// Bytes and index go out together, and the asymmetry between the two
	// orderings is what makes a torn write safe: state-behind-index (the write
	// order below guarantees the index never leads the bytes) would resume a
	// node that is missing applied entries; state-AHEAD-of-index can only
	// happen if an apply lands between the snapshot and the index read (Close
	// racing the feed loop), and re-applying those entries is harmless — every
	// command is a deterministic set-shaped mutation.
	if st.AppliedIndex > 0 {
		var err error
		if st.StateBytes, err = l.applier.SnapshotState(); err != nil {
			l.warn("layout feed: %v", err)
		}
	}
	l.flushedIndex, l.flushedAt = l.applied, time.Now()
	l.mu.Unlock()
	if l.cfg.DataDir == "" {
		return
	}
	raw, err := json.Marshal(st)
	if err != nil {
		return
	}
	// State first, index second: a crash between the two leaves bytes that
	// belong to an EARLIER index than the file claims, and the loader treats
	// that as unusable (a rebuild) — never as a valid continuation.
	if st.AppliedIndex > 0 && st.StateBytes != nil {
		if err := writeFileAtomic(filepath.Join(l.cfg.DataDir, layoutStateFile), st.StateBytes, 0o644); err != nil {
			l.warn("layout feed: persist state: %v", err)
			return
		}
	}
	if err := writeFileAtomic(filepath.Join(l.cfg.DataDir, layoutFeedFile), raw, 0o644); err != nil {
		l.warn("layout feed: persist resume point: %v", err)
	}
}

// writeFileAtomic writes through a temp file + rename, so a reader (a restart)
// never sees a half-written state or index.
func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, mode); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (l *learnerNode) warn(f string, args ...any) {
	if l.logger != nil {
		l.logger.Warn(fmt.Sprintf(f, args...))
	}
}

func (l *learnerNode) info(f string, args ...any) {
	if l.logger != nil {
		l.logger.Info(fmt.Sprintf(f, args...))
	}
}

// ---- the subscription loop -------------------------------------------------

// start runs the feed until Close. It is started by NewNode, before the
// Engine's own loops: applying table commands into the state machine is
// exactly what a restarted voter's log replay does, and the Engine's loops
// reconcile from whatever table they find on their first pass.
func (l *learnerNode) start() { go l.run() }

func (l *learnerNode) run() {
	defer close(l.done)
	attempt := 0
	for {
		select {
		case <-l.stop:
			return
		default:
		}
		startedAt := time.Now()
		err := l.subscribe()
		select {
		case <-l.stop:
			return
		default:
		}
		if err != nil {
			l.warn("layout feed: subscription ended: %v", err)
		}
		if time.Since(startedAt) >= config.LayoutArchiveReset {
			attempt = 0
		}
		attempt++
		select {
		case <-l.stop:
			return
		case <-time.After(reconnectDelay(attempt)):
		}
	}
}

// reconnectDelay backs off to two seconds: a learner whose control plane is
// down must not spin, and one whose leader just moved must land quickly.
func reconnectDelay(attempt int) time.Duration {
	d := 100 * time.Millisecond << uint(min(attempt-1, 4))
	if d > 2*time.Second {
		d = 2 * time.Second
	}
	return d
}

// subscribe tries each target once. A target that answers with a redirect
// updates where the next attempt goes, so a moved leader costs one round trip.
func (l *learnerNode) subscribe() error {
	var firstErr error
	for _, addr := range l.dialTargets() {
		startedAt := time.Now()
		err := l.stream(addr)
		if err == nil {
			return nil
		}
		if firstErr == nil {
			firstErr = err
		}
		// A stream that ran long enough to be useful tells us something about
		// the group, not about this address: go back to the outer loop (which
		// re-derives the target list, including a new leader if we learned
		// one) rather than walking the rest of the seed list.
		if time.Since(startedAt) >= config.LayoutArchiveReset {
			return err
		}
		select {
		case <-l.stop:
			return nil
		default:
		}
	}
	if firstErr == nil {
		firstErr = errors.New("no control-plane address to follow")
	}
	return firstErr
}

// dialTargets is the ordered attempt list: the leader we last heard from
// first (it is the one that serves the feed), then the configured seeds.
func (l *learnerNode) dialTargets() []string {
	l.mu.Lock()
	leader := l.leaderPeer
	l.mu.Unlock()
	var out []string
	seen := map[string]bool{}
	add := func(a string) {
		a = HostPort(a)
		if a == "" || seen[a] {
			return
		}
		seen[a] = true
		out = append(out, a)
	}
	add(leader)
	for _, p := range l.cfg.Peers {
		add(p.PeerAddr)
	}
	return out
}

// stream follows one target until it ends. A nil return means this node is
// shutting down; every other outcome is reported to the caller as an error, so
// the loop's backoff covers it.
func (l *learnerNode) stream(addr string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-l.stop:
			cancel()
		case <-ctx.Done():
		}
	}()

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	l.mu.Lock()
	req := &pushupesv1.WatchLayoutRequest{
		LearnerId: l.cfg.NodeID,
		FromIndex: l.applied,
		FromTerm:  l.appliedTerm,
		Topology:  l.topology,
	}
	l.mu.Unlock()

	stream, err := pushupesv1.NewPeerServiceClient(conn).WatchLayout(ctx, req)
	if err != nil {
		return fmt.Errorf("watch layout at %s: %w", addr, err)
	}
	// The dial budget covers only the wait for the FIRST frame: an unreachable
	// address must not park this node forever, while an idle cluster
	// legitimately sends nothing but its heartbeat commits and must NOT be cut
	// for silence. A timer that cancels the stream before anything arrives is
	// the shape that expresses both (canceling after the first frame — the
	// obvious-looking version — tears down the very stream that just proved
	// the address works).
	dialTimer := time.AfterFunc(config.LayoutDialTimeout, cancel)
	first := true
	for {
		f, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("layout feed from %s: %w", addr, err)
		}
		if first {
			first = false
			dialTimer.Stop()
		}
		if err := l.onFrame(f); err != nil {
			return fmt.Errorf("layout feed from %s: %w", addr, err)
		}
	}
}

// errResubscribe ends the stream on purpose: the next attempt must open a new
// one (a generation change resets the resume point, and the running stream's
// cursor belongs to the old one).
var errResubscribe = errors.New("resubscribe")

func (l *learnerNode) onFrame(f *pushupesv1.LayoutFrame) error {
	switch f.GetKind() {
	case layoutHello:
		return l.onHello(f)
	case layoutRedirect:
		addr := HostPort(f.GetLeaderPeerAddr())
		l.mu.Lock()
		l.leaderID, l.leaderPeer = f.GetLeaderId(), addr
		l.mu.Unlock()
		return fmt.Errorf("%w: not the controller, leader is %s (%s)", errResubscribe, f.GetLeaderId(), addr)
	case layoutSnapshot:
		return l.onSnapshot(f)
	case layoutEntry:
		return l.onEntry(f)
	case layoutCommit:
		l.mu.Lock()
		if f.GetCommitIndex() > l.commit {
			l.commit = f.GetCommitIndex()
		}
		if f.GetTerm() > l.term {
			l.term = f.GetTerm()
		}
		l.mu.Unlock()
		return nil
	default:
		return fmt.Errorf("layout feed: unknown frame kind %d", f.GetKind())
	}
}

func (l *learnerNode) onHello(f *pushupesv1.LayoutFrame) error {
	l.mu.Lock()
	prevTopology, applied := l.topology, l.applied
	l.leaderID, l.leaderPeer = f.GetLeaderId(), HostPort(f.GetLeaderPeerAddr())
	if f.GetTerm() > l.term {
		l.term = f.GetTerm()
	}
	if f.GetCommitIndex() > l.commit {
		l.commit = f.GetCommitIndex()
	}
	if vs := f.GetVoters(); len(vs) > 0 {
		out := make([]raft.Voter, 0, len(vs))
		for _, v := range vs {
			out = append(out, raft.Voter{ID: v.GetId(), Addr: v.GetAddr()})
		}
		l.voters = out
	}
	topology := f.GetTopology()
	changed := topology != "" && topology != prevTopology
	if changed {
		l.topology = topology
		// The generation moved under us: whatever this node applied belongs to
		// the cluster that was rebuilt away. Resetting the state machine is
		// what makes the resume honest — a replay of the new log onto the old
		// table would be silently wrong wherever an op is not a plain set.
		l.applied, l.appliedTerm = 0, 0
	}
	l.mu.Unlock()

	if changed {
		if prevTopology != "" || applied > 0 {
			if err := l.applier.ResetState(); err != nil {
				return fmt.Errorf("reset state after generation change: %w", err)
			}
			l.saveState(true)
			l.info("layout feed: cluster generation is now %s (was %q); local layout reset, re-reading from the log floor", topology, prevTopology)
			return errResubscribe
		}
		l.saveState(true)
	}
	return nil
}

func (l *learnerNode) onSnapshot(f *pushupesv1.LayoutFrame) error {
	var err error
	if len(f.GetData()) == 0 {
		// "Nothing to install" is a real answer: the control plane cannot place
		// this node's progress, so the state machine starts empty and the log
		// is replayed from its floor.
		err = l.applier.ResetState()
	} else {
		err = l.applier.RestoreState(f.GetData())
	}
	if err != nil {
		return fmt.Errorf("install layout image at %d: %w", f.GetIndex(), err)
	}
	l.mu.Lock()
	l.applied, l.appliedTerm = f.GetIndex(), f.GetTerm()
	if f.GetTerm() > l.term {
		l.term = f.GetTerm()
	}
	l.mu.Unlock()
	l.saveState(true)
	l.info("layout feed: installed image at index=%d term=%d bytes=%d", f.GetIndex(), f.GetTerm(), len(f.GetData()))
	return nil
}

func (l *learnerNode) onEntry(f *pushupesv1.LayoutFrame) error {
	l.mu.Lock()
	applied := l.applied
	l.mu.Unlock()
	switch {
	case f.GetIndex() <= applied:
		// Already applied (a replay after a lost resume point): commands are
		// deterministic, so re-applying would be harmless — skipping is just
		// cheaper and keeps the index monotone.
		return nil
	case f.GetIndex() != applied+1:
		// A gap is a transport-level loss, not a divergence: the state at
		// `applied` is still the right one, so the stream just ends and the
		// next attempt asks to continue from there. (A DIVERGENT history is a
		// different answer — the leader sees the term mismatch and sends an
		// image; see layoutfeed.go.)
		return fmt.Errorf("layout feed: entry %d arrives with %d applied (gap)", f.GetIndex(), applied)
	}
	switch uint8(f.GetEntryKind()) {
	case raft.KindCommand:
		if _, err := l.applier.ApplyCommand(f.GetData()); err != nil {
			// A command that cannot be applied says the local state is not the
			// state this entry was written against. Retrying the same
			// continuation would fail identically forever, so the resume point
			// goes and the next attempt rebuilds from the control plane.
			l.forgetResumePoint()
			return fmt.Errorf("apply layout entry %d: %w", f.GetIndex(), err)
		}
	case raft.KindNoop, raft.KindConf:
		// A no-op carries no state, and the voter set is not this node's to
		// hold: it arrives in the HELLO frames (a learner's Members() view).
	default:
		return fmt.Errorf("layout feed: entry %d has unknown kind %d", f.GetIndex(), f.GetEntryKind())
	}
	l.mu.Lock()
	l.applied, l.appliedTerm = f.GetIndex(), f.GetTerm()
	l.mu.Unlock()
	l.saveState(false)
	return nil
}

// ---- the consensus surface -------------------------------------------------

// Apply refuses: a data-plane node does not submit. The Engine treats this as
// "not the controller" (it is the same answer a follower gives), so every
// leader-only path — directory sync, membership change, plan submission —
// takes its non-leader branch here.
func (l *learnerNode) Apply(cmd []byte) ([]byte, error) { return nil, ErrNotLeader }

func (l *learnerNode) IsLeader() bool { return false }

func (l *learnerNode) LeaderID() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.leaderID
}

func (l *learnerNode) LeaderPeerAddr() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return HostPort(l.leaderPeer)
}

func (l *learnerNode) PeerAddrs() map[string]string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]string, len(l.voters))
	for _, v := range l.voters {
		out[v.ID] = v.Addr
	}
	return out
}

func (l *learnerNode) Members() []raft.Voter {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]raft.Voter(nil), l.voters...)
}

func (l *learnerNode) IsMember(id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, v := range l.voters {
		if v.ID == id {
			return true
		}
	}
	return false
}

func (l *learnerNode) AddMember(id, addr string, timeout time.Duration) error {
	return ErrNotLeader
}

func (l *learnerNode) RemoveMember(id string, timeout time.Duration) error {
	return ErrNotLeader
}

func (l *learnerNode) UpdatePeer(id, addr string, timeout time.Duration) error {
	return ErrNotLeader
}

func (l *learnerNode) Cfg() Config { return l.cfg }

// TopologyID is the generation this node's replicated metadata belongs to: the
// control plane's id, learned from the feed. The owner fence keys on it, so a
// data node that kept its slot data across a rebuilt consensus state sees its
// copies as unproven exactly as a voter would.
func (l *learnerNode) TopologyID() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.topology
}

func (l *learnerNode) WaitForLeader(timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		l.mu.Lock()
		id := l.leaderID
		l.mu.Unlock()
		if id != "" {
			return id, nil
		}
		if time.Now().After(deadline) {
			return "", errors.New("cluster: no controller yet")
		}
		select {
		case <-l.stop:
			return "", errors.New("cluster: closed")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// Stats reports the same keys the Raft node does (the Engine and the admin
// status read them by name), with the two indices carrying their learner
// meaning: commit is the leader's horizon as last heard, applied is this
// node's own progress.
func (l *learnerNode) Stats() map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	return map[string]any{
		"state":           "learner",
		"leader":          l.leaderID,
		"term":            l.term,
		"applied-index":   l.applied,
		"applied_index":   l.applied,
		"commit-index":    l.commit,
		"commit_index":    l.commit,
		"last_log_index":  l.applied,
		"layout_topology": l.topology,
	}
}

// LayoutSlice is a control-plane primitive; a data-plane node has no log to
// slice. It answers the error rather than an empty slice so a caller that
// mistook this node for a controller fails loudly instead of feeding learners
// a permanently empty layout.
func (l *learnerNode) LayoutSlice(from, fromTerm uint64, max int) (raft.LayoutSlice, error) {
	return raft.LayoutSlice{}, errors.New("cluster: layout feed is served by the control plane, not by a data-plane node")
}

func (l *learnerNode) Close() error {
	l.stopOnce.Do(func() { close(l.stop) })
	select {
	case <-l.done:
	case <-time.After(2 * time.Second):
		l.warn("layout feed: shutdown timed out waiting for the subscription to end")
	}
	l.saveState(true)
	return nil
}
