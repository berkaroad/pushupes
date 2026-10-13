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

// Layout feed: how a data-plane node (role=learner) gets the replicated
// metadata without voting (DESIGN §12).
//
// The learner holds slot replicas but runs no Raft group, so it cannot read
// the log itself and must not append to it. What it needs is exactly the
// state machine's input: the committed commands, in index order. This file is
// the serving half — one subscription per connected learner, driven by the
// consensus layer's heartbeat tick, each pushed its own frames from a bounded
// buffer.
//
// Two rules shape it:
//
//   - A slow learner is cut, never buffered without bound. The feed is
//     best-effort by construction: the learner reconnects, reports what it
//     applied, and is either continued incrementally or handed an image
//     (LayoutSlice decides). So the design target is "a stalled data node
//     costs the control plane nothing", not "the feed never loses a frame".
//   - Every frame is idempotent for the receiver. A duplicate COMMIT or a
//     replayed ENTRY must be a no-op: the learner keys on index, and the
//     commands are deterministic state (set/delete-shaped), so re-applying one
//     yields the same table.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/berkaroad/pushupes/internal/raft"
)

// Frame kinds. The numbers are the wire values (proto/pushupes/v1/peer.proto,
// LayoutFrame.kind) — the feed carries them as uint32 rather than a proto enum
// so both sides stay free to grow.
const (
	layoutHello uint32 = iota
	layoutEntry
	layoutCommit
	layoutSnapshot
	layoutRedirect
)

// layoutBufFrames bounds one subscriber's buffer, in frames. A tick pushes at
// most a batch of entries plus a commit, so this holds several ticks of
// backlog: enough to absorb a learner that is merely busy, small enough that a
// learner which stopped reading is cut (and re-syncs) instead of growing the
// leader's heap.
const layoutBufFrames = 256

// maxLayoutBatch bounds how many entries one fill sends a subscriber: the
// same order as the consensus layer's append batch, because the learner is
// asked to keep up with the same log the voters do.
const maxLayoutBatch = 64

// errSlowSubscriber marks a subscriber dropped for not draining its buffer.
var errSlowSubscriber = errors.New("layout feed: subscriber too slow")

// layoutFrame is one feed step (the in-process form of the wire LayoutFrame).
type layoutFrame struct {
	Kind        uint32
	Index       uint64
	Term        uint64
	Data        []byte
	CommitIndex uint64
	LeaderID    string
	LeaderPeer  string
	Topology    string
	Voters      []raft.Voter
	FirstIndex  uint64
	EntryKind   uint8
	Reason      string
}

// layoutRequest is a learner's resume point.
type layoutRequest struct {
	LearnerID string
	FromIndex uint64
	FromTerm  uint64
	Topology  string
}

// layoutSub is one connected learner's feed slot.
type layoutSub struct {
	id     string
	ch     chan *layoutFrame
	done   chan struct{}
	closer sync.Once

	// mu guards the cursor and everything derived from it: the serving
	// goroutine fills once up front, the tick loop fills afterwards.
	mu sync.Mutex
	// cursor is the next index to send, term is the term of the index last
	// sent (the pair the next pull is validated against).
	cursor uint64
	term   uint64
	// voters/topology are what this subscriber was last told: the tick
	// re-announces them when the group's configuration moves under it.
	voters   []raft.Voter
	topology string
	// live gates the tick loop: a subscriber is fed by ticks only after the
	// serving goroutine has sent its HELLO, so no frame can precede it.
	live bool
	// sentEmptyImage records that this subscriber was told to rebuild from
	// nothing. Repeating it would reset the learner every tick, so a second
	// request for the same point is an error the caller must see.
	sentEmptyImage bool
}

// push queues one frame without ever blocking the leader: the buffer is
// bounded and a full one means the subscriber is gone or too slow to serve.
func (s *layoutSub) push(f *layoutFrame) bool {
	select {
	case <-s.done:
		return false
	default:
	}
	select {
	case s.ch <- f:
		return true
	case <-s.done:
		return false
	default:
		return false
	}
}

func (s *layoutSub) close() { s.closer.Do(func() { close(s.done) }) }

// layoutHub owns every subscriber on this node. It exists on all nodes (the
// serving path answers non-leaders with a redirect, so a follower's hub never
// sees a subscriber), and runs off the same interval as the consensus
// heartbeat: the feed is not a second scheduler.
type layoutHub struct {
	e        *Engine
	interval time.Duration

	mu   sync.Mutex
	subs map[string]*layoutSub
}

func newLayoutHub(e *Engine) *layoutHub {
	// "Share the heartbeat interval" (DESIGN §12.4): a learner falling behind
	// by one heartbeat is a learner whose data node is one heartbeat stale,
	// which is the resolution the failure detection already works at. A
	// separate, faster cadence would buy nothing and cost the control plane
	// CPU on every node.
	interval := e.node.Cfg().HeartbeatTimeout
	if interval <= 0 {
		interval = time.Second
	}
	return &layoutHub{e: e, interval: interval, subs: map[string]*layoutSub{}}
}

// run drives the feed until ctx ends.
func (h *layoutHub) run(ctx context.Context) {
	t := time.NewTicker(h.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			h.closeAll("node is shutting down")
			return
		case <-t.C:
			h.pump()
		}
	}
}

// pump advances every subscriber by one round: new entries, an image when the
// log can no longer serve its cursor, a commit for progress, and a fresh HELLO
// when the voter set or the cluster generation moved.
func (h *layoutHub) pump() {
	if !h.e.node.IsLeader() {
		// Leadership moved: the frames only the leader can produce (entries,
		// commit) stop making sense here. Ending the streams is what makes the
		// learners re-dial and land on the new leader instead of holding a
		// silent stream that looks healthy.
		h.closeAll("this node is no longer the controller")
		return
	}
	for _, s := range h.snapshotSubs() {
		if err := h.fill(s); err != nil {
			h.e.loggerf("layout feed: dropping %s: %v", s.id, err)
			h.detach(s)
			s.close()
		}
	}
}

// serve is the WatchLayout handler's core: HELLO, the initial catch-up, then
// the tick loop's frames for as long as the caller reads.
func (h *layoutHub) serve(ctx context.Context, req layoutRequest, send func(*layoutFrame) error) error {
	if !h.e.node.IsLeader() {
		// A follower's answer must be a direction, not a quiet stream: the
		// learner would otherwise sit behind an idle-looking feed.
		return send(&layoutFrame{
			Kind:       layoutRedirect,
			LeaderID:   h.e.node.LeaderID(),
			LeaderPeer: h.e.node.LeaderPeerAddr(),
			Reason:     "this node is not the controller",
		})
	}
	// Defence in depth on the resume point: a caller whose bytes belong to
	// another cluster generation must not be CONTINUED, even if its index and
	// term happen to line up with this group's (a rebuilt cluster's election
	// terms start over, so "index 5 term 1" is not by itself evidence of
	// anything). The learner reaches the same conclusion on its own when it
	// sees HELLO's generation (learnernode.go); enforcing it here means an old
	// or misbehaving data node cannot poison its table either.
	if req.Topology != "" && req.Topology != h.e.topologyID() {
		h.e.loggerf("layout feed: %s resumes from generation %s but this cluster is %s; rebuilding it",
			req.LearnerID, req.Topology, h.e.topologyID())
		req.FromIndex, req.FromTerm = 0, 0
	}
	s := h.attach(req)
	defer h.detach(s)

	// One cheap pull (a single entry) to report the floor and the commit
	// horizon in HELLO; the initial fill right after does the real work.
	hello := &layoutFrame{
		Kind:       layoutHello,
		LeaderID:   h.e.self,
		LeaderPeer: h.e.node.Cfg().PeerAddr,
		Topology:   h.e.topologyID(),
		Voters:     h.e.node.Members(),
	}
	if sl, err := h.e.node.LayoutSlice(s.cursor-1, s.term, 1); err == nil {
		hello.Term = sl.Term
		hello.CommitIndex = sl.CommitIndex
		hello.FirstIndex = sl.FirstIndex
	}
	h.remember(s, hello)
	if err := send(hello); err != nil {
		return err
	}
	s.mu.Lock()
	s.live = true
	s.mu.Unlock()
	if err := h.fill(s); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.done:
			return nil
		case f := <-s.ch:
			if err := send(f); err != nil {
				return err
			}
		}
	}
}

// fill advances one subscriber as far as the leader's log allows in one round.
func (h *layoutHub) fill(s *layoutSub) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.live {
		return nil
	}
	if s.cursor == 0 {
		s.cursor = 1
	}
	sl, err := h.e.node.LayoutSlice(s.cursor-1, s.term, maxLayoutBatch)
	if err != nil {
		return err
	}
	if sl.Snapshot != nil {
		snap := sl.Snapshot
		if snap.Index == 0 && len(snap.Data) == 0 {
			if s.sentEmptyImage {
				return fmt.Errorf("learner asks below the log floor (%d) and the group keeps no image", sl.FirstIndex)
			}
			s.sentEmptyImage = true
		}
		if !s.push(&layoutFrame{Kind: layoutSnapshot, Index: snap.Index, Term: snap.Term, Data: snap.Data}) {
			return errSlowSubscriber
		}
		s.cursor = snap.Index + 1
		s.term = snap.Term
	} else {
		for _, en := range sl.Entries {
			if !s.push(&layoutFrame{Kind: layoutEntry, Index: en.Index, Term: en.Term, Data: en.Data, EntryKind: en.Kind}) {
				return errSlowSubscriber
			}
			s.cursor = en.Index + 1
			s.term = en.Term
		}
	}
	// The voter set and the generation ride the feed rather than the entry
	// stream: a learner's own view of who votes is display material, and the
	// generation is what tells a returning data node its copies need re-proving.
	if voters, topo := h.e.node.Members(), h.e.topologyID(); !sameVoters(voters, s.voters) || topo != s.topology {
		f := &layoutFrame{
			Kind:       layoutHello,
			LeaderID:   h.e.self,
			LeaderPeer: h.e.node.Cfg().PeerAddr,
			Topology:   topo,
			Voters:     voters,
			Term:       sl.Term,
			FirstIndex: sl.FirstIndex,
			Reason:     "configuration changed",
		}
		if !s.push(f) {
			return errSlowSubscriber
		}
		h.remember(s, f)
	}
	if !s.push(&layoutFrame{Kind: layoutCommit, CommitIndex: sl.CommitIndex, Term: sl.Term}) {
		return errSlowSubscriber
	}
	return nil
}

// remember records what this subscriber has been told.
func (h *layoutHub) remember(s *layoutSub, f *layoutFrame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f.Voters != nil {
		s.voters = f.Voters
	}
	if f.Topology != "" {
		s.topology = f.Topology
	}
}

func (h *layoutHub) attach(req layoutRequest) *layoutSub {
	s := &layoutSub{
		id:       req.LearnerID,
		ch:       make(chan *layoutFrame, layoutBufFrames),
		done:     make(chan struct{}),
		cursor:   0,
		term:     req.FromTerm,
		topology: req.Topology,
	}
	if req.FromIndex > 0 {
		s.cursor = req.FromIndex + 1
	}
	h.mu.Lock()
	if old, ok := h.subs[req.LearnerID]; ok && old != s {
		// Same node id, new stream: the previous connection is a leftover
		// (it was cut, or the process restarted). Its cursor is stale and it
		// can only confuse the two, so it goes.
		old.close()
	}
	h.subs[req.LearnerID] = s
	h.mu.Unlock()
	return s
}

func (h *layoutHub) detach(s *layoutSub) {
	h.mu.Lock()
	if cur, ok := h.subs[s.id]; ok && cur == s {
		delete(h.subs, s.id)
	}
	h.mu.Unlock()
	s.close()
}

func (h *layoutHub) snapshotSubs() []*layoutSub {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*layoutSub, 0, len(h.subs))
	for _, s := range h.subs {
		out = append(out, s)
	}
	return out
}

// closeAll ends every stream with a reason (leadership moved, shutdown).
func (h *layoutHub) closeAll(reason string) {
	h.mu.Lock()
	subs := make([]*layoutSub, 0, len(h.subs))
	for id, s := range h.subs {
		subs = append(subs, s)
		delete(h.subs, id)
	}
	h.mu.Unlock()
	if len(subs) > 0 {
		h.e.loggerf("layout feed: ending %d stream(s): %s", len(subs), reason)
	}
	for _, s := range subs {
		s.close()
	}
}

func sameVoters(a, b []raft.Voter) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID != b[i].ID || a[i].Addr != b[i].Addr {
			return false
		}
	}
	return true
}
