package cluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	pushupesv1 "github.com/berkaroad/pushupes/internal/grpcapi/pushupes/v1"
	"github.com/berkaroad/pushupes/internal/raft"
)

// fakeControl is a stand-in for the consensus node that happens to be the
// controller: it serves whatever slice the test hands it, so the feed's
// ordering, buffer and redirect rules can be exercised without a Raft group.
type fakeControl struct {
	*learnerNode // the rest of the consensus surface, unused here
	leader       bool
	entries      []raft.Entry
	snapshot     *raft.LayoutSnapshot
	err          error
	firstIndex   uint64
	commit       uint64
}

func newFakeControl(t *testing.T, entries ...raft.Entry) *fakeControl {
	t.Helper()
	ln, err := newLearnerNode(Config{NodeID: "ctrl"}, &fakeApplier{}, nil)
	if err != nil {
		t.Fatalf("learner node: %v", err)
	}
	last := uint64(0)
	if len(entries) > 0 {
		last = entries[len(entries)-1].Index
	}
	return &fakeControl{
		learnerNode: ln,
		leader:      true,
		entries:     entries,
		commit:      last,
		firstIndex:  1,
	}
}

func (f *fakeControl) IsLeader() bool { return f.leader }

func (f *fakeControl) LayoutSlice(from, fromTerm uint64, max int) (raft.LayoutSlice, error) {
	if f.err != nil {
		return raft.LayoutSlice{}, f.err
	}
	out := raft.LayoutSlice{Term: 1, CommitIndex: f.commit, FirstIndex: f.firstIndex}
	if f.snapshot != nil {
		out.Snapshot = f.snapshot
		return out, nil
	}
	for _, e := range f.entries {
		if e.Index <= from {
			continue
		}
		if len(out.Entries) >= max {
			break
		}
		out.Entries = append(out.Entries, e)
	}
	return out, nil
}

func testEntries(n int) []raft.Entry {
	out := make([]raft.Entry, 0, n)
	for i := 1; i <= n; i++ {
		out = append(out, raft.Entry{Index: uint64(i), Term: 1, Kind: raft.KindCommand, Data: []byte(fmt.Sprintf("cmd-%d", i))})
	}
	return out
}

func newTestHub(t *testing.T, ctl consensus) *layoutHub {
	t.Helper()
	return &layoutHub{e: &Engine{node: ctl, self: "ctrl", logger: nil}, interval: 10 * time.Millisecond, subs: map[string]*layoutSub{}}
}

// A subscriber is fed the entries after the point it reported, in index order,
// and nothing before its HELLO: the learner's apply loop is strict, so a frame
// out of order would be a resync rather than a harmless reorder.
func TestLayoutFeedServesEntriesInIndexOrder(t *testing.T) {
	ctl := newFakeControl(t, testEntries(4)...)
	h := newTestHub(t, ctl)

	var frames []*layoutFrame
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- h.serve(ctx, layoutRequest{LearnerID: "node-9", FromIndex: 2, FromTerm: 1}, func(f *layoutFrame) error {
			frames = append(frames, f)
			if len(frames) == 3 { // HELLO, ENTRY 3, ENTRY 4 (COMMIT follows)
				cancel()
			}
			return nil
		})
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("serve did not end")
	}

	if len(frames) == 0 || frames[0].Kind != layoutHello {
		t.Fatalf("first frame = %+v, want HELLO", frames[0])
	}
	var got []uint64
	for _, f := range frames[1:] {
		if f.Kind == layoutEntry {
			got = append(got, f.Index)
		}
	}
	if len(got) != 2 || got[0] != 3 || got[1] != 4 {
		t.Fatalf("entries served = %v, want [3 4] (the caller applied 2)", got)
	}
}

// A follower's answer must be a direction, not a quiet stream: a learner
// parked on an idle-looking follower would never reach the data.
func TestLayoutFeedRedirectsAwayFromAFollower(t *testing.T) {
	ctl := newFakeControl(t)
	ctl.leader = false
	ln, _ := newLearnerNode(Config{NodeID: "ctrl"}, &fakeApplier{}, nil)
	ln.mu.Lock()
	ln.leaderID, ln.leaderPeer = "node-1", "127.0.0.1:9691"
	ln.mu.Unlock()
	ctl.learnerNode = ln
	h := newTestHub(t, ctl)

	var frames []*layoutFrame
	err := h.serve(context.Background(), layoutRequest{LearnerID: "node-9"}, func(f *layoutFrame) error {
		frames = append(frames, f)
		return nil
	})
	if err != nil {
		t.Fatalf("serve: %v", err)
	}
	if len(frames) != 1 || frames[0].Kind != layoutRedirect {
		t.Fatalf("frames = %d (%+v), want exactly one redirect", len(frames), frames[0])
	}
	if frames[0].LeaderID != "node-1" || frames[0].LeaderPeer == "" {
		t.Fatalf("redirect = %+v, want the leader's id and address", frames[0])
	}
}

// A learner that stops reading is cut, never buffered without bound: its
// backlog is what makes the control plane's memory a function of one data
// node's health, and a re-sync costs it one reconnect.
func TestLayoutFeedCutsASlowSubscriber(t *testing.T) {
	ctl := newFakeControl(t, testEntries(layoutBufFrames+maxLayoutBatch+10)...)
	h := newTestHub(t, ctl)
	s := h.attach(layoutRequest{LearnerID: "node-9"})
	s.mu.Lock()
	s.live = true
	s.mu.Unlock()

	var lastErr error
	for i := 0; i < 12; i++ {
		if err := h.fill(s); err != nil {
			lastErr = err
			break
		}
	}
	if lastErr != errSlowSubscriber {
		t.Fatalf("fill ended with %v, want the slow-subscriber drop once %d frames are queued", lastErr, layoutBufFrames)
	}
	// Nobody is draining: the buffer must be full and the frames must stay
	// bounded (that bound is the whole point of the drop).
	if len(s.ch) > layoutBufFrames {
		t.Fatalf("queued %d frames, buffer is %d", len(s.ch), layoutBufFrames)
	}
}

// "Rebuild from nothing" is a legitimate answer exactly once per connection:
// repeating it would reset the learner's state machine every tick.
func TestLayoutFeedRefusesToRepeatAnEmptyImage(t *testing.T) {
	ctl := newFakeControl(t)
	ctl.snapshot = &raft.LayoutSnapshot{Index: 0, Term: 0}
	h := newTestHub(t, ctl)
	s := h.attach(layoutRequest{LearnerID: "node-9"})
	s.mu.Lock()
	s.live = true
	s.mu.Unlock()

	if err := h.fill(s); err != nil {
		t.Fatalf("first fill: %v", err)
	}
	// Drain the first image + commit so the buffer is not what fails us.
	for len(s.ch) > 0 {
		<-s.ch
	}
	if err := h.fill(s); err == nil {
		t.Fatal("a second empty image must be refused (it would reset the learner every tick)")
	}
}

// ---- the learner half -------------------------------------------------------

type fakeApplier struct {
	applied  []string
	restored [][]byte
	resets   int
	// state is what SnapshotState hands back (the cached state bytes).
	state []byte
	// failOn makes ApplyCommand fail for one command: a deterministic apply
	// error, i.e. the local state is not the one the entry was written for.
	failOn string
}

func (f *fakeApplier) ApplyCommand(cmd []byte) ([]byte, error) {
	if f.failOn != "" && string(cmd) == f.failOn {
		return nil, fmt.Errorf("nope: %s", cmd)
	}
	f.applied = append(f.applied, string(cmd))
	return nil, nil
}
func (f *fakeApplier) SnapshotState() ([]byte, error) { return f.state, nil }
func (f *fakeApplier) RestoreState(data []byte) error {
	f.restored = append(f.restored, data)
	return nil
}
func (f *fakeApplier) ResetState() error { f.resets++; return nil }

func newTestLearner(t *testing.T, dir string, ap Applier) *learnerNode {
	t.Helper()
	ln, err := newLearnerNode(Config{NodeID: "node-9", DataDir: dir}, ap, nil)
	if err != nil {
		t.Fatalf("learner node: %v", err)
	}
	return ln
}

func entryFrame(idx uint64, cmd string) *pushupesv1.LayoutFrame {
	return &pushupesv1.LayoutFrame{Kind: layoutEntry, Index: idx, Term: 1, EntryKind: uint32(raft.KindCommand), Data: []byte(cmd)}
}

// The apply loop is strict about indices: a replay is a no-op (cheap and
// since the same command would be harmless anyway), a gap is a resync.
func TestLearnerAppliesInOrderAndSkipsReplays(t *testing.T) {
	ap := &fakeApplier{}
	ln := newTestLearner(t, t.TempDir(), ap)

	if err := ln.onFrame(entryFrame(1, "one")); err != nil {
		t.Fatalf("entry 1: %v", err)
	}
	if err := ln.onFrame(entryFrame(1, "one")); err != nil {
		t.Fatalf("replayed entry 1 must be skipped, not fail: %v", err)
	}
	if err := ln.onFrame(entryFrame(3, "three")); err == nil {
		t.Fatal("a gap (1 applied, 3 arrives) must end the stream so it re-syncs")
	}
	if err := ln.onFrame(entryFrame(2, "two")); err != nil {
		t.Fatalf("entry 2: %v", err)
	}
	if got := ap.applied; len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("applied = %v, want [one two] exactly once each", got)
	}
	if st := ln.Stats(); st["applied-index"] != uint64(2) {
		t.Fatalf("applied-index = %v, want 2", st["applied-index"])
	}
}

// The generation id is how a data node that kept its slot data learns that the
// cluster around it was rebuilt: the state machine must go, not be patched.
func TestLearnerGenerationChangeResetsTheStateMachine(t *testing.T) {
	dir := t.TempDir()
	ap := &fakeApplier{}
	ln := newTestLearner(t, dir, ap)
	ln.topology, ln.applied, ln.appliedTerm = "gen-a", 12, 3
	ln.saveState(true)

	err := ln.onFrame(&pushupesv1.LayoutFrame{Kind: layoutHello, LeaderId: "node-1", LeaderPeerAddr: "127.0.0.1:1", Topology: "gen-b"})
	if err == nil {
		t.Fatal("a generation change must end the stream and re-subscribe from scratch")
	}
	if ap.resets != 1 {
		t.Fatalf("ResetState called %d times, want 1", ap.resets)
	}
	st := ln.Stats()
	if st["applied-index"] != uint64(0) || st["layout_topology"] != "gen-b" {
		t.Fatalf("after the switch: applied=%v topology=%v, want 0 and gen-b", st["applied-index"], st["layout_topology"])
	}
	// The new resume point is durable: a restart asks for the new generation.
	again := newTestLearner(t, dir, ap)
	if got := again.TopologyID(); got != "gen-b" {
		t.Fatalf("persisted topology = %q, want gen-b", got)
	}
}

func TestLearnerInstallsImages(t *testing.T) {
	ap := &fakeApplier{}
	ln := newTestLearner(t, t.TempDir(), ap)

	if err := ln.onFrame(&pushupesv1.LayoutFrame{Kind: layoutSnapshot, Index: 7, Term: 2, Data: []byte("image")}); err != nil {
		t.Fatalf("image: %v", err)
	}
	if len(ap.restored) != 1 || string(ap.restored[0]) != "image" {
		t.Fatalf("restored = %v, want the image bytes", ap.restored)
	}
	if st := ln.Stats(); st["applied-index"] != uint64(7) {
		t.Fatalf("applied-index = %v, want 7", st["applied-index"])
	}
	// An empty image means "rebuild from nothing", which resets.
	if err := ln.onFrame(&pushupesv1.LayoutFrame{Kind: layoutSnapshot, Index: 0, Term: 2}); err != nil {
		t.Fatalf("empty image: %v", err)
	}
	if ap.resets != 1 {
		t.Fatalf("ResetState called %d times, want 1", ap.resets)
	}
}

// A restart must come back with the state it had, not just the index: the
// table is a function of the control plane's log, so (index, bytes) is the
// resume point and an index alone is not one. Without this, a restarted data
// node applies the NEXT entry onto an empty table — observed live as a loop of
// "mark_down: unknown peer node-4" (the command that had admitted it sat below
// the index and was never replayed).
func TestLearnerRestartRestoresItsState(t *testing.T) {
	dir := t.TempDir()
	ap := &fakeApplier{state: []byte("table-at-2")}
	ln := newTestLearner(t, dir, ap)
	if err := ln.onFrame(entryFrame(1, "one")); err != nil {
		t.Fatalf("entry 1: %v", err)
	}
	if err := ln.onFrame(entryFrame(2, "two")); err != nil {
		t.Fatalf("entry 2: %v", err)
	}
	ln.saveState(true)

	fresh := &fakeApplier{state: []byte("table-at-2")}
	restarted := newTestLearner(t, dir, fresh)
	if got := restarted.Stats()["applied-index"]; got != uint64(2) {
		t.Fatalf("applied-index after restart = %v, want 2", got)
	}
	if len(fresh.restored) != 1 || string(fresh.restored[0]) != "table-at-2" {
		t.Fatalf("restored = %v, want the cached state at index 2", fresh.restored)
	}
	if fresh.resets != 0 {
		t.Fatal("a restart with a good cache must not empty the state machine")
	}
}

// A resume point without its state is not a resume point: the node asks to be
// rebuilt from index 0 and starts from an empty state machine.
func TestLearnerWithoutAUsableStateAsksToBeRebuilt(t *testing.T) {
	dir := t.TempDir()
	raw := []byte(`{"topology":"gen-a","applied_index":9,"applied_term":1}`)
	if err := os.WriteFile(filepath.Join(dir, layoutFeedFile), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	ap := &fakeApplier{}
	ln := newTestLearner(t, dir, ap)
	if got := ln.Stats()["applied-index"]; got != uint64(0) {
		t.Fatalf("applied-index = %v, want 0 (index 9 has no state behind it)", got)
	}
	if ap.resets != 1 {
		t.Fatalf("ResetState called %d times, want 1", ap.resets)
	}
}

// A command that cannot be applied invalidates the pair: retrying the same
// continuation would fail identically forever, so the next attempt rebuilds.
func TestLearnerApplyFailureDropsTheResumePoint(t *testing.T) {
	dir := t.TempDir()
	ap := &fakeApplier{failOn: "poison"}
	ln := newTestLearner(t, dir, ap)
	if err := ln.onFrame(entryFrame(1, "poison")); err == nil {
		t.Fatal("a failing apply must end the stream")
	}
	if got := ln.Stats()["applied-index"]; got != uint64(0) {
		t.Fatalf("applied-index = %v, want 0 after the state was invalidated", got)
	}
	if ap.resets != 1 {
		t.Fatalf("ResetState called %d times, want 1", ap.resets)
	}
	if err := ln.onFrame(entryFrame(1, "poison")); err == nil {
		t.Fatal("the rebuilt attempt applies from index 1")
	}
}

// The Engine polls these two keys by name (awaitApplied) and the admin status
// reads "state"; a learner that spelled them differently would stall a
// migration instead of failing loudly.
func TestLearnerStatsCarryTheKeysTheEngineReads(t *testing.T) {
	ln := newTestLearner(t, t.TempDir(), &fakeApplier{})
	ln.commit, ln.applied = 9, 4
	st := ln.Stats()
	if st["state"] != "learner" {
		t.Fatalf("state = %v, want learner", st["state"])
	}
	for _, k := range []string{"commit-index", "commit_index", "applied-index", "applied_index", "last_log_index"} {
		if _, ok := st[k]; !ok {
			t.Fatalf("Stats is missing %q", k)
		}
	}
}
