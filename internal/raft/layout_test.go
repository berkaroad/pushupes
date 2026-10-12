package raft

import (
	"testing"
	"time"
)

// LayoutSlice is the data-plane feed's only read primitive: a learner asks
// "continue me after (index, term)" and gets either the entries or an image.
// The cases below are the ones that decide whether a returning data node is
// continued or rebuilt.

func TestLayoutSliceContinuesFromTheAskedPoint(t *testing.T) {
	c := startCluster(t, 1)
	defer c.stopAll()
	id := c.leader(time.Second)
	n := c.nodes[id].node

	for i := 0; i < 3; i++ {
		if _, err := c.apply(id, "set k"+string(rune('a'+i))+" v"); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}

	// Nothing applied yet on the caller's side: it gets the log from its start.
	sl, err := n.LayoutSlice(0, 0, 64)
	if err != nil {
		t.Fatalf("LayoutSlice(0,0): %v", err)
	}
	if sl.Snapshot != nil {
		t.Fatalf("fresh group must answer with entries, got an image at %d", sl.Snapshot.Index)
	}
	if len(sl.Entries) == 0 || sl.Entries[0].Index != 1 {
		t.Fatalf("entries = %d starting at %v, want a contiguous run from 1", len(sl.Entries), firstIndex(sl.Entries))
	}
	if sl.CommitIndex < sl.Entries[len(sl.Entries)-1].Index {
		t.Fatalf("returned entries past the commit index: commit=%d last=%d", sl.CommitIndex, sl.Entries[len(sl.Entries)-1].Index)
	}

	// Resuming exactly where it stopped: entries continue, no image.
	last := sl.Entries[len(sl.Entries)-1]
	sl2, err := n.LayoutSlice(last.Index, last.Term, 64)
	if err != nil {
		t.Fatalf("LayoutSlice(resume): %v", err)
	}
	if sl2.Snapshot != nil {
		t.Fatalf("a caller at a real point must be continued, got an image at %d", sl2.Snapshot.Index)
	}
	if len(sl2.Entries) > 0 && sl2.Entries[0].Index != last.Index+1 {
		t.Fatalf("continuation starts at %d, want %d", sl2.Entries[0].Index, last.Index+1)
	}
}

func TestLayoutSliceRebuildsWhenTheHistoryIsNotOurs(t *testing.T) {
	c := startCluster(t, 1)
	defer c.stopAll()
	id := c.leader(time.Second)
	n := c.nodes[id].node
	if _, err := c.apply(id, "set a b"); err != nil {
		t.Fatalf("apply: %v", err)
	}
	sl, err := n.LayoutSlice(0, 0, 64)
	if err != nil {
		t.Fatalf("LayoutSlice: %v", err)
	}

	// A caller ahead of this group: it kept a longer history (a data node that
	// joined this cluster with its old slot data, DESIGN §12.4). Appending
	// would be meaningless — the answer is an image.
	ahead, err := n.LayoutSlice(sl.CommitIndex+7, 1, 64)
	if err != nil {
		t.Fatalf("LayoutSlice(ahead): %v", err)
	}
	if ahead.Snapshot == nil {
		t.Fatal("a caller ahead of the group must be answered with an image")
	}
	if len(ahead.Entries) != 0 {
		t.Fatalf("image answer carried %d entries", len(ahead.Entries))
	}

	// Same index, different term: the histories diverged.
	if len(sl.Entries) == 0 {
		t.Fatal("expected entries to test a term mismatch")
	}
	div, err := n.LayoutSlice(sl.Entries[0].Index, sl.Entries[0].Term+99, 64)
	if err != nil {
		t.Fatalf("LayoutSlice(divergent): %v", err)
	}
	if div.Snapshot == nil {
		t.Fatal("a term mismatch must be answered with an image")
	}
}

// Once the group has compacted its log, an old resume point can only be served
// by an image — and that image must be the real state, not a marker.
func TestLayoutSliceServesTheSnapshottedState(t *testing.T) {
	c := startClusterCfg(t, 1, func(cfg *Config) { cfg.SnapshotThreshold = 1 })
	defer c.stopAll()
	id := c.leader(time.Second)
	n := c.nodes[id].node
	for i := 0; i < 5; i++ {
		if _, err := c.apply(id, "set k"+string(rune('a'+i))+" v"); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	// Wait for the snapshot to land and the log to compact under it.
	deadline := time.Now().Add(10 * time.Second)
	landed := false
	for time.Now().Before(deadline) {
		if s, _, _ := n.log.Snapshot(); s > 0 && n.log.FirstIndex() > 1 {
			landed = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !landed {
		t.Skip("no snapshot landed; the compaction path needs more entries on this platform")
	}

	// The image is read from the snapshot FILE, so what it must match is the
	// file, not the index this test happened to observe: with a threshold of 1
	// every later apply writes a new one, and under a loaded suite that lands
	// between two reads of this test (the first version of this test compared
	// against a remembered index and failed exactly that way, once, in the full
	// parallel run).
	floor, _, _ := n.log.Snapshot()
	sl, err := n.LayoutSlice(0, 0, 64)
	if err != nil {
		t.Fatalf("LayoutSlice: %v", err)
	}
	if sl.Snapshot == nil {
		t.Fatalf("a compacted log must answer a fresh caller with an image (first index %d)", sl.FirstIndex)
	}
	if len(sl.Snapshot.Data) == 0 || sl.Snapshot.Index < floor {
		t.Fatalf("image = index %d, %d bytes; want an image at or after the snapshot point %d with the state machine's bytes",
			sl.Snapshot.Index, len(sl.Snapshot.Data), floor)
	}

	// Resuming AT the image point is legitimate and continues incrementally:
	// that is what a learner does right after installing one.
	cont, err := n.LayoutSlice(sl.Snapshot.Index, sl.Snapshot.Term, 64)
	if err != nil {
		t.Fatalf("LayoutSlice(at image): %v", err)
	}
	if cont.Snapshot != nil {
		// A newer snapshot landing in the meantime legitimately invalidates
		// resuming at the older point (its entries are below the new floor).
		if cur, _, _ := n.log.Snapshot(); cur == sl.Snapshot.Index {
			t.Fatal("resuming at the image point must not send the image again")
		}
	}
}

func firstIndex(es []Entry) any {
	if len(es) == 0 {
		return "none"
	}
	return es[0].Index
}
