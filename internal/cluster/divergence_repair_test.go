package cluster

// Divergence self-heal (see noteDivergence / rebuildDivergedCopy): a forked
// replica copy used to be quarantined FOREVER — it stopped reporting, so it
// silently left the ISR, but nothing ever rebuilt it: the slot ran one replica
// short until a migration or restart shuffled it by accident. The repair is
// the same move the migration gate already makes on a diverged target —
// discard the copy and refetch it whole from the leader — applied to the
// steady-state fetch path, with an explicit completion (the copy has landed
// the leader's tail) that lifts the quarantine and returns the replica to
// normal reporting.

import (
	"fmt"
	"testing"
)

func TestDivergedReplicaRebuildsAndUnlatches(t *testing.T) {
	leader, ldr := newTestEngine(t, "node-1")
	join(t, leader, "node-1", "127.0.0.1:1")
	applyCmd(t, leader, &Command{Op: OpPlanSlots})

	agg0 := aggInSlot(t, leader, 0)
	for v := uint64(1); v <= 3; v++ {
		if _, err := ldr.AppendRouted(makeRecord(agg0, uint32(v), fmt.Sprintf("rb-%d", v))); err != nil {
			t.Fatal(err)
		}
	}
	_, _, pay0, err := ldr.ReadSlotBytes(0, 1, 4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	frames0 := splitFrames(t, pay0)

	follower, fdr := newTestEngine(t, "node-2")
	join(t, follower, "node-1", "127.0.0.1:1")
	applyCmd(t, follower, &Command{Op: OpPlanSlots})

	// fork slot 0 at seq 2: seq 1 agrees with the leader, seq 2 is a local
	// record the leader never had.
	if err := fdr.AppendFrameAtSeq(0, 1, frames0[0]); err != nil {
		t.Fatal(err)
	}
	if err := fdr.AppendAtSeq(0, 2, makeRecord(agg0, 99, "local-fork")); err != nil {
		t.Fatal(err)
	}

	var reported []uint64
	report := func(it *FetchItem, from uint64) { reported = append(reported, from) }

	items := []FetchItem{{Slot: 0, FromSeq: 1, NextSeq: 4, Payload: pay0}}
	if _, err := follower.applyFetchItems(items, report); err != nil {
		t.Fatal(err)
	}
	if !follower.isDiverged(0) {
		t.Fatal("the fork must latch the quarantine")
	}
	if leo := fdr.LastSeqOf(0); leo != 0 {
		t.Fatalf("a diverged copy must be discarded for a full refetch, LEO %d want 0", leo)
	}
	follower.divMu.Lock()
	target := follower.rebuildTarget[0]
	follower.divMu.Unlock()
	if target != 3 {
		t.Fatalf("the rebuild must remember the leader's tail as its completion mark, got %d", target)
	}
	if len(reported) != 0 {
		t.Fatalf("a quarantined slot must not report progress, got %v", reported)
	}

	// The next rounds refetch from zero (the drop reset the position): the
	// full log lands, the copy reaches the leader's tail, and the quarantine
	// lifts — reporting resumes.
	items2 := []FetchItem{{Slot: 0, FromSeq: 1, NextSeq: 4, Payload: pay0}}
	if _, err := follower.applyFetchItems(items2, report); err != nil {
		t.Fatal(err)
	}
	if leo := fdr.LastSeqOf(0); leo != 3 {
		t.Fatalf("the rebuilt copy must hold the full log, LEO %d want 3", leo)
	}
	if follower.isDiverged(0) {
		t.Fatal("catching up to the recorded tail must lift the quarantine")
	}
	if len(reported) != 1 || reported[0] != 4 {
		t.Fatalf("the healed slot must report its next position, got %v", reported)
	}

	// And the rebuilt copy serves reads like any replica: the agg is whole.
	records, _, err := fdr.ReadAggregateRouted(agg0, 1, 10, 0)
	if err != nil || len(records) != 3 {
		t.Fatalf("rebuilt copy must serve all versions: records=%d err=%v", len(records), err)
	}
}

func TestDivergedLeaderDoesNotDiscardItsOwnLog(t *testing.T) {
	// The guard rail: the node that LEADS a slot (per its own table) must
	// never run the discard — its WAL carries the records it acknowledged,
	// and no one can refetch a leader's log from below.
	e, dr := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	applyCmd(t, e, &Command{Op: OpPlanSlots})
	agg0 := aggInSlot(t, e, 0)
	for v := uint64(1); v <= 2; v++ {
		if _, err := dr.AppendRouted(makeRecord(agg0, uint32(v), fmt.Sprintf("ld-%d", v))); err != nil {
			t.Fatal(err)
		}
	}
	e.noteDivergence(0, []uint64{2})
	e.rebuildDivergedCopy(0, 2)
	if !e.Leads(0) {
		t.Fatal("setup: this engine must lead slot 0")
	}
	if leo := dr.LastSeqOf(0); leo != 2 {
		t.Fatalf("a leader's own log must survive a divergence report, LEO %d want 2", leo)
	}
	e.divMu.Lock()
	defer e.divMu.Unlock()
	if tgt, ok := e.rebuildTarget[0]; ok {
		t.Fatalf("a leader must not schedule a rebuild from below, target %d", tgt)
	}
}
