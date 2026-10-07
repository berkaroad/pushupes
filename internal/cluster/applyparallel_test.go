package cluster

import (
	"fmt"
	"sync"
	"testing"
)

// TestApplyFetchItemsParallelKeepsSlotsIndependent drives the pooled apply with
// more slots than the parallel threshold and one forked slot: every other slot
// must still land, the forked one must be quarantined, and the progress report
// must carry exactly the slots that applied — a quarantined slot may never be
// reported as in sync, whichever worker applied it.
func TestApplyFetchItemsParallelKeepsSlotsIndependent(t *testing.T) {
	leader, ldr := newTestEngine(t, "node-1")
	join(t, leader, "node-1", "127.0.0.1:1")
	applyCmd(t, leader, &Command{Op: OpPlanSlots})

	slots := applyParallelMin + 4 // always above the threshold: the round takes the pool path
	aggs := make([]string, slots)
	pays := make([][]byte, slots)
	for s := 0; s < slots; s++ {
		aggs[s] = aggInSlot(t, leader, int32(s))
		for v := uint64(1); v <= 3; v++ {
			if _, err := ldr.Append(makeRecord(aggs[s], uint32(v), fmt.Sprintf("s%d-%d", s, v))); err != nil {
				t.Fatal(err)
			}
		}
		_, _, pay, err := ldr.ReadSlotBytes(int32(s), 1, 4, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		pays[s] = pay
	}
	frames0 := splitFrames(t, pays[0])

	follower, fdr := newTestEngine(t, "node-2")
	join(t, follower, "node-1", "127.0.0.1:1")
	applyCmd(t, follower, &Command{Op: OpPlanSlots})

	// Fork slot 0 at seq 2; every other slot starts empty.
	if err := fdr.AppendFrameAtSeq(0, 1, frames0[0]); err != nil {
		t.Fatal(err)
	}
	if err := fdr.AppendAtSeq(0, 2, makeRecord(aggs[0], 99, "local-fork")); err != nil {
		t.Fatal(err)
	}

	items := make([]FetchItem, slots)
	for s := 0; s < slots; s++ {
		items[s] = FetchItem{Slot: int32(s), FromSeq: 1, NextSeq: 4, Payload: pays[s]}
	}
	var mu sync.Mutex
	reported := map[int32]uint64{}
	productive, err := follower.applyFetchItems(items, func(slot int32, from uint64) {
		mu.Lock()
		reported[slot] = from
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("one slot's fork must not fail the round: %v", err)
	}
	if !productive {
		t.Fatal("the round applied nothing")
	}
	if !follower.isDiverged(0) {
		t.Fatal("the forked slot must be quarantined")
	}
	for s := 1; s < slots; s++ {
		if follower.isDiverged(int32(s)) {
			t.Fatalf("slot %d must not be quarantined", s)
		}
		if leo := fdr.LastSeqOf(int32(s)); leo != 3 {
			t.Fatalf("slot %d LEO = %d, want 3 (every slot of the round must land)", s, leo)
		}
		if got, ok := reported[int32(s)]; !ok || got != 4 {
			t.Fatalf("slot %d reported as %d (ok=%v), want 4", s, got, ok)
		}
	}
	if _, ok := reported[0]; ok {
		t.Fatal("a quarantined slot must not be reported as in sync")
	}
}
