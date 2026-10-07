package storage

import (
	"testing"
	"time"

	"pushupes/internal/data"
)

// recFor builds one appendable record: an append needs at least one event, or
// it is rejected before it can be counted.
func recFor(agg string, version uint32, cmd string) *data.EventRecord {
	return &data.EventRecord{
		AggregateID: agg, Version: version, CommandID: cmd,
		Events: []data.Event{{Type: "E", Body: []byte("x")}},
	}
}

// TestFlushStatsTracksDirtyAndFsyncs pins what the flush-path snapshot is for:
// the dirty set with the age of its oldest member (is the flush loop keeping
// up?) and the cost of the fsyncs it issued. A node whose flush loop fell
// behind blocked appends on the slot lock while every other gauge looked idle,
// so these three numbers are the ones that have to be right.
//
// The store is opened with a do-nothing flush policy, so nothing flushes
// behind the test's back and the dirty state is deterministic.
func TestFlushStatsTracksDirtyAndFsyncs(t *testing.T) {
	st, err := OpenStore(t.TempDir(), 8, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if got := st.FlushStats(); got.Dirty != 0 || got.FsyncCount != 0 {
		t.Fatalf("fresh store: dirty=%d fsync_count=%d, want 0/0", got.Dirty, got.FsyncCount)
	}

	rec := recFor("agg-flushstats", 1, "cmd-1")
	slot := st.SlotOf(rec.AggregateID)
	out, err := st.Append(rec)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if out.Status != data.StatusSuccess {
		t.Fatalf("append status = %v (errID %d), want success", out.Status, out.ErrID)
	}

	// A policy of zeros never closes on its own, so the appended slot must be
	// visible as dirty with pending records and no fsync yet.
	got := st.FlushStats()
	if got.Dirty != 1 {
		t.Fatalf("after append: dirty=%d, want 1", got.Dirty)
	}
	if got.Sampled != 1 || got.BusyLocked != 0 {
		t.Fatalf("after append: sampled=%d busy_locked=%d, want 1/0", got.Sampled, got.BusyLocked)
	}
	if got.PendingFlushSum < 1 {
		t.Fatalf("after append: pending_flush_sum=%d, want >=1", got.PendingFlushSum)
	}
	if got.OldestDirtyAgeS <= 0 || got.OldestDirtyAgeS > 60 {
		t.Fatalf("after append: oldest_dirty_age_s=%v, want a small positive age", got.OldestDirtyAgeS)
	}
	if got.FsyncCount != 0 {
		t.Fatalf("after append: fsync_count=%d, want 0 (policy never flushes)", got.FsyncCount)
	}

	// An explicit flush must show up as a booking, empty the dirty set, and
	// leave the pending counter at zero.
	if err := st.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	got = st.FlushStats()
	if got.FsyncCount != 1 {
		t.Fatalf("after flush: fsync_count=%d, want 1", got.FsyncCount)
	}
	if got.FsyncAvgMS < 0 || got.FsyncMaxMS < got.FsyncAvgMS {
		t.Fatalf("after flush: avg=%v max=%v, want 0 <= avg <= max", got.FsyncAvgMS, got.FsyncMaxMS)
	}
	var bucketed uint64
	for _, n := range got.FsyncBuckets {
		bucketed += n
	}
	if bucketed != got.FsyncCount {
		t.Fatalf("buckets hold %d fsyncs, counter says %d", bucketed, got.FsyncCount)
	}
	if got.Dirty != 0 || got.OldestDirtyAgeS != 0 {
		t.Fatalf("after flush: dirty=%d oldest=%v, want 0/0", got.Dirty, got.OldestDirtyAgeS)
	}

	// The slot must have kept its data through all of this.
	if n := st.LastSeqOf(slot); n != 1 {
		t.Fatalf("slot %d last seq = %d, want 1", slot, n)
	}
}

// TestFlushStatsBusySlotIsReportedNotWaitedOn covers the diagnostic property
// that matters during a stall: a slot holding its write lock (i.e. fsyncing,
// with its appends blocked behind that lock) must be reported as busy instead
// of delaying the snapshot.
func TestFlushStatsBusySlotIsReportedNotWaitedOn(t *testing.T) {
	st, err := OpenStore(t.TempDir(), 8, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	rec := recFor("agg-busy", 1, "cmd-1")
	sl, err := st.Slot(st.SlotOf(rec.AggregateID))
	if err != nil {
		t.Fatalf("slot: %v", err)
	}
	if _, err := st.Append(rec); err != nil {
		t.Fatalf("append: %v", err)
	}

	sl.mu.Lock() // simulate a flush in progress holding the slot lock
	done := make(chan FlushStats, 1)
	go func() { done <- st.FlushStats() }()
	select {
	case fs := <-done:
		if fs.BusyLocked != 1 || fs.Sampled != 0 {
			sl.mu.Unlock()
			t.Fatalf("busy slot: busy_locked=%d sampled=%d dirty=%d, want 1/0/1", fs.BusyLocked, fs.Sampled, fs.Dirty)
		}
		if fs.OldestDirtyAgeS <= 0 {
			sl.mu.Unlock()
			t.Fatalf("busy slot: oldest_dirty_age_s=%v, want > 0", fs.OldestDirtyAgeS)
		}
		sl.mu.Unlock()
	case <-time.After(2 * time.Second):
		sl.mu.Unlock()
		t.Fatal("FlushStats blocked on a slot whose write lock is held")
	}
}
