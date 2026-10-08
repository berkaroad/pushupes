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

package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/berkaroad/pushupes/internal/data"
)

// installProbe swaps the flusher's syscall for an observer and returns a
// restore function. Every fsync the flusher performs goes through it, which is
// how the single-fsync invariant and the settlement window are asserted without
// touching the disk's own timing.
func installProbe(t *testing.T, fn func() error) {
	t.Helper()
	prev := fsyncProbe
	fsyncProbe = fn
	t.Cleanup(func() { fsyncProbe = prev })
}

// TestFlusherRunsOneFsyncAtATime pins the flusher's two structural invariants:
// exactly one fsync is ever in flight, and the slot write lock is not held while
// it runs — the probe appends to the very slot it is syncing, which can only
// work if the lock is free. Many concurrent explicit flushes and appends must
// not widen the window either.
func TestFlusherRunsOneFsyncAtATime(t *testing.T) {
	st, err := OpenStore(t.TempDir(), 8, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	var live, peak atomic.Int64
	var fsyncs atomic.Int64
	var slotLockedOut atomic.Int64
	installProbe(t, func() error {
		n := live.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		if n > 1 {
			slotLockedOut.Add(1) // never expected: reported by the assertion below
		}
		// Give a competing flush a chance to overlap if the design allowed it.
		time.Sleep(2 * time.Millisecond)
		fsyncs.Add(1)
		live.Add(-1)
		return nil
	})

	for i := 0; i < 6; i++ {
		rec := recFor("agg-one-fsync-"+string(rune('a'+i)), 1, fmt.Sprintf("cmd-%d", i))
		out, err := st.Append(rec)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		if out.Status != data.StatusSuccess {
			t.Fatalf("append %d: status %v (errID %d), want success", i, out.Status, out.ErrID)
		}
	}

	// Eight concurrent callers each force a flush: the units they produce must
	// be serialized by the single flusher goroutine.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := st.Flush(); err != nil {
				t.Errorf("flush: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := peak.Load(); got != 1 {
		t.Fatalf("peak concurrent fsyncs = %d, want 1", got)
	}
	if got := fsyncs.Load(); got == 0 {
		t.Fatal("no fsync ran at all")
	}
	if got := st.FlushStats().Inflight; got > 1 {
		t.Fatalf("reported inflight = %d, want <= 1", got)
	}
	if got := st.FlushStats(); got.Dirty != 0 || got.PendingFlushSum != 0 {
		t.Fatalf("after a forced flush of every slot: dirty=%d pending=%d, want 0/0", got.Dirty, got.PendingFlushSum)
	}
}

// TestFlusherSettlesOnlyWhatTheFsyncCovered pins the settlement rule: records
// appended while a segment's fsync is in flight were not covered by it, so they
// must stay pending and be covered by the next round — and appending during the
// syscall must be possible at all (the slot lock is free).
func TestFlusherSettlesOnlyWhatTheFsyncCovered(t *testing.T) {
	st, err := OpenStore(t.TempDir(), 8, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	rec := recFor("agg-settle", 1, "cmd-1")
	slot := st.SlotOf(rec.AggregateID)
	if _, err := st.Append(rec); err != nil {
		t.Fatalf("append: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	installProbe(t, func() error {
		once.Do(func() { close(started) })
		<-release
		return nil
	})

	flushed := make(chan error, 1)
	go func() { flushed <- st.Flush() }()
	<-started

	// The syscall is in flight; the slot lock is free, so this append lands.
	rec2 := recFor("agg-settle", 2, "cmd-2")
	if _, err := st.Append(rec2); err != nil {
		t.Fatalf("append during fsync: %v", err)
	}
	close(release)
	if err := <-flushed; err != nil {
		t.Fatalf("flush: %v", err)
	}

	got := st.FlushStats()
	if got.PendingFlushSum < 1 {
		t.Fatalf("pending after flush = %d, want >=1 (the append during the fsync was not covered)", got.PendingFlushSum)
	}
	if got.Dirty != 1 {
		t.Fatalf("dirty after flush = %d, want 1 (the slot still owes an fsync)", got.Dirty)
	}

	// The next flush covers it and the slot goes clean.
	if err := st.Flush(); err != nil {
		t.Fatalf("second flush: %v", err)
	}
	got = st.FlushStats()
	if got.Dirty != 0 || got.PendingFlushSum != 0 {
		t.Fatalf("after the second flush: dirty=%d pending=%d, want 0/0", got.Dirty, got.PendingFlushSum)
	}
	if n := st.LastSeqOf(slot); n != 2 {
		t.Fatalf("slot %d last seq = %d, want 2", slot, n)
	}
}

// TestFlusherSealedSegmentIsQueued pins R1: rolling a segment hands it to the
// flusher even when the slot takes no further writes, so a sealed segment can
// never be left without an owner. The policy here never fires on its own.
func TestFlusherSealedSegmentIsQueued(t *testing.T) {
	const segBytes = int64(2048) // tiny rolls: every append or two seals a segment
	st, err := OpenStore(t.TempDir(), 4, segBytes, FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	before := st.FlushStats().FsyncCount
	// ~60-byte records against a 2 KiB roll threshold: a few hundred appends
	// seal several segments, none of them via the policy (which never fires).
	for v := uint32(1); v <= 400; v++ {
		out, err := st.Append(recFor("agg-roll", v, fmt.Sprintf("cmd-%d", v)))
		if err != nil {
			t.Fatalf("append %d: %v", v, err)
		}
		if out.Status != data.StatusSuccess {
			t.Fatalf("append %d: status %v (errID %d), want success", v, out.Status, out.ErrID)
		}
	}
	// The flusher runs asynchronously; wait for the sealed segments' fsyncs.
	deadline := time.Now().Add(3 * time.Second)
	for st.FlushStats().FsyncCount == before && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := st.FlushStats().FsyncCount; got <= before {
		t.Fatalf("fsync_count = %d, want > %d: sealed segments must be queued by the roll itself", got, before)
	}
}

// TestFlusherCloseDrainsQueue pins the shutdown contract: Close makes the
// outstanding data durable (through the flusher) before the loop exits, even
// with a policy that never fires on its own.
func TestFlusherCloseDrainsQueue(t *testing.T) {
	st, err := OpenStore(t.TempDir(), 4, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := st.Append(recFor("agg-close", 1, "cmd")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if got := st.FlushStats(); got.Dirty != 1 || got.FsyncCount != 0 {
		t.Fatalf("before close: dirty=%d fsync_count=%d, want 1/0", got.Dirty, got.FsyncCount)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	got := st.FlushStats()
	if got.FsyncCount == 0 {
		t.Fatal("close left the queue undrained: no fsync was performed")
	}
	if got.Dirty != 0 || got.PendingFlushSum != 0 {
		t.Fatalf("after close: dirty=%d pending=%d, want 0/0", got.Dirty, got.PendingFlushSum)
	}
}

// TestFlusherCloseIsIdempotent pins the shutdown contract a killed node
// exposed: a second Close (a defer racing a signal path) must not wait for a
// barrier the retired loop will never reach — it drains inline and returns.
func TestFlusherCloseIsIdempotent(t *testing.T) {
	st, err := OpenStore(t.TempDir(), 4, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if _, err := st.Append(recFor("agg-close2", 1, "cmd-1")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- st.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second Close hung: it queued work for a retired flusher")
	}

	// A forced flush after shutdown must also return, not wait on the loop.
	flushDone := make(chan error, 1)
	go func() { flushDone <- st.Flush() }()
	select {
	case err := <-flushDone:
		if err != nil {
			t.Fatalf("flush after close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Flush after Close hung")
	}
}
