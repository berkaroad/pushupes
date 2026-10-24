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
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// budgetUnits builds n distinct queued units that share one file handle: the
// settle step skips unopened slots, so nothing but the fsync path runs.
func budgetUnits(t *testing.T, n int) (*os.File, []*flushUnit) {
	t.Helper()
	fh, err := os.CreateTemp(t.TempDir(), "unit")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fh.Close() })
	units := make([]*flushUnit, 0, n)
	for i := 0; i < n; i++ {
		units = append(units, &flushUnit{
			slot: int32(i % 64),
			seg:  &Segment{},
			file: fh,
			size: 1,
			enq:  time.Now(),
		})
	}
	return fh, units
}

// One drain round stops at the unit cap and reports the work it left behind.
func TestDrainRoundStopsAtTheUnitBudget(t *testing.T) {
	st, _ := newTestStore(t, 64, DefaultSegmentBytes)
	f := newFlusher(st, 1) // standalone: no loop goroutine competing for the queue
	var calls atomic.Int64
	fsyncProbe = func() error { calls.Add(1); return nil }
	defer func() { fsyncProbe = nil }()

	_, units := budgetUnits(t, drainRoundMaxUnits+5)
	f.enqueue(units)

	if !f.drain() {
		t.Fatal("a budget-exhausted round must report the work it left")
	}
	if got := calls.Load(); got != int64(drainRoundMaxUnits) {
		t.Fatalf("first round ran %d fsyncs, want the budget of %d", got, drainRoundMaxUnits)
	}
	if f.drain() {
		t.Fatal("the second round should have drained the rest and reported empty")
	}
	if got := calls.Load(); got != int64(len(units)) {
		t.Fatalf("total fsyncs %d, want %d", got, len(units))
	}
}

// The wall-clock half of the budget binds when the syscalls are slow.
func TestDrainRoundStopsAtTheTimeBudget(t *testing.T) {
	st, _ := newTestStore(t, 64, DefaultSegmentBytes)
	f := newFlusher(st, 1)
	var calls atomic.Int64
	fsyncProbe = func() error {
		calls.Add(1)
		time.Sleep(80 * time.Millisecond)
		return nil
	}
	defer func() { fsyncProbe = nil }()

	_, units := budgetUnits(t, 10)
	f.enqueue(units)

	start := time.Now()
	more := f.drain()
	elapsed := time.Since(start)
	if !more {
		t.Fatal("the round stopped inside the time budget with work left unserved")
	}
	if elapsed < drainRoundMaxAge {
		t.Fatalf("round stopped after %s, before the %s budget could bind", elapsed, drainRoundMaxAge)
	}
	if n := calls.Load(); n < 2 || n > 5 {
		t.Fatalf("time-budgeted round ran %d fsyncs of 10, expected ~%d", n, int(drainRoundMaxAge/(80*time.Millisecond))+1)
	}
}

// A queue deeper than one budget must still be drained by the loop without any
// further kick: the budget only slices the work, it never parks with work left.
func TestDrainBudgetKeepsDrainingWithoutAKick(t *testing.T) {
	st, _ := newTestStore(t, 64, DefaultSegmentBytes)
	var calls atomic.Int64
	fsyncProbe = func() error { calls.Add(1); return nil }
	defer func() { fsyncProbe = nil }()

	_, units := budgetUnits(t, drainRoundMaxUnits*3)
	st.flusher.enqueue(units)

	deadline := time.Now().Add(5 * time.Second)
	for calls.Load() < int64(len(units)) {
		if time.Now().After(deadline) {
			t.Fatalf("loop drained %d of %d units", calls.Load(), len(units))
		}
		time.Sleep(2 * time.Millisecond)
	}
	fs := st.FlushStats()
	if fs.QueueUnits != 0 {
		t.Fatalf("queue not empty after the loop drained it: %d", fs.QueueUnits)
	}
	if fs.DrainPasses < 3 {
		t.Fatalf("expected at least 3 drain rounds for %d units, saw %d", len(units), fs.DrainPasses)
	}
	if fs.DrainBudgetRounds == 0 {
		t.Fatal("no round reported hitting the budget, yet the queue was far deeper than one")
	}
	if fs.QueueHighWater < uint64(len(units)) {
		t.Fatalf("queue high water %d, want at least %d", fs.QueueHighWater, len(units))
	}
}
