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
	"sync/atomic"
	"testing"
	"time"
)

// flushParallelUnits builds n distinct queued units that share one file handle
// (the settle step skips unopened slots, so only the fsync path runs).
func flushParallelUnits(t *testing.T, n int) (*Store, []*flushUnit) {
	t.Helper()
	st, err := OpenStore(t.TempDir(), 8, DefaultSegmentBytes, FlushPolicy{Parallelism: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	fh, units := budgetUnits(t, n)
	t.Cleanup(func() { fh.Close() })
	for i := range units { // the store has 8 slots; syncUnit indexes them
		units[i].slot = int32(i % 8)
	}
	return st, units
}

// The configured parallelism is a real cap on concurrent fsyncs: with four,
// independent units overlap (peak above one) and never exceed four.
func TestFlushParallelismOverlapsFsyncs(t *testing.T) {
	st, units := flushParallelUnits(t, 12)
	if got := st.FlushStats().Parallelism; got != 4 {
		t.Fatalf("reported parallelism = %d, want 4", got)
	}

	var inFlight, peak atomic.Int64
	done := make(chan struct{})
	var completed atomic.Int64
	fsyncProbe = func() error {
		cur := inFlight.Add(1)
		for {
			old := peak.Load()
			if cur <= old || peak.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inFlight.Add(-1)
		if completed.Add(1) == int64(len(units)) {
			close(done)
		}
		return nil
	}
	defer func() { fsyncProbe = nil }()

	st.flusher.enqueue(units)
	go st.flusher.drainAll()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("only %d of %d fsyncs completed", completed.Load(), len(units))
	}
	if got := peak.Load(); got < 2 {
		t.Fatalf("peak concurrent fsyncs = %d, want the units to overlap (>1)", got)
	}
	if got := peak.Load(); got > 4 {
		t.Fatalf("peak concurrent fsyncs = %d, exceeds the configured parallelism of 4", got)
	}
	if got := st.FlushStats().QueueUnits; got != 0 {
		t.Fatalf("queue not empty after the parallel drain: %d", got)
	}
	// The reported view follows the cap, not a hard-wired one.
	deadline := time.Now().Add(2 * time.Second)
	for st.FlushStats().Inflight != 0 {
		if time.Now().After(deadline) {
			t.Fatal("inflight never returned to zero")
		}
		time.Sleep(time.Millisecond)
	}
}

// The default (and the zero value) keeps the historical one-at-a-time pipeline.
func TestFlushParallelismDefaultsToOne(t *testing.T) {
	st, err := OpenStore(t.TempDir(), 8, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if got := st.FlushStats().Parallelism; got != 1 {
		t.Fatalf("default parallelism = %d, want 1", got)
	}
}
