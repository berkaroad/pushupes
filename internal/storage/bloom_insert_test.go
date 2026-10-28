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
	"testing"

	"github.com/berkaroad/pushupes/internal/data"
)

// The command filter answers "definitely not stored", and the idempotency check
// skips its disk lookup on that answer — so the filter may over-report ("maybe"
// for a hash it never placed) but must never under-report. Meanwhile the
// recovery paths replay every record of every segment on every start, so
// placing the same hash twice spends a generation slot for nothing.
//
// The exact "already placed" answer cannot come from the filter itself: a bloom
// "maybe" is also true for the hashes that collide with unrelated records, and
// skipping on it would drop a stored command from the filter for good. It comes
// from the record's slot seq instead — indexMetaLocked runs in ascending seq
// order and the filter carries a watermark of the seqs its bits cover
// (bloomstore.go).
//
// These tests drive the real entry point (indexMetaLocked), because that is
// where the dedup lives; bloomStore.insert alone has no way to know.
func TestBloomIndexingIsIdempotent(t *testing.T) {
	sl := openTestSlot(t)

	const n = 3000
	meta := func(i int) (uint64, data.RecordMeta) {
		id := fmt.Sprintf("idem-%d", i)
		return uint64(i + 1), data.RecordMeta{
			AggregateID: id, Version: 1,
			CommandHash: data.HashCommandID(id),
		}
	}

	// First pass: the write shape, every seq indexed once.
	sl.mu.Lock()
	for i := 0; i < n; i++ {
		seq, m := meta(i)
		sl.indexMetaLocked(seq, m)
	}
	sl.mu.Unlock()
	first := sl.blms.records()

	// Replay the same seqs three more times: exactly what a restart does.
	sl.mu.Lock()
	for round := 0; round < 3; round++ {
		for i := 0; i < n; i++ {
			seq, m := meta(i)
			sl.indexMetaLocked(seq, m)
		}
	}
	sl.mu.Unlock()
	if got := sl.blms.records(); got != first {
		t.Errorf("replaying %d already-indexed seqs 3x changed records: %d -> %d (want no change)",
			n, first, got)
	}

	// The new generations must not have been created either.
	if got := sl.blms.gensCount(); got != 1 {
		t.Errorf("replaying covered seqs opened %d generations, want 1", got)
	}
	for i := 0; i < n; i++ {
		seq, m := meta(i)
		if !sl.blms.maybe(m.CommandHash) {
			t.Fatalf("indexed command %q (seq %d) reports absent", m.AggregateID, seq)
		}
	}
}

// The watermark must survive a reopen so the second start does not place the
// same records again, and the reloaded generation must answer for everything
// it covers.
func TestBloomWatermarkSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	const agg = "wm-agg"
	const copies = 400

	recordsAfterOpen := func(open func() *Store) uint64 {
		st := open()
		slot := st.SlotOf(agg)
		if _, err := st.Slot(slot); err != nil {
			t.Fatal(err)
		}
		sl, err := st.SlotIfLoaded(slot)
		if err != nil || sl == nil {
			t.Fatalf("slot not loaded: %v", err)
		}
		n := sl.blms.records()
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// Seed once.
	st := openStoreAt(t, dir)
	slot := st.SlotOf(agg)
	for i := 0; i < copies; i++ {
		if _, err := st.AppendRouted(&data.EventRecord{
			AggregateID: agg, Version: uint32(i + 1),
			CommandID: fmt.Sprintf("%s-cmd-%d", agg, i),
			Events:    []data.Event{{Type: "E", Body: []byte("x")}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	second := recordsAfterOpen(func() *Store { return openStoreAt(t, dir) })
	third := recordsAfterOpen(func() *Store { return openStoreAt(t, dir) })
	if second != third {
		t.Errorf("filter records differ across restarts: %d then %d (each start re-places records)",
			second, third)
	}
	if second != copies {
		t.Errorf("filter records after reopen: %d, want %d (the header's coverage was not resumed)",
			second, copies)
	}
	// Coverage is proven on the lookup answers too: every seeded command must
	// resolve to its stored seq after a reopen.
	st4 := openStoreAt(t, dir)
	sl4, err := st4.Slot(slot)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < copies; i++ {
		id := fmt.Sprintf("%s-cmd-%d", agg, i)
		rec, seq, err := sl4.RecordByCommand(id)
		if err != nil {
			t.Fatal(err)
		}
		if rec == nil || seq != uint64(i+1) {
			t.Fatalf("seeded command %q not resolved after reopen (found=%v seq=%d, want %d)",
				id, rec != nil, seq, i+1)
		}
	}
	if err := st4.Close(); err != nil {
		t.Fatal(err)
	}
}

// emptyTestBloom returns a filter that answers "maybe" for everything — the
// cold-start shape a test uses to force lookups onto the disk indexes — with
// its own scratch directory so nothing lands in the slot's own filter files.
func emptyTestBloom(t *testing.T) *bloomStore {
	t.Helper()
	return openTestBloom(t, t.TempDir())
}

// openTestSlot returns a bare slot with an empty filter set, for driving
// indexMetaLocked directly.
func openTestSlot(t *testing.T) *Slot {
	t.Helper()
	st := openStoreAt(t, t.TempDir())
	t.Cleanup(func() { st.Close() })
	// Force the slot into memory.
	sl, err := st.Slot(0)
	if err != nil {
		t.Fatal(err)
	}
	return sl
}

// openStoreAt opens a store on an existing directory (no cleanup: the caller
// may reopen it).
func openStoreAt(t *testing.T, dir string) *Store {
	t.Helper()
	st, err := OpenStore(dir, 8, 4096, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	return st
}
