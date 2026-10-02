package storage

import (
	"fmt"
	"testing"

	"pushupes/internal/data"
)

// The command filter answers "definitely not stored", and the idempotency check
// skips its disk lookup on that answer — so the filter may over-report ("maybe"
// for a hash it never placed) but must never under-report. Meanwhile the
// recovery paths replay every record of every segment on every start, and the
// filter grows by doubling with its entry count driving full(), so placing the
// same hash twice costs real bytes-per-entry and raises the false-positive rate
// (1.25 B/entry measured when each command is placed exactly once).
//
// The exact "already placed" answer cannot come from the filter itself: a bloom
// "maybe" is also true for the ~0.8% of hashes that collide with unrelated
// entries, and skipping on it would drop a stored command from the filter for
// good. It comes from the record's slot seq instead — indexMetaLocked runs in
// ascending seq order and the slot tracks a watermark of what it has placed.
//
// These tests drive the real entry point (indexMetaLocked), because that is
// where the dedup lives; bloomSet.insert alone has no way to know.
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
	first := sl.blms.entries()

	// Replay the same seqs three more times: exactly what a restart does.
	sl.mu.Lock()
	for round := 0; round < 3; round++ {
		for i := 0; i < n; i++ {
			seq, m := meta(i)
			sl.indexMetaLocked(seq, m)
		}
	}
	sl.mu.Unlock()
	if got := sl.blms.entries(); got != first {
		t.Errorf("replaying %d already-indexed seqs 3x changed entries: %d -> %d (want no change)",
			n, first, got)
	}

	// The new filters must not have been created either.
	for i := 0; i < n; i++ {
		seq, m := meta(i)
		if !sl.blms.maybe(m.CommandHash) {
			t.Fatalf("indexed command %q (seq %d) reports absent", m.AggregateID, seq)
		}
	}
}

// A filter that comes off a segment's command index must know how loaded it is:
// full() gates whether the chain hands new hashes to a fresh filter, and a
// decoded filter used to report 0 entries — so it stayed "not full" forever,
// absorbing past its capacity and understating the density in every report.
func TestDecodedFilterKnowsItsLoad(t *testing.T) {
	set := &bloomSet{}
	const n = 900 // most of bloomFirstCapacity
	for i := 0; i < n; i++ {
		set.insert(data.HashCommandID(fmt.Sprintf("load-%d", i)))
	}
	got := decodeBloomSet(set.encode())
	if got == nil {
		t.Fatal("decodeBloomSet rejected what encode wrote")
	}
	if got.entries() == 0 {
		t.Fatalf("decoded set reports 0 entries (wrote %d): full() would never trigger", n)
	}
	if diff := got.entries() - n; diff > n/4 || diff < -n/4 {
		t.Errorf("decoded entry estimate %d is too far from the %d placed", got.entries(), n)
	}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("load-%d", i)
		if !got.maybe(data.HashCommandID(id)) {
			t.Fatalf("decoded filter reports %q absent", id)
		}
	}
	// A saturated filter must decode as full, not as empty.
	sat := &bloomSet{}
	for i := 0; i < bloomFirstCapacity*3; i++ {
		sat.insert(data.HashCommandID(fmt.Sprintf("sat-%d", i)))
	}
	dec := decodeBloomSet(sat.encode())
	if dec == nil {
		t.Fatal("saturated set failed to decode")
	}
	filled := false
	for _, f := range dec.filters {
		if f.entries > f.capacity() {
			t.Errorf("decoded entries %d exceed capacity %d", f.entries, f.capacity())
		}
		if f.full() {
			filled = true
		}
	}
	if !filled {
		t.Error("a saturated filter decoded as not-full: the chain would keep growing one filter")
	}
}

// A set that grows past its first filter must keep answering for hashes placed
// in the older ones.
func TestBloomChainKeepsOlderFilters(t *testing.T) {
	set := &bloomSet{}
	const early = 1500
	for i := 0; i < early; i++ {
		set.insert(data.HashCommandID(fmt.Sprintf("early-%d", i)))
	}
	for i := 0; i < 20000; i++ {
		set.insert(data.HashCommandID(fmt.Sprintf("late-%d", i)))
	}
	if len(set.filters) < 2 {
		t.Fatalf("expected the chain to grow, got %d filter(s)", len(set.filters))
	}
	for i := 0; i < early; i++ {
		id := fmt.Sprintf("early-%d", i)
		if !set.maybe(data.HashCommandID(id)) {
			t.Fatalf("hash placed in an older filter (%q) reports absent after growth", id)
		}
	}
	// The exact replay signal (placed=true) must be a no-op even with room in
	// the newest filter — this is the case that kept re-placing old hashes.
	before := set.entries()
	for i := 0; i < 500; i++ {
		set.insertPlaced(data.HashCommandID(fmt.Sprintf("early-%d", i)), true)
	}
	if got := set.entries(); got != before {
		t.Errorf("replaying already-placed hashes changed entries: %d -> %d", before, got)
	}
}

// The watermark must survive a reopen: the second start loads the segments'
// blooms (or rebuilds them) and must not place the same records again. Two
// consecutive reopens must land on the same number.
func TestBloomWatermarkSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	const agg = "wm-agg"
	const copies = 400

	entriesAfterOpen := func(open func() *Store) int {
		st := open()
		slot := st.SlotOf(agg)
		if _, err := st.Slot(slot); err != nil {
			t.Fatal(err)
		}
		sl, err := st.SlotIfLoaded(slot)
		if err != nil || sl == nil {
			t.Fatalf("slot not loaded: %v", err)
		}
		n := sl.blms.entries()
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// Seed once.
	st := openStoreAt(t, dir)
	slot := st.SlotOf(agg)
	for i := 0; i < copies; i++ {
		if _, err := st.Append(&data.EventRecord{
			AggregateID: agg, Version: uint32(i + 1),
			CommandID: fmt.Sprintf("%s-cmd-%d", agg, i),
			Events:    []data.Event{{Type: "E", Body: []byte("x")}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	_ = slot
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	second := entriesAfterOpen(func() *Store { return openStoreAt(t, dir) })
	third := entriesAfterOpen(func() *Store { return openStoreAt(t, dir) })
	if second != third {
		t.Errorf("filter entries differ across restarts: %d then %d (each start re-places records)",
			second, third)
	}
	if second < copies {
		t.Errorf("filter holds %d entries for %d records: commands were dropped", second, copies)
	}
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
