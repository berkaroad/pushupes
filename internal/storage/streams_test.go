package storage

import (
	"fmt"
	"testing"

	"pushupes/internal/data"
)

// appendVersion writes version n of an aggregate (the write path requires
// version+1, so callers walk them in order).
func appendVersion(t *testing.T, st *Store, agg string, version uint32, cmd string) {
	t.Helper()
	if _, err := st.Append(&data.EventRecord{
		AggregateID: agg, Version: version, CommandID: cmd,
		Events: []data.Event{{Type: "t", Body: []byte("x")}},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestStreamPageListsAggregatesOrdered(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir, 16, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	slot := int32(3)
	// Four streams with a page size above the stream count: this is the case
	// that never fills the bounded selection heap, so the order has to come
	// from an explicit sort rather than from popping the heap.
	aggs := []string{aggInSlotForTest(t, st, slot)}
	for i := 1; i < 4; i++ {
		aggs = append(aggs, nextAggInSlot(t, st, slot, aggs[i-1]))
	}
	for i := uint32(1); i <= 2; i++ {
		appendVersion(t, st, aggs[0], i, fmt.Sprintf("a-%d", i)) // two versions
	}
	for i, agg := range aggs[1:] {
		appendVersion(t, st, agg, 1, fmt.Sprintf("b-%d", i))
	}

	page, err := st.StreamPage(slot, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !page.Loaded || page.Total != 4 || len(page.Streams) != 4 {
		t.Fatalf("page=%+v, want loaded with 4 streams", page)
	}
	if page.NextAfter != "" {
		t.Fatalf("NextAfter=%q, want empty on a complete page", page.NextAfter)
	}
	// Strictly ascending ids, so the cursor can walk the slot without gaps.
	for i := 1; i < len(page.Streams); i++ {
		if page.Streams[i-1].AggregateID >= page.Streams[i].AggregateID {
			t.Fatalf("streams not ordered: %v", page.Streams)
		}
	}
	got := map[string]uint32{}
	for _, s := range page.Streams {
		got[s.AggregateID] = s.Version
	}
	if got[aggs[0]] != 2 {
		t.Fatalf("versions=%v, want %s:2", got, aggs[0])
	}
	for _, agg := range aggs[1:] {
		if got[agg] != 1 {
			t.Fatalf("versions=%v, want %s:1", got, agg)
		}
	}

	// A page asked for by cursor starts strictly after it.
	if p, err := st.StreamPage(slot, aggs[0], 10); err != nil {
		t.Fatal(err)
	} else if len(p.Streams) != 3 || p.Streams[0].AggregateID == aggs[0] {
		t.Fatalf("after=%s page=%+v, want the 3 later streams", aggs[0], p)
	}
}

func TestStreamPageWalksWithCursor(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir, 16, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	slot := int32(7)
	want := map[string]bool{}
	prev := ""
	for i := 0; i < 5; i++ {
		agg := prev
		if agg == "" {
			agg = aggInSlotForTest(t, st, slot)
		} else {
			agg = nextAggInSlot(t, st, slot, agg)
		}
		prev = agg
		appendVersion(t, st, agg, 1, fmt.Sprintf("walk-%d", i))
		want[agg] = true
	}

	seen := []string{}
	after := ""
	for page := 0; ; page++ {
		if page > 10 {
			t.Fatal("cursor did not terminate")
		}
		p, err := st.StreamPage(slot, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		if p.Total != 5 {
			t.Fatalf("Total=%d, want 5 on every page", p.Total)
		}
		for _, s := range p.Streams {
			if len(seen) > 0 && s.AggregateID <= seen[len(seen)-1] {
				t.Fatalf("ids not ascending across pages: %v then %s", seen, s.AggregateID)
			}
			seen = append(seen, s.AggregateID)
		}
		if p.NextAfter == "" {
			break
		}
		if p.NextAfter == after {
			t.Fatalf("cursor did not advance past %q", after)
		}
		after = p.NextAfter
	}
	if len(seen) != 5 {
		t.Fatalf("walked %d streams, want 5: %v", len(seen), seen)
	}
	for _, id := range seen {
		if !want[id] {
			t.Fatalf("unexpected stream %s", id)
		}
	}
}

// The listing must not open a slot: opening would load every segment and walk
// its frames, which is the whole-file scan the console route exists to avoid.
func TestStreamPageLeavesColdSlotCold(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir, 16, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cold := int32(9)
	if s := st.slots[cold].Load(); s != nil {
		t.Fatal("test setup: slot already loaded")
	}
	page, err := st.StreamPage(cold, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if page.Loaded {
		t.Fatalf("cold slot reported loaded: %+v", page)
	}
	if page.Total != 0 || len(page.Streams) != 0 {
		t.Fatalf("cold slot returned streams: %+v", page)
	}
	if page.Streams == nil {
		t.Fatal("streams must encode as [] not null")
	}
	if s := st.slots[cold].Load(); s != nil {
		t.Fatal("listing loaded (and would have scanned) a cold slot")
	}

	// Out-of-range slots are a caller error, not an empty page.
	if _, err := st.StreamPage(-1, "", 10); err == nil {
		t.Fatal("negative slot accepted")
	}
	if _, err := st.StreamPage(st.SlotCount, "", 10); err == nil {
		t.Fatal("out-of-range slot accepted")
	}
}

// A huge slot must not materialise its whole index per page.
func TestStreamPageBoundsWork(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir, 16, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	slot := int32(11)
	agg := aggInSlotForTest(t, st, slot)
	const n = 50
	for i := 0; i < n; i++ {
		if i > 0 {
			agg = nextAggInSlot(t, st, slot, agg)
		}
		appendVersion(t, st, agg, 1, fmt.Sprintf("many-%d", i))
	}

	allocs := testing.AllocsPerRun(20, func() {
		p, err := st.StreamPage(slot, "", 5)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Streams) != 5 || p.Total != n {
			t.Fatalf("page=%+v", p)
		}
	})
	// O(limit) heap + page + a few interface boxes; nowhere near one allocation
	// per aggregate.
	if allocs > 40 {
		t.Fatalf("StreamPage allocated %.0f objects for a 5-entry page of %d streams", allocs, n)
	}
}

// nextAggInSlot finds another aggregate id that hashes to the same slot.
func nextAggInSlot(t *testing.T, st *Store, slot int32, after string) string {
	t.Helper()
	start := 0
	if _, err := fmt.Sscanf(after, "scan-agg-%d", &start); err != nil {
		t.Fatalf("unexpected helper id %q", after)
	}
	for i := start + 1; i < start+100000; i++ {
		id := fmt.Sprintf("scan-agg-%d", i)
		if st.SlotOf(id) == slot {
			return id
		}
	}
	t.Fatalf("no further aggregate found for slot %d", slot)
	return ""
}

// The console gauges (WAL bytes + stream count per slot) must describe loaded
// slots without opening anything: a polled endpoint cannot afford the
// whole-section scan that opening a cold slot performs.
func TestSlotGaugesReportLoadedSlotsOnly(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir, 16, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	slot := int32(5)
	aggA := aggInSlotForTest(t, st, slot)
	aggB := nextAggInSlot(t, st, slot, aggA)
	appendVersion(t, st, aggA, 1, "g-a1")
	appendVersion(t, st, aggA, 2, "g-a2")
	appendVersion(t, st, aggB, 1, "g-b1")
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	bytes, streams := st.SlotGauges()
	if len(bytes) != int(st.SlotCount) || len(streams) != int(st.SlotCount) {
		t.Fatalf("gauge arrays are %d/%d long, want %d", len(bytes), len(streams), st.SlotCount)
	}
	if streams[slot] != 2 {
		t.Fatalf("stream count %d, want 2", streams[slot])
	}
	sl, err := st.Slot(slot)
	if err != nil {
		t.Fatal(err)
	}
	if bytes[slot] != sl.TotalSize() || bytes[slot] <= 0 {
		t.Fatalf("bytes %d, want the slot's %d (>0)", bytes[slot], sl.TotalSize())
	}

	// Slot 6 was never written: the gauge reads zero and the slot stays cold.
	cold := int32(6)
	if s := st.slots[cold].Load(); s != nil {
		t.Fatal("test setup: slot 6 already loaded")
	}
	bytes, streams = st.SlotGauges()
	if bytes[cold] != 0 || streams[cold] != 0 {
		t.Fatalf("cold slot gauges = %d bytes / %d streams, want 0/0", bytes[cold], streams[cold])
	}
	if s := st.slots[cold].Load(); s != nil {
		t.Fatal("gauges opened a cold slot")
	}
}
