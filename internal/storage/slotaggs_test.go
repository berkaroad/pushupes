package storage

import (
	"fmt"
	"testing"

	"pushupes/internal/data"
)

// The seq arena grows in geometric chunks: the walk to a seq must stay correct
// across chunk boundaries, the aggregate's seq range must be exact, and a slot
// with a handful of records must not reserve a whole big chunk.
func TestSeqArenaChunksAndBounds(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir, 8, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	slotID := int32(1)
	agg := aggInSlotForTest(t, st, slotID)
	// Chunks hold 1024 seqs each, so this crosses two of them.
	const n = 2000
	for v := uint32(1); v <= n; v++ {
		if _, err := st.Append(&data.EventRecord{
			AggregateID: agg, Version: v, CommandID: fmt.Sprintf("arena-%d", v),
			Events: []data.Event{{Type: "t", Body: []byte("x")}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	sl, err := st.Slot(slotID)
	if err != nil {
		t.Fatal(err)
	}
	if chunks := len(sl.seqChunks); chunks != (n+seqChunkSize-1)/seqChunkSize {
		t.Fatalf("arena has %d chunks for %d seqs, want %d", chunks, n, (n+seqChunkSize-1)/seqChunkSize)
	}
	if got := sl.LastVersionOf(agg); got != n {
		t.Fatalf("LastVersionOf=%d want %d", got, n)
	}
	if got := sl.CurrentVersion(agg); got != n {
		t.Fatalf("CurrentVersion=%d want %d", got, n)
	}
	// Growth slack stays under one chunk: total capacity is a small multiple,
	// not the 2x-per-slice the old per-aggregate slices paid.
	total := 0
	for _, c := range sl.seqChunks {
		total += cap(c)
	}
	if total-n >= seqChunkSize {
		t.Fatalf("arena capacity %d for %d seqs wastes a whole chunk (chunks %v)", total, n, chunkCaps(sl))
	}
	// Every version must map to the seq of the record that carries it, read
	// straight from the WAL (the arena and the records agree).
	for _, v := range []uint32{1, 255, 256, 257, 767, 768, 769, 1999, 2000} {
		recs, seqs, err := sl.AggregateVersion(agg, v, 1, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) != 1 || recs[0].Version != v {
			t.Fatalf("version %d: got %d records, first %+v", v, len(recs), recs)
		}
		stored, err := sl.readBySeqLocked(seqs[0])
		if err != nil || stored == nil {
			t.Fatalf("version %d: read seq %d: %v", v, seqs[0], err)
		}
		if stored.Version != v || stored.AggregateID != agg {
			t.Fatalf("version %d: seq %d holds version %d/%s", v, seqs[0], stored.Version, stored.AggregateID)
		}
	}

	// A slot that holds one record must not reserve a full-size chunk.
	small := int32(2)
	if _, err := st.Append(&data.EventRecord{
		AggregateID: aggInSlotForTest(t, st, small), Version: 1, CommandID: "tiny",
		Events: []data.Event{{Type: "t", Body: []byte("x")}},
	}); err != nil {
		t.Fatal(err)
	}
	sl2, err := st.Slot(small)
	if err != nil {
		t.Fatal(err)
	}
	if len(sl2.seqChunks) != 1 || cap(sl2.seqChunks[0]) != seqChunkSize {
		t.Fatalf("one record reserved %v (caps %v), want a single %d-entry chunk",
			len(sl2.seqChunks), chunkCaps(sl2), seqChunkSize)
	}
	if slack := cap(sl2.seqChunks[0]) - sl2.seqLen; slack > seqChunkSize-1 {
		t.Fatalf("one record wastes %d entries", slack)
	}
}

func chunkCaps(sl *Slot) []int {
	caps := make([]int, 0, len(sl.seqChunks))
	for _, c := range sl.seqChunks {
		caps = append(caps, cap(c))
	}
	return caps
}
