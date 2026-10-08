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

// Each aggregate keeps its own chunked seq list: the mapping from a version to
// the seq of the record carrying it must stay correct across chunk boundaries,
// interlaced aggregates must not resolve to each other's records, and a slot
// with a handful of records must not reserve a big chunk.
func TestAggSeqChunksAndBounds(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir, 8, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	slotID := int32(1)
	agg := aggInSlotForTest(t, st, slotID)
	// 16 + 64 + 256 + 1024 + 1024 entries, so this crosses four boundaries.
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
	e := sl.aggs[agg]
	if e.n != n {
		t.Fatalf("list holds %d seqs, want %d", e.n, n)
	}
	total := 0
	for _, c := range e.chunks {
		total += len(c)
	}
	if total-n >= seqChunkMax {
		t.Fatalf("list capacity %d for %d seqs wastes a whole chunk (%v)", total, n, chunkLens(e))
	}
	if got := sl.LastVersionOf(agg); got != n {
		t.Fatalf("LastVersionOf=%d want %d", got, n)
	}
	if got := sl.CurrentVersion(agg); got != n {
		t.Fatalf("CurrentVersion=%d want %d", got, n)
	}
	// Every version must map to the seq of the record that carries it, read
	// straight from the WAL (the list and the records agree).
	for _, v := range []uint32{1, 15, 16, 17, 79, 80, 81, 335, 336, 337, 1359, 1360, 1361, 1999, 2000} {
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

	// Two aggregates interlaced in one slot: each must resolve to its own
	// records, not to whatever sits at that position in a shared arena.
	twin := int32(2)
	twinA := aggInSlotForTest(t, st, twin)
	twinB := ""
	for i := 0; i < 10000 && twinB == ""; i++ {
		cand := fmt.Sprintf("twin-%d", i)
		if cand != twinA && data.SlotOf(cand, int(st.SlotCount)) == twin {
			twinB = cand
		}
	}
	if twinB == "" {
		t.Fatal("no sibling aggregate found")
	}
	const twinVersions = 40
	for v := 1; v <= twinVersions; v++ {
		for _, id := range []string{twinA, twinB} {
			if _, err := st.Append(&data.EventRecord{
				AggregateID: id, Version: uint32(v), CommandID: fmt.Sprintf("twin-cmd-%s-%d", id, v),
				Events: []data.Event{{Type: "t", Body: []byte("y")}},
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	sl2, err := st.Slot(twin)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{twinA, twinB} {
		recs, seqs, err := sl2.AggregateVersion(id, 1, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) != twinVersions {
			t.Fatalf("%s: %d records, want %d", id, len(recs), twinVersions)
		}
		for i, rec := range recs {
			if rec.AggregateID != id || rec.Version != uint32(i+1) {
				t.Fatalf("%s: record %d is %s v%d", id, i, rec.AggregateID, rec.Version)
			}
			if i > 0 && seqs[i] <= seqs[i-1] {
				t.Fatalf("%s: seqs not ascending at %d: %v", id, i, seqs[i-1:i+1])
			}
		}
	}

	// A slot that holds one record must not reserve a full-size chunk.
	small := int32(3)
	if _, err := st.Append(&data.EventRecord{
		AggregateID: aggInSlotForTest(t, st, small), Version: 1, CommandID: "tiny",
		Events: []data.Event{{Type: "t", Body: []byte("x")}},
	}); err != nil {
		t.Fatal(err)
	}
	sl3, err := st.Slot(small)
	if err != nil {
		t.Fatal(err)
	}
	var tiny aggEntry
	for _, en := range sl3.aggs {
		tiny = en
	}
	if len(tiny.chunks) != 1 || len(tiny.chunks[0]) != 16 {
		t.Fatalf("one record reserved %d chunks (%v), want a single 16-entry chunk",
			len(tiny.chunks), chunkLens(tiny))
	}
}

func chunkLens(e aggEntry) []int {
	lens := make([]int, 0, len(e.chunks))
	for _, c := range e.chunks {
		lens = append(lens, len(c))
	}
	return lens
}
