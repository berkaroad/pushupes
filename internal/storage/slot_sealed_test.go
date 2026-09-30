package storage

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"pushupes/internal/data"
)

// Every version of every aggregate must come back as that aggregate's own
// record, including the versions a sealed segment's aggregate index serves. A
// shared arena got this wrong the moment two aggregates interleaved: their seqs
// are not a contiguous range of it, so indexing by version resolved to whatever
// record happened to sit at that position.
func TestSealedAggregateIndexServesEveryVersion(t *testing.T) {
	dir := t.TempDir()
	const slotBytes = int64(8 << 10) // small segments: several seals
	flush := FlushPolicy{Interval: time.Millisecond}
	aggA := "agg-alpha"
	slot := data.SlotOf(aggA, 8)
	aggs := []string{aggA}
	for i := 0; i < 10000 && len(aggs) < 3; i++ {
		cand := fmt.Sprintf("agg-beta-%d", i)
		if data.SlotOf(cand, 8) == slot {
			aggs = append(aggs, cand)
		}
	}
	if len(aggs) < 3 {
		t.Fatal("need three aggregates sharing one slot")
	}
	const versions = 60
	st, err := OpenStore(dir, 8, slotBytes, flush)
	if err != nil {
		t.Fatal(err)
	}
	body := strings.Repeat("x", 80)
	for v := 1; v <= versions; v++ {
		for _, agg := range aggs {
			if _, err := st.Append(&data.EventRecord{
				AggregateID: agg,
				Version:     uint32(v),
				CommandID:   fmt.Sprintf("cmd-%s-%d", agg, v),
				Events:      []data.Event{{Type: "t", Body: []byte(body)}},
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	sl, err := st.Slot(slot)
	if err != nil {
		t.Fatal(err)
	}
	if err := sl.Flush(); err != nil {
		t.Fatal(err)
	}
	if n := sl.SegmentCount(); n < 3 {
		t.Fatalf("expected several segments, got %d", n)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// A start writes the sealed aggregate indexes, so the sealed seqs can leave
	// memory — and every version must still resolve.
	st, err = OpenStore(dir, 8, slotBytes, flush)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	sl, err = st.Slot(slot)
	if err != nil {
		t.Fatal(err)
	}
	for _, agg := range aggs {
		recs, seqs, err := sl.AggregateVersion(agg, 1, 0, 0)
		if err != nil {
			t.Fatalf("%s: %v", agg, err)
		}
		if len(recs) != versions {
			t.Fatalf("%s: %d records, want %d", agg, len(recs), versions)
		}
		for i, rec := range recs {
			if rec.AggregateID != agg || rec.Version != uint32(i+1) {
				t.Fatalf("%s: record %d is %s v%d", agg, i, rec.AggregateID, rec.Version)
			}
			if i > 0 && seqs[i] <= seqs[i-1] {
				t.Fatalf("%s: seqs not ascending at %d: %v", agg, i, seqs[i-1:i+1])
			}
		}
		if got := sl.LastVersionOf(agg); got != versions {
			t.Fatalf("%s: last version %d, want %d", agg, got, versions)
		}
		if rec, _, err := sl.RecordByCommand(fmt.Sprintf("cmd-%s-1", agg)); err != nil || rec == nil {
			t.Fatalf("%s: command lookup for a sealed version: %v %v", agg, rec, err)
		}
	}
	// The sealed seqs must have left memory: that is the point of the index.
	sl.mu.RLock()
	sealed, live := 0, 0
	for _, agg := range aggs {
		sealed += sl.aggs[agg].sealedN
	}
	live = sl.arenaBytes()
	sl.mu.RUnlock()
	if sealed == 0 {
		t.Fatal("no sealed seqs were dropped: nothing was saved")
	}
	if live > 16<<10 {
		t.Fatalf("live seq lists still hold %d bytes", live)
	}
	t.Logf("sealed seqs dropped=%d, live list bytes=%d", sealed, live)
}
