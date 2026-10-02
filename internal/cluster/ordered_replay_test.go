package cluster

import (
	"strconv"
	"testing"

	"pushupes/internal/data"
	"pushupes/internal/storage"
)

// COUNTER-EXAMPLE, deliberately skipped — do not "fix" this test into passing.
//
// The obvious repair for the rule-3 gap (DESIGN.md §1.1) is to push the missing
// records in ascending seq order, so the directory adopts each version as it
// arrives. This test builds that scenario and shows the premise is wrong, twice
// over:
//
//	replicate agg seq 1: seq 1 diverged on replay   (target holds another
//	replicate agg seq 2: seq 2 diverged on replay    aggregate at that seq)
//	replicate agg seq 9: seq 9 not contiguous (counter 5)
//
// The early versions have no seq left to occupy — a slot's seq space is already
// spent on other aggregates (rule 1), and only counter+1 can be written (rule
// 2). This is why the fix is to REBUILD the target slot instead of shipping an
// increment (see migration.go), and why this test must keep failing if someone
// re-enables it: a green run would mean the invariants moved.
func TestSequentialReplayClosesTheGap(t *testing.T) {
	t.Skip("counter-example: incremental ordered replay cannot close the gap; the target is rebuilt instead (see DESIGN.md §1.1 rule 3)")

	srcStore, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer srcStore.Close()
	_ = NewEngine(nil, srcStore, "node-1", nil)

	tgtStore, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer tgtStore.Close()
	tgt := NewEngine(nil, tgtStore, "node-2", nil)

	agg := "order-agg"
	slot := srcStore.SlotOf(agg)
	partner := ""
	for i := 0; i < 8192; i++ {
		id := "order-partner-" + strconv.Itoa(i)
		if srcStore.SlotOf(id) == slot {
			partner = id
			break
		}
	}
	if partner == "" {
		t.Skip("no partner aggregate shares the slot")
	}

	rec := func(id string, v uint32) *data.EventRecord {
		return &data.EventRecord{
			AggregateID: id, Version: v,
			CommandID: id + "-c" + strconv.Itoa(int(v)),
			Events:    []data.Event{{Type: "E", Body: []byte("x")}},
		}
	}

	// Source layout: agg v1..v3 (seqs 1..3), partner v1..v5 (seqs 4..8),
	// agg v4..v5 (seqs 9..10).
	for v := uint32(1); v <= 3; v++ {
		if _, err := srcStore.Append(rec(agg, v)); err != nil {
			t.Fatal(err)
		}
	}
	for v := uint32(1); v <= 5; v++ {
		if _, err := srcStore.Append(rec(partner, v)); err != nil {
			t.Fatal(err)
		}
	}
	for v := uint32(4); v <= 5; v++ {
		if _, err := srcStore.Append(rec(agg, v)); err != nil {
			t.Fatal(err)
		}
	}

	// Target: LEO 5 from the partner only, agg absent. Its slot LEO (5) is
	// already past agg's first records (seqs 1..3).
	for v := uint32(1); v <= 5; v++ {
		if err := tgtStore.AppendAtSeq(slot, uint64(v), rec(partner, v)); err != nil {
			t.Fatal(err)
		}
	}

	// The premise, part 1: can the source serve agg's OLD range by seq?
	// Walk the whole slot and collect agg's seqs in order.
	type frame struct {
		seq uint64
		raw []byte
	}
	var aggFrames []frame
	seq := uint64(1)
	srcLEO := srcStore.LastSeqOf(slot)
	for seq <= srcLEO {
		_, next, payload, err := srcStore.ReadSlotBytes(slot, seq, srcLEO+1, 1<<20)
		if err != nil {
			t.Fatalf("source read at %d: %v", seq, err)
		}
		if len(payload) == 0 {
			break
		}
		rest := payload
		s := seq
		for len(rest) > 0 {
			m, consumed, derr := data.DecodeRecordMeta(rest)
			if derr != nil {
				t.Fatalf("decode at %d: %v", s, derr)
			}
			if m.AggregateID == agg {
				aggFrames = append(aggFrames, frame{seq: s, raw: append([]byte(nil), rest[:consumed]...)})
			}
			rest = rest[consumed:]
			s++
		}
		if next <= seq {
			break
		}
		seq = next
	}
	if len(aggFrames) != 5 {
		t.Fatalf("collected %d agg frames by walking the slot, want 5", len(aggFrames))
	}
	t.Logf("source serves agg at seqs %d,%d,%d,%d,%d (ascending)",
		aggFrames[0].seq, aggFrames[1].seq, aggFrames[2].seq, aggFrames[3].seq, aggFrames[4].seq)

	// The premise, part 2: replay that aggregate's frames into the target in
	// ASCENDING SEQ ORDER (not in the slot's LEO-relative order) and see whether
	// the directory adopts each version.
	for _, f := range aggFrames {
		if err := tgt.HandleReplicate(slot, f.seq, f.raw); err != nil {
			t.Logf("replicate agg seq %d: %v", f.seq, err)
		}
	}

	tgtTail, err := tgtStore.TailVersionOf(agg, 0)
	if err != nil {
		t.Fatalf("target tail after ordered replay: %v", err)
	}
	tgtRecs, _, err := tgtStore.ReadAggregate(agg, 1, 0, 0)
	if err != nil {
		t.Fatalf("target read after ordered replay: %v", err)
	}
	srcTail, _ := srcStore.TailVersionOf(agg, 0)
	t.Logf("after ordered replay: target tail=%d records=%d (source tail=%d)",
		tgtTail, len(tgtRecs), srcTail)

	if tgtTail != srcTail || len(tgtRecs) != int(srcTail) {
		t.Fatalf("ordered replay did NOT close the gap: target tail %d records %d, want %d — "+
			"the premise for the fix is wrong and pushFrames must not be changed on it",
			tgtTail, len(tgtRecs), srcTail)
	}
}
