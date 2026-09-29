package storage

import (
	"fmt"
	"os"
	"testing"

	"pushupes/internal/data"
)

func mkRec(agg string, ver uint32, cmd string) *data.EventRecord {
	return &data.EventRecord{
		AggregateID: agg,
		Version:     ver,
		UnixTime:    1700000000,
		CommandID:   cmd,
		Events:      []data.Event{{Type: "E" + fmt.Sprint(ver), Body: []byte(`{"v":` + fmt.Sprint(ver) + `}`)}},
	}
}

func newTestStore(t *testing.T, slotCount int32, segBytes int64) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	st, err := OpenStore(dir, slotCount, segBytes, FlushPolicy{})
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st, dir
}

// Rule 1: duplicate command_id returns exists + stored record.
func TestAppendIdempotent(t *testing.T) {
	st, _ := newTestStore(t, 128, DefaultSegmentBytes)

	out, err := st.Append(mkRec("agg-1", 1, "cmd-a"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != data.StatusSuccess || out.Seq != 1 {
		t.Fatalf("first append: %+v", out)
	}

	// Replay the same command (even with a conflicting version): exists wins.
	out2, err := st.Append(mkRec("agg-1", 99, "cmd-a"))
	if err != nil {
		t.Fatal(err)
	}
	if out2.Status != data.StatusExists {
		t.Fatalf("expected exists, got %+v", out2)
	}
	if out2.Record == nil || out2.Record.Version != 1 || out2.Seq != 1 {
		t.Fatalf("exists must return stored record: %+v", out2)
	}
	if out2.Record.Events[0].Type != "E1" {
		t.Fatalf("stored event mismatch: %+v", out2.Record.Events)
	}
}

// Rule 2: version must advance exactly by 1.
func TestAppendVersionRules(t *testing.T) {
	st, _ := newTestStore(t, 128, DefaultSegmentBytes)

	// new aggregate must start at 1
	out, err := st.Append(mkRec("agg-x", 5, "cmd-0"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != data.StatusFail || out.ErrID != data.ErrIDVersionConflict || out.CurrentVersion != 0 {
		t.Fatalf("expected fail/1001/cur=0, got %+v", out)
	}

	if _, err := st.Append(mkRec("agg-x", 1, "cmd-1")); err != nil {
		t.Fatal(err)
	}
	// skip 2 -> fail
	out, _ = st.Append(mkRec("agg-x", 3, "cmd-2"))
	if out.Status != data.StatusFail || out.ErrID != data.ErrIDVersionConflict || out.CurrentVersion != 1 {
		t.Fatalf("expected fail/1001/cur=1, got %+v", out)
	}
	// old version -> fail
	out, _ = st.Append(mkRec("agg-x", 1, "cmd-3"))
	if out.Status != data.StatusFail || out.ErrID != data.ErrIDVersionConflict {
		t.Fatalf("expected fail on old version, got %+v", out)
	}
	// correct increment -> success
	out, _ = st.Append(mkRec("agg-x", 2, "cmd-4"))
	if out.Status != data.StatusSuccess {
		t.Fatalf("expected success, got %+v", out)
	}
}

func TestSlotRouting(t *testing.T) {
	if data.SlotOf("a", 128) != data.SlotOf("a", 128) {
		t.Fatal("routing not deterministic")
	}
	seen := map[int32]bool{}
	for i := 0; i < 5000; i++ {
		s := data.SlotOf(fmt.Sprintf("agg-%d", i), 128)
		if s < 0 || s >= 128 {
			t.Fatalf("slot out of range: %d", s)
		}
		seen[s] = true
	}
	if len(seen) < 120 {
		t.Fatalf("bad distribution: %d/128 slots hit", len(seen))
	}
}

// Segment rolling + multi-segment reads.
func TestRollAndReadAcrossSegments(t *testing.T) {
	const seg = 4096
	st, _ := newTestStore(t, 128, seg)

	for i := uint64(1); i <= 300; i++ {
		out, err := st.Append(mkRec("agg-r", uint32(i), fmt.Sprintf("cmd-%d", i)))
		if err != nil || out.Status != data.StatusSuccess {
			t.Fatalf("append %d: %+v %v", i, out, err)
		}
	}
	slotID := st.SlotOf("agg-r")
	slot, _ := st.Slot(slotID)
	if slot.SegmentCount() < 2 {
		t.Fatalf("expected multiple segments, got %d", slot.SegmentCount())
	}

	recs, seqs, err := st.ReadAggregate("agg-r", 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 300 || recs[0].Version != 1 || recs[299].Version != 300 {
		t.Fatalf("read back wrong: n=%d first=%d last=%d", len(recs), recs[0].Version, recs[len(recs)-1].Version)
	}
	if seqs[0] != 1 || seqs[len(seqs)-1] != 300 {
		t.Fatalf("seqs wrong: %d..%d", seqs[0], seqs[len(seqs)-1])
	}
	// window read
	recs, _, err = st.ReadAggregate("agg-r", 100, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 10 || recs[0].Version != 100 {
		t.Fatalf("window read wrong: %d %+v", len(recs), recs[0])
	}
	// HW cap: records beyond uptoSeq invisible
	recs, _, err = st.ReadAggregate("agg-r", 1, 0, 150)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 150 {
		t.Fatalf("HW cap read wrong: %d", len(recs))
	}
}

// Zero-copy byte ranges cover exact record bytes and stitch across segments.
func TestReadRange(t *testing.T) {
	st, dir := newTestStore(t, 128, 4096)
	for i := uint64(1); i <= 300; i++ {
		if _, err := st.Append(mkRec("agg-f", uint32(i), fmt.Sprintf("cmd-f-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	slotID := st.SlotOf("agg-f")
	slot, _ := st.Slot(slotID)
	ranges, next, err := slot.ReadRange(50, 120, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if next != 120 {
		t.Fatalf("next=%d want 120", next)
	}
	if len(ranges) < 2 {
		t.Fatalf("expected ranges across segments, got %d", len(ranges))
	}
	var total int64
	for _, r := range ranges {
		b, err := os.ReadFile(dir + "/slot-" + fmt.Sprintf("%03d", slotID) + "/" + r.Segment)
		if err != nil {
			t.Fatal(err)
		}
		chunk := b[r.Start:r.End]
		var firstLen uint32
		for i := 0; i < 4; i++ {
			firstLen = firstLen<<8 | uint32(chunk[i])
		}
		if int(firstLen)+4 > len(chunk) {
			t.Fatalf("range covers incomplete record")
		}
		total += r.End - r.Start
	}
	if total == 0 {
		t.Fatal("empty ranges")
	}
}

// Crash recovery: reopen from disk, indexes rebuilt, torn tail truncated.
func TestRecoveryAndTornTail(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir, 128, 4096, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	slotID := st.SlotOf("agg-c")
	for i := uint64(1); i <= 50; i++ {
		if _, err := st.Append(mkRec("agg-c", uint32(i), fmt.Sprintf("cmd-c-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// simulate a torn tail: append garbage to the last segment
	entries, _ := os.ReadDir(dir + "/slot-" + fmt.Sprintf("%03d", slotID))
	last := entries[len(entries)-1].Name()
	f, _ := os.OpenFile(dir+"/slot-"+fmt.Sprintf("%03d", slotID)+"/"+last, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write([]byte{0x00, 0x00, 0xff, 0xff, 'g', 'a', 'r', 'b', 'a', 'g', 'e'}) // huge bogus len
	f.Close()

	st2, err := OpenStore(dir, 128, 4096, FlushPolicy{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	slot2, _ := st2.Slot(slotID)
	if slot2.LastSeq() != 50 {
		t.Fatalf("recovery LEO=%d want 50", slot2.LastSeq())
	}
	if slot2.CurrentVersion("agg-c") != 50 {
		t.Fatalf("recovery version=%d want 50", slot2.CurrentVersion("agg-c"))
	}
	out, _ := st2.Append(mkRec("agg-c", 1, "cmd-c-1"))
	if out.Status != data.StatusExists || out.Seq != 1 {
		t.Fatalf("cmd index lost: %+v", out)
	}
	// new append continues contiguously
	out, _ = st2.Append(mkRec("agg-c", 51, "cmd-c-51"))
	if out.Status != data.StatusSuccess || out.Seq != 51 {
		t.Fatalf("post-recovery append: %+v", out)
	}
}

// Replica-style AppendAtSeq.
func TestAppendAtSeq(t *testing.T) {
	dir := t.TempDir()
	st, _ := OpenStore(dir, 128, DefaultSegmentBytes, FlushPolicy{})
	defer st.Close()
	slotID := st.SlotOf("agg-rep")
	slot, _ := st.Slot(slotID)

	// leader-style records with fixed seq
	for i := uint64(1); i <= 10; i++ {
		rec := mkRec("agg-rep", uint32(i), fmt.Sprintf("cmd-rep-%d", i))
		if err := slot.AppendAtSeq(i, rec); err != nil {
			t.Fatalf("AppendAtSeq %d: %v", i, err)
		}
	}
	// idempotent replay
	rec := mkRec("agg-rep", 5, "cmd-rep-5")
	if err := slot.AppendAtSeq(5, rec); err != nil {
		t.Fatalf("replay: %v", err)
	}
	// divergence detection
	bad := mkRec("agg-rep", 5, "cmd-DIFFERENT")
	if err := slot.AppendAtSeq(5, bad); err == nil {
		t.Fatal("expected divergence error")
	}
	// gap rejected
	if err := slot.AppendAtSeq(13, mkRec("agg-rep", 11, "cmd-rep-11")); err == nil {
		t.Fatal("expected gap error")
	}
	if slot.LastSeq() != 10 {
		t.Fatalf("LEO=%d want 10", slot.LastSeq())
	}
}

func TestBadRequests(t *testing.T) {
	st, _ := newTestStore(t, 128, DefaultSegmentBytes)
	out, err := st.Append(&data.EventRecord{AggregateID: "a", Version: 1, CommandID: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != data.StatusFail || out.ErrID != data.ErrIDBadRequest {
		t.Fatalf("empty events should fail 1002: %+v", out)
	}
}
