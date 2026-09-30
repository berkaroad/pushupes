package storage

import (
	"bytes"
	"fmt"
	"testing"

	"pushupes/internal/data"
)

// Follower-style frame landing: the leader's encoded bytes reach the WAL
// verbatim, and the header-only index stays as correct as a decoded append.
func TestAppendFrameAtSeqLandsLeaderBytes(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir, 128, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	slotID := st.SlotOf("agg-frame")
	slot, _ := st.Slot(slotID)

	const n = 10
	frames := make([][]byte, 0, n)
	for i := uint64(1); i <= n; i++ {
		frames = append(frames, mkRec("agg-frame", uint32(i), fmt.Sprintf("cmd-frame-%d", i)).EncodeBinary(nil))
	}
	for i, f := range frames {
		if err := slot.AppendFrameAtSeq(uint64(i+1), f); err != nil {
			t.Fatalf("AppendFrameAtSeq %d: %v", i+1, err)
		}
	}
	if got := slot.LastSeq(); got != n {
		t.Fatalf("LEO=%d want %d", got, n)
	}

	// The WAL payload must be the concatenation of the leader's frames.
	_, _, payload, err := st.ReadSlotBytes(slotID, 1, n+1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Join(frames, nil)
	if !bytes.Equal(payload, want) {
		t.Fatalf("WAL payload differs from leader frames: got %d bytes want %d", len(payload), len(want))
	}

	// Indexes were rebuilt from frame metadata alone.
	if cur := slot.aggVersions["agg-frame"]; cur != n {
		t.Fatalf("aggVersions=%d want %d", cur, n)
	}
	// The command index keeps hashes; a lookup confirms against the record.
	if rec, seq, err := slot.RecordByCommand("cmd-frame-4"); err != nil || rec == nil || seq != 4 {
		t.Fatalf("RecordByCommand(cmd-frame-4)=%v,%d,%v want the record at seq 4", rec, seq, err)
	}
	if rec, _, err := slot.RecordByCommand("cmd-not-there"); err != nil || rec != nil {
		t.Fatalf("RecordByCommand(cmd-not-there)=%v,%v want nil,nil", rec, err)
	}

	// Idempotent replay of the same frame is a no-op...
	if err := slot.AppendFrameAtSeq(5, frames[4]); err != nil {
		t.Fatalf("replay: %v", err)
	}
	// ...but a different record at the same seq is divergence.
	if bad := mkRec("agg-frame", 5, "cmd-DIVERGED").EncodeBinary(nil); slot.AppendFrameAtSeq(5, bad) == nil {
		t.Fatal("expected divergence error on replayed seq")
	}
	// Gaps and malformed frames are rejected before touching the WAL.
	if err := slot.AppendFrameAtSeq(n+3, mkRec("agg-frame", uint32(n+3), "cmd-gap").EncodeBinary(nil)); err == nil {
		t.Fatal("expected gap error")
	}
	if err := slot.AppendFrameAtSeq(n+1, frames[0][:20]); err == nil {
		t.Fatal("expected truncated-frame error")
	}
	if got := slot.LastSeq(); got != n {
		t.Fatalf("LEO=%d want %d after rejected appends", got, n)
	}
}

// Frames written through the replication path must survive a restart exactly
// as records written through the local append path: same scan, same dedupe.
func TestAppendFrameAtSeqSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir, 128, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	slotID := st.SlotOf("agg-reopen")
	slot, _ := st.Slot(slotID)
	frame := mkRec("agg-reopen", 1, "cmd-r1").EncodeBinary(nil)
	if err := slot.AppendFrameAtSeq(1, frame); err != nil {
		t.Fatal(err)
	}
	local := mkRec("agg-reopen", 2, "cmd-r2")
	if _, err := st.Append(local); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := OpenStore(dir, 128, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	slot2, err := st2.Slot(slotID)
	if err != nil {
		t.Fatal(err)
	}
	if got := slot2.LastSeq(); got != 2 {
		t.Fatalf("recovered LEO=%d want 2", got)
	}
	_, _, payload, err := st2.ReadSlotBytes(slotID, 1, 3, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if want := append(append([]byte{}, frame...), local.EncodeBinary(nil)...); !bytes.Equal(payload, want) {
		t.Fatal("recovered payload differs from the bytes that were written")
	}
	_, n, err := data.DecodeRecordMeta(payload)
	if err != nil || n != len(frame) {
		t.Fatalf("first recovered frame: n=%d err=%v", n, err)
	}
}

// The pooled encode buffers must hand out exactly the requested length, and
// buffers outside the pooled size classes must not be recycled as if they were.
func TestEncodeBufPool(t *testing.T) {
	for _, n := range []int{1, 30, 8192, 8193, 100 << 10, 1 << 20} {
		b := getEncodeBuf(n)
		if len(b) != n {
			t.Fatalf("getEncodeBuf(%d): len=%d", n, len(b))
		}
		putEncodeBuf(b)
	}
	// A buffer whose capacity is not a pool class must be dropped, never
	// handed out as if it were bigger than it is.
	odd := make([]byte, 1000, 1000)
	putEncodeBuf(odd)
	if got := getEncodeBuf(1000); len(got) != 1000 || cap(got) < 1000 {
		t.Fatalf("getEncodeBuf(1000): len=%d cap=%d", len(got), cap(got))
	}
	// Reuse: same size class should hand the same backing array back.
	first := getEncodeBuf(64 << 10)
	putEncodeBuf(first)
	reused := false
	for i := 0; i < 32; i++ {
		again := getEncodeBuf(64 << 10)
		if &again[0] == &first[0] {
			reused = true
			putEncodeBuf(again)
			break
		}
		putEncodeBuf(again)
	}
	if !reused {
		t.Fatal("64KiB encode buffers are not being recycled")
	}
	// Oversized records bypass the pool entirely.
	huge := getEncodeBuf(3 << 20)
	if len(huge) != 3<<20 || cap(huge) != 3<<20 {
		t.Fatalf("oversized buffer: len=%d cap=%d", len(huge), cap(huge))
	}
}
