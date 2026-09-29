package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// The leader append path writes scattered segments now; the bytes on disk must
// be exactly what the contiguous encoder produces, both when scatter writes are
// available and when the filesystem refuses them (silent fallback).
func TestAppendRecordLandsIdenticalBytes(t *testing.T) {
	for _, scatter := range []bool{true, false} {
		scatterDisabled.Store(!scatter)
		t.Cleanup(func() { scatterDisabled.Store(false) })

		dir := t.TempDir()
		st, err := OpenStore(dir, 128, DefaultSegmentBytes, FlushPolicy{})
		if err != nil {
			t.Fatal(err)
		}
		slotID := st.SlotOf("agg-scatter")
		slot, _ := st.Slot(slotID)

		const n = 12
		want := make([]byte, 0, 1<<16)
		for i := uint64(1); i <= n; i++ {
			rec := mkRec("agg-scatter", uint32(i), fmt.Sprintf("cmd-s-%d", i))
			rec.Events[0].Body = bytes.Repeat([]byte{byte(i), 0x00, 0xff}, 2048)
			want = append(want, rec.EncodeBinary(nil)...)
			if _, err := st.Append(rec); err != nil {
				t.Fatalf("scatter=%v append %d: %v", scatter, i, err)
			}
		}
		_, _, got, err := st.ReadSlotBytes(slotID, 1, n+1, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("scatter=%v: WAL bytes differ from EncodeBinary output (%d vs %d bytes)", scatter, len(got), len(want))
		}
		if slot.LastSeq() != n {
			t.Fatalf("scatter=%v: LEO=%d want %d", scatter, slot.LastSeq(), n)
		}

		// The same bytes must survive a reopen (recovery rescans the frames).
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		st2, err := OpenStore(dir, 128, DefaultSegmentBytes, FlushPolicy{})
		if err != nil {
			t.Fatal(err)
		}
		_, _, again, err := st2.ReadSlotBytes(slotID, 1, n+1, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(again, want) {
			t.Fatalf("scatter=%v: recovered bytes differ from what was written", scatter)
		}
		st2.Close()
	}
}

// A record with many events spans more than one pwritev call (IOV_MAX); the
// batching and short-write resume must not disturb the byte stream.
func TestPwritevAllBatchesBeyondIOVMax(t *testing.T) {
	path := filepath.Join(t.TempDir(), "iov.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	const segs = scatterMaxIOV*2 + 7
	parts := make([][]byte, 0, segs)
	var want []byte
	for i := 0; i < segs; i++ {
		p := []byte(fmt.Sprintf("%05d", i))
		parts = append(parts, p)
		want = append(want, p...)
	}
	if err := pwritevAll(f, parts, 0); err != nil {
		t.Fatalf("pwritevAll: %v", err)
	}
	got := make([]byte, len(want))
	if _, err := f.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("batched scatter write produced different bytes")
	}

	// advanceParts is the resume primitive behind short writes.
	rest := advanceParts([][]byte{[]byte("abc"), []byte("de"), []byte("f")}, 4)
	var tail []byte
	for _, p := range rest {
		tail = append(tail, p...)
	}
	if string(tail) != "ef" {
		t.Fatalf("advanceParts left %q, want %q", tail, "ef")
	}
}
