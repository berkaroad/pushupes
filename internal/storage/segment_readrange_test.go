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
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/berkaroad/pushupes/internal/data"
)

// The byte-range walk that replica fetch and migration use must cover exactly
// the frames it covered per-frame: this file pins the windowed rewrite (one
// pread per window instead of one per record) against the original per-frame
// implementation, kept below as the oracle.

// readRangePerFrameRef is the implementation ReadRange used before the
// windowed rewrite: it read a 4-byte length header per record to find frame
// boundaries, so fetching the segment tail paid one pread per frame between
// the sparse index hint and the wanted seq. Kept verbatim as the oracle.
func readRangePerFrameRef(s *Segment, fromSeq, untilSeq uint64, maxBytes int64) ([]data.ByteRange, uint64, error) {
	if s.RecordCnt == 0 {
		return nil, fromSeq, nil
	}
	if fromSeq < s.BaseSeq {
		fromSeq = s.BaseSeq
	}
	if fromSeq > s.LastSeq {
		return nil, fromSeq, nil
	}
	off := s.indexPos(fromSeq)
	seq := s.indexSeqAt(off)
	var ranges []data.ByteRange
	var total int64
	next := fromSeq
	buf := make([]byte, 64<<10)
	for off < s.sizeBytes {
		if untilSeq > 0 && seq >= untilSeq {
			break
		}
		if seq < fromSeq {
			h := buf[:4]
			if _, err := s.File.ReadAt(h, off); err != nil {
				return ranges, next, err
			}
			recLen := int64(binary.BigEndian.Uint32(h))
			if recLen < minRecordLen || off+4+recLen > s.sizeBytes {
				break
			}
			off += 4 + recLen
			seq++
			continue
		}
		h := buf[:4]
		if _, err := s.File.ReadAt(h, off); err != nil {
			return ranges, next, err
		}
		recLen := int64(binary.BigEndian.Uint32(h))
		if recLen < minRecordLen || off+4+recLen > s.sizeBytes {
			break
		}
		if total > 0 && total+4+recLen > maxBytes {
			break
		}
		if len(ranges) > 0 && ranges[len(ranges)-1].End == off {
			ranges[len(ranges)-1].End = off + 4 + recLen
		} else {
			ranges = append(ranges, data.ByteRange{
				Segment: filepath.Base(s.Path), Start: off, End: off + 4 + recLen,
			})
		}
		total += 4 + recLen
		next = seq + 1
		off += 4 + recLen
		seq++
		if total >= maxBytes {
			break
		}
	}
	return ranges, next, nil
}

const testSlotCount = 16

// twoAggsOnOneSlot returns two aggregate ids that route to the same slot: the
// identity assertion below compares per-slot seq numbers, so both streams must
// share one slot (a second slot would renumber seq from 1 and collide).
func twoAggsOnOneSlot(t testing.TB) (string, string, int32) {
	t.Helper()
	a := "agg-a"
	slot := data.SlotOf(a, testSlotCount)
	for i := 0; i < 100000; i++ {
		b := fmt.Sprintf("agg-b%d", i)
		if data.SlotOf(b, testSlotCount) == slot {
			return a, b, slot
		}
	}
	t.Fatal("no second aggregate found on the same slot")
	return "", "", 0
}

// appendRecords writes n records (bodySize bytes each) alternating between the
// given aggregates. The store is left OPEN: the caller decides whether to close
// it (the segment-level test must, to load the file independently).
func appendRecords(t testing.TB, st *Store, aggs []string, n, bodySize int) {
	t.Helper()
	body := bytes.Repeat([]byte("x"), bodySize)
	vers := make([]uint32, len(aggs))
	for i := 0; i < n; i++ {
		k := i % len(aggs)
		vers[k]++
		rec := &data.EventRecord{
			AggregateID: aggs[k], Version: vers[k], UnixTime: 1700000000,
			CommandID: fmt.Sprintf("cmd-%s-%d", aggs[k], vers[k]),
			Events:    []data.Event{{Type: "t", Body: body}},
		}
		out, err := st.Append(rec)
		if err != nil || out.Status != data.StatusSuccess {
			t.Fatalf("append %d: %+v %v", i, out, err)
		}
	}
}

func openSlotStore(t testing.TB) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	st, err := OpenStore(dir, testSlotCount, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	return st, dir
}

// TestSegmentReadRangeMatchesPerFrameWalk pins the windowed walk to the
// per-frame oracle across the shapes the per-frame version had to handle:
// skip zone before fromSeq, untilSeq bound, maxBytes cap, past-LEO, clamped
// fromSeq, and a torn tail.
func TestSegmentReadRangeMatchesPerFrameWalk(t *testing.T) {
	a, b, slot := twoAggsOnOneSlot(t)
	st, dir := openSlotStore(t)
	appendRecords(t, st, []string{a, b}, 400, 512)
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	paths, err := SegmentFilesOf(filepath.Join(dir, fmt.Sprintf("slot-%03d", slot)))
	if err != nil || len(paths) == 0 {
		t.Fatalf("segment files: %v %v", paths, err)
	}
	seg, err := LoadSegment(paths[0])
	if err != nil {
		t.Fatalf("load segment: %v", err)
	}
	defer seg.Close()
	if seg.RecordCnt < 50 {
		t.Fatalf("segment too small for the equivalence shapes: %d records", seg.RecordCnt)
	}
	last := seg.LastSeq
	mid := seg.BaseSeq + uint64(seg.RecordCnt)/2

	shapes := []struct {
		name                  string
		from, until, maxBytes uint64
	}{
		{"from-base", seg.BaseSeq, 0, 1 << 20},
		{"skip-zone-mid", mid, 0, 1 << 20},
		{"until-bound", seg.BaseSeq, mid, 1 << 20},
		{"maxbytes-cap", seg.BaseSeq, 0, 3 * minRecordLen},
		{"maxbytes-single", mid, 0, 1},
		{"past-leo", last + 5, 0, 1 << 20},
		{"from-below-base", 1, 0, 1 << 20},
		{"until-before-from", mid, seg.BaseSeq, 1 << 20},
		{"until-past-leo", seg.BaseSeq, last + 9, 1 << 20},
	}
	for _, sh := range shapes {
		wantR, wantN, wantErr := readRangePerFrameRef(seg, sh.from, sh.until, int64(sh.maxBytes))
		gotR, gotN, gotErr := seg.ReadRange(sh.from, sh.until, int64(sh.maxBytes))
		if (wantErr == nil) != (gotErr == nil) {
			t.Fatalf("%s: err mismatch: ref=%v windowed=%v", sh.name, wantErr, gotErr)
		}
		if wantN != gotN {
			t.Fatalf("%s: next mismatch: ref=%d windowed=%d", sh.name, wantN, gotN)
		}
		if len(wantR) != len(gotR) {
			t.Fatalf("%s: ranges count mismatch: ref=%d windowed=%d (%v vs %v)",
				sh.name, len(wantR), len(gotR), wantR, gotR)
		}
		for i := range wantR {
			if wantR[i] != gotR[i] {
				t.Fatalf("%s: range %d mismatch: ref=%+v windowed=%+v", sh.name, i, wantR[i], gotR[i])
			}
		}
	}

	// A torn tail (the file lost bytes behind the tracked size) is where the
	// two implementations intentionally differ: the per-frame oracle surfaced
	// the unreadable header as an I/O error, while the windowed walk stops at
	// the last complete frame the way the recovery walk does. The covered
	// prefix must still be identical, and nothing may be claimed beyond it.
	raw, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	bornTorn := filepath.Join(t.TempDir(), filepath.Base(paths[0]))
	if err := os.WriteFile(bornTorn, raw[:len(raw)-9], 0o644); err != nil {
		t.Fatal(err)
	}
	torn, err := LoadSegment(bornTorn)
	if err != nil {
		t.Fatal(err)
	}
	defer torn.Close()
	// Put the tracked size back to the untruncated length so the walk sees a
	// segment whose file lost bytes underneath it (what a crash between the
	// write and the size update leaves behind).
	torn.sizeBytes = int64(len(raw))
	wantR, wantN, _ := readRangePerFrameRef(torn, torn.BaseSeq, 0, 1<<20)
	gotR, gotN, gotErr := torn.ReadRange(torn.BaseSeq, 0, 1<<20)
	if gotErr != nil {
		t.Fatalf("torn tail: windowed walk returned %v, want a clean stop", gotErr)
	}
	if wantN != gotN || len(wantR) != len(gotR) {
		t.Fatalf("torn tail: ref=(%d ranges,next=%d) windowed=(%d ranges,next=%d)",
			len(wantR), wantN, len(gotR), gotN)
	}
	for i := range wantR {
		if wantR[i] != gotR[i] {
			t.Fatalf("torn tail: range %d mismatch: ref=%+v windowed=%+v", i, wantR[i], gotR[i])
		}
	}
	if gotN == 0 || gotN > torn.LastSeq+1 {
		t.Fatalf("torn tail: next=%d outside the complete frame range (last=%d)", gotN, torn.LastSeq)
	}
}

// TestReadSlotBytesPayloadMatchesRecords checks the fetch unit of work by
// identity, not by count: the payload must decode to exactly the records
// fromSeq..next-1 with matching aggregate, version and command id.
func TestReadSlotBytesPayloadMatchesRecords(t *testing.T) {
	a, b, slot := twoAggsOnOneSlot(t)
	st, _ := openSlotStore(t)
	defer st.Close()
	appendRecords(t, st, []string{a, b}, 300, 256)

	want := map[uint64]string{} // slot seq -> "agg|version|command"
	for _, agg := range []string{a, b} {
		recs, seqs, err := st.ReadAggregate(agg, 1, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		for i, r := range recs {
			want[seqs[i]] = fmt.Sprintf("%s|%d|%s", r.AggregateID, r.Version, r.CommandID)
		}
	}
	leo := st.LastSeqOf(slot)

	for _, from := range []uint64{1, leo, leo - 7, leo / 2} {
		for _, capBytes := range []int64{1 << 20, 4096} {
			ranges, next, payload, err := st.ReadSlotBytes(slot, from, 0, capBytes)
			if err != nil {
				t.Fatalf("ReadSlotBytes(from=%d cap=%d): %v", from, capBytes, err)
			}
			if len(ranges) == 0 && next != from {
				t.Fatalf("from=%d cap=%d: next moved to %d with no ranges", from, capBytes, next)
			}
			off := 0
			for seq := from; seq < next; seq++ {
				rec, n, err := data.DecodeRecord(payload[off:])
				if err != nil {
					t.Fatalf("from=%d cap=%d seq=%d: decode: %v", from, capBytes, seq, err)
				}
				got := fmt.Sprintf("%s|%d|%s", rec.AggregateID, rec.Version, rec.CommandID)
				if want[seq] != got {
					t.Fatalf("from=%d cap=%d seq=%d: got %q want %q", from, capBytes, seq, got, want[seq])
				}
				off += n
			}
			if off != len(payload) {
				t.Fatalf("from=%d cap=%d: %d payload bytes left over after decoding up to seq %d",
					from, capBytes, len(payload)-off, next-1)
			}
		}
	}
}

// The tail read is what a steady replica asks for every round: one record,
// starting from a sparse index hint that can sit up to indexIntervalB behind.
func BenchmarkReadSlotBytesTail(b *testing.B) {
	benchReadSlotBytes(b, func(leo uint64) uint64 { return leo }, 4<<20)
}

// A replica that fell behind asks for a window of records.
func BenchmarkReadSlotBytesCatchup(b *testing.B) {
	benchReadSlotBytes(b, func(leo uint64) uint64 { return leo - 200 }, 4<<20)
}

// A cold replica replays the whole segment.
func BenchmarkReadSlotBytesWholeSegment(b *testing.B) {
	benchReadSlotBytes(b, func(uint64) uint64 { return 1 }, 1<<30)
}

func benchReadSlotBytes(b *testing.B, from func(leo uint64) uint64, capBytes int64) {
	a, bb, _ := twoAggsOnOneSlot(b)
	st, _ := openSlotStore(b)
	defer st.Close()
	appendRecords(b, st, []string{a, bb}, 6000, 1024)
	slot := st.SlotOf(a)
	leo := st.LastSeqOf(slot)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, _, err := st.ReadSlotBytes(slot, from(leo), 0, capBytes); err != nil {
			b.Fatal(err)
		}
	}
}
