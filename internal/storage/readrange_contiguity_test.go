// Copyright 2026 berkaroad
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/berkaroad/pushupes/internal/data"
)

// A fetch payload is a seq window the replica applies by BACK-COMPUTING its
// first seq as NextSeq - frameCount (applyFetchPayload): frames carry no seq,
// so the batch must cover consecutive seqs or every frame is misnumbered.
//
// The live-cluster incident this pins: a fresh empty seat asked from seq 1
// with the standard 4MiB budget. The first segment stopped mid-log (a small
// positive remainder stayed at the slot level), and the loop then visited
// the NEXT segment — whose per-segment budget restarts empty and always
// takes at least one frame — jumping over the seq gap. `next` advanced past
// that frame, so the follower's back-computed start landed deep inside the
// first segment ("seq 56970 not contiguous (counter 0)", repeating forever:
// every round reproduced the identical gapped batch).
func TestReadRangeBatchIsSeqContiguous(t *testing.T) {
	dir := t.TempDir()
	const slotCount = 8
	const segBytes = 16 << 10

	write := func(t *testing.T) {
		st, err := OpenStore(dir, slotCount, segBytes, FlushPolicy{})
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		slot := st.SlotOf("a")
		// ~3.5 segments of one aggregate: version == slot seq throughout.
		for v := uint32(1); v*80 < 3*segBytes+segBytes/2; v++ {
			rec := &data.EventRecord{
				AggregateID: "a", Version: v,
				CommandID: fmt.Sprintf("c-%d", v),
				Events:    []data.Event{{Type: "E", Body: []byte("payload-body")}},
			}
			if _, err := st.Append(rec); err != nil {
				t.Fatalf("append v%d: %v", v, err)
			}
		}
		leo := st.LastSeqOf(slot)
		if leo < 2*segBytes/100 { // sanity: several segments, not a hairball
			t.Fatalf("leo=%d — not enough records for segment rolls", leo)
		}
		sl, err := st.Slot(slot)
		if err != nil {
			t.Fatal(err)
		}
		if len(sl.segments) < 2 {
			t.Fatalf("expected multiple segments, got %d", len(sl.segments))
		}
	}
	read := func(t *testing.T, fromSeq, maxBytes uint64) ([]data.ByteRange, uint64, []byte) {
		st, err := OpenStore(dir, slotCount, segBytes, FlushPolicy{}) // restart state
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		ranges, next, payload, err := st.ReadSlotBytes(st.SlotOf("a"), fromSeq, 0, int64(maxBytes))
		if err != nil {
			t.Fatal(err)
		}
		return ranges, next, payload
	}

	// assertContiguous pins the consumer contract: walking the payload must
	// find exactly next-from frames, and the versions decoded from the frames
	// must match the seq window [next-count, next) the follower will apply.
	assertContiguous := func(t *testing.T, name string, fromSeq uint64, payload []byte, next uint64) {
		n := 0
		off := 0
		var versions []uint32
		for off+4 <= len(payload) {
			l := int(binary.BigEndian.Uint32(payload[off:]))
			if l < 30 || off+4+l > len(payload) {
				t.Fatalf("%s: torn walk at %d", name, off)
			}
			meta, _, err := data.DecodeRecordMeta(payload[off : off+4+l])
			if err != nil {
				t.Fatal(err)
			}
			versions = append(versions, meta.Version)
			off += 4 + l
			n++
		}
		if uint64(n) != next-fromSeq {
			t.Fatalf("%s: batch is not a contiguous seq window: from=%d next=%d frames=%d (the follower back-computes start as next-frames=%d, misnumbering every frame by %d)",
				name, fromSeq, next, n, next-uint64(n), fromSeq-(next-uint64(n)))
		}
		for i, v := range versions {
			if uint64(v) != next-uint64(n)+uint64(i) {
				t.Fatalf("%s: frame %d version %d not at seq %d", name, i, v, next-uint64(n)+uint64(i))
			}
		}
	}

	write(t)

	st0, err := OpenStore(dir, slotCount, segBytes, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	sl0, _ := st0.Slot(st0.SlotOf("a"))
	seg1 := sl0.segments[0]
	seg1Bytes := seg1.sizeBytes - int64(WALHeaderLen)
	leo := st0.LastSeqOf(st0.SlotOf("a"))
	st0.Close()

	// 1) The incident shape: from seq 1, budget stops one frame short of the
	//    first segment's end — the remainder must not open the next segment.
	_, next1, p1 := read(t, 1, uint64(seg1Bytes)-8)
	assertContiguous(t, "cap-mid-segment-1", 1, p1, next1)

	// 2) Healthy boundary crossing: the batch drains segment 1 completely and
	//    continues — contiguous by construction, must stay so.
	fromTail := seg1.BaseSeq + uint64(seg1.RecordCnt) - 2 // 2 frames before the boundary
	_, next2, p2 := read(t, fromTail, 4<<20)
	assertContiguous(t, "drain-then-continue", fromTail, p2, next2)

	// 3) Fresh-seat full catch-up: one huge budget from 1 to LEO.
	_, next3, p3 := read(t, 1, 64<<20)
	assertContiguous(t, "whole-log", 1, p3, next3)
	if next3 != leo+1 {
		t.Fatalf("whole-log batch ended at %d, want LEO+1=%d", next3, leo+1)
	}
}
