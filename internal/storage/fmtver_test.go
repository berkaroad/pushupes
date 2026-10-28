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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/berkaroad/pushupes/internal/data"
)

// v1SegmentFile lays down a legacy (flags-less frame) segment by hand: the
// starting point for the format-version tests.
func v1SegmentFile(t *testing.T, dir string, slotID int32, baseSeq uint64, recs []struct {
	agg, cmd, typ string
	body          []byte
	ver           uint32
}) {
	t.Helper()
	path := filepath.Join(dir, segmentName(baseSeq))
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := make([]byte, WALHeaderLen)
	copy(h[0:4], WALMagic)
	h[4] = byte(WALFormatVerLegacy)
	binary.BigEndian.PutUint32(h[8:12], uint32(slotID))
	binary.BigEndian.PutUint64(h[12:20], baseSeq)
	if _, err := f.Write(h); err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		var payload bytes.Buffer
		binary.Write(&payload, binary.BigEndian, uint16(len(r.agg)))
		payload.WriteString(r.agg)
		binary.Write(&payload, binary.BigEndian, r.ver)
		binary.Write(&payload, binary.BigEndian, uint64(1000+int(r.ver)))
		binary.Write(&payload, binary.BigEndian, uint16(len(r.cmd)))
		payload.WriteString(r.cmd)
		binary.Write(&payload, binary.BigEndian, uint16(1))
		binary.Write(&payload, binary.BigEndian, uint16(len(r.typ)))
		payload.WriteString(r.typ)
		binary.Write(&payload, binary.BigEndian, uint32(len(r.body)))
		payload.Write(r.body)
		frame := make([]byte, 4+payload.Len())
		binary.BigEndian.PutUint32(frame[:4], uint32(payload.Len()))
		copy(frame[4:], payload.Bytes())
		if _, err := f.Write(frame); err != nil {
			t.Fatal(err)
		}
	}
}

// A non-empty legacy tail stays sealed history and a current-format segment
// takes over the appends: frames never mix layouts inside one segment.
func TestLegacyTailRollsToCurrentFormat(t *testing.T) {
	dir := t.TempDir()
	slotID := int32(3)
	v1SegmentFile(t, dir, slotID, 1, []struct {
		agg, cmd, typ string
		body          []byte
		ver           uint32
	}{
		{"agg-old", "cmd-old-1", "ev", []byte("one"), 1},
		{"agg-old", "cmd-old-2", "ev", []byte("two"), 2},
	})

	sl, err := OpenSlotRepairing(dir, slotID, 4<<20, FlushPolicy{Interval: time.Millisecond})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer sl.Close()
	if len(sl.segments) != 2 {
		t.Fatalf("segments = %d want 2 (legacy history + new tail)", len(sl.segments))
	}
	old, tail := sl.segments[0], sl.segments[1]
	if old.Writable() {
		t.Fatal("the legacy tail must be sealed history")
	}
	if old.fmtVer != WALFormatVerLegacy || tail.fmtVer != WALFormatVer {
		t.Fatalf("versions wrong: old %d new %d", old.fmtVer, tail.fmtVer)
	}
	if tail.BaseSeq != old.LastSeq+1 {
		t.Fatalf("new tail base %d want %d", tail.BaseSeq, old.LastSeq+1)
	}
	if got := sl.seqCounter.Load(); got != old.LastSeq {
		t.Fatalf("seqCounter = %d want %d", got, old.LastSeq)
	}
	// The legacy header must not have been rewritten.
	h := make([]byte, WALHeaderLen)
	ff, err := os.Open(old.Path)
	if err != nil {
		t.Fatal(err)
	}
	ff.Read(h)
	ff.Close()
	if h[4] != byte(WALFormatVerLegacy) {
		t.Fatal("sealed legacy header must stay verbatim")
	}

	// Legacy history stays readable and idempotency keeps its reach across
	// the format boundary: the repair start built the sealed segment's
	// command index, and its v1 frames decode under the stamped version.
	rec, seq, err := sl.RecordByCommand("cmd-old-1")
	if err != nil || rec == nil {
		t.Fatalf("RecordByCommand legacy: %v rec=%v", err, rec)
	}
	if seq != 1 || rec.AggregateID != "agg-old" || string(rec.Events[0].Body) != "one" {
		t.Fatalf("legacy record wrong: seq %d rec %+v", seq, rec)
	}

	// A v2 append lands in the new segment and round-trips.
	out, err := sl.Append(&data.EventRecord{
		AggregateID: "agg-new", Version: 1, CommandID: "cmd-new",
		Events: []data.Event{{Type: "ev", Body: []byte("hello")}},
	})
	if err != nil || out.Status != data.StatusSuccess {
		t.Fatalf("append: %v out %+v", err, out)
	}
	got, _, err := sl.RecordByCommand("cmd-new")
	if err != nil || got == nil || got.Version != 1 {
		t.Fatalf("new record: %v %v", err, got)
	}
	if tail.RecordCnt != 1 {
		t.Fatalf("new record must land in the current-format tail, cnt=%d", tail.RecordCnt)
	}

	// Reopen: the legacy segment must still read under its stamped version,
	// the new tail (also legacy-format-free) under the current one.
	sl2, err := OpenSlot(dir, slotID, 4<<20, FlushPolicy{Interval: time.Millisecond})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer sl2.Close()
	rec2, _, err := sl2.RecordByCommand("cmd-new")
	if err != nil || rec2 == nil {
		t.Fatalf("reopen lookup: %v", err)
	}
}

// An empty legacy tail is header-only history that never was: flip its
// header to the current format and resume on it.
func TestEmptyLegacyTailFlipsInPlace(t *testing.T) {
	dir := t.TempDir()
	slotID := int32(7)
	v1SegmentFile(t, dir, slotID, 1, nil)

	sl, err := OpenSlotRepairing(dir, slotID, 4<<20, FlushPolicy{Interval: time.Millisecond})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer sl.Close()
	if len(sl.segments) != 1 || !sl.segments[0].Writable() {
		t.Fatalf("want one writable segment, got %d", len(sl.segments))
	}
	seg := sl.segments[0]
	if seg.fmtVer != WALFormatVer {
		t.Fatalf("empty legacy tail must flip to the current format")
	}
	h := make([]byte, WALHeaderLen)
	ff, err := os.Open(seg.Path)
	if err != nil {
		t.Fatal(err)
	}
	ff.Read(h)
	ff.Close()
	if h[4] != byte(WALFormatVer) {
		t.Fatal("the on-disk header must carry the flipped version")
	}
	out, err := sl.Append(&data.EventRecord{
		AggregateID: "agg-a", Version: 1, CommandID: "cmd-a",
		Events: []data.Event{{Type: "ev", Body: []byte("x")}},
	})
	if err != nil || out.Status != data.StatusSuccess {
		t.Fatalf("append after flip: %v %+v", err, out)
	}
}
