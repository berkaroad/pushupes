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
	"fmt"
	"os"
	"testing"

	"github.com/berkaroad/pushupes/internal/data"
)

// The header-only recovery walk must report exactly the records the full decode
// does: same seq, aggregate, version and command id, including for a frame
// larger than the read window. A torn tail must still be cut at the last whole
// record and must not make the header walk report a partial frame.
func TestScanHeadersMatchesFullScan(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir, 16, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	// Closed explicitly (not deferred) below: reopening the files is the point
	// of the test, and Store.Close is not idempotent.

	slot := int32(2)
	agg := aggInSlotForTest(t, st, slot)
	// Body sizes per record; the third record's frame (~1.5 MiB) exceeds
	// frameWindow, which takes the dedicated large-frame read path.
	specs := [][]int{{64}, {1024}, {768 << 10, 768 << 10}, {4096}}
	for i, sizes := range specs {
		if i > 0 {
			agg = nextAggInSlot(t, st, slot, agg)
		}
		evs := make([]data.Event, 0, len(sizes))
		for _, n := range sizes {
			evs = append(evs, data.Event{Type: "t", Body: bytes.Repeat([]byte("y"), n)})
		}
		rec := &data.EventRecord{
			AggregateID: agg, Version: 1, CommandID: fmt.Sprintf("hdr-%d", i),
			Events: evs,
		}
		out, err := st.Append(rec)
		if err != nil {
			t.Fatal(err)
		}
		if out.Status != data.StatusSuccess {
			t.Fatalf("append %d (bodies %v) -> %+v", i, sizes, out)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	paths, err := SegmentFilesOf(dir + "/slot-002")
	if err != nil || len(paths) == 0 {
		t.Fatalf("segment files: %v %v", paths, err)
	}

	// Full decode (the old recovery path) and the header-only walk must agree.
	segFull, err := LoadSegment(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		seq     uint64
		agg     string
		version uint32
		cmdHash uint64
	}
	var want []row
	if err := segFull.ScanFrom(segFull.BaseSeq, func(seq uint64, rec *data.EventRecord) bool {
		want = append(want, row{seq, rec.AggregateID, rec.Version, data.HashCommandID(rec.CommandID)})
		return true
	}); err != nil {
		t.Fatal(err)
	}
	segFull.Close()

	segMeta, err := LoadSegment(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	var got []row
	if err := segMeta.ScanHeaders(segMeta.BaseSeq, func(seq uint64, meta data.RecordMeta) bool {
		got = append(got, row{seq, meta.AggregateID, meta.Version, meta.CommandHash})
		return true
	}); err != nil {
		t.Fatal(err)
	}
	segMeta.Close()

	if len(want) != len(specs) {
		t.Fatalf("full scan saw %d records, want %d", len(want), len(specs))
	}
	if len(got) != len(want) {
		t.Fatalf("header scan saw %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("record %d: header walk %+v, full scan %+v", i, got[i], want[i])
		}
	}

	// Torn tail: a length prefix whose body is missing must be cut, and the
	// header walk must still see exactly the whole records.
	fi, err := os.Stat(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	whole := fi.Size()
	f, err := os.OpenFile(paths[0], os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	partial := append([]byte{0, 0, 1, 0}, bytes.Repeat([]byte("z"), 128)...) // claims 256B, has 128
	if _, err := f.Write(partial); err != nil {
		t.Fatal(err)
	}
	f.Close()

	segTorn, err := LoadSegment(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	defer segTorn.Close()
	if segTorn.RecordCnt != int64(len(specs)) {
		t.Fatalf("after truncation the segment holds %d records, want %d", segTorn.RecordCnt, len(specs))
	}
	if fi, err := os.Stat(paths[0]); err != nil {
		t.Fatal(err)
	} else if fi.Size() != whole || segTorn.sizeBytes != whole {
		t.Fatalf("torn tail not truncated: file %d, segment %d, want %d", fi.Size(), segTorn.sizeBytes, whole)
	}
	n := 0
	if err := segTorn.ScanHeaders(segTorn.BaseSeq, func(uint64, data.RecordMeta) bool { n++; return true }); err != nil {
		t.Fatal(err)
	}
	if n != len(specs) {
		t.Fatalf("header walk after truncation saw %d records, want %d", n, len(specs))
	}
}
