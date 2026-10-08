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
	"os"
	"testing"

	"github.com/berkaroad/pushupes/internal/data"
)

// The on-disk aggregate index must map (aggregate, version) to the seq of the
// record holding it, and must never serve a file it cannot validate.
func TestSegAggIndexRoundTrip(t *testing.T) {
	dir := t.TempDir()
	seg := &Segment{SlotID: 3, BaseSeq: 100, Path: dir + "/0000000000000100.wal"}
	if err := os.WriteFile(seg.Path, make([]byte, 64), 0o644); err != nil {
		t.Fatal(err)
	}
	aggA := "agg-a"
	aggB := "agg-b"
	lists := []segAggList{
		{hash: data.HashCommandID(aggA), firstVer: 7, firstOrd: 3, pairs: []segAggPair{
			{ord: 3, off: 4096, unix: 1700000003}, {ord: 5, off: 8192, unix: 1700000005}, {ord: 9, off: 12288, unix: 1700000009},
		}},
		{hash: data.HashCommandID(aggB), firstVer: 1, firstOrd: 0, pairs: []segAggPair{
			{ord: 0, off: 1024, unix: 1700000001}, {ord: 2, off: 2048, unix: 1700000002},
		}},
	}
	if err := writeSegAggIndex(seg, lists); err != nil {
		t.Fatal(err)
	}
	ix, err := openSegAggIndex(dir, 3, 100)
	if err != nil || ix == nil {
		t.Fatalf("open: %v %v", ix, err)
	}
	if !ix.Valid() {
		t.Fatal("fresh index does not validate")
	}
	defer ix.Close()

	// version -> record: entry.firstVer is the first version and firstOrd the
	// ordinal of that record, so index i covers version firstVer+i, offset
	// offs[i], and seq baseSeq+firstOrd+i.
	var firstVer, firstOrd uint32
	gotOrd, gotOff := uint32(0), uint32(0)
	gotUnix := int64(0)
	found := ix.Lookup(data.HashCommandID(aggA), func(e segAggEntry) bool {
		firstVer, firstOrd = e.firstVer, e.firstOrd
		ord, off, unix, ok := ix.Pair(e, 1) // the second record of the entry
		if !ok {
			return false
		}
		gotOrd, gotOff, gotUnix = ord, off, unix
		return true
	})
	if !found || firstVer != 7 || firstOrd != 3 || gotOrd != 5 || gotOff != 8192 || gotUnix != 1700000005 {
		t.Fatalf("lookup: found=%v firstVer=%d firstOrd=%d ord=%d off=%d unix=%d",
			found, firstVer, firstOrd, gotOrd, gotOff, gotUnix)
	}
	if _, _, _, ok := ix.Pair(segAggEntry{count: 2}, 2); ok {
		t.Fatal("out of range pair reported")
	}
	// A missing aggregate must come back empty, not wrong.
	if ix.Lookup(data.HashCommandID("nope"), func(segAggEntry) bool { return true }) {
		t.Fatal("missing aggregate reported present")
	}
	// A different slot must not be served this file.
	if other, err := openSegAggIndex(dir, 4, 100); err != nil || other != nil {
		t.Fatalf("slot mismatch: %v %v", other, err)
	}
	// A damaged byte must fail validation rather than be trusted.
	f, err := os.OpenFile(segAidxPath(dir, 100), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, int64(segAidxHeaderByte)+4); err != nil {
		t.Fatal(err)
	}
	f.Close()
	ix2, err := openSegAggIndex(dir, 3, 100)
	if err != nil || ix2 == nil {
		t.Fatalf("reopen: %v %v", ix2, err)
	}
	if ix2.Valid() {
		t.Fatal("damage was not detected")
	}
	ix2.Close()
}
