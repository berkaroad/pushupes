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
	"encoding/binary"
	"fmt"
	"os"
	"testing"
)

// A .cidx written by an earlier build carries a bloom section at its tail. The
// current reader searches the entry blocks and leaves that tail alone, so a data
// directory from before the slot-level filter must open and answer as usual.
func TestSegCmdIndexWithLegacyBloomSection(t *testing.T) {
	dir := t.TempDir()
	const n = 300

	sl, err := OpenSlotRepairing(dir, 7, 1<<40, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		mustAppend(t, sl, mkRec(fmt.Sprintf("a%04d", i), 1, fmt.Sprintf("c%04d", i)))
	}
	roll(t, sl)
	mustAppend(t, sl, mkRec("a9999", 1, "c9999"))
	base := sl.segments[0].BaseSeq
	if err := sl.Close(); err != nil {
		t.Fatal(err)
	}

	// Append a fake bloom tail and point the header's bloom length at it: the
	// exact layout the previous format produced.
	path := segCidxPath(dir, base)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	tail := make([]byte, 64)
	for i := range tail {
		tail[i] = byte(i)
	}
	if _, err := f.WriteAt(tail, st.Size()); err != nil {
		t.Fatal(err)
	}
	hdr := make([]byte, 32)
	if _, err := f.ReadAt(hdr, 0); err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint32(hdr[28:32], uint32(len(tail)))
	if _, err := f.WriteAt(hdr, 0); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// A start leaves a valid .cidx alone; every command still resolves.
	sl2, err := OpenSlotRepairing(dir, 7, 1<<40, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer sl2.Close()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("c%04d", i)
		rec, seq, err := sl2.RecordByCommand(id)
		if err != nil {
			t.Fatal(err)
		}
		if rec == nil || seq != uint64(i+1) {
			t.Fatalf("command %q with a legacy bloom tail: found=%v seq=%d, want seq %d",
				id, rec != nil, seq, i+1)
		}
	}
	if rec, seq, err := sl2.RecordByCommand("c-never"); err != nil || rec != nil || seq != 0 {
		t.Fatalf("missing command resolved: rec=%v seq=%d err=%v", rec, seq, err)
	}
}
