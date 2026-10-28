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
	"path/filepath"
	"testing"
)

// TestOpenStoreLoadsOnlyTheSlotsOnDisk pins the segment-scoped startup: a node
// starts on what it actually holds, not on every id the fixed table can name.
//
// The table is fixed at 2048 slots (DESIGN §7.2.7) while a node carries the
// slots its segment assigns it — a handful at the small generations, a few
// hundred at the largest. Walking all 2048 ids on every start meant thousands of
// stat() calls, and a slot id that has never been used in this cluster could find
// itself with a directory if any reader took the "open it if it is cold" path.
func TestOpenStoreLoadsOnlyTheSlotsOnDisk(t *testing.T) {
	dir := t.TempDir()
	const slotCount = 64
	withData := []int32{3, 7, 40}

	st, err := OpenStore(dir, slotCount, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i, slot := range withData {
		if _, err := st.AppendToSlot(slot, mkRec("agg-lazy", uint32(i+1), "cmd-lazy-"+string(rune('a'+i)))); err != nil {
			t.Fatalf("append to slot %d: %v", slot, err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := OpenStore(dir, slotCount, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	loaded := map[int32]bool{}
	for i := int32(0); i < slotCount; i++ {
		if s, err := reopened.SlotIfLoaded(i); err != nil {
			t.Fatalf("SlotIfLoaded(%d): %v", i, err)
		} else if s != nil {
			loaded[i] = true
		}
	}
	if len(loaded) != len(withData) {
		t.Fatalf("startup loaded %d slots, want %d: %v", len(loaded), len(withData), loaded)
	}
	for _, slot := range withData {
		if !loaded[slot] {
			t.Fatalf("slot %d holds records but was not loaded: %v", slot, loaded)
		}
	}

	// A slot the node does not hold must stay absent on disk: asking about it
	// (an admin view, a health scan) may not materialise it.
	if _, err := os.Stat(filepath.Join(dir, "slot-005")); !os.IsNotExist(err) {
		t.Fatalf("an untouched slot was materialised on disk (stat err = %v)", err)
	}
	if s, _ := reopened.SlotIfLoaded(5); s != nil {
		t.Fatal("SlotIfLoaded must not open a cold slot")
	}

	// The write path still allocates on demand: Slot() is the only intake.
	sl, err := reopened.Slot(5)
	if err != nil || sl == nil {
		t.Fatalf("Slot(5) must lazily open on demand: sl=%v err=%v", sl, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "slot-005")); err != nil {
		t.Fatalf("a slot opened for writing must exist on disk: %v", err)
	}
}
