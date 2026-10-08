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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"pushupes/internal/data"
)

// TestSlotDiskBytesCoversLoadedAndColdCopies pins the per-slot source of the
// cluster storage gauge. Two cases matter and they come from different places:
//
//   - a copy this process holds answers from its own accounting (the segment
//     sizes it tracks, including the tail it just wrote);
//   - a copy that is on disk but was never opened by this process — OpenStore
//     loads every non-empty slot directory at startup, so this is the directory
//     that appeared afterwards (a migration push before its reload) — is
//     stat'ed from the files, because reporting 0 there would silently shrink
//     the gauge.
func TestSlotDiskBytesCoversLoadedAndColdCopies(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir, 4, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	rec := &data.EventRecord{
		AggregateID: "agg-1",
		Version:     1,
		UnixTime:    time.Now().Unix(),
		CommandID:   "cmd-1",
		Events:      []data.Event{{Type: "T", Body: []byte("{}")}},
	}
	if _, err := st.Append(rec); err != nil {
		t.Fatalf("append: %v", err)
	}
	// Appends sit in the segment's write buffer until they are flushed; the
	// footprint on disk (what an operator watches) only exists after that.
	if err := st.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// The aggregate routes to its own slot: the footprint is that slot's.
	slot := st.SlotOf(rec.AggregateID)
	if got := st.SlotDiskBytes(slot); got <= 0 {
		t.Fatalf("loaded copy of slot %d reports %d bytes", slot, got)
	}

	// Slot ids used below: a copy that lands later, and one that never exists.
	cold := int32(2)
	for cold == slot {
		cold++
	}
	absent := int32(1)
	for absent == slot || absent == cold {
		absent++
	}
	// A slot with no directory at all has no footprint — and asking for a size
	// must not create one (the gauge is read on a timer, it cannot be the thing
	// that materialises 1680 slot directories).
	if err := os.RemoveAll(st.SlotDir(absent)); err != nil {
		t.Fatal(err)
	}
	if got := st.SlotDiskBytes(absent); got != 0 {
		t.Fatalf("a slot without a directory reports %d bytes, want 0", got)
	}
	if st.SlotPresent(absent) {
		t.Fatal("asking for a slot's size created state on disk")
	}

	// A copy that lands on disk after the store opened (never opened here).
	if err := os.MkdirAll(st.SlotDir(cold), 0o755); err != nil {
		t.Fatal(err)
	}
	const size = 4096
	if err := os.WriteFile(filepath.Join(st.SlotDir(cold), fmt.Sprintf("%016d.wal", 1)), make([]byte, size), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := st.SlotDiskBytes(cold); got != size {
		t.Fatalf("cold copy reports %d bytes, want the %d on disk", got, size)
	}
}
