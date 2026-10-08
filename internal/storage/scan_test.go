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
	"testing"

	"github.com/berkaroad/pushupes/internal/data"
)

// ScanFetchState is the leader's per-round answer for every reported position:
// "has data at or beyond from" plus a wake handle to park on. Both halves matter
// — a missing handle turns a long poll into a hot loop.
func TestScanFetchState(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir, 16, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// slot 2 gets two records; slot 5 stays untouched (never opened).
	agg := aggInSlotForTest(t, st, 2)
	for i := 1; i <= 2; i++ {
		if _, err := st.Append(&data.EventRecord{
			AggregateID: agg, Version: uint32(i), CommandID: fmt.Sprintf("scan-%d", i),
			Events: []data.Event{{Type: "t", Body: []byte("x")}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	slots := []int32{2, 5, 2, -1, 999}
	froms := []uint64{1, 1, 3, 1, 1}
	moved := make([]bool, len(slots))
	wakes := make([]<-chan struct{}, len(slots))
	st.ScanFetchState(slots, froms, moved, wakes)

	want := []bool{true, false, false, false, false}
	for i := range want {
		if moved[i] != want[i] {
			t.Errorf("entry %d (slot %d from %d): moved=%v want %v", i, slots[i], froms[i], moved[i], want[i])
		}
	}
	for i, s := range slots {
		if s < 0 || s >= st.SlotCount {
			if wakes[i] != nil {
				t.Errorf("entry %d: out-of-range slot returned a wake handle", i)
			}
			continue
		}
		if wakes[i] == nil {
			t.Fatalf("entry %d (slot %d): no wake handle — a long poll would have nothing to park on", i, s)
		}
	}

	// The parked handle must fire on the next append (no-lost-wake).
	watch := make([]<-chan struct{}, 1)
	st.ScanFetchState([]int32{5}, []uint64{1}, make([]bool, 1), watch)
	if watch[0] == nil {
		t.Fatal("slot 5: no wake handle to park on")
	}
	before := watch[0]
	if _, err := st.Append(&data.EventRecord{
		AggregateID: aggInSlotForTest(t, st, 5), Version: 1, CommandID: "scan-wake",
		Events: []data.Event{{Type: "t", Body: []byte("y")}},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-before:
	default:
		t.Fatal("wake handle taken before the append did not fire")
	}
}

// aggInSlotForTest finds an aggregate id that hashes to the wanted slot.
func aggInSlotForTest(t *testing.T, st *Store, slot int32) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		id := fmt.Sprintf("scan-agg-%d", i)
		if st.SlotOf(id) == slot {
			return id
		}
	}
	t.Fatalf("no aggregate found for slot %d", slot)
	return ""
}
