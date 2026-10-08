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
	"strings"
	"testing"
	"time"

	"pushupes/internal/data"
)

// Recovery from an index must be indistinguishable from recovery by walking the
// WAL — and a start where the index is missing, damaged or stale must both
// produce that same state and leave a complete index behind for the next start.
func TestIndexDrivenRecoveryMatchesWalk(t *testing.T) {
	dir := t.TempDir()
	const slotBytes = int64(8 << 10) // small segments: force several rolls
	flush := FlushPolicy{Interval: time.Millisecond}

	// Two aggregates that share a slot, so the slot's directory holds more than
	// one stream and the arena is exercised.
	aggA := "agg-alpha"
	slot := data.SlotOf(aggA, 8)
	var aggB string
	for i := 0; i < 10000 && aggB == ""; i++ {
		cand := fmt.Sprintf("agg-beta-%d", i)
		if data.SlotOf(cand, 8) == slot && cand != aggA {
			aggB = cand
		}
	}
	if aggB == "" {
		t.Fatal("no sibling aggregate found")
	}
	const versions = 60
	st, err := OpenStore(dir, 8, slotBytes, flush)
	if err != nil {
		t.Fatal(err)
	}
	body := strings.Repeat("x", 80)
	for v := 1; v <= versions; v++ {
		for _, agg := range []string{aggA, aggB} {
			rec := &data.EventRecord{
				AggregateID: agg,
				Version:     uint32(v),
				CommandID:   fmt.Sprintf("cmd-%s-%d", agg, v),
				Events:      []data.Event{{Type: "t", Body: []byte(body)}},
			}
			if _, err := st.Append(rec); err != nil {
				t.Fatal(err)
			}
		}
	}
	sl, err := st.Slot(slot)
	if err != nil {
		t.Fatal(err)
	}
	if err := sl.Flush(); err != nil {
		t.Fatal(err)
	}
	if n := sl.SegmentCount(); n < 2 {
		t.Fatalf("expected several segments, got %d", n)
	}
	// Keep the appends and the index files the write path produced.
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	aggs := []string{aggA, aggB}
	want := slotState(t, dir, slot, aggs, slotBytes, flush, false)

	// 1. With the indexes in place: a normal start restores from them.
	beforeUsed := segIndexUsed.Load()
	got := slotState(t, dir, slot, aggs, slotBytes, flush, false)
	if got != want {
		t.Fatalf("index start diverged:\n%s\nwant:\n%s", got, want)
	}
	if segIndexUsed.Load() == beforeUsed {
		t.Fatal("index was not used")
	}

	// 2. Indexes removed: startup walks and then writes them.
	removeIndexes(t, dir, slot)
	built := segIndexBuilt.Load()
	got = slotState(t, dir, slot, aggs, slotBytes, flush, false)
	if got != want {
		t.Fatalf("walked start diverged:\n%s\nwant:\n%s", got, want)
	}
	if segIndexBuilt.Load() == built {
		t.Fatal("startup did not write the missing index")
	}
	assertIndexComplete(t, dir, slot)

	// 3. The next start is index-driven again (nothing left to build).
	built = segIndexBuilt.Load()
	used := segIndexUsed.Load()
	got = slotState(t, dir, slot, aggs, slotBytes, flush, false)
	if got != want {
		t.Fatalf("second start diverged:\n%s\nwant:\n%s", got, want)
	}
	if segIndexBuilt.Load() != built || segIndexUsed.Load() == used {
		t.Fatalf("second start rebuilt (built %d) or did not use the index (used %d)",
			segIndexBuilt.Load()-built, segIndexUsed.Load()-used)
	}

	// 4. A damaged block: the validated prefix loads, the rest is replayed and
	// the index is repaired.
	damageFirstBlock(t, dir, slot)
	damaged := segIndexDamaged.Load()
	got = slotState(t, dir, slot, aggs, slotBytes, flush, false)
	if got != want {
		t.Fatalf("damaged-index start diverged:\n%s\nwant:\n%s", got, want)
	}
	if segIndexDamaged.Load() == damaged {
		t.Fatal("damage was not detected")
	}
	assertIndexComplete(t, dir, slot)

	// 5. A stale index (only a prefix): the tail of each segment is replayed.
	truncateIndexes(t, dir, slot)
	replayed := segIndexReplayed.Load()
	got = slotState(t, dir, slot, aggs, slotBytes, flush, false)
	if got != want {
		t.Fatalf("stale-index start diverged:\n%s\nwant:\n%s", got, want)
	}
	if segIndexReplayed.Load() == replayed {
		t.Fatal("stale index was not replayed")
	}
	assertIndexComplete(t, dir, slot)

	// 6. Runtime (no repair): indexes removed, a running node still recovers
	// the same state and leaves no index behind.
	removeIndexes(t, dir, slot)
	built = segIndexBuilt.Load()
	running := slotState(t, dir, slot, aggs, slotBytes, flush, true)
	if running != want {
		t.Fatalf("runtime start diverged:\n%s\nwant:\n%s", running, want)
	}
	if segIndexBuilt.Load() != built {
		t.Fatal("a running node repaired an index")
	}
	if files := indexFiles(t, dir, slot); len(files) != 0 {
		t.Fatalf("runtime open created %v", files)
	}
}

// slotState opens the store the way the caller asks and folds the slot's state
// into a comparable string: per aggregate latest version, seq count and every
// record reachable by version, plus command lookups.
func slotState(t *testing.T, dir string, slot int32, aggs []string, segmentBytes int64, flush FlushPolicy, runtimeOpen bool) string {
	t.Helper()
	var (
		st  *Store
		err error
	)
	if runtimeOpen {
		st, err = openStoreMode(dir, 8, segmentBytes, flush, slotOpenLoad)
	} else {
		st, err = OpenStore(dir, 8, segmentBytes, flush)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	sl, err := st.Slot(slot)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "last=%d bytes=%d segments=%d\n", sl.LastSeq(), sl.TotalSize(), sl.SegmentCount())
	for _, agg := range aggs {
		recs, seqs, err := sl.AggregateVersion(agg, 1, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&b, "%s latest=%d count=%d records=%d\n", agg, sl.CurrentVersion(agg), sl.LastVersionOf(agg), len(recs))
		for i, r := range recs {
			fmt.Fprintf(&b, "  seq=%d v=%d cmd=%s body=%d\n", seqs[i], r.Version, r.CommandID, len(r.Events[0].Body))
		}
	}
	for _, agg := range aggs {
		for _, v := range []int{1, 30, 60} {
			cmd := fmt.Sprintf("cmd-%s-%d", agg, v)
			rec, seq, err := sl.RecordByCommand(cmd)
			if err != nil {
				t.Fatal(err)
			}
			if rec == nil {
				fmt.Fprintf(&b, "cmd %s: missing\n", cmd)
				continue
			}
			fmt.Fprintf(&b, "cmd %s: seq=%d v=%d\n", cmd, seq, rec.Version)
		}
	}
	return b.String()
}

func indexFiles(t *testing.T, dir string, slot int32) []string {
	t.Helper()
	slotDir := dir + "/" + fmt.Sprintf("slot-%03d", slot)
	entries, err := os.ReadDir(slotDir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".idx") || strings.HasSuffix(e.Name(), ".agx") || strings.HasSuffix(e.Name(), ".spx") {
			out = append(out, e.Name())
		}
	}
	return out
}

// removeIndexes deletes every index file of the slot, leaving the WAL behind.
func removeIndexes(t *testing.T, dir string, slot int32) {
	t.Helper()
	for _, n := range indexFiles(t, dir, slot) {
		if err := os.Remove(dir + "/" + fmt.Sprintf("slot-%03d", slot) + "/" + n); err != nil {
			t.Fatal(err)
		}
	}
}

// truncateIndexes leaves a fraction of each .idx (a stale index) behind.
func truncateIndexes(t *testing.T, dir string, slot int32) {
	t.Helper()
	slotDir := dir + "/" + fmt.Sprintf("slot-%03d", slot)
	for _, n := range indexFiles(t, dir, slot) {
		if !strings.HasSuffix(n, ".idx") {
			continue
		}
		p := slotDir + "/" + n
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if st.Size() <= segIdxHeaderBytes+64 {
			continue
		}
		f, err := os.OpenFile(p, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Truncate(st.Size() / 2); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
}

// damageFirstBlock flips a byte inside the first record block of each .idx.
func damageFirstBlock(t *testing.T, dir string, slot int32) {
	t.Helper()
	slotDir := dir + "/" + fmt.Sprintf("slot-%03d", slot)
	for _, n := range indexFiles(t, dir, slot) {
		if !strings.HasSuffix(n, ".idx") {
			continue
		}
		f, err := os.OpenFile(slotDir+"/"+n, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt([]byte{0x5A}, segIdxHeaderBytes+8); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
}

// assertIndexComplete checks every segment of the slot now has index files.
func assertIndexComplete(t *testing.T, dir string, slot int32) {
	t.Helper()
	slotDir := dir + "/" + fmt.Sprintf("slot-%03d", slot)
	entries, err := os.ReadDir(slotDir)
	if err != nil {
		t.Fatal(err)
	}
	wals := map[string]bool{}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".wal") {
			wals[strings.TrimSuffix(e.Name(), ".wal")] = true
		}
	}
	for base := range wals {
		for _, suffix := range []string{".idx", ".agx", ".spx"} {
			if _, err := os.Stat(slotDir + "/" + base + suffix); err != nil {
				t.Fatalf("segment %s is missing %s: %v", base, suffix, err)
			}
		}
	}
}
