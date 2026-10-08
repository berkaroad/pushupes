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
	"strings"
	"testing"
	"time"

	"github.com/berkaroad/pushupes/internal/data"
)

// A migration target receives ONLY the .wal files of the sealed segments
// (pushSealedSegments never sends .idx/.aidx), so it reloads with slotOpenLoad
// — which writes no index — and then seals a new segment at runtime. Before the
// fix, the seal dropped every seq up to the new segment's last seq from memory
// while writing an .aidx that covered only the NEW segment: the dropped
// versions then resolved to nothing and a read failed (or silently came back
// short). This is the exact shape of the "written but not readable after a
// migration" report.
func TestReloadWithoutIndexThenSealKeepsVersionsReadable(t *testing.T) {
	srcDir := t.TempDir()
	const slotBytes = int64(4 << 10) // tiny segments: several seals
	const slotCount = int32(8)
	const agg = "agg-alpha"
	slot := data.SlotOf(agg, int(slotCount))
	dirName := fmt.Sprintf("slot-%03d", slot)

	st, err := OpenStore(srcDir, slotCount, slotBytes, FlushPolicy{Interval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	sl, _ := st.Slot(slot)
	const first = 120
	for v := 1; v <= first; v++ {
		appendOneVersion(t, sl, agg, uint32(v))
	}
	sl.Flush()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// Target: copy ONLY the WAL files, as a snapshot transfer does.
	srcSlotDir := filepath.Join(srcDir, dirName)
	ents, err := os.ReadDir(srcSlotDir)
	if err != nil {
		t.Fatal(err)
	}
	tgtDir := t.TempDir()
	tgtSlotDir := filepath.Join(tgtDir, dirName)
	if err := os.MkdirAll(tgtSlotDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if !strings.HasSuffix(e.Name(), ".wal") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(srcSlotDir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(tgtSlotDir, e.Name()), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Reload exactly as Store.ReloadSlot does (no repair, no index written).
	tsl, err := openSlot(tgtSlotDir, slot, slotBytes, FlushPolicy{Interval: time.Millisecond}, slotOpenLoad)
	if err != nil {
		t.Fatal(err)
	}
	const total = 200
	for v := first + 1; v <= total; v++ {
		appendOneVersion(t, tsl, agg, uint32(v))
	}
	tsl.Flush()

	// Every acknowledged version must be readable, and the directory's claimed
	// version must equal the number of resolvable versions.
	tsl.mu.RLock()
	e := tsl.aggs[agg]
	version, resolvable := e.version, uint32(e.sealedN)+uint32(e.n)
	tsl.mu.RUnlock()
	if version != resolvable {
		t.Fatalf("desync: version=%d resolvable=%d", version, resolvable)
	}
	if version != total {
		t.Fatalf("version=%d, want %d", version, total)
	}
	recs, _, err := tsl.AggregateVersion(agg, 1, 0, 0)
	if err != nil {
		t.Fatalf("read from 1 after reload+seal: %v", err)
	}
	if len(recs) != total {
		t.Fatalf("SHORT READ after reload+seal: got %d records, want %d", len(recs), total)
	}
	for i, rec := range recs {
		if rec.Version != uint32(i+1) {
			t.Fatalf("record %d is version %d", i, rec.Version)
		}
	}
}

// A directory whose claimed latest version disagrees with the number of seqs it
// can resolve must fail the read LOUDLY. Returning "as many as we have" would
// be a silent short read: the caller sees a truncated stream, no error, and
// believes it complete. This is the invariant the reported incident violated
// (version 9507, only 5225 resolvable, no error).
func TestAggregateVersionDesyncIsNotSilent(t *testing.T) {
	st, _ := newTestStore(t, 8, DefaultSegmentBytes)
	sl, err := st.Slot(data.SlotOf("agg-desync", 8))
	if err != nil {
		t.Fatal(err)
	}
	for v := 1; v <= 3; v++ {
		appendOneVersion(t, sl, "agg-desync", uint32(v))
	}

	// Inject the exact corruption the incident exposed: the latest version
	// advanced past the seq list (a record indexed without its seq recorded).
	sl.mu.Lock()
	e := sl.aggs["agg-desync"]
	e.version = 9507 // resolvable stays 3
	sl.aggs["agg-desync"] = e
	sl.mu.Unlock()

	recs, _, err := sl.AggregateVersion("agg-desync", 1, 0, 0)
	if err == nil {
		t.Fatalf("a desynced directory must not read silently: got %d records, no error", len(recs))
	}
	if !strings.Contains(err.Error(), "desync") {
		t.Fatalf("error should name the desync, got: %v", err)
	}
	// A read that starts beyond the claimed version is a legitimate empty
	// range, not a desync.
	sl.mu.Lock()
	e = sl.aggs["agg-desync"]
	e.version = 3
	sl.aggs["agg-desync"] = e
	sl.mu.Unlock()
	recs, _, err = sl.AggregateVersion("agg-desync", 4, 0, 0)
	if err != nil || len(recs) != 0 {
		t.Fatalf("reading past the end must be an empty, error-free range: %d %v", len(recs), err)
	}
}

func appendOneVersion(t *testing.T, sl *Slot, agg string, v uint32) {
	t.Helper()
	out, err := sl.Append(&data.EventRecord{
		AggregateID: agg,
		Version:     v,
		CommandID:   fmt.Sprintf("c-%s-%d", agg, v),
		Events:      []data.Event{{Type: "E", Body: []byte(strings.Repeat("x", 100))}},
	})
	if err != nil {
		t.Fatalf("append %s v%d: %v", agg, v, err)
	}
	if out.Status != data.StatusSuccess {
		t.Fatalf("append %s v%d: status %s", agg, v, out.Status)
	}
}
