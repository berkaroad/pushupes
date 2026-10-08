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

	"github.com/berkaroad/pushupes/internal/data"
)

// A command written into a sealed segment must be findable with the in-memory
// table empty: that is what lets the table (32 B per record at last measure)
// leave memory. The tail has no sealed index, so this covers sealed history;
// records still in the writable segment stay the in-memory structures' job.
func TestSealedCommandIndexServesColdLookups(t *testing.T) {
	dir := t.TempDir()
	const segBytes = int64(16 << 10) // small segments: seal quickly
	flush := FlushPolicy{Interval: time.Millisecond}
	agg := "agg-cold"

	write := func(n int) {
		st, err := OpenStore(dir, 4, segBytes, flush)
		if err != nil {
			t.Fatal(err)
		}
		sl, err := st.Slot(data.SlotOf(agg, 4))
		if err != nil {
			t.Fatal(err)
		}
		start := sl.LastSeq()
		for i := int(start) + 1; i <= int(start)+n; i++ {
			rec := &data.EventRecord{
				AggregateID: agg,
				Version:     uint32(i),
				CommandID:   fmt.Sprintf("cmd-%d", i),
				Events:      []data.Event{{Type: "t", Body: []byte(strings.Repeat("x", 60))}},
			}
			if _, err := st.Append(rec); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
	}
	write(600)

	open := func(repair bool) *Slot {
		st, err := openStoreMode(dir, 4, segBytes, flush, map[bool]slotOpenMode{true: slotOpenLoadAndRepair, false: slotOpenLoad}[repair])
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { st.Close() })
		sl, err := st.SlotIfLoaded(data.SlotOf(agg, 4))
		if err != nil {
			t.Fatal(err)
		}
		if sl == nil {
			t.Fatal("slot not loaded")
		}
		return sl
	}

	sl := open(true)
	sealed := sl.SealedSegments()
	if len(sealed) == 0 {
		t.Fatalf("expected a sealed segment, have %d segments", sl.SegmentCount())
	}
	// Force the cold path: no in-memory table at all.
	sl.blms = &bloomSet{}
	rec, seq, err := sl.RecordByCommand("cmd-1")
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil || seq != 1 || rec.Version != 1 {
		t.Fatalf("cold lookup cmd-1: rec=%v seq=%d", rec, seq)
	}
	if got := len(rec.Events[0].Body); got != 60 {
		t.Fatalf("cold lookup body len %d", got)
	}
	// A command that does not exist must come back empty, not wrong.
	if rec, seq, err := sl.RecordByCommand("cmd-never-written"); err != nil || rec != nil {
		t.Fatalf("missing command: rec=%v seq=%d err=%v", rec, seq, err)
	}
	// Every command of the sealed segments is reachable from the disk indexes.
	sealedRecords := int64(0)
	for _, seg := range sealed {
		sealedRecords += seg.RecordCnt
	}
	for i := 1; i <= int(sealedRecords); i++ {
		rec, seq, err := sl.RecordByCommand(fmt.Sprintf("cmd-%d", i))
		if err != nil || rec == nil {
			t.Fatalf("cold lookup cmd-%d: rec=%v seq=%d err=%v", i, rec, seq, err)
		}
		if seq != uint64(i) || rec.Version != uint32(i) {
			t.Fatalf("cold lookup cmd-%d: seq=%d version=%d", i, seq, rec.Version)
		}
	}

	// Delete the command indexes: a start rewrites them, and cold lookups work
	// again. A non-repairing start leaves them absent (a running node does not
	// re-index history), so the lookup finds nothing until the next start.
	removeCmdIndexes(t, dir, data.SlotOf(agg, 4))
	running := open(false)
	running.blms = &bloomSet{}
	if rec, _, err := running.RecordByCommand("cmd-1"); err != nil || rec != nil {
		t.Fatalf("without an index a running node should find nothing: rec=%v err=%v", rec, err)
	}
	repaired := open(true)
	if got := countCmdIndexes(t, dir, data.SlotOf(agg, 4)); got == 0 {
		t.Fatal("startup did not write the missing command index")
	}
	repaired.blms = &bloomSet{}
	if rec, seq, err := repaired.RecordByCommand("cmd-1"); err != nil || rec == nil || seq != 1 {
		t.Fatalf("after repair: rec=%v seq=%d err=%v", rec, seq, err)
	}

	// Damage a block: a start must notice and rewrite it rather than serve it.
	damageCmdIndexBlock(t, dir, data.SlotOf(agg, 4))
	built := segCmdBuilt.Load()
	healed := open(true)
	if segCmdBuilt.Load() == built {
		t.Fatal("a damaged command index was not rebuilt at startup")
	}
	healed.blms = &bloomSet{}
	if rec, seq, err := healed.RecordByCommand("cmd-1"); err != nil || rec == nil || seq != 1 {
		t.Fatalf("after damage repair: rec=%v seq=%d err=%v", rec, seq, err)
	}
}

func cmdIndexFiles(dir string, slot int32) []string {
	slotDir := dir + "/" + fmt.Sprintf("slot-%03d", slot)
	entries, err := os.ReadDir(slotDir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".cidx") {
			out = append(out, e.Name())
		}
	}
	return out
}

func countCmdIndexes(t *testing.T, dir string, slot int32) int {
	t.Helper()
	return len(cmdIndexFiles(dir, slot))
}

func removeCmdIndexes(t *testing.T, dir string, slot int32) {
	t.Helper()
	slotDir := dir + "/" + fmt.Sprintf("slot-%03d", slot)
	for _, n := range cmdIndexFiles(dir, slot) {
		if err := os.Remove(slotDir + "/" + n); err != nil {
			t.Fatal(err)
		}
	}
}

// damageCmdIndexBlock flips a byte inside the first entry block.
func damageCmdIndexBlock(t *testing.T, dir string, slot int32) {
	t.Helper()
	slotDir := dir + "/" + fmt.Sprintf("slot-%03d", slot)
	files := cmdIndexFiles(dir, slot)
	if len(files) == 0 {
		t.Fatal("no command index to damage")
	}
	f, err := os.OpenFile(slotDir+"/"+files[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0x77}, segCidxHeaderByte+4); err != nil {
		t.Fatal(err)
	}
	f.Close()
}
