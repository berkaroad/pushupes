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

	"github.com/berkaroad/pushupes/internal/data"
)

// recAt builds one record with an explicit unix_time stamp.
func recAt(agg string, ver uint32, ts int64) *data.EventRecord {
	return &data.EventRecord{
		AggregateID: agg, Version: ver, UnixTime: ts,
		CommandID: fmt.Sprintf("%s-v%d", agg, ver),
		Events:    []data.Event{{Type: "E", Body: []byte("x")}},
	}
}

// VersionAtOrBeforeTime answers "the highest version stamped at or before T".
// The cases below pin the semantics a point-in-time replay client relies on:
// <= (a query at an exact record time DOES resolve that record — the record
// written at T is part of the state as of T),
// non-monotonic timestamps (the bound is caller-supplied and the walk cannot
// binary-search — the answer is the last hit from the tail), the sealed/live
// split (versions dropped from memory resolve through the segment index),
// visibility bounds, and the desync gate shared with AggregateVersion.
func TestVersionAtOrBeforeTime(t *testing.T) {
	// Tiny segments so most versions seal to disk and the live tail is short:
	// every read path (sealedVersionSeqLocked and seqAt+readBySeqLocked) is
	// exercised by the same data.
	st, _ := newTestStore(t, 8, 1<<10)
	const agg = "time-agg"
	// stamp v_i at 1000+i, so version v answers any T in [stamp(v), stamp(v+1)-1]
	for v := uint32(1); v <= 40; v++ {
		if _, err := st.Append(recAt(agg, v, int64(1000+v))); err != nil {
			t.Fatalf("append v%d: %v", v, err)
		}
	}
	// A query AT an exact stamp resolves THAT record (the <= boundary).
	got, err := st.VersionAtOrBeforeTime(agg, 1010, 0)
	if err != nil || got != 10 {
		t.Fatalf("exact stamp: want 10 got %d err %v", got, err)
	}
	// The next stamp advances the answer by exactly one version.
	if got, _ := st.VersionAtOrBeforeTime(agg, 1011, 0); got != 11 {
		t.Fatalf("at stamp 1011: want 11 got %d", got)
	}
	// Before the first record: nothing resolves.
	if got, err := st.VersionAtOrBeforeTime(agg, 1000, 0); err != nil || got != 0 {
		t.Fatalf("before head: want 0 got %d err %v", got, err)
	}
	// At the first record's stamp: that record answers.
	if got, err := st.VersionAtOrBeforeTime(agg, 1001, 0); err != nil || got != 1 {
		t.Fatalf("at first stamp: want 1 got %d err %v", got, err)
	}
	// Past the last stamp: the whole tail answers.
	if got, err := st.VersionAtOrBeforeTime(agg, 1100, 0); err != nil || got != 40 {
		t.Fatalf("past tail: want 40 got %d err %v", got, err)
	}

	// Non-monotonic stamps: v41 is stamped in the past, so a query exactly at
	// its stamp answers 41 — the WALK from the tail decides, not monotonicity.
	if _, err := st.Append(recAt(agg, 41, 1005)); err != nil {
		t.Fatal(err)
	}
	if got, err := st.VersionAtOrBeforeTime(agg, 1005, 0); err != nil || got != 41 {
		t.Fatalf("non-monotonic tail hit at exact stamp: want 41 got %d err %v", got, err)
	}

	// Visibility bound: with only the first 20 seqs visible, a query past all
	// stamps must not resolve a version beyond what is visible.
	// (seqs 1..20 cover versions 1..20 for this single-aggregate stream.)
	firstTwenty := func() uint64 {
		recs, seqs, err := st.ReadAggregate(agg, 20, 1, 0)
		if err != nil || len(recs) != 1 {
			t.Fatalf("locate v20 seq: %v %v", recs, err)
		}
		return seqs[0]
	}
	bound := firstTwenty()
	if got, err := st.VersionAtOrBeforeTime(agg, 1100, bound); err != nil || got != 20 {
		t.Fatalf("bounded at v20 seq: want 20 got %d err %v", got, err)
	}
	if got, err := st.VersionAtOrBeforeTime(agg, 1100, st.LastSeqOf(st.SlotOf(agg))); err != nil || got != 41 {
		t.Fatalf("full LEO: want 41 got %d err %v", got, err)
	}

	// Unknown aggregate: 0, no error (a missing stream is not a desync).
	if got, err := st.VersionAtOrBeforeTime("no-such-agg", 1100, 0); err != nil || got != 0 {
		t.Fatalf("unknown agg: want 0 got %d err %v", got, err)
	}
}

// The desync gate must refuse a time answer from a directory that cannot
// resolve its claimed versions — the same refusal AggregateVersion and
// TailVersion make, so a corrupt stream cannot half-answer a point-in-time
// probe.
func TestVersionAtOrBeforeTimeDesyncGate(t *testing.T) {
	st, _ := newTestStore(t, 8, DefaultSegmentBytes)
	const agg = "time-desync"
	sl, err := st.Slot(st.SlotOf(agg))
	if err != nil {
		t.Fatal(err)
	}
	for v := uint32(1); v <= 3; v++ {
		if _, err := st.Append(recAt(agg, v, int64(1000+v))); err != nil {
			t.Fatal(err)
		}
	}
	sl.mu.Lock()
	e := sl.aggs[agg]
	e.version = 9
	sl.aggs[agg] = e
	sl.mu.Unlock()

	_, err = sl.VersionAtOrBeforeTime(agg, 9999, 0)
	if err == nil || !strings.Contains(err.Error(), "desync") {
		t.Fatalf("desynced directory must refuse the time query, got: %v", err)
	}
}

// Restart replay: sealed + live reads survive a reopen exactly like
// AggregateVersion — the time query must agree with what the stream itself
// reads back after recovery.
func TestVersionAtOrBeforeTimeAfterReopen(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir, 8, 1<<10, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	const agg = "time-reopen"
	for v := uint32(1); v <= 30; v++ {
		if _, err := st.Append(recAt(agg, v, int64(1000+v))); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st2, err := OpenStore(dir, 8, 1<<10, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st2.Close() })
	// stamp(v)=1000+v: a query at the exact stamp 1020 resolves v20 (<=).
	if got, err := st2.VersionAtOrBeforeTime(agg, 1020, 0); err != nil || got != 20 {
		t.Fatalf("after reopen at exact stamp: want 20 got %d err %v", got, err)
	}
}

// An index file written by an older layout — or one whose header is simply
// damaged — is a CACHE, not data: the next start must reject it, rewrite it
// from the frames, and keep answering. Without that, a segment written before
// the record index carried unix_time would stay unindexed forever (every
// append would fail its header check), and its stamps would be unreadable.
func TestStaleIndexFilesAreRebuilt(t *testing.T) {
	dir := t.TempDir()
	const comps = 8
	st, err := OpenStore(dir, comps, 1<<10, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	const agg = "stale-idx"
	for v := uint32(1); v <= 30; v++ {
		if _, err := st.Append(recAt(agg, v, int64(1000+v))); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// Age every index file: byte 4 is the format version, and no version this
	// build ever wrote is 0.
	files, err := filepath.Glob(filepath.Join(dir, "slot-*", "*"))
	if err != nil {
		t.Fatal(err)
	}
	aged := 0
	for _, p := range files {
		if ext := filepath.Ext(p); ext != ".idx" && ext != ".agx" && ext != ".spx" && ext != ".aidx" {
			continue
		}
		f, err := os.OpenFile(p, os.O_RDWR, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt([]byte{0}, 4); err != nil {
			t.Fatal(err)
		}
		f.Close()
		aged++
	}
	if aged == 0 {
		t.Fatal("no index files to age: the test would assert nothing")
	}

	st2, err := OpenStore(dir, comps, 1<<10, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	if got, err := st2.VersionAtOrBeforeTime(agg, 1020, 0); err != nil || got != 20 {
		t.Fatalf("time query after a stale-index repair: want 20 got %d err %v", got, err)
	}
	// The rewritten files must carry the CURRENT format version: a reset that
	// left the old bytes in place would disable the segment again next start.
	rewritten, err := filepath.Glob(filepath.Join(dir, "slot-*", "*"))
	if err != nil {
		t.Fatal(err)
	}
	sawIdx, sawAidx := false, false
	for _, p := range rewritten {
		h := make([]byte, segIdxHeaderBytes)
		f, err := os.Open(p)
		if err != nil {
			t.Fatal(err)
		}
		_, rerr := f.Read(h)
		f.Close()
		if rerr != nil {
			continue // empty file (e.g. a segment with no records)
		}
		switch filepath.Ext(p) {
		case ".idx":
			sawIdx = true
			if h[4] != segIdxVersion {
				t.Fatalf("%s: record index version=%d, want %d", p, h[4], segIdxVersion)
			}
		case ".aidx":
			sawAidx = true
			if h[4] != segAidxVersion {
				t.Fatalf("%s: aggregate index version=%d, want %d", p, h[4], segAidxVersion)
			}
		}
	}
	if !sawIdx || !sawAidx {
		t.Fatalf("expected rewritten .idx and .aidx files, saw idx=%v aidx=%v", sawIdx, sawAidx)
	}
}
