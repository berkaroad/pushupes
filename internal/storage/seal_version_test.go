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
)

// Versions must stay contiguous across seals: the directory counts sealed and
// live seqs (sealedN + n) and the append rule accepts only current+1, so a seal
// that drops arena entries before the .aidx covers them (or drops the wrong
// ones) shows up here as a stuck version.
func TestVersionsSurviveManySeals(t *testing.T) {
	st, _ := newTestStore(t, 8, 4096) // tiny segments: a seal every few records
	sl, err := st.Slot(3)
	if err != nil {
		t.Fatal(err)
	}
	const total = 300
	for i := 1; i <= total; i++ {
		out, err := sl.Append(mkRec("agg-x", uint32(i), fmt.Sprintf("cmd-%04d", i)))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if out.Status != "success" {
			t.Fatalf("append %d: %+v", i, out)
		}
		if got := sl.CurrentVersion("agg-x"); got != uint32(i) {
			t.Fatalf("after append %d: CurrentVersion=%d", i, got)
		}
		if got := sl.LastVersionOf("agg-x"); got != uint32(i) {
			t.Fatalf("after append %d: LastVersionOf=%d", i, got)
		}
		if i%40 == 0 {
			waitSealIdle(t, st)
			if got := sl.CurrentVersion("agg-x"); got != uint32(i) {
				t.Fatalf("after seal settle at %d: CurrentVersion=%d", i, got)
			}
			if got := sl.LastVersionOf("agg-x"); got != uint32(i) {
				t.Fatalf("after seal settle at %d: LastVersionOf=%d", i, got)
			}
		}
	}
	waitSealIdle(t, st)
	if n := sl.SegmentCount(); n < 3 {
		t.Fatalf("expected several sealed segments, got %d", n)
	}
	if got := sl.CurrentVersion("agg-x"); got != total {
		t.Fatalf("final CurrentVersion=%d want %d", got, total)
	}
	// Every version must still resolve, sealed or live.
	for i := 1; i <= total; i++ {
		rec, seq, err := sl.AggregateVersion("agg-x", uint32(i), 1, 0)
		if err != nil {
			t.Fatalf("AggregateVersion(%d): %v", i, err)
		}
		if len(rec) != 1 || rec[0].Version != uint32(i) || seq[0] == 0 {
			t.Fatalf("version %d: recs=%v seqs=%v", i, rec, seq)
		}
	}
}

// dropFirst's compaction re-places the live entries through appendSeq, which
// counts them: n must be cleared with the buffers or the count doubles. This is
// the shape the seal job's later settle produces — the arena already holds the
// next segment's records when the sealed prefix leaves.
func TestDropFirstCompactionKeepsCount(t *testing.T) {
	var e aggEntry
	for i := uint64(1); i <= 80; i++ {
		e.appendSeq(i, int64(i))
	}
	e.dropFirst(79)
	if e.n != 1 {
		t.Fatalf("after dropping 79 of 80: n=%d want 1", e.n)
	}
	if got := e.seqAt(0); got != 80 {
		t.Fatalf("remaining seq=%d want 80", got)
	}
	if got := e.stampAt(0); got != 80 {
		t.Fatalf("remaining stamp=%d want 80", got)
	}
}
