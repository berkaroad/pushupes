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
)

// waitSealIdle blocks until the store's seal worker has no queued or running
// job, then returns. Tests assert on the materialized state through it.
func waitSealIdle(t *testing.T, st *Store) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !st.flusher.sealIdle() {
		if time.Now().After(deadline) {
			t.Fatal("seal worker still busy after 10s")
		}
		time.Sleep(time.Millisecond)
	}
}

// blockSeal stops the seal worker inside its first job until the returned
// release function is called; started reports the segment the job took.
func blockSeal(t *testing.T) (release func(), started <-chan *Segment) {
	t.Helper()
	ch := make(chan *Segment, 1)
	gate := make(chan struct{})
	sealHook = func(seg *Segment) {
		select {
		case ch <- seg:
		default:
		}
		<-gate
	}
	var once bool
	release = func() {
		if !once {
			once = true
			close(gate)
		}
		sealHook = nil
	}
	t.Cleanup(func() {
		release()
		sealHook = nil
	})
	return release, ch
}

// The roll hands the segment's indexes to the seal worker: the append that
// rolls returns with the materialization still blocked, later appends to the
// same slot do not wait for it, and the .cidx/.aidx land once it runs.
func TestSealMaterializesOffTheWriteLock(t *testing.T) {
	st, _ := newTestStore(t, 8, DefaultSegmentBytes)
	sl, err := st.Slot(2)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 60; i++ {
		mustAppend(t, sl, mkRec("a", uint32(i), fmt.Sprintf("c%03d", i)))
	}

	release, started := blockSeal(t)
	roll(t, sl)

	// The append that rolls must return even though nothing was materialized.
	rolled := make(chan struct{})
	go func() {
		defer close(rolled)
		mustAppend(t, sl, mkRec("a", 61, "c061"))
	}()
	select {
	case <-rolled:
	case <-time.After(5 * time.Second):
		t.Fatal("the append that rolled the segment waited for index materialization")
	}

	var seg *Segment
	select {
	case seg = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("seal worker never started")
	}
	base := seg.BaseSeq
	if _, err := os.Stat(segCidxPath(filepath.Dir(seg.Path), base)); err == nil {
		t.Fatal("the .cidx exists while the materialization is still blocked")
	}
	// Nothing was written yet, so nothing left the arena.
	sl.mu.Lock()
	sealedN := sl.aggs["a"].sealedN
	sl.mu.Unlock()
	if sealedN != 0 {
		t.Fatalf("sealed seqs left the arena before the index was on disk: sealedN=%d", sealedN)
	}

	// Appends continue on the new segment while the worker is blocked.
	appended := make(chan struct{})
	go func() {
		defer close(appended)
		mustAppend(t, sl, mkRec("a", 62, "c062"))
	}()
	select {
	case <-appended:
	case <-time.After(5 * time.Second):
		t.Fatal("append blocked behind index materialization")
	}

	release()
	waitSealIdle(t, st)

	for _, p := range []string{segCidxPath(filepath.Dir(seg.Path), base), segAidxPath(filepath.Dir(seg.Path), base)} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("index file %s missing after materialization: %v", p, err)
		}
	}
	sl.mu.Lock()
	sealedN = sl.aggs["a"].sealedN
	sl.mu.Unlock()
	if sealedN != 60 {
		t.Fatalf("sealed seqs were not dropped after materialization: sealedN=%d, want 60", sealedN)
	}
	// The materialized .cidx answers for the sealed records.
	rec, _, err := sl.RecordByCommand("c001")
	if err != nil || rec == nil {
		t.Fatalf("lookup after materialization: rec=%v err=%v", rec, err)
	}
}

// A drop must not overtake an in-flight seal job: the job reads the segment and
// settles under the slot lock, so detach waits for it before the files go away.
func TestDropWaitsForInFlightSealJob(t *testing.T) {
	st, _ := newTestStore(t, 8, DefaultSegmentBytes)
	sl, err := st.Slot(5)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 40; i++ {
		mustAppend(t, sl, mkRec("a", uint32(i), fmt.Sprintf("c%03d", i)))
	}
	release, started := blockSeal(t)
	roll(t, sl)
	mustAppend(t, sl, mkRec("a", 41, "c041"))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("seal worker never started")
	}

	dropped := make(chan error, 1)
	go func() { dropped <- st.DropSlot(5) }()
	select {
	case err := <-dropped:
		t.Fatalf("DropSlot returned while the seal job held the segment (err=%v)", err)
	case <-time.After(150 * time.Millisecond):
	}
	release()
	select {
	case err := <-dropped:
		if err != nil {
			t.Fatalf("DropSlot: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("DropSlot did not finish after the seal job was released")
	}
}
