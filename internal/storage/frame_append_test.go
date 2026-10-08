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
	"bytes"
	"fmt"
	"testing"

	"github.com/berkaroad/pushupes/internal/data"
)

// Follower-style frame landing: the leader's encoded bytes reach the WAL
// verbatim, and the header-only index stays as correct as a decoded append.
func TestAppendFrameAtSeqLandsLeaderBytes(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir, 128, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	slotID := st.SlotOf("agg-frame")
	slot, _ := st.Slot(slotID)

	const n = 10
	frames := make([][]byte, 0, n)
	for i := uint64(1); i <= n; i++ {
		frames = append(frames, mkRec("agg-frame", uint32(i), fmt.Sprintf("cmd-frame-%d", i)).EncodeBinary(nil))
	}
	for i, f := range frames {
		if err := slot.AppendFrameAtSeq(uint64(i+1), f); err != nil {
			t.Fatalf("AppendFrameAtSeq %d: %v", i+1, err)
		}
	}
	if got := slot.LastSeq(); got != n {
		t.Fatalf("LEO=%d want %d", got, n)
	}

	// The WAL payload must be the concatenation of the leader's frames.
	_, _, payload, err := st.ReadSlotBytes(slotID, 1, n+1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Join(frames, nil)
	if !bytes.Equal(payload, want) {
		t.Fatalf("WAL payload differs from leader frames: got %d bytes want %d", len(payload), len(want))
	}

	// Indexes were rebuilt from frame metadata alone.
	if cur := slot.CurrentVersion("agg-frame"); cur != n {
		t.Fatalf("CurrentVersion=%d want %d", cur, n)
	}
	if back := slot.LastVersionOf("agg-frame"); back != n {
		t.Fatalf("LastVersionOf=%d want %d", back, n)
	}
	// The command index keeps hashes; a lookup confirms against the record.
	if rec, seq, err := slot.RecordByCommand("cmd-frame-4"); err != nil || rec == nil || seq != 4 {
		t.Fatalf("RecordByCommand(cmd-frame-4)=%v,%d,%v want the record at seq 4", rec, seq, err)
	}
	if rec, _, err := slot.RecordByCommand("cmd-not-there"); err != nil || rec != nil {
		t.Fatalf("RecordByCommand(cmd-not-there)=%v,%v want nil,nil", rec, err)
	}

	// Idempotent replay of the same frame is a no-op...
	if err := slot.AppendFrameAtSeq(5, frames[4]); err != nil {
		t.Fatalf("replay: %v", err)
	}
	// ...but a different record at the same seq is divergence.
	if bad := mkRec("agg-frame", 5, "cmd-DIVERGED").EncodeBinary(nil); slot.AppendFrameAtSeq(5, bad) == nil {
		t.Fatal("expected divergence error on replayed seq")
	}
	// Gaps and malformed frames are rejected before touching the WAL.
	if err := slot.AppendFrameAtSeq(n+3, mkRec("agg-frame", uint32(n+3), "cmd-gap").EncodeBinary(nil)); err == nil {
		t.Fatal("expected gap error")
	}
	if err := slot.AppendFrameAtSeq(n+1, frames[0][:20]); err == nil {
		t.Fatal("expected truncated-frame error")
	}
	if got := slot.LastSeq(); got != n {
		t.Fatalf("LEO=%d want %d after rejected appends", got, n)
	}
}

// Frames written through the replication path must survive a restart exactly
// as records written through the local append path: same scan, same dedupe.
func TestAppendFrameAtSeqSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir, 128, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	slotID := st.SlotOf("agg-reopen")
	slot, _ := st.Slot(slotID)
	frame := mkRec("agg-reopen", 1, "cmd-r1").EncodeBinary(nil)
	if err := slot.AppendFrameAtSeq(1, frame); err != nil {
		t.Fatal(err)
	}
	local := mkRec("agg-reopen", 2, "cmd-r2")
	if _, err := st.Append(local); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := OpenStore(dir, 128, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	slot2, err := st2.Slot(slotID)
	if err != nil {
		t.Fatal(err)
	}
	if got := slot2.LastSeq(); got != 2 {
		t.Fatalf("recovered LEO=%d want 2", got)
	}
	_, _, payload, err := st2.ReadSlotBytes(slotID, 1, 3, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if want := append(append([]byte{}, frame...), local.EncodeBinary(nil)...); !bytes.Equal(payload, want) {
		t.Fatal("recovered payload differs from the bytes that were written")
	}
	_, n, err := data.DecodeRecordMeta(payload)
	if err != nil || n != len(frame) {
		t.Fatalf("first recovered frame: n=%d err=%v", n, err)
	}
}

// The pooled encode buffers must hand out exactly the requested length, and
// buffers outside the pooled size classes must not be recycled as if they were.
func TestEncodeBufPool(t *testing.T) {
	for _, n := range []int{1, 30, 8192, 8193, 100 << 10, 1 << 20} {
		b := getEncodeBuf(n)
		if len(b) != n {
			t.Fatalf("getEncodeBuf(%d): len=%d", n, len(b))
		}
		putEncodeBuf(b)
	}
	// A buffer whose capacity is not a pool class must be dropped, never
	// handed out as if it were bigger than it is.
	odd := make([]byte, 1000, 1000)
	putEncodeBuf(odd)
	if got := getEncodeBuf(1000); len(got) != 1000 || cap(got) < 1000 {
		t.Fatalf("getEncodeBuf(1000): len=%d cap=%d", len(got), cap(got))
	}
	// Reuse is deliberately NOT asserted by identity: whether a pooled buffer
	// comes back is the runtime's business, not this code's. A GC may drop pooled
	// objects; sync.Pool's per-P private slot is only visible to the P that stored
	// it; and under -race the probe below fails even with the collector stopped,
	// one P and the thread locked, while it passes in plain builds. What this
	// test owns is the class contract putEncodeBuf and getEncodeBuf agree on:
	// exactly the requested length, capacity a power-of-two pool class (or exactly
	// the request when it is over the biggest class). Retention is what the
	// encode benchmarks observe.
	for _, tc := range []struct{ n, wantLen, wantCap int }{
		{1, 1, 1 << 13},
		{30, 30, 1 << 13},
		{8 << 10, 8 << 10, 1 << 13},
		{64 << 10, 64 << 10, 1 << 16},
		{100 << 10, 100 << 10, 1 << 17},
		{1 << 20, 1 << 20, 1 << 20},
		{3 << 20, 3 << 20, 3 << 20}, // over the biggest class: bypasses the pool
	} {
		b := getEncodeBuf(tc.n)
		if len(b) != tc.wantLen || cap(b) != tc.wantCap {
			t.Fatalf("getEncodeBuf(%d): len=%d cap=%d, want len=%d cap=%d", tc.n, len(b), cap(b), tc.wantLen, tc.wantCap)
		}
		putEncodeBuf(b)
	}
	// Oversized records bypass the pool entirely.
	huge := getEncodeBuf(3 << 20)
	if len(huge) != 3<<20 || cap(huge) != 3<<20 {
		t.Fatalf("oversized buffer: len=%d cap=%d", len(huge), cap(huge))
	}
}
