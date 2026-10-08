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
	"testing"

	"pushupes/internal/data"
)

// TestWriteByteCountsTracksDurableWrites pins the byte side of the write-rate
// reporting: every durable write contributes its frame bytes, and only durable
// writes do. The three paths that count writes (client append, replica apply,
// frame apply) must each add to writeBytes in lockstep with writes — a client
// that diffs both arrays against its last snapshot turns one window into a
// message rate and a byte rate, and they have to cover the same writes.
func TestWriteByteCountsTracksDurableWrites(t *testing.T) {
	st, err := OpenStore(t.TempDir(), 8, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if got := st.WriteByteCounts(); len(got) != 8 {
		t.Fatalf("fresh store: len=%d, want 8", len(got))
	}

	rec := recFor("agg-writebytes", 1, "cmd-1")
	slot := st.SlotOf(rec.AggregateID)
	wantFrame := uint64(rec.EncodedSize())

	out, err := st.Append(rec)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if out.Status != data.StatusSuccess {
		t.Fatalf("append status = %v (errID %d), want success", out.Status, out.ErrID)
	}
	if got := st.WriteByteCounts()[slot]; got != wantFrame {
		t.Fatalf("after append: bytes=%d, want %d", got, wantFrame)
	}

	// A rejected append (version conflict) moves no bytes.
	conflict := recFor(rec.AggregateID, 1, "cmd-dup-version")
	if _, err := st.Append(conflict); err != nil {
		t.Fatalf("conflict append: %v", err)
	}
	if got := st.WriteByteCounts()[slot]; got != wantFrame {
		t.Fatalf("after rejected append: bytes=%d, want %d (unchanged)", got, wantFrame)
	}

	// An exists replay (same command id) is not a new durable write either.
	if _, err := st.Append(rec); err != nil {
		t.Fatalf("replay append: %v", err)
	}
	if got := st.WriteByteCounts()[slot]; got != wantFrame {
		t.Fatalf("after exists replay: bytes=%d, want %d (unchanged)", got, wantFrame)
	}

	// The replica apply path counts its record's frame bytes.
	rec2 := recFor("agg-writebytes-2", 1, "cmd-2")
	slot2 := st.SlotOf(rec2.AggregateID)
	frame2 := uint64(rec2.EncodedSize())
	if err := st.AppendAtSeq(slot2, 1, rec2); err != nil {
		t.Fatalf("append at seq: %v", err)
	}
	if got := st.WriteByteCounts()[slot2]; got != frame2 {
		t.Fatalf("after AppendAtSeq: bytes=%d, want %d", got, frame2)
	}

	// The frame apply path counts the leader-encoded frame verbatim.
	rec3 := recFor("agg-writebytes-3", 1, "cmd-3")
	frame := rec3.EncodeBinary(nil)
	slot3 := st.SlotOf(rec3.AggregateID)
	if err := st.AppendFrameAtSeq(slot3, 1, frame); err != nil {
		t.Fatalf("append frame at seq: %v", err)
	}
	if got := st.WriteByteCounts()[slot3]; got != uint64(len(frame)) {
		t.Fatalf("after AppendFrameAtSeq: bytes=%d, want %d", got, len(frame))
	}

	// An idempotent replay of the same seq lands no new bytes.
	before := st.WriteByteCounts()[slot3]
	if err := st.AppendFrameAtSeq(slot3, 1, frame); err != nil {
		t.Fatalf("replayed frame: %v", err)
	}
	if got := st.WriteByteCounts()[slot3]; got != before {
		t.Fatalf("after replayed frame: bytes=%d, want %d (unchanged)", got, before)
	}

	// writes and writeBytes stay index-aligned and agree on which slots moved.
	ws := st.WriteCounts()
	bs := st.WriteByteCounts()
	if len(ws) != len(bs) {
		t.Fatalf("len mismatch: writes=%d bytes=%d", len(ws), len(bs))
	}
	for i := range ws {
		if (ws[i] > 0) != (bs[i] > 0) {
			t.Fatalf("slot %d: writes=%d bytes=%d disagree on being touched", i, ws[i], bs[i])
		}
	}
}
