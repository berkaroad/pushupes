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

	out, err := st.AppendRouted(rec)
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
	if _, err := st.AppendRouted(conflict); err != nil {
		t.Fatalf("conflict append: %v", err)
	}
	if got := st.WriteByteCounts()[slot]; got != wantFrame {
		t.Fatalf("after rejected append: bytes=%d, want %d (unchanged)", got, wantFrame)
	}

	// An exists replay (same command id) is not a new durable write either.
	if _, err := st.AppendRouted(rec); err != nil {
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

// TestClientWriteCountsOnlyClientPath pins the split the console's 写入速度
// needs: the client-origin counters grow ONLY on the batch-append serving
// path, while the catch-up paths (AppendAtSeq, AppendFrameAtSeq — how a
// replica or a migration target lands a leader's records) move the lifetime
// counters but never these. Diffing the client pair into a rate is therefore
// immune to rebalance and migration traffic reading as write speed.
func TestClientWriteCountsOnlyClientPath(t *testing.T) {
	st, err := OpenStore(t.TempDir(), 8, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	cw := st.ClientWriteCounts()
	cb := st.ClientWriteByteCounts()
	if len(cw) != 8 || len(cb) != 8 {
		t.Fatalf("fresh store: len=%d/%d, want 8/8", len(cw), len(cb))
	}

	// The client path moves both families.
	rec := recFor("agg-client-only", 1, "cmd-c1")
	out, err := st.AppendRouted(rec)
	if err != nil || out.Status != data.StatusSuccess {
		t.Fatalf("client append: err=%v status=%v", err, out.Status)
	}
	slot := st.SlotOf(rec.AggregateID)
	if got := st.ClientWriteCounts()[slot]; got != 1 {
		t.Fatalf("client path: client_writes=%d, want 1", got)
	}
	if got := st.ClientWriteByteCounts()[slot]; got != uint64(rec.EncodedSize()) {
		t.Fatalf("client path: client_bytes=%d, want %d", got, rec.EncodedSize())
	}

	// Catch-up lands: the lifetime counters move, the client counters do not.
	rec2 := recFor("agg-client-only-2", 1, "cmd-c2")
	slot2 := st.SlotOf(rec2.AggregateID)
	if err := st.AppendAtSeq(slot2, 1, rec2); err != nil {
		t.Fatalf("append at seq: %v", err)
	}
	if st.WriteCounts()[slot2] != 1 || st.ClientWriteCounts()[slot2] != 0 {
		t.Fatalf("AppendAtSeq: writes=%d client_writes=%d, want 1/0",
			st.WriteCounts()[slot2], st.ClientWriteCounts()[slot2])
	}
	rec3 := recFor("agg-client-only-3", 1, "cmd-c3")
	frame := rec3.EncodeBinary(nil)
	slot3 := st.SlotOf(rec3.AggregateID)
	if err := st.AppendFrameAtSeq(slot3, 1, frame); err != nil {
		t.Fatalf("append frame at seq: %v", err)
	}
	if st.WriteCounts()[slot3] != 1 || st.ClientWriteCounts()[slot3] != 0 || st.ClientWriteByteCounts()[slot3] != 0 {
		t.Fatalf("AppendFrameAtSeq: writes=%d client=%d/%d, want 1/0/0",
			st.WriteCounts()[slot3], st.ClientWriteCounts()[slot3], st.ClientWriteByteCounts()[slot3])
	}

	// A rejected version conflict on the client path counts nowhere — the
	// split must not quietly become "every Append call counts".
	if _, err := st.AppendRouted(recFor(rec.AggregateID, 1, "cmd-dup")); err != nil {
		t.Fatalf("conflict append: %v", err)
	}
	if st.ClientWriteCounts()[slot] != 1 || st.ClientWriteByteCounts()[slot] != uint64(rec.EncodedSize()) {
		t.Fatalf("after conflict: client=%d/%d, want 1/%d (unchanged)",
			st.ClientWriteCounts()[slot], st.ClientWriteByteCounts()[slot], rec.EncodedSize())
	}
}

// zzFreshSlot returns an aggregate name routing to a slot outside `taken`.
func zzFreshSlot(st *Store, taken ...int32) string {
	for i := 0; ; i++ {
		name := fmt.Sprintf("zz-seed-agg-%d", i)
		s := st.SlotOf(name)
		clash := false
		for _, t := range taken {
			if t == s {
				clash = true
			}
		}
		if !clash {
			return name
		}
	}
}

// TestDurableCountersSeededFromCopy pins the counters' restart story: an
// open seeds `writes`/`writeBytes` from the copy's loaded ledger, so the
// number answers "what does this copy hold" — not "what landed since boot".
// The seed must equal the counter the previous process held (same books:
// seq ledger and commitFrame bytes), must be a Store not an Add (a reload
// mid-process does not double), must leave the client pair process-local,
// and must keep advancing as frames land afterwards. Dropping the copy
// retires the whole ledger with it.
func TestDurableCountersSeededFromCopy(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir, 64, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	rec := recFor(zzFreshSlot(st), 1, "cmd-r1")
	s1 := st.SlotOf(rec.AggregateID)
	if _, err := st.AppendRouted(rec); err != nil {
		t.Fatalf("client append: %v", err)
	}
	rec2 := recFor(zzFreshSlot(st, s1), 1, "cmd-r2")
	s2 := st.SlotOf(rec2.AggregateID)
	if err := st.AppendAtSeq(s2, 1, rec2); err != nil {
		t.Fatalf("append at seq: %v", err)
	}
	rec3 := recFor(zzFreshSlot(st, s1, s2), 1, "cmd-r3")
	s3 := st.SlotOf(rec3.AggregateID)
	if err := st.AppendFrameAtSeq(s3, 1, rec3.EncodeBinary(nil)); err != nil {
		t.Fatalf("append frame at seq: %v", err)
	}
	beforeW, beforeB := st.WriteCounts(), st.WriteByteCounts()

	// A reload re-derives the same truth: Store, not Add.
	if err := st.ReloadSlot(s1); err != nil {
		t.Fatalf("reload slot: %v", err)
	}
	if got := st.WriteCounts()[s1]; got != beforeW[s1] {
		t.Fatalf("after ReloadSlot: writes=%d, want %d (seed must not double)", got, beforeW[s1])
	}
	if got := st.WriteByteCounts()[s1]; got != beforeB[s1] {
		t.Fatalf("after ReloadSlot: bytes=%d, want %d", got, beforeB[s1])
	}

	// Drop retires the whole ledger with the copy.
	if err := st.DropSlot(s2); err != nil {
		t.Fatalf("drop slot: %v", err)
	}
	if st.WriteCounts()[s2] != 0 || st.WriteByteCounts()[s2] != 0 ||
		st.ClientWriteCounts()[s2] != 0 || st.ClientWriteByteCounts()[s2] != 0 {
		t.Fatalf("after DropSlot: counters=%d/%d/%d/%d, want all zero",
			st.WriteCounts()[s2], st.WriteByteCounts()[s2],
			st.ClientWriteCounts()[s2], st.ClientWriteByteCounts()[s2])
	}
	beforeW, beforeB = st.WriteCounts(), st.WriteByteCounts()
	st.Close()

	st2, err := OpenStore(dir, 64, DefaultSegmentBytes, FlushPolicy{})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer st2.Close()
	for _, s := range []int32{s1, s3} {
		if got := st2.WriteCounts()[s]; got != beforeW[s] {
			t.Fatalf("reopen slot %d: writes=%d, want %d (seeded from copy)", s, got, beforeW[s])
		}
		if got := st2.WriteByteCounts()[s]; got != beforeB[s] {
			t.Fatalf("reopen slot %d: bytes=%d, want %d", s, got, beforeB[s])
		}
	}
	if st2.WriteCounts()[s2] != 0 {
		t.Fatalf("reopen dropped slot: writes=%d, want 0", st2.WriteCounts()[s2])
	}
	// The client pair stays process-local: a restart served none of those.
	for _, s := range []int32{s1, s3} {
		if st2.ClientWriteCounts()[s] != 0 || st2.ClientWriteByteCounts()[s] != 0 {
			t.Fatalf("reopen slot %d: client pair=%d/%d, want 0/0",
				s, st2.ClientWriteCounts()[s], st2.ClientWriteByteCounts()[s])
		}
	}
	// And new frames advance FROM the seed, not beside it.
	next := recFor(rec.AggregateID, 2, "cmd-r4")
	if _, err := st2.AppendRouted(next); err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if got := st2.WriteCounts()[s1]; got != beforeW[s1]+1 {
		t.Fatalf("append after reopen: writes=%d, want %d", got, beforeW[s1]+1)
	}
	if got := st2.WriteByteCounts()[s1]; got != beforeB[s1]+uint64(next.EncodedSize()) {
		t.Fatalf("append after reopen: bytes=%d, want %d", got, beforeB[s1]+uint64(next.EncodedSize()))
	}
}
