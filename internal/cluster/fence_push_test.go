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

package cluster

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/berkaroad/pushupes/internal/data"
	"github.com/berkaroad/pushupes/internal/storage"
)

// pushFrames ships the source's frozen tail to a migration target, and it
// decides WHAT to ship from the target's self-reported LEO alone:
//
//	tgtLEO, _ := e.remoteLEO(ctx, addr, slot)   // HandleLEO -> store.LastSeqOf
//	e.pushFrames(ctx, addr, slot, tgtLEO+1, leo)
//
// That LEO is a SLOT counter: it counts every record the slot holds, from every
// aggregate. The frames it reads from tgtLEO+1 therefore start wherever the slot
// counter left off — but a frame carries the VERSION of the aggregate it belongs
// to, and nothing in this path checks that the version continues the target's
// stream for that aggregate.
//
// A target can hold a slot with a high LEO and a SHORT stream for one aggregate:
// the aggregate may simply not have been touched since early in the slot's life,
// or the target may have imported the slot's sealed segments without the tail.
// Pushing from tgtLEO+1 then delivers that aggregate's record at a version that
// is not its next one.
//
// This test pins what the target does with such a frame, so the gap is visible
// in a test instead of only as a drained-but-unreadable aggregate in a live
// cluster.
func TestFencePushDoesNotStrandTargetAggregate(t *testing.T) {
	// --- the source: a slot holding several aggregates, so versions and seqs
	// do not advance in lockstep.
	srcStore, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer srcStore.Close()
	src := NewEngine(nil, srcStore, "node-1", nil)

	// --- the target: same slot, but with a short stream for the aggregate we
	// will push. Its LEO is advanced by OTHER aggregates, which is what makes
	// tgtLEO+1 land in the middle of this aggregate's stream.
	tgtStore, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer tgtStore.Close()
	tgt := NewEngine(nil, tgtStore, "node-2", nil)

	// Find an aggregate and a second aggregate sharing its slot: routing is
	// mix64(FNV-1a(id)) % slot_count, so the partner must be SEARCHED for.
	agg := "fence-agg"
	slot := srcStore.SlotOf(agg)
	partner := ""
	for i := 0; i < 8192; i++ {
		id := "fence-partner-" + strconv.Itoa(i)
		if srcStore.SlotOf(id) == slot {
			partner = id
			break
		}
	}
	if partner == "" {
		t.Skip("no partner aggregate shares the slot")
	}

	// Source: v1..v3 of `agg`, then a run of `partner` to push the slot LEO
	// well past agg's own seqs.
	for v := 1; v <= 3; v++ {
		if _, err := srcStore.Append(&data.EventRecord{
			AggregateID: agg, Version: uint32(v),
			CommandID: agg + "-c" + strconv.Itoa(v),
			Events:    []data.Event{{Type: "E", Body: []byte("x")}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	for v := 1; v <= 5; v++ {
		if _, err := srcStore.Append(&data.EventRecord{
			AggregateID: partner, Version: uint32(v),
			CommandID: partner + "-c" + strconv.Itoa(v),
			Events:    []data.Event{{Type: "E", Body: []byte("x")}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	// Two more of `agg` at the TOP of the log: these land inside the window the
	// fence pushes (tgtLEO+1 .. leo), carrying versions 4 and 5 while the
	// target's stream for `agg` is empty. This is the shape the fence can
	// actually deliver: the slot counter moved on, the aggregate did not.
	for v := 4; v <= 5; v++ {
		if _, err := srcStore.Append(&data.EventRecord{
			AggregateID: agg, Version: uint32(v),
			CommandID: agg + "-c" + strconv.Itoa(v),
			Events:    []data.Event{{Type: "E", Body: []byte("x")}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Target: the SAME slot's counter advanced by the partner only, so its LEO
	// is high while `agg` has no records there at all — the state an import
	// that took the sealed segments but not the tail leaves behind.
	for v := 1; v <= 5; v++ {
		if err := tgtStore.AppendAtSeq(slot, uint64(v), &data.EventRecord{
			AggregateID: partner, Version: uint32(v),
			CommandID: partner + "-c" + strconv.Itoa(v),
			Events:    []data.Event{{Type: "E", Body: []byte("x")}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	addr := newPeerHarness(t, tgt)
	srcAddr := newPeerHarness(t, src)
	src.registerPeerForTest(t, "node-2", addr)
	tgt.registerPeerForTest(t, "node-1", srcAddr)

	leo := srcStore.LastSeqOf(slot)
	tgtLEO := tgtStore.LastSeqOf(slot)
	if tgtLEO >= leo {
		t.Fatalf("test setup: target LEO %d should trail source LEO %d", tgtLEO, leo)
	}
	t.Logf("slot=%d source_leo=%d target_leo=%d target_agg_tail=%d",
		slot, leo, tgtLEO, tailOf(t, tgtStore, agg))

	// Push the tail the way the fence does: from the target's reported LEO.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := src.pushFrames(ctx, addr, slot, tgtLEO+1, leo); err != nil {
		t.Fatalf("pushFrames: %v", err)
	}

	// The target's view of the pushed aggregate must still be a stream a reader
	// can walk: either the record arrived at its next version, or it did not
	// arrive at all. What must NOT happen is a directory claiming a version it
	// cannot serve, which makes every read of that aggregate fail.
	tgtTail, err := tgtStore.TailVersionOf(agg, 0)
	if err != nil {
		t.Fatalf("target aggregate unreadable after the push: %v", err)
	}
	recs, _, err := tgtStore.ReadAggregate(agg, 1, 0, 0)
	if err != nil {
		t.Fatalf("target read of %s failed after the push: %v", agg, err)
	}
	t.Logf("after push: target tail=%d readable_records=%d", tgtTail, len(recs))
	// Is the target's `partner` stream duplicated? Its tail vs what it can serve.
	partnerTail, _ := tgtStore.TailVersionOf(partner, 0)
	pRecs, _, _ := tgtStore.ReadAggregate(partner, 1, 0, 0)
	t.Logf("  partner: tail=%d readable=%d (v1..v5 written once by the harness)", partnerTail, len(pRecs))
	if partnerTail != 5 {
		t.Errorf("partner tail on the target = %d, want 5: the fence re-pushed records the "+
			"target already held, and the directory counted them again", partnerTail)
	}
	// Where did the pushed `agg` records go? Walk the target's WAL by seq.
	tgtLast := tgtStore.LastSeqOf(slot)
	t.Logf("  target slot last_seq=%d", tgtLast)
	for seq := uint64(1); seq <= tgtLast; seq++ {
		_, _, payload, perr := tgtStore.ReadSlotBytes(slot, seq, seq+1, 1<<20)
		if perr != nil || len(payload) == 0 {
			t.Logf("  target seq %d: (no bytes: %v)", seq, perr)
			continue
		}
		m, _, derr := data.DecodeRecordMeta(payload)
		if derr != nil {
			t.Logf("  target seq %d: decode err %v", seq, derr)
			continue
		}
		t.Logf("  target seq %d: agg=%s version=%d", seq, m.AggregateID, m.Version)
	}
	if tgtTail != uint32(len(recs)) {
		t.Errorf("target tail %d but only %d records readable: the pushed frame advanced the "+
			"directory past what the slot can serve", tgtTail, len(recs))
	}
}

// registerPeerForTest installs a peer entry directly, without Raft.
func (e *Engine) registerPeerForTest(t *testing.T, id, peerAddr string) {
	t.Helper()
	c := &Command{Op: OpJoinNode, Peer: &Peer{
		ID: id, PeerAddr: peerAddr, AdminAddr: peerAddr, ClientAddr: peerAddr,
	}}
	if _, err := e.ApplyCommand(c.Encode()); err != nil {
		t.Fatal(err)
	}
}

// tailOf reports an aggregate's claimed tail on a store, or -1 when the store
// cannot answer.
func tailOf(t *testing.T, st *storage.Store, agg string) int {
	t.Helper()
	v, err := st.TailVersionOf(agg, 0)
	if err != nil {
		return -1
	}
	return int(v)
}
