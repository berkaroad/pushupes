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

	"pushupes/internal/data"
	"pushupes/internal/storage"
)

// DESIGN.md §1.1 rule 3: a record that occupies a seq and is written to the WAL
// must be locatable through its aggregate's directory, or nothing can read it
// while its LEO still counts it.
//
// The migration catch-up gate compares LEOs (rule 2 territory) and so cannot see
// a rule-3 gap. The fence's pushFrames is where such a gap can be created: it
// ships from the target's self-reported LEO, so an aggregate whose records sit
// above that LEO arrives at a version the target does not continue, lands in the
// WAL, and is not adopted by the directory.
//
// This test drives the gate itself with such a target and records whether it
// passes.
func TestCatchUpGateAcceptsInvisibleRecords(t *testing.T) {
	// --- source: a slot with the aggregate pushed to a high seq by a partner.
	srcStore, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer srcStore.Close()
	src := NewEngine(nil, srcStore, "node-1", nil)

	// --- target: same slot, LEO advanced by the partner only, so the pushed
	// aggregate's stream is short there.
	tgtStore, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer tgtStore.Close()
	tgt := NewEngine(nil, tgtStore, "node-2", nil)

	agg := "gate-agg"
	slot := srcStore.SlotOf(agg)
	partner := ""
	for i := 0; i < 8192; i++ {
		id := "gate-partner-" + strconv.Itoa(i)
		if srcStore.SlotOf(id) == slot {
			partner = id
			break
		}
	}
	if partner == "" {
		t.Skip("no partner aggregate shares the slot")
	}

	rec := func(id string, v uint32) *data.EventRecord {
		return &data.EventRecord{
			AggregateID: id, Version: v,
			CommandID: id + "-c" + strconv.Itoa(int(v)),
			Events:    []data.Event{{Type: "E", Body: []byte("x")}},
		}
	}

	// Source: agg v1..v3, then partner v1..v5, then agg v4..v5 at the top.
	for v := uint32(1); v <= 3; v++ {
		if _, err := srcStore.Append(rec(agg, v)); err != nil {
			t.Fatal(err)
		}
	}
	for v := uint32(1); v <= 5; v++ {
		if _, err := srcStore.Append(rec(partner, v)); err != nil {
			t.Fatal(err)
		}
	}
	for v := uint32(4); v <= 5; v++ {
		if _, err := srcStore.Append(rec(agg, v)); err != nil {
			t.Fatal(err)
		}
	}

	// Target: only the partner, injected at the same seqs.
	for v := uint32(1); v <= 5; v++ {
		if err := tgtStore.AppendAtSeq(slot, uint64(v), rec(partner, v)); err != nil {
			t.Fatal(err)
		}
	}

	srcAddr := newPeerHarness(t, src)
	tgtAddr := newPeerHarness(t, tgt)

	// Register the three peers and plan the slot table, so the slot has a real
	// placement: the fence resolves the target's peer address from the table and
	// only acts on a slot the source believes is migrating out.
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		applyCmd(t, src, &Command{Op: OpJoinNode, Peer: &Peer{
			ID: id, PeerAddr: "127.0.0.1:1", AdminAddr: "127.0.0.1:1", ClientAddr: "127.0.0.1:1",
		}})
	}
	applyCmd(t, src, &Command{Op: OpPlanSlots})
	if _, ok := src.TableSnapshot().Slots[slot]; !ok {
		t.Fatalf("test setup: slot %d not planned", slot)
	}
	src.registerPeerForTest(t, "node-2", tgtAddr)
	src.registerPeerForTest(t, "node-1", srcAddr)

	applyCmd(t, src, &Command{Op: OpSlotState, Slots: []int32{slot},
		State: SlotMigratingOut, MigratingTo: "node-2"})
	if got := src.migrationTargetOf(slot); got != "node-2" {
		t.Fatalf("test setup: slot %d migrating to %q, want node-2", slot, got)
	}

	srcLEO := srcStore.LastSeqOf(slot)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The fence's actual work: the source brings the target up to the frozen LEO.
	// This must go through pushFencedTail, not pushFrames directly — the
	// consistency check and the rebuild live there, and calling pushFrames
	// bypasses exactly the guard this test exists to verify.
	tgtLEO := tgtStore.LastSeqOf(slot)
	if err := src.pushFencedTail(ctx, slot, srcLEO); err != nil {
		t.Fatalf("pushFencedTail: %v", err)
	}

	// What the gate observes: the target's LEO now equals the source's.
	nowLEO := tgtStore.LastSeqOf(slot)
	if nowLEO != srcLEO {
		t.Fatalf("target LEO %d, want %d (the fence should have brought it level)", nowLEO, srcLEO)
	}
	t.Logf("gate inputs: srcLEO=%d tgtLEO=%d -> tgtLEO >= srcLEO is %v (gate would PASS)",
		srcLEO, nowLEO, nowLEO >= srcLEO)
	_ = tgtLEO

	// What a reader observes for the pushed aggregate.
	srcTail, err := srcStore.TailVersionOf(agg, 0)
	if err != nil {
		t.Fatalf("source tail: %v", err)
	}
	tgtTail, err := tgtStore.TailVersionOf(agg, 0)
	if err != nil {
		t.Fatalf("target tail: %v", err)
	}
	tgtRecs, _, err := tgtStore.ReadAggregate(agg, 1, 0, 0)
	if err != nil {
		t.Fatalf("target read: %v", err)
	}
	t.Logf("source tail=%d | target tail=%d records=%d", srcTail, tgtTail, len(tgtRecs))

	// The gap: records are on the target's disk but the directory does not serve
	// them, while the LEO the gate compares has already counted them.
	_, _, payload, err := tgtStore.ReadSlotBytes(slot, uint64(tgtLEO+4), uint64(tgtLEO+5), 1<<20)
	if err == nil && len(payload) > 0 {
		m, _, derr := data.DecodeRecordMeta(payload)
		if derr == nil {
			t.Logf("target seq %d on disk: agg=%s version=%d (invisible: tail says %d)",
				tgtLEO+4, m.AggregateID, m.Version, tgtTail)
		}
	}

	if tgtTail != srcTail {
		t.Errorf("rule 3 violated and the catch-up gate cannot see it: target tail %d != source tail %d "+
			"while the gate compares LEOs (%d >= %d passes), so a migration would commit with records "+
			"the new leader cannot serve", tgtTail, srcTail, nowLEO, srcLEO)
	}
}

// The companion negative control: when the target genuinely holds the aggregate,
// the same push brings its tail level with the source's. Without this, the test
// above could pass by the push simply never working.
func TestCatchUpGateSeesCompleteTarget(t *testing.T) {
	srcStore, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer srcStore.Close()
	src := NewEngine(nil, srcStore, "node-1", nil)

	tgtStore, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer tgtStore.Close()
	tgt := NewEngine(nil, tgtStore, "node-2", nil)

	agg := "gate-ok"
	slot := srcStore.SlotOf(agg)
	rec := func(v uint32) *data.EventRecord {
		return &data.EventRecord{
			AggregateID: agg, Version: v,
			CommandID: agg + "-c" + strconv.Itoa(int(v)),
			Events:    []data.Event{{Type: "E", Body: []byte("x")}},
		}
	}
	for v := uint32(1); v <= 5; v++ {
		if _, err := srcStore.Append(rec(v)); err != nil {
			t.Fatal(err)
		}
	}
	// Target holds the first three, so the push delivers a CONTIGUOUS tail.
	for v := uint32(1); v <= 3; v++ {
		if err := tgtStore.AppendAtSeq(slot, uint64(v), rec(v)); err != nil {
			t.Fatal(err)
		}
	}

	tgtAddr := newPeerHarness(t, tgt)
	src.registerPeerForTest(t, "node-2", tgtAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srcLEO := srcStore.LastSeqOf(slot)
	if err := src.pushFrames(ctx, tgtAddr, slot, tgtStore.LastSeqOf(slot)+1, srcLEO); err != nil {
		t.Fatalf("pushFrames: %v", err)
	}

	srcTail, _ := srcStore.TailVersionOf(agg, 0)
	tgtTail, err := tgtStore.TailVersionOf(agg, 0)
	if err != nil {
		t.Fatalf("target tail: %v", err)
	}
	if tgtTail != srcTail {
		t.Errorf("contiguous push left target tail %d != source tail %d", tgtTail, srcTail)
	}
}
