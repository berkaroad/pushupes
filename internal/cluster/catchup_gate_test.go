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

// DESIGN.md §1.1 rule 3: a record that occupies a seq and is written to the WAL
// must be locatable through its aggregate's directory, or nothing can read it
// while its LEO still counts it.
//
// A migration target can be in exactly that state: its LEO (which counts seqs)
// has caught up while one aggregate's stream is short, so every records-per-LEO
// comparison passes. The COMMIT GATE is what refuses such a copy — it compares
// the two directories (aggregate/version/resolvable counts), and a divergence at
// the same log end offset is the one case fetching can never repair, so the gate
// reports it and the hand-over drops the copy (rebalanceRound, MigrateSlot)
// instead of moving leadership onto records the new leader cannot serve.
//
// This test builds that copy and pins the gate's verdict on it.
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
		applyCmd(t, src, &Command{Op: OpJoinNode, Peer: &Peer{Role: RoleLearner,
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

	// Two verdicts, in the order a hand-over meets them. First the copy is
	// BEHIND (its log end offset still trails the source's): nothing failed,
	// the fetch path has work left, so the gate waits. Reading the peers goes
	// through the peer plane (empty srcAddr = this node's own copy).
	ready, diverged, err := src.handoverReady(ctx, slot, "node-1", "node-2")
	if err != nil {
		t.Fatalf("handoverReady (behind): %v", err)
	}
	if ready || diverged {
		t.Fatalf("a copy that is still behind must make the gate WAIT (ready=%v diverged=%v)", ready, diverged)
	}
	t.Logf("gate while behind: srcLEO=%d tgtLEO=%d -> wait", srcLEO, tgtStore.LastSeqOf(slot))

	// Let the copy reach the source's log end offset, the way the fetch path
	// and the fence's tail push do. The records counted by that LEO are on the
	// target's disk; its DIRECTORY is short, which is the part an LEO-only
	// comparison cannot see.
	if err := src.pushFencedTail(ctx, slot, srcLEO); err != nil {
		t.Fatalf("pushFencedTail: %v", err)
	}
	if got := tgtStore.LastSeqOf(slot); got != srcLEO {
		t.Fatalf("target LEO %d, want %d (the tail push should level it)", got, srcLEO)
	}
	ready, diverged, err = src.handoverReady(ctx, slot, "node-1", "node-2")
	if err != nil {
		t.Fatalf("handoverReady (level): %v", err)
	}
	if ready || !diverged {
		t.Fatalf("a short directory AT the source's log end offset must be reported DIVERGED (ready=%v diverged=%v): "+
			"fetching cannot repair it, so the hand-over drops the copy and it pulls again from empty", ready, diverged)
	}

	// The observable cause: the records are on the target's disk while its
	// directory does not serve them.
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
	t.Logf("source tail=%d | target tail=%d records=%d (same LEO, different directory)",
		srcTail, tgtTail, len(tgtRecs))
	if tgtTail >= srcTail {
		t.Fatalf("test setup: the target's directory should be short of the source's (target tail %d, source tail %d)", tgtTail, srcTail)
	}
}
