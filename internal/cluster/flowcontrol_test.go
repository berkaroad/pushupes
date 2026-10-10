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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/berkaroad/pushupes/internal/data"
)

// TestFlowBucketTakeAndReturn pins the operator's model: capacity takes per
// period, refusals between, and every taken token back in the bucket once
// its stamped deadline passes.
func TestFlowBucketTakeAndReturn(t *testing.T) {
	const capacity = 3
	b := &slotBucket{capacity: capacity, period: int64(time.Second)}
	now := int64(1_000_000_000)
	for i := 0; i < capacity; i++ {
		if !b.take(now) {
			t.Fatalf("take %d refused on an empty bucket", i)
		}
	}
	if b.take(now) {
		t.Fatal("the (cap+1)-th take inside one period must be refused")
	}
	// One nanosecond before the deadline: still refused.
	if b.take(now + int64(time.Second) - 1) {
		t.Fatal("a token must not return before its deadline")
	}
	// At the deadline: all three are back.
	for i := 0; i < capacity; i++ {
		if !b.take(now + int64(time.Second)) {
			t.Fatalf("take %d after the return deadline refused", i)
		}
	}
}

// TestFlowBucketRefund pins the refund rule: a token paid for a record that
// never landed comes back immediately (not after the period), and the
// refund-marked deadlines must not be credited a second time when they
// expire — away = un-refunded entries, so capacity accounting stays honest.
func TestFlowBucketRefund(t *testing.T) {
	b := &slotBucket{capacity: 1, period: int64(time.Hour)}
	if !b.take(0) {
		t.Fatal("first take refused")
	}
	if b.take(0) {
		t.Fatal("capacity 1: second take must be refused before any refund")
	}
	b.refund()
	if !b.take(0) {
		t.Fatal("a refunded token must be takeable again without waiting out the period")
	}
	if b.take(0) {
		t.Fatal("capacity 1: with one token away the next take must be refused")
	}
	b.refund()
	if b.away() != 0 {
		t.Fatalf("away = %d after two refunds, want 0", b.away())
	}
	// The two refund-marked deadlines expire far in the future; a take now
	// must be allowed, and must remain allowed after the sweep has eaten
	// the stale entries (the sweep must not double-credit them).
	if !b.take(0) {
		t.Fatal("post-refund take refused")
	}
}

// TestFlowConfigUnlimited pins the default and both clearing forms.
func TestFlowConfigUnlimited(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	if _, b := e.takeFor(0); b != nil {
		t.Fatal("an engine that never set a config must not build buckets")
	}
	e.SetFlow(FlowConfig{Tokens: 1, Period: time.Hour})
	ok, b := e.takeFor(0)
	if !ok || b == nil {
		t.Fatal("configured throttle must build a bucket and allow the first take")
	}
	if ok, _ := e.takeFor(0); ok {
		t.Fatal("capacity 1: the second immediate take must be refused")
	}
	e.SetFlow(FlowConfig{Tokens: 0, Period: time.Hour})
	if _, b := e.takeFor(0); b != nil {
		t.Fatal("tokens<=0 must mean unlimited")
	}
	e.SetFlow(FlowConfig{Tokens: 5, Period: 0})
	if _, b := e.takeFor(0); b != nil {
		t.Fatal("period<=0 must mean unlimited")
	}
}

// TestFlowGenerationSwap pins the config-change semantics: the budget
// rebuilds fresh under the new generation, and the per-slot hit HISTORY
// survives the swap (the console diffs cumulative counters).
func TestFlowGenerationSwap(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	e.SetFlow(FlowConfig{Tokens: 1, Period: time.Hour})
	if ok, _ := e.takeFor(3); !ok {
		t.Fatal("first take refused")
	}
	if ok, _ := e.takeFor(3); ok {
		t.Fatal("second take must be refused")
	}
	e.SetFlow(FlowConfig{Tokens: 1, Period: time.Hour})
	if ok, _ := e.takeFor(3); !ok {
		t.Fatal("after a re-set the bucket must be rebuilt with a fresh token")
	}
	cfg, total, slots := e.FlowStats()
	if cfg.Unlimited() {
		t.Fatal("stats must read back the live config")
	}
	if total != 1 || len(slots) != 1 || slots[0].Slot != 3 {
		t.Fatalf("hit history lost across the swap: total=%d slots=%v", total, slots)
	}
}

// sameSlotAggregates finds n distinct aggregates that all route to one slot,
// so a test can drive one bucket from the write path.
func sameSlotAggregates(t *testing.T, e *Engine, n int) []string {
	t.Helper()
	slot := e.SlotOf("anchor")
	out := make([]string, 0, n)
	for i := 0; len(out) < n; i++ {
		cand := fmt.Sprintf("flow-agg-%d", i)
		if e.SlotOf(cand) == slot {
			out = append(out, cand)
		}
		if i > 1_000_000 {
			t.Fatal("no same-slot aggregates found")
		}
	}
	return out
}

// TestFlowControlOnWritePath drives real appends and batches through the
// throttle: refusal answers fail/1006 before the WAL, a business-rule fail
// refunds, a drained bucket also refuses an otherwise-invalid record (the
// throttle is checked first), the batch truncates at the first refusal, and
// clearing the config unblocks everything.
func TestFlowControlOnWritePath(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	aggs := sameSlotAggregates(t, e, 4)
	slot := e.SlotOf(aggs[0])
	e.SetFlow(FlowConfig{Tokens: 2, Period: time.Hour})

	append1 := func(agg string, ver uint32, cmd string) *data.AppendResponse {
		t.Helper()
		resp, err := e.SubmitAppend(context.Background(), makeRecord(agg, ver, cmd))
		if err != nil {
			t.Fatalf("append %s: %v", agg, err)
		}
		return resp
	}
	if resp := append1(aggs[0], 1, "a-1"); resp.Status != data.StatusSuccess {
		t.Fatalf("append 1 = %s (%s), want success", resp.Status, resp.Err)
	}
	// A version conflict takes its token and gives it back: the bucket is
	// NOT drained by records that never land.
	resp := append1(aggs[0], 9, "a-conflict")
	if resp.Status != data.StatusFail || resp.ErrID != data.ErrIDVersionConflict {
		t.Fatalf("bad-version append = %s/%d, want fail/1001", resp.Status, resp.ErrID)
	}
	if resp := append1(aggs[1], 1, "b-1"); resp.Status != data.StatusSuccess {
		t.Fatalf("post-refund append = %s (%s), want success (refund must have returned the token)", resp.Status, resp.Err)
	}
	// Bucket now holds both tokens away. Even an invalid record answers 1006
	// — the throttle is consulted BEFORE the business rules.
	resp = append1(aggs[1], 99, "b-conflict")
	if resp.Status != data.StatusFail || resp.ErrID != data.ErrIDFlowControl || resp.Err != "FlowControl" {
		t.Fatalf("drained-bucket append = %s/%d (%s), want fail/1006 FlowControl", resp.Status, resp.ErrID, resp.Err)
	}

	// Batch on the drained slot: the group is not atomic, but every record
	// past the first refusal never lands. Here the bucket is empty at
	// record 1, so the WHOLE group answers 1006 and the WAL does not move.
	recs := []*data.EventRecord{
		makeRecord(aggs[0], 2, "g-1"),
		makeRecord(aggs[1], 2, "g-2"),
		makeRecord(aggs[2], 1, "g-3"),
	}
	outs, err := e.SubmitBatch(context.Background(), slot, recs)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	for i, o := range outs {
		if o.Status != data.StatusFail || o.ErrID != data.ErrIDFlowControl {
			t.Fatalf("batch record %d = %s/%d, want fail/1006", i, o.Status, o.ErrID)
		}
	}
	if w := st.WriteCount(slot); w != 2 {
		t.Fatalf("slot write count = %d, want 2 (only the two pre-throttle appends landed)", w)
	}

	// Clearing the config unblocks the same records: they all succeed, and
	// the WAL now holds exactly the batch too.
	e.SetFlow(FlowConfig{})
	outs, err = e.SubmitBatch(context.Background(), slot, recs)
	if err != nil {
		t.Fatalf("post-clear batch: %v", err)
	}
	for i, o := range outs {
		if o.Status != data.StatusSuccess {
			t.Fatalf("post-clear record %d = %s/%d (%s), want success", i, o.Status, o.ErrID, o.Err)
		}
	}
	if w := st.WriteCount(slot); w != 5 {
		t.Fatalf("slot write count = %d, want 5", w)
	}
}

// TestFlowBucketSharedAcrossSlots pins the new granularity: ONE node budget
// for every led slot. Draining the budget through slot A immediately
// refuses slot B's otherwise-valid write — the defining difference from the
// old per-slot buckets, where A's drain could not touch B. The refusal is
// still COUNTED at B (the attribution slot, not the budget slot).
func TestFlowBucketSharedAcrossSlots(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	slotA := e.SlotOf("anchor")
	// an aggregate on a DIFFERENT slot
	var aggB string
	for i := 0; ; i++ {
		cand := fmt.Sprintf("other-slot-%d", i)
		if e.SlotOf(cand) != slotA {
			aggB = cand
			break
		}
	}
	slotB := e.SlotOf(aggB)

	e.SetFlow(FlowConfig{Tokens: 1, Period: time.Hour})
	resp, err := e.SubmitAppend(context.Background(), makeRecord("anchor", 1, "sa-1"))
	if err != nil || resp.Status != data.StatusSuccess {
		t.Fatalf("slot A first write = %+v (%v), want success (takes the only token)", resp, err)
	}
	// slot B writes a fresh aggregate — under per-slot buckets it would
	// have had its own token; the node-wide budget must refuse it.
	resp, err = e.SubmitAppend(context.Background(), makeRecord(aggB, 1, "sb-1"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != data.StatusFail || resp.ErrID != data.ErrIDFlowControl {
		t.Fatalf("slot B write = %+v, want fail/1006 (node budget drained by slot A)", resp)
	}
	// attribution: the refusal counted slot B, not slot A.
	_, total, slots := e.FlowStats()
	if total != 1 {
		t.Fatalf("total hits = %d, want 1", total)
	}
	for _, st := range slots {
		if st.Slot == slotA && st.Hits > 0 {
			t.Fatalf("slot A counted a refusal that happened at slot B")
		}
		if st.Slot == slotB {
			return
		}
	}
	t.Fatalf("per-slot history must carry slot B's refusal: %+v", slots)
}

// TestFlowControlConcurrent hammers the node bucket from many goroutines:
// the takes that succeed inside one period never exceed the capacity.
func TestFlowControlConcurrent(t *testing.T) {
	b := &slotBucket{capacity: 50, period: int64(time.Hour)}
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n := 0
			for b.take(0) {
				n++
			}
			mu.Lock()
			allowed += n
			mu.Unlock()
		}()
	}
	wg.Wait()
	if allowed != 50 {
		t.Fatalf("allowed takes = %d, want exactly the capacity 50", allowed)
	}
}
