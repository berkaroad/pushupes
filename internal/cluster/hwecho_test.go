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
	"fmt"
	"testing"
	"time"
)

// TestMFetchAnswersEarlyOnWatermarkAdvance pins the tight echo: a fetch round
// parked with NO data pending must still return well before its wait deadline
// when the slot's watermark advances (a report from another replica moving the
// in-sync min). The advance kicks the engine-wide hwPing; the parked round
// re-scans its armed handle, books the fresh watermark into its own response
// ledger, and answers — so the follower's read-admission proof catches up to
// an acknowledged watermark inside the round already in flight, not one report
// round later. This is the mechanism that closes the short-read window found
// on the live cluster (acked seq invisible at a synced replica for a round).
func TestMFetchAnswersEarlyOnWatermarkAdvance(t *testing.T) {
	e, store := newTestEngine(t, "node-1")

	const slot int32 = 3
	e.tableMu.Lock()
	e.table.Slots[slot] = &Placement{Leader: "node-1", Replicas: []string{"node-1", "node-2", "node-c"}, Epoch: 1, State: SlotStable}
	e.tableMu.Unlock()

	// Seed six records so the leader's LEO is 6.
	for i := uint64(1); i <= 6; i++ {
		if _, err := store.AppendRouted(makeRecord(aggInSlot(t, e, slot), uint32(i), fmt.Sprintf("seed-%d", i))); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	// node-c (an in-sync replica) sits at LEO 3, holding the watermark down;
	// node-2 (the requesting follower) has CAUGHT UP (LEO 6, reports from-seq
	// 7): its round has no payload to serve and parks — the exact idle shape
	// in which an acked watermark must still reach it the moment node-c's
	// report lifts the min.
	sr := newSlotRepl()
	now := time.Now()
	sr.touch("node-2", 6, now)
	sr.touch("node-c", 3, now)
	e.replMu.Lock()
	e.repl[slot] = sr
	e.replMu.Unlock()
	e.advanceHW(slot)
	e.replMu.Lock()
	got := sr.hw
	e.replMu.Unlock()
	if got != 3 {
		t.Fatalf("precondition: hw should sit at 3 (node-c holds it), got %d", got)
	}

	const wait = 2 * time.Second
	done := make(chan *MFetchResponse, 1)
	errs := make(chan error, 1)
	start := time.Now()
	go func() {
		resp, err := e.HandleMFetch(MFetchRequest{
			Follower: "node-2", WaitMS: wait.Milliseconds(),
			Slots: []int32{slot}, FromSeqs: []uint64{7}, // node-2 caught up: nothing to serve
		})
		if err != nil {
			errs <- err
			return
		}
		done <- resp
	}()

	// Let the round reach its park, then move the watermark the way a report
	// from ANOTHER replica (node-c) would: it catches up to the leader's LEO,
	// the in-sync min rises to 6, and advanceHW kicks the gate + hwPing.
	time.Sleep(150 * time.Millisecond)
	e.replMu.Lock()
	sr.touch("node-c", 6, time.Now())
	e.replMu.Unlock()
	e.advanceHW(slot)

	select {
	case resp := <-done:
		if el := time.Since(start); el > wait/2 {
			t.Fatalf("round ignored the watermark wake: waited %v (deadline %v)", el, wait)
		}
		var hw uint64
		var seen bool
		for i, s := range resp.HwSlots {
			if s == slot {
				hw, seen = resp.HwValues[i], true
			}
		}
		if !seen || hw != 6 {
			t.Fatalf("early answer must book (slot %d, hw 6): got slots=%v values=%v", slot, resp.HwSlots, resp.HwValues)
		}
		if len(resp.Items) != 0 {
			t.Fatalf("a pure watermark move carries no payload; got %d items", len(resp.Items))
		}
		if e.diag.mfetchByHW.Load() == 0 {
			t.Fatal("the early answer did not book the hw-echo wake counter")
		}
	case err := <-errs:
		t.Fatalf("round errored: %v", err)
	case <-time.After(wait):
		t.Fatal("fetch round did NOT answer early on the watermark advance")
	}
}
