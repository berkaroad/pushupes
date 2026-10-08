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
	"testing"
	"time"

	"github.com/berkaroad/pushupes/internal/storage"
)

// The wait after attempt n doubles from the round interval up to the cap: a
// slot whose move cannot stick must not be re-attempted (and, when its target
// is diverged, fully rebuilt) once per round forever.
func TestRebalanceRetryDelayEscalatesThenCaps(t *testing.T) {
	want := []time.Duration{
		2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second,
		32 * time.Second, time.Minute, time.Minute, // capped
	}
	for i, w := range want {
		if got := rebalanceRetryDelay(i + 1); got != w {
			t.Fatalf("delay after attempt %d = %s, want %s", i+1, got, w)
		}
	}
	if got := rebalanceRetryDelay(1000); got != time.Minute {
		t.Fatalf("a huge attempt count must stay capped, got %s", got)
	}
}

// The gate is read-only on its own (a slot merely waiting for its target to
// catch up must not accumulate backoff), counting happens at the attempt, and
// the record is keyed on the move: a failover gives a fresh try.
func TestRebalanceRetryGateAndAttemptCounting(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	now := time.Now()
	const slot = int32(7)

	if !e.rebalanceRetryReady(slot, "node-2", "node-1", now) {
		t.Fatal("nothing recorded: the first attempt must be allowed")
	}
	// Reading repeatedly must not consume anything.
	for i := 0; i < 5; i++ {
		if !e.rebalanceRetryReady(slot, "node-2", "node-1", now) {
			t.Fatal("the gate must not count attempts by itself")
		}
	}
	e.recordRebalanceAttempt(slot, "node-2", "node-1", now)
	if e.rebalanceRetryReady(slot, "node-2", "node-1", now) {
		t.Fatal("the retry after the first attempt waits one round interval")
	}
	if !e.rebalanceRetryReady(slot, "node-2", "node-1", now.Add(2*time.Second)) {
		t.Fatal("the retry must be allowed when the wait is over")
	}
	// A different move for the same slot is not a retry.
	if !e.rebalanceRetryReady(slot, "node-3", "node-1", now) {
		t.Fatal("a different from/to must get a fresh try (failover)")
	}
	if !e.rebalanceRetryReady(9, "node-2", "node-1", now) {
		t.Fatal("another slot must be unaffected")
	}

	// The second attempt of the SAME move waits longer than the first: one
	// attempt at t0 (next at t0+2s), the second at t0+2s (next at t0+6s).
	e.recordRebalanceAttempt(slot, "node-3", "node-1", now)
	e.recordRebalanceAttempt(slot, "node-3", "node-1", now.Add(2*time.Second))
	if e.rebalanceRetryReady(slot, "node-3", "node-1", now.Add(5*time.Second)) {
		t.Fatal("second attempt: still inside the escalated wait (4s)")
	}
	if !e.rebalanceRetryReady(slot, "node-3", "node-1", now.Add(6*time.Second)) {
		t.Fatal("second attempt: the escalated wait must end")
	}
}

// The round consults the gate. Both copies of the slot are equivalent here, so
// the round would hand it back at once; with an attempt already recorded for
// this exact move it must leave the slot alone. Without the gate the round
// moves it — that is the every-round rebuild this backoff exists to stop.
// (That an UNthrottled round still moves slots is pinned by
// TestRebalanceHandBackEndToEnd, which drives the same round through a full
// hand-back with the gate in place.)
func TestRebalanceRoundHonoursRetryBackoff(t *testing.T) {
	oldStore, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer oldStore.Close()
	old := NewEngine(nil, oldStore, "node-1", nil)

	newStore, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer newStore.Close()
	neu := NewEngine(nil, newStore, "node-2", nil)
	neu.node = newTestRaftNode(t, neu)

	oldAddr := newPeerHarness(t, old)
	newAddr := newPeerHarness(t, neu)

	for _, e := range []*Engine{old, neu} {
		applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{ID: "node-1", PeerAddr: oldAddr}})
		applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{ID: "node-2", PeerAddr: newAddr}})
		applyCmd(t, e, &Command{Op: OpRegister, Peer: &Peer{ID: "node-1", AdminAddr: oldAddr, ClientAddr: oldAddr}})
		applyCmd(t, e, &Command{Op: OpRegister, Peer: &Peer{ID: "node-2", AdminAddr: newAddr, ClientAddr: newAddr}})
		pinReplicas(t, e, 2)
		applyCmd(t, e, &Command{Op: OpPlanSlots})
	}
	// Slot 0 belongs to node-1 on the ring; put node-2 there so the round wants
	// it back.
	applyCmd(t, neu, &Command{Op: OpLeaderMove, Slots: []int32{0}, NewLeader: "node-2"})
	moves := PlanLeaderRebalance(neu.TableSnapshot())
	if len(moves) != 1 || moves[0].Slot != 0 || moves[0].To != "node-1" {
		t.Fatalf("setup: want node-2 to hand slot 0 back, plan: %v", moves)
	}
	m := moves[0]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	neu.recordRebalanceAttempt(m.Slot, m.From, m.To, time.Now())
	if done := neu.rebalanceRound(ctx, 8); done != 0 {
		t.Fatalf("a backed-off hand-over was attempted anyway (%d moves)", done)
	}
	if p, _ := neu.TableSnapshot().Slots[m.Slot]; p.Leader != m.From {
		t.Fatalf("slot %d moved despite the backoff: %+v", m.Slot, p)
	}
}
