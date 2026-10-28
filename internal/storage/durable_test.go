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
	"time"
)

// The fsync coverage watermark is what the write-ack path waits on: a record
// may be confirmed only once it has passed an fsync on the leader's own disk,
// so a whole-cluster power loss cannot erase a confirmed write. These tests
// pin the shapes of that promise: a timed policy round lands the claim, a
// parked acknowledgement fires its own round immediately whatever the window
// says, a watermark that has not been earned is never claimed, and a loaded
// copy opens covered.

func openDurStore(t *testing.T, dir string, policy FlushPolicy) *Store {
	t.Helper()
	st, err := OpenStore(dir, 128, DefaultSegmentBytes, policy)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestAckGateTimedWindowLandsTheClaim(t *testing.T) {
	st := openDurStore(t, t.TempDir(), FlushPolicy{IntervalMessages: 1 << 40, Interval: 30 * time.Millisecond})

	var slot int32
	var last uint64
	for i := 1; i <= 3; i++ {
		out, err := st.AppendRouted(mkRec("agg-dur", uint32(i), "cmd-dur"))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		slot = st.SlotOf("agg-dur")
		last = out.Seq
	}
	obs := st.AwaitDurable(slot, last, time.Now().Add(5*time.Second))
	if obs < last {
		t.Fatalf("timed policy round never covered seq %d (watermark %d)", last, obs)
	}
	if got := st.DurableSeqOf(slot); got < last {
		t.Fatalf("DurableSeqOf reports %d below covered seq %d", got, last)
	}
}

func TestAckGateWaiterKicksDisabledPolicy(t *testing.T) {
	// FlushPolicy{} runs no timed round and no threshold: without the parked
	// waiter making the slot due, an acknowledgement here could never be
	// completed. Success therefore proves the kick path fired a real flush —
	// the reopen control below reads the claim off the disk, not memory.
	st := openDurStore(t, t.TempDir(), FlushPolicy{})
	out, err := st.AppendRouted(mkRec("agg-kick", 1, "cmd-kick"))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	slot := st.SlotOf("agg-kick")
	if obs := st.AwaitDurable(slot, out.Seq, time.Now().Add(5*time.Second)); obs < out.Seq {
		t.Fatalf("a parked waiter did not produce a flush (watermark %d, want >= %d)", obs, out.Seq)
	}
}

func TestAckGateWaiterFiresItsOwnRound(t *testing.T) {
	// The negative half: a watermark never fabricates coverage — with the next
	// timed round a minute away and nobody waiting, a fresh append reads as
	// uncovered. The positive half: once an acknowledgement parks, the waiter
	// makes the slot due NOW, so the ack pays fsync time, not the interval.
	st := openDurStore(t, t.TempDir(), FlushPolicy{IntervalMessages: 1 << 40, Interval: time.Minute})
	out, err := st.AppendRouted(mkRec("agg-slow", 1, "cmd-slow"))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	slot := st.SlotOf("agg-slow")
	if got := st.DurableSeqOf(slot); got >= out.Seq {
		t.Fatalf("fresh record reads as covered before any round settled (watermark %d)", got)
	}
	t0 := time.Now()
	if obs := st.AwaitDurable(slot, out.Seq, time.Now().Add(5*time.Second)); obs < out.Seq {
		t.Fatalf("a parked waiter did not fire its round (watermark %d, want >= %d)", obs, out.Seq)
	}
	if d := time.Since(t0); d > time.Second {
		t.Fatalf("the waiter-fired round took %v — acknowledgements must pay fsync time, not the timed window", d)
	}
}

func TestDurableWatermarkSeededFromOpenCopy(t *testing.T) {
	// A loaded copy IS its disk image: the watermark opens at the log end, so
	// a restart never re-asks for coverage it can already prove.
	dir := t.TempDir()
	st, err := OpenStore(dir, 128, DefaultSegmentBytes, FlushPolicy{IntervalMessages: 1 << 40, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	out, err := st.AppendRouted(mkRec("agg-reopen", 1, "cmd-reopen"))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	slot := st.SlotOf("agg-reopen")
	if obs := st.AwaitDurable(slot, out.Seq, time.Now().Add(5*time.Second)); obs < out.Seq {
		t.Fatalf("pre-reopen coverage missing (watermark %d, want >= %d)", obs, out.Seq)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	st2, err := OpenStore(dir, 128, DefaultSegmentBytes, FlushPolicy{IntervalMessages: 1 << 40, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	if got := st2.DurableSeqOf(slot); got < out.Seq {
		t.Fatalf("reopened copy reads watermark %d below its loaded log end %d", got, out.Seq)
	}
}
