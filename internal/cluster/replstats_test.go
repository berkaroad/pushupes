package cluster

import (
	"testing"
	"time"

	"pushupes/internal/storage"
)

// TestReplStatsTracksStuckWatermarkAndParkedRounds pins the replication
// self-view against live state built through the real fetch entry point: a
// round that parks, the same round ending on a record wake, and a slot whose
// log runs ahead of its watermark — the exact shape a client append waits on
// (hw < seq) while nothing else on the node looks busy.
func TestReplStatsTracksStuckWatermarkAndParkedRounds(t *testing.T) {
	store, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	e := NewEngine(nil, store, "node-1", nil)
	e.tableMu.Lock()
	e.table.Slots[3] = &Placement{Leader: "node-1", Replicas: []string{"node-1", "node-2"}, Epoch: 1, State: SlotStable}
	e.tableMu.Unlock()

	agg := aggInSlot(t, e, 3)
	if _, err := store.Append(makeRecord(agg, 1, "c1")); err != nil {
		t.Fatalf("append: %v", err)
	}

	// The follower reports LEO 1 (it holds everything) and asks for seq 2,
	// which does not exist yet: the round must park and say so.
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := e.HandleMFetch(MFetchRequest{
			Follower: "node-2", WaitMS: 2000,
			Slots: []int32{3}, FromSeqs: []uint64{2},
		})
		if err != nil {
			t.Errorf("mfetch: %v", err)
			return
		}
		if len(resp.Items) != 1 {
			t.Errorf("round served %d items, want 1 (the record appended while parked)", len(resp.Items))
		}
	}()
	waitFor(t, 3*time.Second, func() bool { return e.ReplStats().Mfetch.ParkedNow == 1 }, "fetch round to park")

	v := e.ReplStats()
	if v.Mfetch.Rounds != 1 || v.Mfetch.ParkedRounds != 1 || v.Mfetch.ServedRounds != 0 {
		t.Fatalf("rounds=%d parked=%d served=%d, want 1/1/0",
			v.Mfetch.Rounds, v.Mfetch.ParkedRounds, v.Mfetch.ServedRounds)
	}
	// The follower's report covers everything the leader holds: nothing waits.
	if v.StuckSlots != 0 || v.TrackedSlots != 1 {
		t.Fatalf("tracked=%d stuck=%d, want 1/0", v.TrackedSlots, v.StuckSlots)
	}

	// A new record must end the park through the store wake, not the deadline.
	if _, err := store.Append(makeRecord(agg, 2, "c2")); err != nil {
		t.Fatalf("append: %v", err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("parked round did not wake on the append")
	}

	v = e.ReplStats()
	if v.Mfetch.EndedByWake != 1 || v.Mfetch.EndedByDeadline != 0 || v.Mfetch.EndedByCtx != 0 {
		t.Fatalf("ended_by_wake=%d deadline=%d ctx=%d, want 1/0/0",
			v.Mfetch.EndedByWake, v.Mfetch.EndedByDeadline, v.Mfetch.EndedByCtx)
	}
	if v.Mfetch.ParkedNow != 0 {
		t.Fatalf("parked_now=%d after the round returned, want 0", v.Mfetch.ParkedNow)
	}
	// The log sits at seq 2 while the follower still reports 1: one slot is
	// stuck, one record behind.
	if v.StuckSlots != 1 || v.StuckMaxGap != 1 {
		t.Fatalf("stuck_slots=%d max_gap=%d, want 1/1", v.StuckSlots, v.StuckMaxGap)
	}
	if got := e.ReplStatsTop(8).Worst; len(got) != 1 || got[0].Slot != 3 || got[0].Gap != 1 {
		t.Fatalf("worst=%+v, want slot 3 gap 1", got)
	}
	if len(v.Peers) != 1 {
		t.Fatalf("peers=%v, want one entry", v.Peers)
	}
	if p := v.Peers[0]; p.Follower != "node-2" || p.Slots != 1 || p.InISRSlots != 1 || p.WorstLEOLag != 1 {
		t.Fatalf("peer view = %+v, want node-2 slots=1 in_isr=1 lag=1", p)
	}

	// The stuck age is what a client of that slot is sitting through.
	time.Sleep(250 * time.Millisecond)
	if got := e.ReplStats().StuckOldestAgeS; got < 0.2 {
		t.Fatalf("stuck_oldest_age_s=%v after 250ms, want >= 0.2", got)
	}
}

// TestAggregateReplPure pins the aggregation's edge cases without a store: what
// counts as stuck, which slot sets the age and the gap, and how a stale or
// lagging follower is summarised. A slot that is level with its watermark must
// not contribute an age however old its last watermark move is.
func TestAggregateReplPure(t *testing.T) {
	now := time.Now()
	repl := map[int32]*slotRepl{
		// Behind by 3, watermark last moved 3s ago; follower node-2 is 3
		// records behind with a fresh report, node-3 is 7 behind and stale.
		1: {
			node:   []string{"node-2", "node-3"},
			leo:    []uint64{5, 1},
			lastOK: []time.Time{now.Add(-time.Second), now.Add(-30 * time.Second)},
			hw:     5,
			hwAt:   now.Add(-3 * time.Second),
		},
		// Level with its log, so it is not stuck even though its watermark has
		// not moved for 100s; follower node-3 is level here too.
		2: {node: []string{"node-3"}, leo: []uint64{9}, lastOK: []time.Time{now}, hw: 9, hwAt: now.Add(-100 * time.Second)},
	}
	leos := map[int32]uint64{1: 8, 2: 9}

	stuck, gap, oldest, worst, peers := aggregateRepl(repl, func(s int32) uint64 { return leos[s] }, now, 4)
	if stuck != 1 || gap != 3 {
		t.Fatalf("stuck=%d gap=%d, want 1/3", stuck, gap)
	}
	if oldest < 2.5 || oldest > 3.5 {
		t.Fatalf("oldest=%v, want ~3 (only the stuck slot contributes)", oldest)
	}
	if len(worst) != 1 || worst[0].Slot != 1 || worst[0].Gap != 3 || worst[0].AgeS < 2.5 {
		t.Fatalf("worst=%+v, want only slot 1, gap 3, age ~3", worst)
	}
	if len(peers) != 2 || peers[0].Follower != "node-2" || peers[1].Follower != "node-3" {
		t.Fatalf("peers=%+v, want node-2 then node-3", peers)
	}
	if p := peers[0]; p.Slots != 1 || p.InISRSlots != 1 || p.WorstLEOLag != 3 || p.OldestOKAgeS < 0.5 {
		t.Fatalf("node-2 view = %+v, want slots=1 in_isr=1 lag=3 age~1", p)
	}
	// node-3: two slots, one report stale (30s > isrStaleAfter), 7 behind on
	// slot 1 and level on slot 2.
	if p := peers[1]; p.Slots != 2 || p.InISRSlots != 1 || p.WorstLEOLag != 7 || p.OldestOKAgeS < 25 {
		t.Fatalf("node-3 view = %+v, want slots=2 in_isr=1 lag=7 age~30", p)
	}
}
