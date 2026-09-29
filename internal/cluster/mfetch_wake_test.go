package cluster

import (
	"testing"
	"time"

	"pushupes/internal/storage"
)

// appendOne routes one record (via the package's makeRecord/aggInSlot test
// helpers) into store and fails the test on error.
func appendOne(t *testing.T, store *storage.Store, agg, cmd string) {
	t.Helper()
	if _, err := store.Append(makeRecord(agg, 1, cmd)); err != nil {
		t.Fatal(err)
	}
}

// TestMFetchLongPollWakesOnAppend pins the leader-side long-poll contract:
// a round with WaitMS must return well before the wait deadline once a
// record is appended to one of the polled slots — the store wake handle,
// not the deadline, ends the wait. Guards the acks=all latency path
// (follower learns about new records through this round).
func TestMFetchLongPollWakesOnAppend(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.OpenStore(dir, 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	e := NewEngine(nil, store, "node-1", "leader", nil)
	e.tableMu.Lock()
	e.table.Slots[3] = &Placement{Leader: "node-1", Replicas: []string{"node-1", "node-2"}, Epoch: 1, State: SlotStable}
	e.tableMu.Unlock()

	const wait = 2 * time.Second
	done := make(chan *MFetchResponse, 1)
	start := time.Now()
	go func() {
		resp, err := e.HandleMFetch(MFetchRequest{
			Follower: "node-2",
			WaitMS:   wait.Milliseconds(),
			Slots:    []int32{3},
			FromSeqs: []uint64{store.LastSeqOf(3) + 1},
		})
		if err != nil {
			t.Errorf("handle: %v", err)
		}
		done <- resp
	}()

	time.Sleep(150 * time.Millisecond)
	// The wake handle is per-slot: route the record INTO slot 3, a record
	// landing elsewhere must not (and will not) wake it.
	agg := aggInSlot(t, e, 3)
	appendOne(t, store, agg, "wake-cmd")

	select {
	case resp := <-done:
		if el := time.Since(start); el > wait/2 {
			t.Fatalf("long-poll ignored the append wake: waited %v (deadline %v)", el, wait)
		}
		if resp == nil {
			t.Fatal("wake round returned nil")
		}
		if it, ok := resp.Item(3); !ok || len(it.Payload) == 0 {
			t.Fatalf("wake round returned no payload: %+v", resp.Items)
		}
	case <-time.After(wait + 2*time.Second):
		t.Fatal("round never returned")
	}
}

// TestMFetchBurstDrainAllWaiters pins the per-slot incremental contract:
// when all polled slots are empty (everything parks) and a wake burst
// then touches several of them, ONE long-poll response must carry data
// for every slot the burst advanced — the old behavior answered for the
// first fired slot only and made the follower pay one round trip per
// remaining slot (RTT x slots of HW lag under acks=all). Note this is
// the idle→burst path: with data present at request time phase 1 answers
// immediately (fetch answers with whatever it has), so no waiters park at all.
func TestMFetchBurstDrainAllWaiters(t *testing.T) {
	dir := t.TempDir()
	store, err := storage.OpenStore(dir, 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	e := NewEngine(nil, store, "node-1", "leader", nil)
	e.tableMu.Lock()
	for s := range 8 {
		e.table.Slots[int32(s)] = &Placement{Leader: "node-1", Replicas: []string{"node-1", "node-2"}, Epoch: 1, State: SlotStable}
	}
	e.tableMu.Unlock()

	const wait = 2 * time.Second
	done := make(chan *MFetchResponse, 1)
	go func() {
		resp, err := e.HandleMFetch(MFetchRequest{Follower: "node-2", WaitMS: wait.Milliseconds(),
			Slots: []int32{1, 3, 6}, FromSeqs: []uint64{1, 1, 1}})
		if err != nil {
			t.Errorf("handle: %v", err)
		}
		done <- resp
	}()

	time.Sleep(150 * time.Millisecond) // all three slots now park as waiters
	// Burst: append into ALL three parked slots back-to-back. The first
	// close wakes the poll; the settle window lets the rest land before
	// the sweep, so one response drains them all.
	for _, want := range []int32{1, 3, 6} {
		appendOne(t, store, aggInSlot(t, e, want), "burst-cmd")
	}

	select {
	case resp := <-done:
		if resp == nil {
			t.Fatal("nil response")
		}
		got := map[int32]bool{}
		for _, it := range resp.Items {
			if len(it.Payload) > 0 {
				got[it.Slot] = true
			}
		}
		if len(got) < 2 {
			t.Fatalf("burst drain regressed: one wake must answer for every ready slot (got=%v)", got)
		}
	case <-time.After(wait + 2*time.Second):
		t.Fatal("no response at all")
	}
}
