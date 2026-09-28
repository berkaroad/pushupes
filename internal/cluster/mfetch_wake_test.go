package cluster

import (
	"strconv"
	"testing"
	"time"

	"pushupes/internal/data"
	"pushupes/internal/storage"
)

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
	go func() {
		resp, err := e.HandleMFetch(MFetchRequest{
			Follower: "node-2",
			WaitMS:   wait.Milliseconds(),
			Items:    []FetchItem{{Slot: 3, FromSeq: store.LastSeqOf(3) + 1}},
		})
		if err != nil {
			t.Errorf("handle: %v", err)
		}
		done <- resp
	}()

	time.Sleep(150 * time.Millisecond)
	// route the record INTO slot 3 (CRC16(agg)%8 == 3): the wake handle is
	// per-slot, a record landing elsewhere must not (and will not) wake it.
	agg := ""
	for i := 0; i < 100000 && agg == ""; i++ {
		c := "wake-agg-" + strconv.Itoa(i)
		if store.SlotOf(c) == 3 {
			agg = c
		}
	}
	if agg == "" {
		t.Fatal("no agg routed to slot 3")
	}
	rec := &data.EventRecord{AggregateID: agg, Version: 1, UnixTime: time.Now().Unix(), CommandID: "wake-cmd",
		Events: []data.Event{{Type: "T", Body: []byte(`{}`)}}}
	if _, err := store.Append(rec); err != nil {
		t.Fatal(err)
	}

	select {
	case resp := <-done:
		if resp == nil || len(resp.Items) == 0 || len(resp.Items[0].Payload) == 0 {
			t.Fatalf("wake round returned no payload: %+v", resp)
		}
	case <-time.After(wait + time.Second):
		t.Fatalf("long-poll did not wake on append within %v: wake handle broken", wait)
	}
}
