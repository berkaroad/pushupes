package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"pushupes/internal/data"
)

// aggsOnSlot returns n aggregate ids that hash to the given slot.
func aggsOnSlot(t *testing.T, e *Engine, slot int32, n int) []string {
	t.Helper()
	out := make([]string, 0, n)
	seen := map[string]bool{}
	for i := 0; i < 500000 && len(out) < n; i++ {
		cand := fmt.Sprintf("ack-%d-%d", slot, i)
		if seen[cand] || e.SlotOf(cand) != slot {
			continue
		}
		seen[cand] = true
		out = append(out, cand)
	}
	if len(out) < n {
		t.Fatalf("found %d aggregates on slot %d, want %d", len(out), slot, n)
	}
	return out
}

// TestAckStatsSplitsTheChain drives one group through a frozen watermark and
// checks that the self-view attributes the time to the right step: the landing
// step is fast, the watermark wait carries the deadline, and the records above
// the watermark are booked as failures rather than successes.
func TestAckStatsSplitsTheChain(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	leadSlot0(t, e)
	e.hwWait = 120 * time.Millisecond

	// one fresh in-sync replica whose watermark sits at seq 2 and never moves
	e.replMu.Lock()
	e.repl[0] = &slotRepl{node: []string{"node-2"}, leo: []uint64{2}, lastOK: []time.Time{time.Now()}, hw: 2}
	e.replMu.Unlock()

	aggs := aggsOnSlot(t, e, 0, 4)
	recs := make([]*data.EventRecord, len(aggs))
	for i, a := range aggs {
		recs[i] = makeRecord(a, 1, fmt.Sprintf("ack-cmd-%d", i))
	}
	t0 := time.Now()
	resps, err := e.SubmitBatch(context.Background(), 0, recs)
	if err != nil {
		t.Fatalf("SubmitBatch: %v", err)
	}
	elapsed := time.Since(t0)

	v := e.AckStats()
	if v.Appends != uint64(len(recs)) || v.Batches != 1 || v.Calls != 1 {
		t.Fatalf("counters: appends=%d batches=%d calls=%d, want %d/1/1", v.Appends, v.Batches, v.Calls, len(recs))
	}
	// the group paid the deadline once (one stall), not once per record
	if v.HwStalls != 1 {
		t.Fatalf("hw_stalls = %d, want 1 (the group pays the deadline once)", v.HwStalls)
	}
	if v.WaitersNow != 0 {
		t.Fatalf("waiters_now = %d, want 0 after the calls returned", v.WaitersNow)
	}
	if v.HwWait.P99MS < 100 {
		t.Fatalf("hw_wait p99 = %.1fms, want the %v deadline to show there", v.HwWait.P99MS, e.hwWait)
	}
	if v.HwWait.MaxMS > elapsed.Seconds()*1000+50 {
		t.Fatalf("hw_wait max %.1fms exceeds the call's own %.1fms", v.HwWait.MaxMS, elapsed.Seconds()*1000)
	}
	if v.Total.P99MS < v.HwWait.P99MS {
		t.Fatalf("total p99 %.1fms < hw_wait p99 %.1fms: the steps must partition the call",
			v.Total.P99MS, v.HwWait.P99MS)
	}
	// landing is the fast step: it must not be where 120ms went
	if v.WalLand.MaxMS >= v.HwWait.P99MS {
		t.Fatalf("wal_land max %.1fms vs hw_wait p99 %.1fms: the split is not distinguishing the steps",
			v.WalLand.MaxMS, v.HwWait.P99MS)
	}
	if v.HwLagRecords.MaxMS < 1 {
		t.Fatalf("hw_lag_records max = %.0f, want the watermark lag recorded", v.HwLagRecords.MaxMS)
	}
	// the watermark covered seq 2; records above it are failures, not successes
	wantFail := uint64(0)
	for _, r := range resps {
		if r.Status == data.StatusFail {
			wantFail++
		}
	}
	if wantFail == 0 {
		t.Fatal("fixture produced no failures above the watermark")
	}
	if v.Fail != wantFail {
		t.Fatalf("fail = %d, want %d (the settled statuses)", v.Fail, wantFail)
	}
	if v.Success+v.Exists+v.Fail != uint64(len(recs)) {
		t.Fatalf("statuses do not add up: success=%d exists=%d fail=%d, want %d total",
			v.Success, v.Exists, v.Fail, len(recs))
	}
}

// TestAckStatsWorstSlotNamesTheSlowOne checks that ?worst=N points at the slot
// whose watermark wait is worst — the whole point of the view during an
// incident — and that a fast slot on the same node is not picked.
func TestAckStatsWorstSlotNamesTheSlowOne(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	leadSlot0(t, e)
	e.hwWait = 100 * time.Millisecond

	// slot 0 waits on a frozen watermark one record behind its log; slot 1 is
	// never appended to, so it must not show up as a worst slot
	e.replMu.Lock()
	e.repl[0] = &slotRepl{node: []string{"node-2"}, leo: []uint64{1}, lastOK: []time.Time{time.Now()}, hw: 1}
	e.replMu.Unlock()

	aggs := aggsOnSlot(t, e, 0, 3)
	recs := make([]*data.EventRecord, len(aggs))
	for i, a := range aggs {
		recs[i] = makeRecord(a, 1, fmt.Sprintf("ack-worst-%d", i))
	}
	if _, err := e.SubmitBatch(context.Background(), 0, recs); err != nil {
		t.Fatalf("SubmitBatch: %v", err)
	}

	worst := e.AckStatsTop(3).Worst
	if len(worst) != 1 {
		t.Fatalf("worst = %d entries, want only the slot that was written", len(worst))
	}
	if worst[0].Slot != 0 || worst[0].Appends != uint64(len(recs)) {
		t.Fatalf("worst[0] = slot %d appends %d, want slot 0 with %d", worst[0].Slot, worst[0].Appends, len(recs))
	}
	if worst[0].HwLagPeak < 1 {
		t.Fatalf("worst[0] hw_lag_peak = %d, want the lag recorded", worst[0].HwLagPeak)
	}
	if worst[0].LastStallAgeS < 0 {
		t.Fatalf("worst[0] last_stall_age_s = %v, want an age after the stall", worst[0].LastStallAgeS)
	}
	if v := e.AckStatsTop(0); len(v.Worst) != 0 {
		t.Fatalf("AckStatsTop(0) listed %d slots, want none (worst is opt-in)", len(v.Worst))
	}
}

// TestAckStatsCountsRedirects: a slot another node leads answers MOVED, and
// that has to show up as a redirect (the client's latency) rather than as a
// failure in this node's chain.
func TestAckStatsCountsRedirects(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	pinReplicas(t, e, 2)
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// find a slot this node does not lead, and pin its leader elsewhere
	var slot int32 = -1
	for s, p := range e.TableSnapshot().Slots {
		if p.Leader != "" && p.Leader != "node-1" {
			slot = s
			break
		}
	}
	if slot < 0 {
		t.Skip("plan produced no remote-led slot")
	}
	agg := aggsOnSlot(t, e, slot, 1)[0]
	if _, err := e.SubmitAppend(context.Background(), makeRecord(agg, 1, "ack-redir-1")); err == nil {
		t.Fatalf("append on remote-led slot %d was not redirected", slot)
	}

	v := e.AckStats()
	if v.Redirects != 1 {
		t.Fatalf("redirects = %d, want 1", v.Redirects)
	}
	if v.Fail != 0 {
		t.Fatalf("fail = %d, want 0: a redirect is the client's latency, not this node's failure", v.Fail)
	}
	if v.Calls != 1 || v.Appends != 1 {
		t.Fatalf("calls=%d appends=%d, want 1/1", v.Calls, v.Appends)
	}
	// the fence/route step of a redirect is sub-millisecond, so it lands in the
	// histogram's low buckets rather than in max
	if v.Total.P50MS <= 0 || v.FenceRoute.P50MS > v.Total.P99MS {
		t.Fatalf("timings not booked sanely: fence=%+v total=%+v", v.FenceRoute, v.Total)
	}
}

// TestAckStatsFastPathStaysClean: with the watermark already past the record
// there is no stall, no waiters, and the wait step stays small — the view must
// not manufacture a problem out of a healthy append.
func TestAckStatsFastPathStaysClean(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	leadSlot0(t, e)
	e.hwWait = 5 * time.Second

	aggs := aggsOnSlot(t, e, 0, 1)
	if _, err := e.SubmitAppend(context.Background(), makeRecord(aggs[0], 1, "ack-fast-1")); err != nil {
		t.Fatalf("SubmitAppend: %v", err)
	}

	v := e.AckStats()
	if v.HwStalls != 0 {
		t.Fatalf("hw_stalls = %d, want 0 on a healthy append", v.HwStalls)
	}
	if v.WaitersNow != 0 {
		t.Fatalf("waiters_now = %d, want 0", v.WaitersNow)
	}
	if v.Success != 1 || v.Fail != 0 {
		t.Fatalf("success=%d fail=%d, want 1/0", v.Success, v.Fail)
	}
	if v.HwLagRecords.P50MS > 0 && v.HwLagRecords.MaxMS > v.HwLagRecords.P50MS {
		t.Fatalf("hw_lag makes no sense: p50=%v max=%v", v.HwLagRecords.P50MS, v.HwLagRecords.MaxMS)
	}
}

// TestAckStatsSurvivesNoSlots: a store whose slot count is smaller than the id
// a caller asks about must not panic the self-view.
func TestAckStatsSurvivesNoSlots(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	if e.slotAckOf(-1) != nil || e.slotAckOf(int32(len(e.ack.slots))) != nil {
		t.Fatal("slotAckOf must return nil outside the store's slot range")
	}
	e.noteLand(99999, time.Millisecond)
	e.noteWait(99999, time.Millisecond, 3, false)
	if v := e.AckStats(); v.WalLand.P50MS == 0 {
		t.Fatal("out-of-range slot must still feed the node-wide chain")
	}
}
