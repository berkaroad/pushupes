package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"pushupes/internal/data"
)

// leadSlot0 plans the table and pins slot 0 on node-1, then installs a
// replication view with one fresh in-sync replica, so waitForHW evaluates
// sr.hw (a frozen watermark exercises the timeout path; an advancing one
// exercises the fast path).
func leadSlot0(t *testing.T, e *Engine) {
	t.Helper()
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	applyCmd(t, e, &Command{Op: OpPlanSlots})
	if p, _ := e.TableSnapshot().Slots[0]; p.Leader != "node-1" {
		applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{0}, NewLeader: "node-1"})
	}
	if p, _ := e.TableSnapshot().Slots[0]; p.Leader != "node-1" || len(p.Replicas) <= 1 {
		t.Fatalf("setup: slot 0 not local-led with replicas: %+v", p)
	}
}

// TestSubmitBatchMergedHWWait pins the batch wait shape:
//
//   - with the watermark caught up, the whole group lands and every record
//     answers success quickly (the wait costs ONE observation, not N);
//   - with the watermark frozen mid-group, the group pays the deadline ONCE
//     (measured), the records the watermark covers keep their success — their
//     durability promise IS met — and only the records above it answer
//     fail/1005 (still durable in the leader's WAL; a retry converges on
//     exists).
func TestSubmitBatchMergedHWWait(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	leadSlot0(t, e)
	e.hwWait = 150 * time.Millisecond

	// fresh in-sync replica, watermark pinned at seq 2
	e.replMu.Lock()
	e.repl[0] = &slotRepl{node: []string{"node-2"}, leo: []uint64{2}, lastOK: []time.Time{time.Now()}, hw: 2}
	e.replMu.Unlock()

	const n = 4
	aggs := make([]string, n)
	seen := map[string]bool{}
	for i := 0; i < 200000 && nAggs(aggs, seen) < n; i++ {
		cand := fmt.Sprintf("mb-%d", i)
		if e.SlotOf(cand) == 0 && !seen[cand] {
			for k := range aggs {
				if aggs[k] == "" {
					aggs[k] = cand
					seen[cand] = true
					break
				}
			}
		}
	}
	if nAggs(aggs, seen) < n {
		t.Fatal("no slot-0 aggregates found")
	}
	recs := make([]*data.EventRecord, n)
	for i := 0; i < n; i++ {
		recs[i] = makeRecord(aggs[i], 1, fmt.Sprintf("mb-cmd-%d", i))
	}
	t0 := time.Now()
	resps, err := e.SubmitBatch(context.Background(), 0, recs)
	elapsed := time.Since(t0)
	if err != nil {
		t.Fatal(err)
	}
	if len(resps) != n {
		t.Fatalf("responses: %d want %d", len(resps), n)
	}
	// seqs landed in request order
	for i, r := range resps {
		if r.Seq != uint64(i+1) {
			t.Fatalf("resp %d seq %d (WAL order must equal request order)", i, r.Seq)
		}
	}
	// the watermark (2) covers seq 1..2: those keep success
	for i := 0; i < 2; i++ {
		if resps[i].Status != data.StatusSuccess {
			t.Fatalf("resp %d must stay success under the covered watermark: %+v", i, resps[i])
		}
	}
	// seq 3..4 sit above the frozen watermark: fail/1005, durable in the WAL
	for i := 2; i < n; i++ {
		if resps[i].Status != data.StatusFail || resps[i].ErrID != data.ErrIDNotLeader {
			t.Fatalf("resp %d must fail 1005 above the watermark: %+v", i, resps[i])
		}
	}
	if leo := st.LastSeqOf(0); leo != n {
		t.Fatalf("all %d records must be durable in the leader WAL, LEO %d", n, leo)
	}
	// ONE deadline for the group, not N: 4 records x 150ms serial would be
	// ~600ms; the merged wait must finish near a single 150ms.
	if elapsed < e.hwWait || elapsed > 3*e.hwWait {
		t.Fatalf("group wait took %s; want ~one deadline of %s (merged, not per-record)", elapsed, e.hwWait)
	}

	// retry converges on exists for the 1005 records (they are durable)
	retry, err := e.SubmitBatch(context.Background(), 0, []*data.EventRecord{recs[3]})
	if err != nil {
		t.Fatal(err)
	}
	if retry[0].Status != data.StatusExists {
		t.Fatalf("retry of a durable record must answer exists: %+v", retry[0])
	}
}

// TestSubmitBatchHWWaitFastPath: when the watermark already covers the group
// the merged wait must return immediately.
func TestSubmitBatchHWWaitFastPath(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	leadSlot0(t, e)
	e.hwWait = 10 * time.Second // would stall the test if the fast path broke

	e.replMu.Lock()
	e.repl[0] = &slotRepl{node: []string{"node-2"}, leo: []uint64{100}, lastOK: []time.Time{time.Now()}, hw: 100}
	e.replMu.Unlock()

	recs := []*data.EventRecord{
		makeRecord("mf-agg-0", 1, "mf-cmd-0"),
		makeRecord("mf-agg-1", 1, "mf-cmd-1"),
		makeRecord("mf-agg-1", 1, "mf-cmd-1"), // duplicate aggregate is the CALLER's rule (grpcapi rejects); engine still idempotent-answers exists
	}
	t0 := time.Now()
	resps, err := e.SubmitBatch(context.Background(), 0, recs)
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(t0); d > time.Second {
		t.Fatalf("covered watermark must not pay the deadline: %s", d)
	}
	if resps[0].Status != data.StatusSuccess || resps[1].Status != data.StatusSuccess {
		t.Fatalf("first two must succeed: %+v %+v", resps[0], resps[1])
	}
	if resps[2].Status != data.StatusExists {
		t.Fatalf("replayed command in group must answer exists: %+v", resps[2])
	}
}

// TestSubmitBatchRedirectIsSlotLevel: a batch group routed to another node
// answers the slot-level redirect (the caller surfaces it per record).
func TestSubmitBatchRedirectIsSlotLevel(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	leadSlot0(t, e)
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{0}, NewLeader: "node-2"})

	_, err := e.SubmitBatch(context.Background(), 0, []*data.EventRecord{makeRecord("mr-agg-0", 1, "mr-cmd-0")})
	var redir *RedirectError
	if !asRedirect(err, &redir) || redir.Kind != data.ErrIDSlotNotLocal {
		t.Fatalf("want MOVED for the group, got %v", err)
	}
}

func asRedirect(err error, target **RedirectError) bool {
	if r, ok := err.(*RedirectError); ok {
		*target = r
		return true
	}
	return false
}

func nAggs(aggs []string, seen map[string]bool) int {
	c := 0
	for _, a := range aggs {
		if a != "" {
			c++
		}
	}
	return c
}
