package cluster

// The empty-shell sibling of TestLeaderWithoutCopyAnswersRetryable (live
// cluster, Oct 24): a copy that was dropped or discarded while this node was
// the former migration source, then re-granted as a fresh empty seat (dir +
// header-only WAL, LEO 0). A floorless handover (offline-source leader_move /
// failover pin) then names THIS node leader at epoch >= 2 while an in-set seat
// still reports real history. SlotPresent is TRUE here, so the
// missing-copy guard never fires, and the empty directory answers "current
// version 0" to every client — the same adopt-and-rewrite storm the missing
// copy guard exists to refuse. The refusal therefore requires:
//   - the local copy reads empty (present, LEO 0) at epoch >= 2 under self,
//   - a seat IN the current replica set has reported LEO > 0 (leader-side
//     progress ledger — zero new RPC), and
//   - the reading confirmed across the double-read window (a copy mid-rebuild
//     briefly reads empty: confirmDiverged's lesson).
// Both write shapes must refuse: single SubmitAppend and BatchAppend share
// landLocalAppend, the guard sits there.

import (
	"context"
	"testing"
	"time"

	"github.com/berkaroad/pushupes/internal/data"
)

// reportSeat injects a follower progress report into the leader-side ledger.

func TestEmptyShellLeaderAnswersRetryable(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:0")
	join(t, e, "node-2", "127.0.0.1:1")
	applyCmd(t, e, &Command{Op: OpReplanSlots})
	const agg = "agg-a"
	slot := st.SlotOf(agg)

	// Open the slot so the dir + header-only WAL exist (SlotPresent true)
	// while it holds zero records.
	if _, err := st.Slot(slot); err != nil {
		t.Fatal(err)
	}
	if !e.store.SlotPresent(slot) || e.store.LastSeqOf(slot) != 0 {
		t.Fatalf("fixture: want present-empty copy, got present=%v leo=%d",
			e.store.SlotPresent(slot), e.store.LastSeqOf(slot))
	}
	// Handover history: away and back, so epoch >= 2 and this node leads.
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{slot}, NewLeader: "node-2"})
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{slot}, NewLeader: e.self})
	p, _ := e.slotPlacement(slot)
	if p.Leader != e.self || p.Epoch < 2 {
		t.Fatalf("fixture: want self leader at epoch>=2, got %+v", p)
	}

	// The seat's report: node-2 holds history this leader never saw.
	reportSeat := func(leo uint64) {
		prog := e.progressPool.Get().(*progressBatch)
		prog.follower, prog.stamp, prog.now = "node-2", true, time.Now()
		prog.add(slot, leo)
		prog.apply(e)
		prog.reset()
		e.progressPool.Put(prog)
	}

	rec := &data.EventRecord{
		AggregateID: agg, Version: 5, CommandID: "cmd-5",
		Events: []data.Event{{Type: "E", Body: []byte("x")}},
	}
	wantRefused := func(t *testing.T, where string, resp *data.AppendResponse) {
		t.Helper()
		if resp == nil || resp.Status != data.StatusFail {
			t.Fatalf("%s: want a fail answer, got %+v", where, resp)
		}
		if resp.ErrID != data.ErrIDReplicaCatchingUp {
			t.Fatalf("%s: want 1008 replica-catching-up (retryable, keeps the client's ledger), got errID=%d", where, resp.ErrID)
		}
		if resp.CurrentVersion != 0 {
			t.Fatalf("%s: refusal must not carry an adoptable version: %d", where, resp.CurrentVersion)
		}
	}

	e.emptyLeaderConfirm = 0
	reportSeat(57)

	// First sighting records, second refuses — the double-read discipline
	// (a re-granted empty seat whose ledger still holds the dead copy's old
	// reports must not be judged on a single read).
	resp, err := e.SubmitAppend(context.Background(), rec)
	if err != nil {
		t.Fatal(err)
	}
	if resp != nil && resp.Status == data.StatusFail && resp.ErrID == data.ErrIDReplicaCatchingUp {
		t.Fatalf("the first sighting alone must not refuse: %+v", resp)
	}
	resp, err = e.SubmitAppend(context.Background(), rec)
	if err != nil {
		t.Fatal(err)
	}
	wantRefused(t, "single append", resp)

	outs, err := e.SubmitBatch(context.Background(), slot, []*data.EventRecord{rec})
	if err != nil {
		t.Fatal(err)
	}
	if len(outs) != 1 {
		t.Fatalf("want one answer per record, got %d", len(outs))
	}
	wantRefused(t, "batch append", outs[0])

	// Without seat history the empty copy is legitimate (a slot the layout
	// just (re)granted): clearing the evidence must let the leader serve
	// again — the guard refuses a CONTESTED empty, not emptiness itself.
	e.replMu.Lock()
	sr := e.repl[slot]
	for i := range sr.node {
		sr.leo[i] = 0
	}
	e.replMu.Unlock()
	if _, err := e.store.AppendRouted(&data.EventRecord{
		AggregateID: agg, Version: 1, CommandID: "cmd-1",
		Events: []data.Event{{Type: "E", Body: []byte("x")}},
	}); err != nil {
		t.Fatalf("seed v1 after evidence clears: %v", err)
	}
	resp, err = e.SubmitAppend(context.Background(), &data.EventRecord{
		AggregateID: agg, Version: 2, CommandID: "cmd-2",
		Events: []data.Event{{Type: "E", Body: []byte("x")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.Status == data.StatusFail {
		t.Fatalf("an un-contested empty leader must serve: %+v", resp)
	}
}

// TestEmptyShellLeaderWaitsForTheConfirmWindow mirrors
// TestDivergedConfirmWaitsForTheWindow: one sighting is an instant, not a
// verdict — a copy mid-rebuild can briefly read empty while its seats report
// old history.
func TestEmptyShellLeaderWaitsForTheConfirmWindow(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:0")
	join(t, e, "node-2", "127.0.0.1:1")
	applyCmd(t, e, &Command{Op: OpReplanSlots})
	const agg = "agg-w"
	slot := st.SlotOf(agg)
	if _, err := st.Slot(slot); err != nil {
		t.Fatal(err)
	}
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{slot}, NewLeader: "node-2"})
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{slot}, NewLeader: e.self})

	prog := e.progressPool.Get().(*progressBatch)
	prog.follower, prog.stamp, prog.now = "node-2", true, time.Now()
	prog.add(slot, 40)
	prog.apply(e)
	prog.reset()
	e.progressPool.Put(prog)

	e.emptyLeaderConfirm = time.Hour // far away: nothing may be refused yet
	rec := &data.EventRecord{
		AggregateID: agg, Version: 5, CommandID: "cmd-5",
		Events: []data.Event{{Type: "E", Body: []byte("x")}},
	}
	resp, err := e.SubmitAppend(context.Background(), rec)
	if err != nil {
		t.Fatal(err)
	}
	if resp != nil && resp.Status == data.StatusFail && resp.ErrID == data.ErrIDReplicaCatchingUp {
		t.Fatalf("the first sighting alone must not refuse (window still open): %+v", resp)
	}
	// Age the sighting past the window: the second reading refuses.
	e.rebMu.Lock()
	if seen, ok := e.emptySeen[slot]; ok {
		e.emptySeen[slot] = seen.Add(-2 * time.Hour)
	} else {
		e.rebMu.Unlock()
		t.Fatal("first sighting was not recorded")
	}
	e.rebMu.Unlock()
	resp, err = e.SubmitAppend(context.Background(), rec)
	if err != nil {
		t.Fatal(err)
	}
	if resp == nil || resp.Status != data.StatusFail || resp.ErrID != data.ErrIDReplicaCatchingUp {
		t.Fatalf("confirmed empty-shell leader must refuse retryably: %+v", resp)
	}
}
