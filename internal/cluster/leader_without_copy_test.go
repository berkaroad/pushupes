package cluster

// A leader whose local copy of the slot is ABSENT must not answer the client's
// version. Its directory starts empty, the CAS reports "current version 0", and
// a client that adopts that turns a slot with history into a fresh version-1
// stream — the dg22 storm (hundreds of 1001s, a copy crawling 0→6 frames while
// the real chain sat at ~550). The line is epoch >= 2: an epoch-1 slot is one
// the plan just created and legitimately starts empty.

import (
	"context"
	"testing"

	"github.com/berkaroad/pushupes/internal/data"
)

// leaderWithoutCopy moves a slot's leadership away and back so the table names
// this node the leader at epoch >= 2 while the store holds no copy of it.
func leaderWithoutCopy(t *testing.T, e *Engine, slot int32, peer string) {
	t.Helper()
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{slot}, NewLeader: peer})
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{slot}, NewLeader: e.self})
	p, ok := e.slotPlacement(slot)
	if !ok || p.Leader != e.self || p.Epoch < 2 {
		t.Fatalf("fixture: want this node leading at epoch>=2, got %+v", p)
	}
	if e.store.SlotPresent(slot) {
		t.Fatalf("fixture: slot %d must have no local copy", slot)
	}
}

func TestLeaderWithoutCopyAnswersRetryable(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:0")
	join(t, e, "node-2", "127.0.0.1:1")
	applyCmd(t, e, &Command{Op: OpReplanSlots})
	const agg = "agg-a"
	slot := st.SlotOf(agg)
	leaderWithoutCopy(t, e, slot, "node-2")

	rec := &data.EventRecord{
		AggregateID: agg, Version: 5, CommandID: "cmd-5",
		Events: []data.Event{{Type: "E", Body: []byte("x")}},
	}
	resp, err := e.SubmitAppend(context.Background(), rec)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if resp == nil || resp.Status != data.StatusFail {
		t.Fatalf("want a fail answer (retryable), got %+v", resp)
	}
	if resp.ErrID != data.ErrIDNotLeader {
		t.Fatalf("want NOT_LEADER (retryable, keeps the client's ledger), got errID=%d", resp.ErrID)
	}
	if resp.CurrentVersion != 0 {
		t.Fatalf("a refused answer must not carry a version the client could adopt: %d", resp.CurrentVersion)
	}

	// Once the copy exists the very same append is served normally: the guard
	// is about a MISSING copy, not about the slot's history.
	if _, err := st.Append(&data.EventRecord{
		AggregateID: agg, Version: 1, CommandID: "cmd-1",
		Events: []data.Event{{Type: "E", Body: []byte("x")}},
	}); err != nil {
		t.Fatalf("seed copy: %v", err)
	}
	// The copy holds version 1 now, so version 2 is the next link and must be
	// accepted: the leader really is serving this slot again.
	next := &data.EventRecord{
		AggregateID: agg, Version: 2, CommandID: "cmd-2",
		Events: []data.Event{{Type: "E", Body: []byte("x")}},
	}
	resp, err = e.SubmitAppend(context.Background(), next)
	if err != nil {
		t.Fatalf("submit with copy: %v", err)
	}
	if resp == nil || resp.Status == data.StatusFail {
		t.Fatalf("a leader holding the copy must serve: %+v", resp)
	}
}
