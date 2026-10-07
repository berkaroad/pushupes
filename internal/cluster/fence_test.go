package cluster

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"pushupes/internal/data"
)

// TestWriteFenceBlocksAppendsAndDoesNotFailThem pins the core fence contract:
// while the migration commit fence is held on the source, an append to that
// slot BLOCKS (it is never rejected) and nothing lands on the WAL; once the
// leader move is applied the fence releases and the blocked append is
// redirected to the new leader. This is the window that used to let the source
// accept a record the target never received — the same-seq-different-record
// fork the fence exists to close.
func TestWriteFenceBlocksAppendsAndDoesNotFailThem(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	applyCmd(t, e, &Command{Op: OpPlanSlots}) // single node: node-1 leads every slot
	join(t, e, "node-2", "127.0.0.1:2")       // a registered move target (no replan runs)

	agg := aggInSlot(t, e, 0)
	other := aggInSlot(t, e, 3) // a different slot: must stay writable
	if _, err := st.Append(makeRecord(agg, 1, "w-1")); err != nil {
		t.Fatal(err)
	}

	if err := e.acquireSlotFence(context.Background(), 0); err != nil {
		t.Fatalf("acquire fence: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := e.SubmitAppend(context.Background(), makeRecord(agg, 2, "w-2"))
		done <- err
	}()

	select {
	case <-done:
		t.Fatal("an append completed while the slot was fenced: nothing may land after the frozen LEO")
	case <-time.After(150 * time.Millisecond):
	}
	if leo := st.LastSeqOf(0); leo != 1 {
		t.Fatalf("a fenced append reached the WAL: LEO %d want 1", leo)
	}

	// Other slots are not fenced: the fence is per slot.
	if out, err := e.SubmitAppend(context.Background(), makeRecord(other, 1, "o-1")); err != nil || out.Status != data.StatusSuccess {
		t.Fatalf("an unrelated slot must stay writable under a fence: %+v %v", out, err)
	}

	// Simulate the commit: the leader move makes node-1 no longer lead the
	// slot, which releases the fence (see syncMigrationState) and wakes the
	// blocked append, which must now redirect rather than write locally.
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{0}, NewLeader: "node-2"})

	select {
	case err := <-done:
		if _, ok := err.(*RedirectError); !ok {
			t.Fatalf("a blocked append must redirect to the new leader after the fence releases, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a fenced append never woke after the fence was released")
	}
	if leo := st.LastSeqOf(0); leo != 1 {
		t.Fatalf("no write may land on the former source after the move: LEO %d want 1", leo)
	}
}

// TestAcquireFenceDrainsInFlightAppend pins the drain half of the fence: taking
// the fence must wait for appends already inside their critical section, so the
// LEO the caller samples after acquire has no in-flight write behind it.
func TestAcquireFenceDrainsInFlightAppend(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	leave := e.enterWriteFence(0) // an in-flight append has registered
	acquired := make(chan error, 1)
	go func() { acquired <- e.acquireSlotFence(context.Background(), 0) }()

	select {
	case <-acquired:
		t.Fatal("the fence must wait for an in-flight append to drain, not sample a moving LEO")
	case <-time.After(150 * time.Millisecond):
	}

	// Once the fence is closing, a NEW append must block on it.
	blocked := make(chan struct{})
	go func() { l := e.enterWriteFence(0); l(); close(blocked) }()
	select {
	case <-blocked:
		t.Fatal("a new append must block while the fence is held")
	case <-time.After(100 * time.Millisecond):
	}

	leave() // the in-flight append finishes: the drain can complete
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("acquire after drain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the fence never drained after the in-flight append left")
	}

	e.releaseSlotFence(0)
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("a blocked append never woke after the fence was released")
	}
}

// TestHandleFenceSlotPushesFrozenTailAndHoldsFence is the end-to-end fence test
// over the real peer plane: the source freezes the slot, ships the tail the
// target was missing over the existing Replicate path, answers only once the
// target confirms the frozen LEO, keeps holding the fence, and releases it when
// the leader move is applied.
func TestHandleFenceSlotPushesFrozenTailAndHoldsFence(t *testing.T) {
	src, srcStore := newTestEngine(t, "node-1")
	tgt, tgtStore := newTestEngine(t, "node-2")
	tgtAddr := newPeerHarness(t, tgt)

	join(t, src, "node-1", "127.0.0.1:1")
	join(t, src, "node-2", tgtAddr)
	applyCmd(t, src, &Command{Op: OpPlanSlots})

	agg := aggInSlot(t, src, 0)
	for v := uint64(1); v <= 3; v++ {
		if out, err := srcStore.Append(makeRecord(agg, uint32(v), fmt.Sprintf("ft-%d", v))); err != nil || out.Status != data.StatusSuccess {
			t.Fatalf("seed v%d: %+v %v", v, out, err)
		}
	}
	// stage the migration so migrationTargetOf resolves to node-2
	applyCmd(t, src, &Command{Op: OpSlotState, Slots: []int32{0}, State: SlotMigratingOut, MigratingTo: "node-2"})
	src.syncMigrationState()

	if leo := tgtStore.LastSeqOf(0); leo != 0 {
		t.Fatalf("test setup: target must start empty, LEO %d", leo)
	}

	leo, err := src.HandleFenceSlot(context.Background(), 0)
	if err != nil {
		t.Fatalf("fence: %v", err)
	}
	if leo != 3 {
		t.Fatalf("frozen LEO %d want 3", leo)
	}
	// The tail really moved: the target confirmed the frozen LEO.
	if got := tgtStore.LastSeqOf(0); got != 3 {
		t.Fatalf("target LEO %d want 3 (the frozen tail must have been pushed)", got)
	}
	_, _, want, err := srcStore.ReadSlotBytes(0, 1, 4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	_, _, got, err := tgtStore.ReadSlotBytes(0, 1, 4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("target bytes differ from the source's frozen tail")
	}
	// The fence stays held after a successful call: the move has not committed.
	done := make(chan error, 1)
	go func() {
		_, err := src.SubmitAppend(context.Background(), makeRecord(agg, 4, "ft-4"))
		done <- err
	}()
	select {
	case <-done:
		t.Fatal("the fence must stay held until the move commits")
	case <-time.After(150 * time.Millisecond):
	}

	// The commit releases it; the blocked append redirects to the new leader.
	applyCmd(t, src, &Command{Op: OpLeaderMove, Slots: []int32{0}, NewLeader: "node-2"})
	select {
	case err := <-done:
		if _, ok := err.(*RedirectError); !ok {
			t.Fatalf("blocked append must redirect after commit, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the fence was not released by the leader move")
	}
}

// TestHandleFenceSlotAbortsOnUnreachableTarget pins the clean-abort half: a
// target that cannot be reached (or confirmed) must make the fence fail without
// leaving it held — the source keeps leading and the migration controller rolls
// the slot back to stable.
func TestHandleFenceSlotAbortsOnUnreachableTarget(t *testing.T) {
	src, srcStore := newTestEngine(t, "node-1")
	join(t, src, "node-1", "127.0.0.1:1")
	join(t, src, "node-2", "127.0.0.1:9") // nothing listening
	applyCmd(t, src, &Command{Op: OpPlanSlots})

	agg := aggInSlot(t, src, 0)
	if _, err := srcStore.Append(makeRecord(agg, 1, "ab-1")); err != nil {
		t.Fatal(err)
	}
	applyCmd(t, src, &Command{Op: OpSlotState, Slots: []int32{0}, State: SlotMigratingOut, MigratingTo: "node-2"})
	src.syncMigrationState()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := src.HandleFenceSlot(ctx, 0); err == nil {
		t.Fatal("an unreachable target must fail the fence, not report success")
	}
	// The failed fence must not stay held: writes must keep flowing.
	if out, err := src.SubmitAppend(context.Background(), makeRecord(agg, 2, "ab-2")); err != nil || out.Status != data.StatusSuccess {
		t.Fatalf("a failed fence must not wedge writes: %+v %v", out, err)
	}
}

// TestApplyFetchPayloadSkipsForkedSeq pins the new per-record semantics: a seq
// the local log already holds with different bytes is skipped (and reported),
// while the records after it still land.
func TestApplyFetchPayloadSkipsForkedSeq(t *testing.T) {
	leader, ldr := newTestEngine(t, "node-1")
	_, fdr := newTestEngine(t, "node-2")

	agg := aggInSlot(t, leader, 0)
	for v := uint64(1); v <= 3; v++ {
		if _, err := ldr.Append(makeRecord(agg, uint32(v), fmt.Sprintf("sp-%d", v))); err != nil {
			t.Fatal(err)
		}
	}
	_, _, payload, err := ldr.ReadSlotBytes(0, 1, 4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	frames := splitFrames(t, payload)

	// The follower holds the leader's seq 1 but a DIFFERENT record at seq 2.
	if err := fdr.AppendFrameAtSeq(0, 1, frames[0]); err != nil {
		t.Fatal(err)
	}
	if err := fdr.AppendAtSeq(0, 2, makeRecord(agg, 99, "local-fork")); err != nil {
		t.Fatal(err)
	}

	diverged, err := applyFetchPayload(fdr, 0, 4, payload)
	if err != nil {
		t.Fatalf("a fork must not fail the batch: %v", err)
	}
	if len(diverged) != 1 || diverged[0] != 2 {
		t.Fatalf("diverged seqs %v want [2]", diverged)
	}
	if leo := fdr.LastSeqOf(0); leo != 3 {
		t.Fatalf("records after the fork must still land: LEO %d want 3", leo)
	}
}

// TestApplyFetchItemsQuarantinesOneSlotKeepsSession is the session-level
// regression test: one slot's fork must not fail the whole multiplexed round.
// The forked slot is quarantined (and stops being reported as in-sync) while
// every other slot of the same round is still replicated.
func TestApplyFetchItemsQuarantinesOneSlotKeepsSession(t *testing.T) {
	leader, ldr := newTestEngine(t, "node-1")
	join(t, leader, "node-1", "127.0.0.1:1")
	applyCmd(t, leader, &Command{Op: OpPlanSlots})

	agg0, agg1 := aggInSlot(t, leader, 0), aggInSlot(t, leader, 1)
	for v := uint64(1); v <= 3; v++ {
		if _, err := ldr.Append(makeRecord(agg0, uint32(v), fmt.Sprintf("q0-%d", v))); err != nil {
			t.Fatal(err)
		}
		if _, err := ldr.Append(makeRecord(agg1, uint32(v), fmt.Sprintf("q1-%d", v))); err != nil {
			t.Fatal(err)
		}
	}
	_, _, pay0, err := ldr.ReadSlotBytes(0, 1, 4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	_, _, pay1, err := ldr.ReadSlotBytes(1, 1, 4, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	frames0 := splitFrames(t, pay0)

	follower, fdr := newTestEngine(t, "node-2")
	join(t, follower, "node-1", "127.0.0.1:1")
	applyCmd(t, follower, &Command{Op: OpPlanSlots})

	// fork slot 0 at seq 2; leave slot 1 empty.
	if err := fdr.AppendFrameAtSeq(0, 1, frames0[0]); err != nil {
		t.Fatal(err)
	}
	if err := fdr.AppendAtSeq(0, 2, makeRecord(agg0, 99, "local-fork")); err != nil {
		t.Fatal(err)
	}

	items := []FetchItem{
		{Slot: 0, FromSeq: 1, NextSeq: 4, Payload: pay0},
		{Slot: 1, FromSeq: 1, NextSeq: 4, Payload: pay1},
	}
	productive, err := follower.applyFetchItems(items, nil)
	if err != nil {
		t.Fatalf("one slot's fork must not fail the round: %v", err)
	}
	if !productive {
		t.Fatal("the round applied nothing")
	}
	if !follower.isDiverged(0) {
		t.Fatal("the forked slot must be quarantined")
	}
	if follower.isDiverged(1) {
		t.Fatal("an unrelated slot must not be quarantined")
	}
	if leo := fdr.LastSeqOf(1); leo != 3 {
		t.Fatalf("the unrelated slot must still replicate: LEO %d want 3", leo)
	}
	if leo := fdr.LastSeqOf(0); leo != 3 {
		t.Fatalf("the forked slot must still land the records after the fork: LEO %d want 3", leo)
	}
}

// TestTableChangeKicksReplicaSessions pins the follow-up to the fence: a leader
// move must make the followers re-group their fetch sessions at once, not on
// the next 1s tick. Until they do, the new leader has no replica progress report
// and its watermark is stuck at 0 — a full-second stall of every append
// right after each migration.
func TestTableChangeKicksReplicaSessions(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	select { // drain anything queued at construction
	case <-e.sessKick:
	default:
	}
	join(t, e, "node-1", "127.0.0.1:1") // a table change
	select {
	case <-e.sessKick:
	case <-time.After(time.Second):
		t.Fatal("a table change must kick the replica session loop instead of waiting for the 1s tick")
	}
}

// TestFenceReleaseWaitsForTargetToApplyMove pins the commit-fence gate. After
// the leader move is applied on the SOURCE, the fence must stay held (writers
// blocked, never failed) until the migration TARGET's own table names the
// target as the slot's leader — otherwise a client is bounced between a source
// that has applied the move and a target that has not (the MOVED/ASK ping-pong
// that made clients with a small redirect budget fail). Only once the target
// applies it may the blocked writer wake and redirect.
func TestFenceReleaseWaitsForTargetToApplyMove(t *testing.T) {
	src, _ := newTestEngine(t, "node-1")
	tgt, _ := newTestEngine(t, "node-2")
	tgtAddr := newPeerHarness(t, tgt)

	// src's view: node-1 leads every slot (single node at plan time); node-2 is
	// a registered migration target.
	join(t, src, "node-1", "127.0.0.1:1")
	applyCmd(t, src, &Command{Op: OpPlanSlots})
	join(t, src, "node-2", tgtAddr)

	// tgt's own view: same peers, but slot 0 explicitly led by node-1, so the
	// gate has a definite "not yet moved" state to observe.
	join(t, tgt, "node-1", "127.0.0.1:1")
	join(t, tgt, "node-2", tgtAddr)
	applyCmd(t, tgt, &Command{Op: OpPlanSlots})
	applyCmd(t, tgt, &Command{Op: OpLeaderMove, Slots: []int32{0}, NewLeader: "node-1"})

	agg := aggInSlot(t, src, 0)

	// Hold the fence exactly as HandleFenceSlot does for a live migration.
	applyCmd(t, src, &Command{Op: OpSlotState, Slots: []int32{0}, State: SlotMigratingOut, MigratingTo: "node-2"})
	src.syncMigrationState()
	if err := src.acquireSlotFence(context.Background(), 0); err != nil {
		t.Fatalf("acquire fence: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := src.SubmitAppend(context.Background(), makeRecord(agg, 1, "gate-1"))
		done <- err
	}()

	// Commit on the source only: the move lands here, but NOT yet on the target.
	// The fence must keep holding the writer instead of releasing into a
	// redirect race.
	applyCmd(t, src, &Command{Op: OpLeaderMove, Slots: []int32{0}, NewLeader: "node-2"})
	select {
	case <-done:
		t.Fatal("the fence released before the target applied the leader move: a client would bounce between the two nodes")
	case <-time.After(400 * time.Millisecond):
	}

	// The target applies the move: the gate observes it and releases; the
	// blocked writer wakes and redirects to the target.
	applyCmd(t, tgt, &Command{Op: OpLeaderMove, Slots: []int32{0}, NewLeader: "node-2"})
	select {
	case err := <-done:
		if _, ok := err.(*RedirectError); !ok {
			t.Fatalf("the blocked writer must redirect to the new leader once the target applied the move, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the fence never released after the target applied the leader move")
	}
}

// splitFrames splits a concatenated record payload into its frames.
func splitFrames(t *testing.T, payload []byte) [][]byte {
	t.Helper()
	var out [][]byte
	rest := payload
	for len(rest) > 0 {
		_, consumed, err := data.DecodeRecordMeta(rest)
		if err != nil {
			t.Fatalf("decode frame: %v", err)
		}
		out = append(out, rest[:consumed])
		rest = rest[consumed:]
	}
	return out
}
