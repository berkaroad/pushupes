package cluster

import (
	"os"
	"testing"
	"time"

	"pushupes/internal/storage"
)

// TestDropRetentionDefaultAndResolution pins the knob's contract: the default
// window is the documented 30s, a positive value is used as given, a configured
// 0 means "the default" (a Go flag's zero value is "not set"), a negative
// window is rejected — and the cleanup cannot be turned off at all, because a
// surplus copy left on disk forever is how a node fills up.
func TestDropRetentionDefaultAndResolution(t *testing.T) {
	if DefaultDropRetention != 30*time.Second {
		t.Fatalf("DefaultDropRetention = %s, want 30s", DefaultDropRetention)
	}
	e, _ := newTestEngine(t, "node-1")
	if got := e.DropRetention(); got != DefaultDropRetention {
		t.Fatalf("engine default retention = %s, want %s", got, DefaultDropRetention)
	}
	cases := []struct {
		in   time.Duration
		want time.Duration
	}{
		{0, DefaultDropRetention},
		{time.Second, time.Second},
		{2 * time.Minute, 2 * time.Minute},
		{time.Hour, time.Hour},
	}
	for _, c := range cases {
		got, err := ResolveDropRetention(c.in)
		if err != nil || got != c.want {
			t.Fatalf("ResolveDropRetention(%s) = (%s, %v), want (%s, nil)", c.in, got, err, c.want)
		}
	}
	for _, bad := range []time.Duration{-time.Nanosecond, -time.Second, -time.Hour} {
		if got, err := ResolveDropRetention(bad); err == nil {
			t.Fatalf("ResolveDropRetention(%s) accepted a negative retention (got %s)", bad, got)
		}
	}
	// The engine never ends up without a usable window, whatever it is handed.
	e.SetDropRetention(0)
	if got := e.DropRetention(); got != DefaultDropRetention {
		t.Fatalf("SetDropRetention(0) = %s, want the default %s", got, DefaultDropRetention)
	}
	e.SetDropRetention(2 * time.Minute)
	if got := e.DropRetention(); got != 2*time.Minute {
		t.Fatalf("SetDropRetention(2m) did not stick: %s", got)
	}
}

// dropSetup builds an engine for `self` with three joined peers and every slot
// planned (factor 2), so slot 0 is led by node-1 with node-2 as its replica and
// slot 1 is led by node-2 with node-3 as its replica.
func dropSetup(t *testing.T, self string) (*Engine, *storage.Store) {
	t.Helper()
	e, st := newTestEngine(t, self)
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})
	return e, st
}

// fillSlot gives slot a real local copy and returns its directory.
func fillSlot(t *testing.T, e *Engine, st *storage.Store, slot int32) string {
	t.Helper()
	appendOne(t, st, aggInSlot(t, e, slot), "drop-1")
	dir := st.SlotDir(slot)
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("test setup: no local copy of slot %d at %s: %v", slot, dir, err)
	}
	return dir
}

func dirGone(dir string) bool {
	_, err := os.Stat(dir)
	return os.IsNotExist(err)
}

// TestOutOfSetHandoverArmsOnlyWhenTheSourceLeavesTheSet walks the state a real
// migration to an out-of-set target produces on the FORMER SOURCE: the target
// is admitted, the leader moves, and only the post-commit reclaim takes this
// node out of the replica set. Nothing may be queued (and nothing dropped)
// before that reclaim, because until then this node is still part of the
// replication factor.
func TestOutOfSetHandoverArmsOnlyWhenTheSourceLeavesTheSet(t *testing.T) {
	e, st := dropSetup(t, "node-2")
	const rt = 150 * time.Millisecond
	e.SetDropRetention(rt)
	dir := fillSlot(t, e, st, 0) // node-2 replicates slot 0 (led by node-1)

	// the migration: admit node-3, stage, commit the leader move
	applyCmd(t, e, &Command{Op: OpSlotAddReplica, Slots: []int32{0}, NodeID: "node-3"})
	applyCmd(t, e, &Command{Op: OpSlotState, Slots: []int32{0}, State: SlotMigratingOut, MigratingTo: "node-3"})
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{0}, NewLeader: "node-3"})

	if _, ok := e.PendingDrop(0); ok {
		t.Fatal("a node that is still a replica of the slot must not be queued for cleanup")
	}
	time.Sleep(3 * rt)
	if dirGone(dir) {
		t.Fatalf("the copy was dropped before this node left the replica set: %s", dir)
	}

	// the reclaim: the former source leaves the set — now the copy is surplus
	applyCmd(t, e, &Command{Op: OpSlotRemoveReplica, Slots: []int32{0}, NodeID: "node-2"})
	deadline, ok := e.PendingDrop(0)
	if !ok {
		t.Fatal("leaving the replica set must queue the surplus copy for cleanup")
	}
	if left := time.Until(deadline); left <= 0 || left > rt {
		t.Fatalf("queued deadline %s is not the configured window %s", left, rt)
	}
	if dirGone(dir) {
		t.Fatal("the copy was dropped before the retention window elapsed")
	}
	if got := e.PendingDrops()[0]; got == 0 {
		t.Fatal("the per-slot admin array must carry the deadline")
	}

	waitFor(t, 3*time.Second, func() bool { return dirGone(dir) }, "the surplus copy to be dropped")
	if _, ok := e.PendingDrop(0); ok {
		t.Fatal("the schedule must be cleared once the drop ran")
	}
}

// TestInSetHandoverNeverQueuesNorDropsAMemberCopy is the counter-case the
// console exposed: a migration between two members of the SAME replica set
// removes nobody, so the former source is still part of the replication factor
// and its copy must never be queued (no marker, no greyed tag) nor dropped —
// not at the retention deadline, not ever.
func TestInSetHandoverNeverQueuesNorDropsAMemberCopy(t *testing.T) {
	e, st := dropSetup(t, "node-2")
	const rt = 120 * time.Millisecond
	e.SetDropRetention(rt)
	dir := fillSlot(t, e, st, 1) // node-2 leads slot 1, with node-3 as its replica

	// an in-set hand-over: the target is already a replica, no admission and
	// no reclaim happen; the leader simply moves to node-3.
	applyCmd(t, e, &Command{Op: OpSlotState, Slots: []int32{1}, State: SlotMigratingOut, MigratingTo: "node-3"})
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{1}, NewLeader: "node-3"})

	if _, ok := e.PendingDrop(1); ok {
		t.Fatal("an in-set hand-over must not queue the former source's copy: it is still a replica")
	}
	if got := e.PendingDrops()[1]; got != 0 {
		t.Fatalf("the admin array must not publish a drop for a member's copy, got %d", got)
	}
	time.Sleep(5 * rt)
	if dirGone(dir) {
		t.Fatalf("an in-set hand-over dropped a copy that is still part of the replica set: %s", dir)
	}
	if p := e.TableSnapshot().Slots[1]; !replicaSetHas(p.Replicas, "node-2") {
		t.Fatalf("test premise broken: node-2 left the set (%+v)", p)
	}
}

// TestRepeatedHandoversRearmAndNeverDeleteEarly is the regression for the
// reported "delayed deletion is actually immediate" symptom. A schedule belongs
// to ONE hand-over: when the same slot leaves this node's replica set again
// while an earlier window is still running, the new window must be armed
// freshly. The old code silently kept the older — nearly expired — deadline,
// so the copy was deleted the moment that stale window ran out, i.e. right
// after the newer hand-over returned.
func TestRepeatedHandoversRearmAndNeverDeleteEarly(t *testing.T) {
	e, st := dropSetup(t, "node-2")
	const rt = 400 * time.Millisecond
	e.SetDropRetention(rt)
	dir := fillSlot(t, e, st, 0)

	// three hops that each take node-2 out of the set and back in. Every hop
	// must arm its own window and every re-join must cancel it.
	var last time.Time
	for hop := 1; hop <= 3; hop++ {
		applyCmd(t, e, &Command{Op: OpSlotRemoveReplica, Slots: []int32{0}, NodeID: "node-2"})
		deadline, ok := e.PendingDrop(0)
		if !ok {
			t.Fatalf("hop %d: leaving the set did not arm a countdown", hop)
		}
		if left := time.Until(deadline); left <= rt*3/4 || left > rt {
			t.Fatalf("hop %d: armed window %s is not a fresh %s window", hop, left, rt)
		}
		if !last.IsZero() && !deadline.After(last) {
			t.Fatalf("hop %d: deadline %s did not move past the previous one %s", hop, deadline, last)
		}
		last = deadline
		time.Sleep(rt / 3) // a third of the window: the copy is still owed
		if dirGone(dir) {
			t.Fatalf("hop %d: the copy was dropped inside its own retention window", hop)
		}
		applyCmd(t, e, &Command{Op: OpSlotAddReplica, Slots: []int32{0}, NodeID: "node-2"})
		if _, ok := e.PendingDrop(0); ok {
			t.Fatalf("hop %d: a copy the node is on again must not stay queued", hop)
		}
	}
	if dirGone(dir) {
		t.Fatal("three in-and-out hops deleted the copy: no hop ever elapsed its own window")
	}

	// the last hop leaves for good: the copy must survive its own window and
	// then go.
	applyCmd(t, e, &Command{Op: OpSlotRemoveReplica, Slots: []int32{0}, NodeID: "node-2"})
	final, ok := e.PendingDrop(0)
	if !ok {
		t.Fatal("the final hop did not arm a countdown")
	}
	if left := time.Until(final); left <= rt*3/4 {
		t.Fatalf("the final hop inherited a stale deadline (%s left of %s)", left, rt)
	}
	waitFor(t, 3*time.Second, func() bool { return dirGone(dir) }, "the surplus copy to be dropped after its own window")
	if _, ok := e.PendingDrop(0); ok {
		t.Fatal("the schedule must be cleared once the drop ran")
	}
}

// TestDropFireTimeRecheckKeepsACopyTheTableStillExpects pins the second half of
// the guard: even if a schedule somehow survives (a racing apply, or a table
// change that did not go through the cancel path), the timer re-reads the table
// when it wakes up and keeps a copy this node is a member of.
func TestDropFireTimeRecheckKeepsACopyTheTableStillExpects(t *testing.T) {
	e, st := dropSetup(t, "node-2")
	const rt = 150 * time.Millisecond
	e.SetDropRetention(rt)
	dir := fillSlot(t, e, st, 0)

	applyCmd(t, e, &Command{Op: OpSlotRemoveReplica, Slots: []int32{0}, NodeID: "node-2"})
	if _, ok := e.PendingDrop(0); !ok {
		t.Fatal("test premise: the copy should be queued")
	}
	// Put the node back on the slot behind syncMigrationState's back, so only
	// the fire-time check can save the copy.
	e.tableMu.Lock()
	e.table.Slots[0].Replicas = []string{"node-1", "node-2", "node-3"}
	e.tableMu.Unlock()

	time.Sleep(4 * rt)
	if dirGone(dir) {
		t.Fatalf("the timer dropped a copy the table still expects: %s", dir)
	}
}

// TestRestartDoesNotDropAnything: the countdown lives in memory. A node that
// restarts (a fresh engine over the same store, restored from the replicated
// table) loses the timer and KEEPS the copy — restarting must never be a way
// for data to disappear, and it must not resurrect a schedule either.
func TestRestartDoesNotDropAnything(t *testing.T) {
	e1, st := dropSetup(t, "node-2")
	const rt = 120 * time.Millisecond
	e1.SetDropRetention(rt)
	dir := fillSlot(t, e1, st, 0)

	applyCmd(t, e1, &Command{Op: OpSlotRemoveReplica, Slots: []int32{0}, NodeID: "node-2"})
	if _, ok := e1.PendingDrop(0); !ok {
		t.Fatal("test premise: the copy should be queued")
	}
	snap, err := e1.SnapshotState()
	if err != nil {
		t.Fatalf("snapshot state: %v", err)
	}
	// the process dies: its timers die with it (cancelPendingDrop is exactly
	// that: the entry is gone, so the running goroutine finds itself
	// superseded and touches nothing).
	e1.cancelPendingDrop(0)

	// the restart: a fresh engine over the same data dir and the restored table.
	e2 := NewEngine(nil, st, "node-2", "leader", nil)
	e2.SetDropRetention(rt)
	if err := e2.RestoreState(snap); err != nil {
		t.Fatalf("restore state: %v", err)
	}
	if _, ok := e2.PendingDrop(0); ok {
		t.Fatal("a restart must not resurrect a cleanup schedule")
	}
	time.Sleep(4 * rt)
	if dirGone(dir) {
		t.Fatalf("a restart dropped a local copy: %s", dir)
	}
}

// TestDropCountdownNotArmedByRollback: a migration that ends without this node
// leaving the replica set (rollback puts the slot back to stable with the same
// leader, and this node is still a member) must leave no schedule and no
// leftover timer behind.
func TestDropCountdownNotArmedByRollback(t *testing.T) {
	e, st := dropSetup(t, "node-2")
	e.SetDropRetention(100 * time.Millisecond)
	dir := fillSlot(t, e, st, 0)

	applyCmd(t, e, &Command{Op: OpSlotState, Slots: []int32{0}, State: SlotMigratingOut, MigratingTo: "node-1"})
	applyCmd(t, e, &Command{Op: OpSlotState, Slots: []int32{0}, State: SlotStable}) // rollbackMigration
	if _, ok := e.PendingDrop(0); ok {
		t.Fatal("a rolled-back migration must not arm the countdown")
	}
	time.Sleep(300 * time.Millisecond)
	if _, ok := e.PendingDrop(0); ok {
		t.Fatal("no timer may be left running after a rollback")
	}
	if dirGone(dir) {
		t.Fatalf("rolled-back data must be kept: %s", dir)
	}
}

// TestDropCleanupCannotBeDisabled: there is no "off" setting — a configured 0
// means the default window, so a copy that becomes surplus is always queued and
// always reclaimed (never left to fill the disk).
func TestDropCleanupCannotBeDisabled(t *testing.T) {
	e, st := dropSetup(t, "node-2")
	e.SetDropRetention(0) // "not set": the default 30s, NOT "off"
	dir := fillSlot(t, e, st, 0)

	applyCmd(t, e, &Command{Op: OpSlotRemoveReplica, Slots: []int32{0}, NodeID: "node-2"})
	deadline, ok := e.PendingDrop(0)
	if !ok {
		t.Fatal("the countdown must be armed even when the retention was configured as 0")
	}
	if left := time.Until(deadline); left <= 29*time.Second || left > 30*time.Second {
		t.Fatalf("0 must resolve to the 30s default, got a deadline %s out", left)
	}
	if dirGone(dir) {
		t.Fatalf("nothing may be dropped before the retention elapses: %s", dir)
	}
	e.cancelPendingDrop(0) // do not wait out the default window in a unit test
}

// TestRejoiningTheSetCancelsAPendingCleanup is the early-delete regression, in
// the shape the user hit: a copy that is queued (the node really left the set),
// then becomes a member again INSIDE the window (the next hop brings the slot
// back to it), must not be deleted when the earlier window runs out. The old
// code reused the first, already-expiring window for the later hop and dropped
// the copy moments after that hop committed — the "delayed deletion is actually
// immediate" report — even though the node was a replica again by then.
func TestRejoiningTheSetCancelsAPendingCleanup(t *testing.T) {
	e, st := dropSetup(t, "node-2")
	const rt = 250 * time.Millisecond
	e.SetDropRetention(rt)
	dir := fillSlot(t, e, st, 1) // node-2 leads slot 1, with node-3 as its replica

	// hop 1: hand slot 1 to node-1 (outside the set): admit, move, reclaim
	applyCmd(t, e, &Command{Op: OpSlotAddReplica, Slots: []int32{1}, NodeID: "node-1"})
	applyCmd(t, e, &Command{Op: OpSlotState, Slots: []int32{1}, State: SlotMigratingOut, MigratingTo: "node-1"})
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{1}, NewLeader: "node-1"})
	applyCmd(t, e, &Command{Op: OpSlotRemoveReplica, Slots: []int32{1}, NodeID: "node-2"})
	if _, ok := e.PendingDrop(1); !ok {
		t.Fatal("hop 1: leaving the replica set should queue the surplus copy")
	}

	// hop 2, well inside hop 1's window: node-2 is admitted back as a replica
	time.Sleep(rt / 4)
	applyCmd(t, e, &Command{Op: OpSlotAddReplica, Slots: []int32{1}, NodeID: "node-2"})
	if _, ok := e.PendingDrop(1); ok {
		t.Fatal("hop 2: a copy that rejoined the replica set must not stay queued")
	}

	// hop 1's window now elapses. The copy is part of the replica set again, so
	// it must still be there.
	time.Sleep(rt + rt/2)
	if dirGone(dir) {
		t.Fatalf("the previous hop's window deleted a copy that had rejoined the replica set: %s", dir)
	}
}

// waitFor polls cond until it holds or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
