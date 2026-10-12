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

package cluster

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// replicaSetHas reports whether a replica list already contains a node.
func replicaSetHas(list []string, id string) bool {
	for _, v := range list {
		if v == id {
			return true
		}
	}
	return false
}

// TestSlotAddReplicaJoinsOneSlot pins the op a migration uses to admit a
// target that is not a replica yet (the case the console's 迁移 hits when the
// chosen node is outside the slot's current replica set).
func TestSlotAddReplicaJoinsOneSlot(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	before, _ := e.TableSnapshot().Slots[0]
	if before.Leader != "node-1" || len(before.Replicas) != 2 || replicaSetHas(before.Replicas, "node-3") {
		t.Fatalf("test setup: slot 0 placement %+v", before)
	}
	other := e.TableSnapshot().Slots[1]

	applyCmd(t, e, &Command{Op: OpSlotAddReplica, Slots: []int32{0}, NodeID: "node-3"})
	p, _ := e.TableSnapshot().Slots[0]
	if !replicaSetHas(p.Replicas, "node-3") {
		t.Fatalf("node-3 not joined: %+v", p)
	}
	if len(p.Replicas) != 3 || p.Replicas[0] != before.Replicas[0] || p.Replicas[1] != before.Replicas[1] {
		t.Fatalf("replica order/disturbance: %+v (was %+v)", p.Replicas, before.Replicas)
	}
	if p.Leader != before.Leader || p.Epoch != before.Epoch {
		t.Fatalf("leader/epoch must not move when adding a replica: %+v", p)
	}
	// only the named slot changes
	if q, _ := e.TableSnapshot().Slots[1]; len(q.Replicas) != len(other.Replicas) {
		t.Fatalf("neighbour slot touched: %+v", q)
	}
	// re-applying is a no-op (the FSM must be deterministic/idempotent)
	applyCmd(t, e, &Command{Op: OpSlotAddReplica, Slots: []int32{0}, NodeID: "node-3"})
	if p, _ := e.TableSnapshot().Slots[0]; len(p.Replicas) != 3 {
		t.Fatalf("replay grew the replica set: %+v", p.Replicas)
	}
	// guards
	if _, err := e.ApplyCommand((&Command{Op: OpSlotAddReplica, Slots: []int32{999}, NodeID: "node-3"}).Encode()); err == nil {
		t.Fatal("unknown slot must be refused")
	}
	if _, err := e.ApplyCommand((&Command{Op: OpSlotAddReplica, Slots: []int32{0}}).Encode()); err == nil {
		t.Fatal("missing node must be refused")
	}
}

// TestMigrateSlotAcceptsTargetOutsideReplicaSet is the regression test for
// "migrate a slot to a node outside its current replica set". The controller
// used to refuse such a target outright ("target X is not a replica of slot
// N") even though the console offers every node as a target, so a move could
// not be expressed at all. It must instead join the target to the replica set
// first — that admission is what starts the target's fetch, which is now the
// only way the data gets there — and commit only once the target holds the log.
//
// Nothing listens on the test's peer addresses, so the handover aborts while
// waiting for the copy: what is asserted is the state the controller commits
// before that — the target joined the replica set, the slot is not left
// migrating (the migrating_out mark is only taken at the commit), and an
// in-set target does not grow the set.
func TestMigrateSlotAcceptsTargetOutsideReplicaSet(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	e.node = newTestRaftNode(t, e)
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	before, _ := e.TableSnapshot().Slots[0]
	if before.Leader != "node-1" || len(before.Replicas) != 2 || replicaSetHas(before.Replicas, "node-3") {
		t.Fatalf("test setup: slot 0 placement %+v", before)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := e.MigrateSlot(ctx, 0, "node-3")
	if err != nil && strings.Contains(err.Error(), "is not a replica") {
		t.Fatalf("a target outside the replica set must be accepted, got %v", err)
	}

	after, _ := e.TableSnapshot().Slots[0]
	if !replicaSetHas(after.Replicas, "node-3") {
		t.Fatalf("target node-3 was never joined to the replica set: %+v", after)
	}
	if after.State != SlotStable || after.MigratingTo != "" {
		t.Fatalf("slot left in a migration state after the aborted run: %+v", after)
	}

	// The mirror case must not grow the set: a target that already is a
	// replica (slot 1 is led by node-2 with node-3 as its replica) takes the
	// ordinary in-set path.
	inside, _ := e.TableSnapshot().Slots[1]
	if !replicaSetHas(inside.Replicas, "node-3") {
		t.Fatalf("test setup: slot 1 placement %+v", inside)
	}
	_ = e.MigrateSlot(ctx, 1, "node-3")
	got, _ := e.TableSnapshot().Slots[1]
	if len(got.Replicas) != len(inside.Replicas) {
		t.Fatalf("an in-set target must not add a replica: %+v (was %+v)", got.Replicas, inside.Replicas)
	}
}

// TestReclaimYieldsToAnInFlightHandover pins the race between the surplus-seat
// reclaim and a hand-over: an operator may name a target the ring plan does not
// prescribe for the slot, so the reclaim would see the seat the hand-over is
// building as surplus and drop it mid-catch-up (a commit then fails with
// "is not a replica yet"). The seat must survive while the hand-over is in
// flight, and go back to being surplus once it is over.
//
// The copies that stay answer over the peer plane (they are empty here, so they
// report in sync), which is what the reclaim waits for before dropping anything.
func TestReclaimYieldsToAnInFlightHandover(t *testing.T) {
	src, _ := newTestEngine(t, "node-1")
	src.node = newTestRaftNode(t, src)
	peer, _ := newTestEngine(t, "node-2")
	peerAddr := newPeerHarness(t, peer)

	join(t, src, "node-1", "127.0.0.1:1")
	join(t, src, "node-2", peerAddr)
	join(t, src, "node-3", "127.0.0.1:3")
	applyCmd(t, src, &Command{Op: OpPlanSlots})
	ctx := context.Background()

	before, _ := src.TableSnapshot().Slots[0]
	if replicaSetHas(before.Replicas, "node-3") {
		t.Fatalf("test setup: slot 0 placement %+v", before)
	}
	grow := func() {
		applyCmd(t, src, &Command{Op: OpSlotAddReplica, Slots: []int32{0}, NodeID: "node-3"})
		if p, _ := src.TableSnapshot().Slots[0]; !replicaSetHas(p.Replicas, "node-3") {
			t.Fatalf("test setup: node-3 not admitted: %+v", p)
		}
	}

	// Baseline: a surplus seat the plan does not prescribe is reclaimed.
	grow()
	if n := src.reclaimRound(ctx, 1); n != 1 {
		t.Fatalf("reclaim dropped %d seats, want 1", n)
	}
	if p, _ := src.TableSnapshot().Slots[0]; replicaSetHas(p.Replicas, "node-3") {
		t.Fatalf("surplus seat kept: %+v", p)
	}

	// With a hand-over in flight the same seat is off limits.
	grow()
	src.markMigrationInFlight(0)
	if n := src.reclaimRound(ctx, 1); n != 0 {
		t.Fatalf("reclaim dropped %d seats during a hand-over, want 0", n)
	}
	if p, _ := src.TableSnapshot().Slots[0]; !replicaSetHas(p.Replicas, "node-3") {
		t.Fatalf("reclaim dropped the seat a hand-over was building: %+v", p)
	}

	// Once the hand-over is over the seat is surplus again.
	src.clearMigrationInFlight(0)
	if n := src.reclaimRound(ctx, 1); n != 1 {
		t.Fatalf("reclaim dropped %d seats after the hand-over, want 1", n)
	}
	if p, _ := src.TableSnapshot().Slots[0]; replicaSetHas(p.Replicas, "node-3") {
		t.Fatalf("surplus seat kept after the hand-over: %+v", p)
	}
}

// TestFenceDurabilityProbeMakesTheTailDurable pins the commit fence's new gate:
// the target must be able to prove the frozen tail is ON DISK, not merely
// applied. HandleSlotFlush flushes the slot and answers the offset that landed;
// a slot this node does not hold answers 0, which never satisfies a frozen tail.
func TestFenceDurabilityProbeMakesTheTailDurable(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	addr := newPeerHarness(t, e)
	slot := int32(1)
	agg := aggInSlotOf(t, e, slot, "durable-")
	for v := 1; v <= 3; v++ {
		if _, err := st.Append(makeRecord(agg, uint32(v), fmt.Sprintf("d-%d", v))); err != nil {
			t.Fatal(err)
		}
	}

	leo := st.LastSeqOf(slot)
	durable, err := e.HandleSlotFlush(slot)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if leo == 0 || durable < leo {
		t.Fatalf("flush answered durable=%d, want durable >= leo=%d > 0", durable, leo)
	}

	// The same probe over the peer plane, which is how the fence asks.
	ctx := context.Background()
	dur, err := e.peerDurableLEO(ctx, addr, slot)
	if err != nil {
		t.Fatalf("peer durable probe: %v", err)
	}
	if dur < leo {
		t.Fatalf("peer durable probe answered %d, want >= %d", dur, leo)
	}

	// A slot this node does not hold can never satisfy a frozen tail.
	other := e.store.SlotCount - 1
	if other == slot {
		t.Fatalf("test setup: slot %d is both held and unheld", slot)
	}
	if held, err := e.HandleSlotFlush(other); err != nil || held != 0 {
		t.Fatalf("unheld slot answered (%d, %v), want (0, nil)", held, err)
	}
}

// TestHandoverEntryPointGuards pins the two entry points of the handover state
// machine on a live table: the admission step is idempotent and leaves the slot
// stable under its leader (no fence, no epoch bump), and the commit step refuses
// a target that was never admitted — the data would not be there, and moving the
// leader onto an empty copy is the fork the gate exists to prevent.
func TestHandoverEntryPointGuards(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	e.node = newTestRaftNode(t, e)
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})
	ctx := context.Background()

	base, _ := e.TableSnapshot().Slots[0]
	if base.Leader != "node-1" || replicaSetHas(base.Replicas, "node-3") {
		t.Fatalf("test setup: slot 0 placement %+v", base)
	}

	// commit before stage: refused, and nothing moves.
	if err := e.commitMigration(ctx, 0, "node-3"); err == nil {
		t.Fatal("committing a handover to a node that is not a replica must be refused")
	}
	if p, _ := e.TableSnapshot().Slots[0]; p.Leader != base.Leader || p.Epoch != base.Epoch {
		t.Fatalf("a refused handover must not move the leader: %+v", p)
	}

	// stage: admit the target; the slot stays stable under its leader.
	changed, err := e.stageMigration(ctx, 0, "node-3")
	if err != nil || !changed {
		t.Fatalf("stage: changed=%v err=%v", changed, err)
	}
	after, _ := e.TableSnapshot().Slots[0]
	if !replicaSetHas(after.Replicas, "node-3") {
		t.Fatalf("stage did not admit the target: %+v", after)
	}
	if after.State != SlotStable || after.Leader != base.Leader || after.Epoch != base.Epoch {
		t.Fatalf("staging must leave the slot stable under its leader: %+v", after)
	}

	// stage again: idempotent.
	if changed, err := e.stageMigration(ctx, 0, "node-3"); err != nil || changed {
		t.Fatalf("staging an in-set target must be a no-op: changed=%v err=%v", changed, err)
	}
	// staging the slot's own leader is refused (that is a commit, not a stage).
	if _, err := e.stageMigration(ctx, 0, base.Leader); err == nil {
		t.Fatal("staging the current leader must be refused")
	}
}

// TestSlotRemoveReplicaTableSemantics pins the op that reclaims a surplus
// replica (the mirror of OpSlotAddReplica): it must drop exactly the named
// node from exactly one slot, be idempotent on replay, refuse the slot's
// leader, and never move leader or epoch.
func TestSlotRemoveReplicaTableSemantics(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	before, _ := e.TableSnapshot().Slots[0]
	if before.Leader != "node-1" || len(before.Replicas) != 2 || replicaSetHas(before.Replicas, "node-3") {
		t.Fatalf("test setup: slot 0 placement %+v", before)
	}
	other := e.TableSnapshot().Slots[1]

	// grow the set so there is a spurious (non-leader) member to reclaim
	applyCmd(t, e, &Command{Op: OpSlotAddReplica, Slots: []int32{0}, NodeID: "node-3"})
	if p, _ := e.TableSnapshot().Slots[0]; !replicaSetHas(p.Replicas, "node-3") || len(p.Replicas) != 3 {
		t.Fatalf("test setup: add did not grow slot 0: %+v", p)
	}

	applyCmd(t, e, &Command{Op: OpSlotRemoveReplica, Slots: []int32{0}, NodeID: "node-3"})
	p, _ := e.TableSnapshot().Slots[0]
	if replicaSetHas(p.Replicas, "node-3") {
		t.Fatalf("node-3 not removed: %+v", p)
	}
	if len(p.Replicas) != 2 || p.Replicas[0] != before.Replicas[0] || p.Replicas[1] != before.Replicas[1] {
		t.Fatalf("replica order/disturbance: %+v (was %+v)", p.Replicas, before.Replicas)
	}
	if p.Leader != before.Leader || p.Epoch != before.Epoch {
		t.Fatalf("removing a non-leader replica must not move leader/epoch: %+v", p)
	}
	// only the named slot changes
	if q, _ := e.TableSnapshot().Slots[1]; len(q.Replicas) != len(other.Replicas) {
		t.Fatalf("neighbour slot touched: %+v", q)
	}
	// re-applying (now-absent node) is a no-op, no error
	applyCmd(t, e, &Command{Op: OpSlotRemoveReplica, Slots: []int32{0}, NodeID: "node-3"})
	if p, _ := e.TableSnapshot().Slots[0]; len(p.Replicas) != 2 {
		t.Fatalf("replay changed the replica set: %+v", p.Replicas)
	}

	// guards
	if _, err := e.ApplyCommand((&Command{Op: OpSlotRemoveReplica, Slots: []int32{999}, NodeID: "node-3"}).Encode()); err == nil {
		t.Fatal("unknown slot must be refused")
	}
	if _, err := e.ApplyCommand((&Command{Op: OpSlotRemoveReplica, Slots: []int32{0}}).Encode()); err == nil {
		t.Fatal("missing node must be refused")
	}
	if _, err := e.ApplyCommand((&Command{Op: OpSlotRemoveReplica, Slots: []int32{0}, NodeID: "node-1"}).Encode()); err == nil {
		t.Fatal("removing the slot leader must be refused")
	}
	if p, _ := e.TableSnapshot().Slots[0]; !replicaSetHas(p.Replicas, "node-1") || p.Leader != "node-1" {
		t.Fatalf("a refused leader removal must leave the slot intact: %+v", p)
	}
}

// TestRemoveReplicaEngineGuards pins the submitter-side guards on the engine
// entry point: they must reject a non-replica, the slot leader, an unknown
// node/slot, a slot mid-migration and a removal that would empty the set —
// without committing anything.
func TestRemoveReplicaEngineGuards(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	e.node = newTestRaftNode(t, e)
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})
	ctx := context.Background()

	p0, _ := e.TableSnapshot().Slots[0]
	if replicaSetHas(p0.Replicas, "node-3") {
		t.Fatalf("test setup: node-3 unexpectedly in slot 0: %+v", p0)
	}
	if err := e.RemoveReplica(ctx, 0, "node-3"); err == nil {
		t.Fatal("removing a non-replica must be refused")
	}
	if err := e.RemoveReplica(ctx, 0, p0.Leader); err == nil {
		t.Fatal("removing the slot leader must be refused")
	}
	if err := e.RemoveReplica(ctx, 0, "node-9"); err == nil {
		t.Fatal("removing an unknown node must be refused")
	}
	if err := e.RemoveReplica(ctx, 999, "node-2"); err == nil {
		t.Fatal("removing from an unknown slot must be refused")
	}
	applyCmd(t, e, &Command{Op: OpSlotState, Slots: []int32{0}, State: SlotMigratingOut, MigratingTo: "node-3"})
	if err := e.RemoveReplica(ctx, 0, "node-2"); err == nil {
		t.Fatal("removing a replica mid-migration must be refused")
	}
	applyCmd(t, e, &Command{Op: OpSlotState, Slots: []int32{0}, State: SlotStable})

	// last-replica guard: construct the degenerate single-replica state (a
	// slot whose only replica is not its leader) that the guard protects.
	e.tableMu.Lock()
	e.table.Slots[0].Replicas = []string{"node-2"}
	e.tableMu.Unlock()
	if err := e.RemoveReplica(ctx, 0, "node-2"); err == nil {
		t.Fatal("removing the last replica must be refused")
	}

	// happy path: a surplus (non-leader) replica is reclaimed
	applyCmd(t, e, &Command{Op: OpSlotAddReplica, Slots: []int32{0}, NodeID: "node-3"})
	if err := e.RemoveReplica(ctx, 0, "node-3"); err != nil {
		t.Fatalf("removing a surplus replica must succeed: %v", err)
	}
	if p, _ := e.TableSnapshot().Slots[0]; replicaSetHas(p.Replicas, "node-3") {
		t.Fatalf("node-3 still a replica after remove: %+v", p)
	}
}

// TestRemoveReplicaCombination walks the whole admit/reclaim cycle a real
// migration produces on one slot: admit a target that is OUTSIDE the replica
// set (the case the migration fix covers), roll the staging back by removing
// that target, then take the committed path — admit the target again, move the
// leader onto it — and finally reclaim the former SOURCE node, now a surplus
// replica.
func TestRemoveReplicaCombination(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	e.node = newTestRaftNode(t, e)
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})
	ctx := context.Background()

	base, _ := e.TableSnapshot().Slots[0]
	if base.Leader != "node-1" || replicaSetHas(base.Replicas, "node-3") {
		t.Fatalf("test setup: slot 0 placement %+v", base)
	}

	// 1. stage: admit the out-of-set target (MigrateSlot's first step)
	applyCmd(t, e, &Command{Op: OpSlotAddReplica, Slots: []int32{0}, NodeID: "node-3"})

	// 2. roll the staging back: drop the target again
	if err := e.RemoveReplica(ctx, 0, "node-3"); err != nil {
		t.Fatalf("rolling back the staged target must succeed: %v", err)
	}
	if p, _ := e.TableSnapshot().Slots[0]; replicaSetHas(p.Replicas, "node-3") {
		t.Fatalf("rollback left node-3 in the set: %+v", p)
	}

	// 3. committed path: admit the target, move the leader onto it
	applyCmd(t, e, &Command{Op: OpSlotAddReplica, Slots: []int32{0}, NodeID: "node-3"})
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{0}, NewLeader: "node-3"})
	moved, _ := e.TableSnapshot().Slots[0]
	if moved.Leader != "node-3" || !replicaSetHas(moved.Replicas, "node-1") {
		t.Fatalf("after commit slot 0 = %+v", moved)
	}

	// 4. reclaim the former source (a plain replica now)
	if err := e.RemoveReplica(ctx, 0, "node-1"); err != nil {
		t.Fatalf("reclaiming the former source must succeed: %v", err)
	}
	got, _ := e.TableSnapshot().Slots[0]
	if replicaSetHas(got.Replicas, "node-1") {
		t.Fatalf("former source still a replica: %+v", got)
	}
	if len(got.Replicas) != 2 {
		t.Fatalf("replica set must shrink to 2, got %+v", got.Replicas)
	}
	if got.Leader != "node-3" {
		t.Fatalf("reclaiming a replica must not move the leader: %+v", got)
	}
	if got.Epoch != moved.Epoch {
		t.Fatalf("reclaiming a replica must not bump the epoch: %d -> %d", moved.Epoch, got.Epoch)
	}
	// the current leader is now protected
	if err := e.RemoveReplica(ctx, 0, "node-3"); err == nil {
		t.Fatal("removing the current leader must be refused")
	}
}

// TestMigrationAbortKeepsSourceLeaderAndExplainableSet pins the failure side of
// the handover contract. When the handover aborts BEFORE the leader move (here
// the copy cannot be shown to hold the source's log: nothing answers), the slot must not be
// left leaderless or empty: the source still leads and is still a replica, the
// staged target is the only surplus member, and the epoch has not moved. This
// is the state a manual remove-replica (or a retry) can undo.
func TestMigrationAbortKeepsSourceLeaderAndExplainableSet(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	e.node = newTestRaftNode(t, e)
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	factor := e.TableSnapshot().Replicas
	base, _ := e.TableSnapshot().Slots[0]
	if base.Leader != "node-1" || len(base.Replicas) != factor || replicaSetHas(base.Replicas, "node-3") {
		t.Fatalf("test setup: slot 0 placement %+v (factor %d)", base, factor)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.MigrateSlot(ctx, 0, "node-3"); err == nil {
		t.Fatal("a migration to an unreachable target must not report success")
	}

	after, _ := e.TableSnapshot().Slots[0]
	if after.Leader != base.Leader {
		t.Fatalf("an aborted migration must leave the source as leader: %+v", after)
	}
	if after.State != SlotStable || after.MigratingTo != "" {
		t.Fatalf("an aborted migration must not leave the slot migrating: %+v", after)
	}
	if after.Epoch != base.Epoch {
		t.Fatalf("the leader never moved, so the epoch must not move: %d -> %d", base.Epoch, after.Epoch)
	}
	if !replicaSetHas(after.Replicas, base.Leader) {
		t.Fatalf("the source was dropped from its own replica set: %+v", after.Replicas)
	}
	if len(after.Replicas) != factor+1 || !replicaSetHas(after.Replicas, "node-3") {
		t.Fatalf("the aborted set must be exactly the staged admission: %+v (factor %d)", after.Replicas, factor)
	}
}

// TestMigrationReclaimsSurplusReplica is the regression test for the console
// workflow the user hit: migrating a slot to a node OUTSIDE its replica set
// succeeded but left the set at factor+1, because the migration only admitted
// the target and the surplus source had to be dropped by hand. A committed
// migration must now reclaim that surplus itself, bringing the set back to the
// configured factor, dropping the FORMER SOURCE, and keeping the migration
// target as leader (removal must not move leader or epoch).
func TestMigrationReclaimsSurplusReplica(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	e.node = newTestRaftNode(t, e)
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	factor := e.TableSnapshot().Replicas
	base, _ := e.TableSnapshot().Slots[0]
	if base.Leader != "node-1" || len(base.Replicas) != factor || replicaSetHas(base.Replicas, "node-3") {
		t.Fatalf("test setup: slot 0 placement %+v (factor %d)", base, factor)
	}

	// the state a migration to an out-of-set target leaves at commit time:
	// the target was admitted (step 0) and the leader moved onto it (step 5)
	applyCmd(t, e, &Command{Op: OpSlotAddReplica, Slots: []int32{0}, NodeID: "node-3"})
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{0}, NewLeader: "node-3"})
	commit, _ := e.TableSnapshot().Slots[0]
	if len(commit.Replicas) != factor+1 || commit.Leader != "node-3" || commit.State != SlotStable {
		t.Fatalf("test setup: committed placement %+v", commit)
	}

	e.reclaimSurplusReplica(0, "node-1", "node-3")

	got, _ := e.TableSnapshot().Slots[0]
	if len(got.Replicas) != factor {
		t.Fatalf("replica set must be back at the factor %d, got %+v", factor, got.Replicas)
	}
	if replicaSetHas(got.Replicas, "node-1") {
		t.Fatalf("the former source must be the one reclaimed: %+v", got.Replicas)
	}
	if !replicaSetHas(got.Replicas, "node-2") || !replicaSetHas(got.Replicas, "node-3") {
		t.Fatalf("the migration target and the surviving replica must stay: %+v", got.Replicas)
	}
	if got.Leader != "node-3" {
		t.Fatalf("the leader must stay on the migration target, got %q", got.Leader)
	}
	if got.Epoch != commit.Epoch {
		t.Fatalf("reclaiming a replica must not bump the epoch: %d -> %d", commit.Epoch, got.Epoch)
	}
	// a set already at the factor is a no-op (idempotent on retry)
	e.reclaimSurplusReplica(0, "node-1", "node-3")
	if again, _ := e.TableSnapshot().Slots[0]; len(again.Replicas) != factor || again.Leader != "node-3" {
		t.Fatalf("reclaim must be a no-op once the set is at the factor: %+v", again)
	}
	// the reclaimed slot is intact for its leader and the neighbour is untouched
	if p, _ := e.TableSnapshot().Slots[1]; len(p.Replicas) != factor {
		t.Fatalf("a reclaim must not touch other slots: %+v", p)
	}
}

// TestMigrationInSetDoesNotDropReplicas is the counter-case: when the migration
// target is ALREADY a replica (an in-set leader handover) the set is never
// grown, so the reclaim must leave it — and its size — untouched. It pins the
// "只在仍然超出因子时才动，因子内不动" rule.
func TestMigrationInSetDoesNotDropReplicas(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	e.node = newTestRaftNode(t, e)
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	factor := e.TableSnapshot().Replicas
	// slot 1 is led by node-2 with node-3 already in its replica set
	inside, _ := e.TableSnapshot().Slots[1]
	if inside.Leader == "node-3" || !replicaSetHas(inside.Replicas, "node-3") || len(inside.Replicas) != factor {
		t.Fatalf("test setup: slot 1 placement %+v", inside)
	}
	from := inside.Leader

	// an in-set migration: the leader simply moves to a node already in the set
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{1}, NewLeader: "node-3"})
	before, _ := e.TableSnapshot().Slots[1]
	if len(before.Replicas) != factor || before.Leader != "node-3" {
		t.Fatalf("an in-set leader move must not change the set: %+v", before)
	}

	e.reclaimSurplusReplica(1, from, "node-3")

	got, _ := e.TableSnapshot().Slots[1]
	if len(got.Replicas) != factor {
		t.Fatalf("an in-set migration must not drop a replica: %+v (was %+v)", got.Replicas, before.Replicas)
	}
	if !replicaSetHas(got.Replicas, from) {
		t.Fatalf("the former leader was wrongly dropped by an in-set migration: %+v", got.Replicas)
	}
	if got.Leader != "node-3" {
		t.Fatalf("the leader must stay on the migration target, got %q", got.Leader)
	}
	if got.Epoch != before.Epoch {
		t.Fatalf("an in-set migration must not bump the epoch: %d -> %d", before.Epoch, got.Epoch)
	}
}
