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

// TestStartMigrationAcceptsTargetOutsideReplicaSet is the regression test for
// "migrate a slot to a node outside its current replica set". The controller
// used to refuse such a target outright ("target X is not a replica of slot
// N") even though the console offers every node as a target, so a move could
// not be expressed at all. It must instead join the target to the replica set
// first (that is the precondition of the snapshot/catch-up steps, which are
// driven by the target following the slot) and then run the same six steps.
//
// Nothing listens on the test's peer addresses, so the migration aborts at its
// first network step — what is asserted is the state the controller commits
// before it: the target joined the replica set, and the slot is not left
// migrating.
func TestStartMigrationAcceptsTargetOutsideReplicaSet(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := e.StartMigration(ctx, 0, "node-3")
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
	_ = e.StartMigration(ctx, 1, "node-3")
	got, _ := e.TableSnapshot().Slots[1]
	if len(got.Replicas) != len(inside.Replicas) {
		t.Fatalf("an in-set target must not add a replica: %+v (was %+v)", got.Replicas, inside.Replicas)
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

	// 1. stage: admit the out-of-set target (StartMigration's first step)
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
// the migration contract. When the migration aborts BEFORE the leader move
// (here the snapshot/catch-up step cannot reach any peer), the slot must not be
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := e.StartMigration(ctx, 0, "node-3"); err == nil {
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
