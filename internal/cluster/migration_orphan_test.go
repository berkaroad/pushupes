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
	"testing"
	"time"
)

// A migration is staged by StartMigration's first step (a replicated
// migrating_out placement) and cleared only by the goroutine that staged it.
// When that goroutine is gone — the controller restarted mid-hand-over — the
// slot stays non-stable forever: replan_slots skips non-stable slots and the
// rebalancer yields to them, so the whole ring layout freezes (observed: one
// stuck slot left a node leading 958 of 1680 slots for good). The decision of
// what to abandon is a pure function, asserted here.
func TestOrphanedMigrationsListsUnownedUnstableSlots(t *testing.T) {
	tbl := NewTable(8, 2)
	tbl.Peers["node-1"] = Peer{ID: "node-1"}
	tbl.Peers["node-2"] = Peer{ID: "node-2"}
	tbl.Slots[0] = &Placement{Leader: "node-1", Replicas: []string{"node-1", "node-2"}, State: SlotStable}
	tbl.Slots[1] = &Placement{Leader: "node-1", Replicas: []string{"node-1", "node-2"}, State: SlotMigratingOut, MigratingTo: "node-2"}
	tbl.Slots[2] = &Placement{Leader: "node-2", Replicas: []string{"node-2", "node-1"}, State: SlotImportingIn, MigratingTo: "node-2"}
	tbl.Slots[3] = &Placement{Leader: "node-2", Replicas: []string{"node-2", "node-1"}, State: SlotMigratingOut, MigratingTo: "node-1"}

	// Nothing in flight: every non-stable slot is an orphan.
	if got := orphanedMigrations(tbl, nil); len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("orphans = %v, want [1 2 3]", got)
	}
	// A migration this controller is driving must be left alone.
	if got := orphanedMigrations(tbl, map[int32]struct{}{1: {}, 3: {}}); len(got) != 1 || got[0] != 2 {
		t.Fatalf("orphans = %v, want [2]", got)
	}
	// A table with nothing staged has nothing to abandon.
	stable := NewTable(8, 2)
	if got := orphanedMigrations(stable, nil); len(got) != 0 {
		t.Fatalf("orphans = %v, want none", got)
	}
}

// The controller round returns an inherited non-stable slot to stable through
// Raft, and leaves the migration it is driving alone.
func TestControllerReconcilesAbandonedMigration(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	e.node = newTestRaftNode(t, e)
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// An inherited stage: written by a controller that is gone (nothing in
	// this process is driving it).
	applyCmd(t, e, &Command{Op: OpSlotState, Slots: []int32{1}, State: SlotMigratingOut, MigratingTo: "node-2"})
	// A migration this controller is driving right now.
	applyCmd(t, e, &Command{Op: OpSlotState, Slots: []int32{2}, State: SlotMigratingOut, MigratingTo: "node-3"})
	e.markMigrationInFlight(2)
	t.Cleanup(func() { e.clearMigrationInFlight(2) })

	e.reconcileOrphanMigrations()

	waitFor(t, 5*time.Second, func() bool {
		p := e.TableSnapshot().Slots[1]
		return p != nil && p.State == SlotStable && p.MigratingTo == ""
	}, "the abandoned slot never returned to stable")

	if p := e.TableSnapshot().Slots[2]; p.State != SlotMigratingOut || p.MigratingTo != "node-3" {
		t.Fatalf("the in-flight migration was disturbed: %+v", p)
	}
}

// The reported scenario end to end: a controller that dies (here: the node is
// closed, as it is on a restart) leaves the staged state behind in the table,
// which the next process restores. Its in-flight set is empty by construction,
// so the controller round abandons the migration and the slot becomes
// schedulable again.
func TestRestartAbandonsMigrationLeftByTheDeadController(t *testing.T) {
	dir := t.TempDir()
	eng, _ := newTestEngine(t, "node-1")
	join(t, eng, "node-1", "127.0.0.1:1")
	join(t, eng, "node-2", "127.0.0.1:2")
	join(t, eng, "node-3", "127.0.0.1:3")
	applyCmd(t, eng, &Command{Op: OpPlanSlots})
	applyCmd(t, eng, &Command{Op: OpSlotState, Slots: []int32{5}, State: SlotMigratingOut, MigratingTo: "node-2"})

	n1 := startRaftOn(t, dir, eng, 1, 5*time.Second)
	for i := 0; i < 3; i++ {
		if _, err := n1.raft.Apply((&Command{Op: OpMarkUp, NodeID: "node-1"}).Encode(), 5*time.Second); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	waitForSnapshot(t, dir, 10*time.Second)
	if err := n1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// The restart: a fresh engine (and therefore an empty in-flight set) takes
	// over the state the dead controller left, stage included.
	eng2, _ := newTestEngine(t, "node-1")
	n2 := startRaftOn(t, dir, eng2, 1, 5*time.Second)
	eng2.node = n2 // submit() goes through the node, exactly as SetNode wires it
	defer n2.Close()

	inherited := eng2.TableSnapshot().Slots[5]
	if inherited == nil || inherited.State != SlotMigratingOut {
		t.Fatalf("setup: the restarted node inherited %+v, want a staged slot", inherited)
	}
	eng2.reconcileOrphanMigrations()
	// The controller round retries every round, and it has to: right after a
	// restart the engine's leadership view can still lag the raft node's, so
	// the first submit may be refused as "not the controller". Poll like the
	// loop does.
	waitFor(t, 10*time.Second, func() bool {
		eng2.reconcileOrphanMigrations()
		p := eng2.TableSnapshot().Slots[5]
		return p != nil && p.State == SlotStable && p.MigratingTo == ""
	}, "the migration inherited from the dead controller was never abandoned")
}
