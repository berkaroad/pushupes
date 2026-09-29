package cluster

import (
	"reflect"
	"testing"

	"pushupes/internal/storage"
)

func newTestEngine(t *testing.T, self string) (*Engine, *storage.Store) {
	t.Helper()
	st, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	eng := NewEngine(nil, st, self, "leader", nil)
	return eng, st
}

// applyCmd drives the table through the real Raft-apply entry point.
func applyCmd(t *testing.T, e *Engine, c *Command) {
	t.Helper()
	if _, err := e.ApplyCommand(c.Encode()); err != nil {
		t.Fatalf("apply %s: %v", c.Op, err)
	}
}

func join(t *testing.T, e *Engine, id, addr string) {
	// tests treat all three plane addresses as one value; peer-plane
	// dials (fetch/forward/probe) follow whichever address the code under
	// test consults.
	applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{ID: id, PeerAddr: addr, AdminAddr: addr, ClientAddr: addr}})
}

func TestPlanSlotsDistribution(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	tbl := e.TableSnapshot()
	if len(tbl.Slots) != 8 {
		t.Fatalf("expected 8 placements, got %d", len(tbl.Slots))
	}
	counts := map[string]int{}
	for s, p := range tbl.Slots {
		counts[p.Leader]++
		if len(p.Replicas) != 2 {
			t.Fatalf("slot %d replicas=%d want 2", s, len(p.Replicas))
		}
		// ring policy: leader = node[s % N], follower = next node forward
		nodes := tbl.PeerIDs()
		if p.Leader != nodes[int(s)%len(nodes)] {
			t.Fatalf("slot %d leader %s want %s", s, p.Leader, nodes[int(s)%len(nodes)])
		}
		if p.Replicas[1] != nodes[(int(s)+1)%len(nodes)] {
			t.Fatalf("slot %d follower %s want %s", s, p.Replicas[1], nodes[(int(s)+1)%len(nodes)])
		}
		if p.Epoch != 1 || p.State != SlotStable {
			t.Fatalf("slot %d epoch/state %+v", s, p)
		}
	}
	for _, n := range []string{"node-1", "node-2", "node-3"} {
		if counts[n] == 0 {
			t.Fatalf("node %s leads no slot", n)
		}
	}
}

func TestReplanFillsGapsOnly(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// Move slot 0's leader explicitly, then a node joins and replans: the
	// moved placement must survive untouched.
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{0}, NewLeader: "node-2"})
	before, _ := e.table.Slots[0]
	beforeEpoch := before.Epoch

	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpReplanSlots})

	tbl := e.TableSnapshot()
	if len(tbl.Slots) != 8 {
		t.Fatalf("slots %d want 8", len(tbl.Slots))
	}
	if p := tbl.Slots[0]; p.Leader != "node-2" || p.Epoch != beforeEpoch {
		t.Fatalf("replan disturbed slot 0: %+v", p)
	}
	// every slot now has 2 replicas
	for s, p := range tbl.Slots {
		if len(p.Replicas) != 2 {
			t.Fatalf("slot %d replicas %d want 2", s, len(p.Replicas))
		}
	}
}

func TestLeaveNodeFailsLeaderOver(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// slot 0: leader node-1, replica node-2. Killing node-1 must move the
	// leadership to node-2 with an epoch bump (failover path).
	applyCmd(t, e, &Command{Op: OpLeaveNode, NodeID: "node-1"})
	p, ok := e.TableSnapshot().Slots[0]
	if !ok {
		t.Fatal("slot 0 gone")
	}
	if p.Leader != "node-2" {
		t.Fatalf("leader after leave: %s want node-2", p.Leader)
	}
	if p.Epoch != 2 {
		t.Fatalf("epoch after failover: %d want 2", p.Epoch)
	}
	for _, r := range p.Replicas {
		if r == "node-1" {
			t.Fatal("dead node still in replica set")
		}
	}
}

func TestSlotStateMigrationTransitions(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// stage: migrating_out with target
	applyCmd(t, e, &Command{Op: OpSlotState, Slots: []int32{1}, State: SlotMigratingOut, MigratingTo: "node-2"})
	p, _ := e.TableSnapshot().Slots[1]
	if p.State != SlotMigratingOut || p.MigratingTo != "node-2" {
		t.Fatalf("stage: %+v", p)
	}
	if p.Leader != "node-2" {
		// node-2 already leads slot 1 (1%2); force via leader-1 slot instead
	}

	// rollback path (abort): back to stable, target cleared
	applyCmd(t, e, &Command{Op: OpSlotState, Slots: []int32{1}, State: SlotStable, MigratingTo: ""})
	p, _ = e.TableSnapshot().Slots[1]
	if p.State != SlotStable || p.MigratingTo != "" {
		t.Fatalf("abort: %+v", p)
	}
}

func TestTableSnapshotRestore(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	applyCmd(t, e, &Command{Op: OpPlanSlots})
	snap, err := e.SnapshotState()
	if err != nil {
		t.Fatal(err)
	}

	e2, _ := newTestEngine(t, "node-2")
	if err := e2.RestoreState(snap); err != nil {
		t.Fatalf("restore: %v", err)
	}
	a, b := e.TableSnapshot(), e2.TableSnapshot()
	if len(a.Slots) != len(b.Slots) || len(a.Peers) != len(b.Peers) {
		t.Fatalf("round trip mismatch: %+v vs %+v", a, b)
	}
	for s, p := range a.Slots {
		q := b.Slots[s]
		if q == nil || !reflect.DeepEqual(p, q) {
			t.Fatalf("slot %d mismatch: %+v vs %+v", s, p, q)
		}
	}
}
