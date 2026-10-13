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
	"reflect"
	"testing"

	"github.com/berkaroad/pushupes/internal/storage"
)

func newTestEngine(t *testing.T, self string) (*Engine, *storage.Store) {
	t.Helper()
	st, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	eng := NewEngine(nil, st, self, nil)
	return eng, st
}

// applyCmd drives the table through the real Raft-apply entry point.
func applyCmd(t *testing.T, e *Engine, c *Command) {
	t.Helper()
	if _, err := e.ApplyCommand(c.Encode()); err != nil {
		t.Fatalf("apply %s: %v", c.Op, err)
	}
}

// join adds a peer to the test table and then re-derives the replica factor the
// way the controller does after every membership change (OpConfig from
// fixtureFactor). These tests never run the controller, so without
// this the table would keep whatever factor it was constructed with and a
// 3-member table would stay at one copy. See pinReplicas for tests whose
// premise needs a layout the derivation does not produce.
func join(t *testing.T, e *Engine, id, addr string) {
	t.Helper()
	// tests treat all three plane addresses as one value; peer-plane
	// dials (fetch/forward/probe) follow whichever address the code under
	// test consults.
	applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{Role: RoleLearner, ID: id, PeerAddr: addr, AdminAddr: addr, ClientAddr: addr}})
	applyCmd(t, e, &Command{Op: OpConfig, Replicas: fixtureFactor(len(e.TableSnapshot().Peers))})
}

// pinReplicas states an explicit replica factor for a test whose premise is a
// layout the derivation would not produce — "a leader plus a follower" needs
// two copies, while fixtureFactor(2 members) is one copy (a 2-voter
// group survives no failure, so the data is allowed to live in one place).
// Production never sets the factor by hand: the controller derives it.
func pinReplicas(t *testing.T, e *Engine, n int) {
	t.Helper()
	applyCmd(t, e, &Command{Op: OpConfig, Replicas: n})
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
	// Every slot now covers the three-member ring's window at the derived
	// factor — the re-layout ADDS the ring's seats, so a slot may still carry
	// an extra seat it was already replicating until the reclaim takes it off
	// (see reclaimRound).
	nodes := tbl.PeerIDs()
	for s, p := range tbl.Slots {
		if len(p.Replicas) < 2 {
			t.Fatalf("slot %d replicas %d want at least 2", s, len(p.Replicas))
		}
		for i := 0; i < 2; i++ {
			if on := nodes[(int(s)+i)%len(nodes)]; !replicaListHas(p.Replicas, on) {
				t.Fatalf("slot %d does not hold the ring's seat %s: %v", s, on, p.Replicas)
			}
		}
	}
}

func TestLeaveNodeFailsLeaderOver(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	// Failover moves leadership to a REPLICA, so the fixture needs one: two
	// members derive a single copy, hence the explicit two.
	pinReplicas(t, e, 2)
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

// TestSeatGrantsLabelWhyTheSeatAppeared pins the reason a seat exists, which is
// what the fetch round's priority class reads: seats the ring hands a node (it
// joined, came back, or the ring re-spread) are LOAD, seats the replica factor
// adds to an unchanged membership are redundancy. The two look identical in the
// data (both are just "behind"), so the label is recorded when the seat appears.
func TestSeatGrantsLabelWhyTheSeatAppeared(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// The initial layout's seats are the ring's own.
	for s, p := range e.TableSnapshot().Slots {
		for _, r := range p.Replicas {
			if got := p.SeatGrant[r]; got != SeatGrantRing {
				t.Fatalf("slot %d: initial seat %s labelled %q, want %q", s, r, got, SeatGrantRing)
			}
		}
	}

	// A member joins: every seat the ring hands it is load.
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpReplanSlots})
	taken := 0
	for s, p := range e.TableSnapshot().Slots {
		if !replicaSetHas(p.Replicas, "node-3") {
			continue
		}
		taken++
		if got := p.SeatGrant["node-3"]; got != SeatGrantRing {
			t.Fatalf("slot %d: the joining member's seat is labelled %q, want %q", s, got, SeatGrantRing)
		}
	}
	if taken == 0 {
		t.Fatal("test setup: the joining member took no seat")
	}

	// The same membership with a higher factor: only the added seats are new
	// (and they are redundancy).
	before := map[int32][]string{}
	for s, p := range e.TableSnapshot().Slots {
		before[s] = append([]string(nil), p.Replicas...)
	}
	applyCmd(t, e, &Command{Op: OpConfig, Replicas: 3})
	applyCmd(t, e, &Command{Op: OpReplanSlots})
	added := 0
	for s, p := range e.TableSnapshot().Slots {
		for _, r := range p.Replicas {
			if replicaSetHas(before[s], r) {
				continue
			}
			added++
			if got := p.SeatGrant[r]; got != SeatGrantFactor {
				t.Fatalf("slot %d: seat %s added by a factor rise is labelled %q, want %q", s, r, got, SeatGrantFactor)
			}
		}
	}
	if added == 0 {
		t.Fatal("test setup: no slot grew when the factor rose")
	}

	// A member joins WHILE the factor rises: the whole replan is the ring
	// settling a new member set, so every seat it adds — the joiner's share and
	// the churn the repacking gives the members already there — leads.
	before = map[int32][]string{}
	for s, p := range e.TableSnapshot().Slots {
		before[s] = append([]string(nil), p.Replicas...)
	}
	join(t, e, "node-4", "127.0.0.1:4")
	applyCmd(t, e, &Command{Op: OpConfig, Replicas: 4})
	applyCmd(t, e, &Command{Op: OpReplanSlots})
	added = 0
	for s, p := range e.TableSnapshot().Slots {
		for _, r := range p.Replicas {
			if replicaSetHas(before[s], r) {
				continue
			}
			added++
			if got := p.SeatGrant[r]; got != SeatGrantRing {
				t.Fatalf("slot %d: seat %s added while a member joined is labelled %q, want %q", s, r, got, SeatGrantRing)
			}
		}
	}
	if added == 0 {
		t.Fatal("test setup: no slot changed when the member joined")
	}
}

// The generation a data copy is fenced against comes from the REPLICATED
// state, not from whatever id the voter that happens to lead bootstrapped
// with. Voters each write their own random id into their own consensus dir,
// so a fence keyed on the current leader's id declared every copy a different
// generation the moment leadership moved: a plain restart of a 3-voter
// cluster left the whole data plane pending (writes failing err_id 1007)
// until an operator adopted every node by hand.
func TestClusterGenerationComesFromTheReplicatedState(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")

	// A cold start — and the unit harnesses, which have no consensus Node at
	// all — falls back to the node's own bootstrap id: exactly the value the
	// controller is about to submit.
	if got := e.topologyID(); got != "default" {
		t.Fatalf("generation before the entry applies = %q, want the stand-in", got)
	}

	// The entry lands: from here every node reads the same value, whichever
	// voter is leading.
	applyCmd(t, e, &Command{Op: OpSetTopology, Topology: "gen-a"})
	if got := e.topologyID(); got != "gen-a" {
		t.Fatalf("generation = %q, want the replicated gen-a", got)
	}
	if got := e.TableSnapshot().Topology; got != "gen-a" {
		t.Fatalf("table generation = %q, want gen-a", got)
	}

	// A second submit — a stale controller, or a voter that came back with a
	// fresh id of its own — must not rotate it: that would re-fence every
	// copy in the data plane.
	applyCmd(t, e, &Command{Op: OpSetTopology, Topology: "gen-b"})
	if got := e.topologyID(); got != "gen-a" {
		t.Fatalf("generation = %q after a second submit, want gen-a (first writer wins)", got)
	}

	// Provenance records are stamped with it, so a copy written before a
	// failover still matches the fence's value after it.
	rec := desiredOwner(e.topologyID(), "node-1",
		&Placement{Leader: "node-1", Replicas: []string{"node-1"}, Epoch: 1, State: SlotStable})
	if rec.Topology != "gen-a" || !rec.Replica {
		t.Fatalf("owner stamp = %+v, want the replicated generation and the seat", rec)
	}
}
