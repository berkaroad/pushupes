package cluster

import (
	"testing"
)

// reclaimSurplusReplica shrinks a slot's replica set back to the configured
// factor after a migration admitted an out-of-set target. It used to run inside
// a `for {}` whose body cloned the whole slot table each pass; the only path
// that iterated was a successful RemoveReplica, and the second pass existed
// solely to re-read a set that was already back at the factor.
//
// The rewrite reads the placement once and splits the decision into
// surplusForReclaim. That split is what makes the behaviour testable: the
// commit goes through Raft, which a test engine has no node for, so asserting
// "the replica set did not change" cannot tell a correct decline from a failed
// commit (both leave the table identical — verified). Asserting the DECISION
// can.
func TestSurplusForReclaimDecisions(t *testing.T) {
	placement := func(state SlotState, leader string, replicas ...string) *Placement {
		return &Placement{Leader: leader, Replicas: replicas, State: state}
	}

	cases := []struct {
		name      string
		p         *Placement
		factor    int
		from      string
		toNode    string
		wantNode  string
		wantOver  bool
		rationale string
	}{
		{
			name: "at the factor: nothing to do",
			p:    placement(SlotStable, "node-1", "node-1", "node-2"), factor: 2,
			from: "node-2", toNode: "node-3",
			wantNode: "", wantOver: false,
			rationale: "this is the case the removed loop re-read on its second pass",
		},
		{
			name: "under the factor: nothing to do",
			p:    placement(SlotStable, "node-1", "node-1"), factor: 2,
			from: "", toNode: "node-3",
			wantNode: "", wantOver: false,
			rationale: "a short set is not this path's business",
		},
		{
			name: "over the factor: reclaim the source",
			p:    placement(SlotStable, "node-1", "node-1", "node-2", "node-3"), factor: 2,
			from: "node-2", toNode: "node-3",
			wantNode: "node-2", wantOver: true,
			rationale: "the former source holds a full copy and no longer leads",
		},
		{
			name: "over the factor, source already the leader: next non-leader/target",
			p:    placement(SlotStable, "node-1", "node-1", "node-2", "node-3"), factor: 2,
			from: "node-1", toNode: "node-4",
			wantNode: "node-2", wantOver: true,
			rationale: "the leader is never removable; fall back to a sorted candidate",
		},
		{
			name: "over the factor but no candidate is eligible",
			p:    placement(SlotStable, "node-1", "node-1", "node-2"), factor: 1,
			from: "node-1", toNode: "node-2",
			wantNode: "", wantOver: true,
			rationale: "every member is the leader or the target: warn, do not guess",
		},
		{
			name: "not stable: a migration owns the placement",
			p:    placement(SlotMigratingOut, "node-1", "node-1", "node-2", "node-3"), factor: 2,
			from: "node-2", toNode: "node-3",
			wantNode: "", wantOver: false,
			rationale: "it reclaims on commit, not while migrating",
		},
		{
			name: "nil placement: no slot",
			p:    nil, factor: 2, from: "node-2", toNode: "node-3",
			wantNode: "", wantOver: false,
			rationale: "an unassigned slot is left to the caller",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node, over := surplusForReclaim(tc.p, tc.factor, tc.from, tc.toNode)
			if node != tc.wantNode || over != tc.wantOver {
				t.Errorf("surplusForReclaim = (%q, %v), want (%q, %v) — %s",
					node, over, tc.wantNode, tc.wantOver, tc.rationale)
			}
		})
	}
}

// reclaimSurplusReplica must still reach the same conclusion through the table:
// the pure decision is only useful if the caller feeds it the live placement and
// factor. This drives the real function and checks the set is untouched in the
// cases that must not reclaim (the commit path itself needs a Raft node, which
// the table-level cases below deliberately avoid depending on).
func TestReclaimSurplusReplicaLeavesSettledSlotsAlone(t *testing.T) {
	for _, tc := range []struct {
		name   string
		state  SlotState
		others []string // extra replicas beyond node-1
	}{
		{name: "at the factor", state: SlotStable, others: []string{"node-2"}},
		{name: "not stable", state: SlotMigratingOut, others: []string{"node-2", "node-3"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _ := newTestEngine(t, "node-1")
			join(t, e, "node-1", "127.0.0.1:1")
			join(t, e, "node-2", "127.0.0.1:2")
			join(t, e, "node-3", "127.0.0.1:3")
			applyCmd(t, e, &Command{Op: OpPlanSlots})

			// Take a slot this node leads and shape it to the case.
			slot := int32(-1)
			for s, p := range e.TableSnapshot().Slots {
				if p.Leader == "node-1" {
					slot = s
					break
				}
			}
			if slot < 0 {
				t.Fatal("test setup: node-1 leads no slot")
			}
			for _, r := range []string{"node-1", "node-2", "node-3"} {
				member := replicaListHas(e.TableSnapshot().Slots[slot].Replicas, r)
				if r == "node-1" || contains(tc.others, r) {
					if !member {
						applyCmd(t, e, &Command{Op: OpSlotAddReplica, Slots: []int32{slot}, NodeID: r})
					}
					continue
				}
				if member {
					applyCmd(t, e, &Command{Op: OpSlotRemoveReplica, Slots: []int32{slot}, NodeID: r})
				}
			}
			applyCmd(t, e, &Command{Op: OpSlotState, Slots: []int32{slot}, State: tc.state})

			before := append([]string(nil), e.TableSnapshot().Slots[slot].Replicas...)
			e.reclaimSurplusReplica(slot, "node-2", "node-3")
			after := e.TableSnapshot().Slots[slot].Replicas
			if len(before) != len(after) {
				t.Errorf("replica set changed from %v to %v in case %q", before, after, tc.name)
			}
			if e.TableSnapshot().Slots[slot].State != tc.state {
				t.Errorf("slot state changed to %s", e.TableSnapshot().Slots[slot].State)
			}
		})
	}
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}
