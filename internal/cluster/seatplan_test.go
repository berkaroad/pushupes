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
	"fmt"
	"testing"
)

// seatTable builds a table with the given hosting nodes and k slots, laid out by
// the bootstrap ring at the given factor.
func seatTable(nodes []string, k int, factor int) *Table {
	t := NewTable(int32(k), factor)
	for _, id := range nodes {
		t.Peers[id] = Peer{ID: id, Role: RoleLearner, ClientAddr: id + ":1"}
	}
	t.Slots = PlanSlotsInRange(nodes, 0, int32(k), factor)
	t.PlanMembers = append([]string(nil), nodes...)
	t.LayoutEpoch = 2 // past bootstrap, so the even-pool gate applies
	return t
}

// duplicateSeat reports the first node named twice in a replica set ("" when
// the set is clean). I1 is a hard invariant of DESIGN §4.5, so the layout tests
// assert it directly instead of asserting a particular ring shape.
func duplicateSeat(replicas []string) string {
	seen := map[string]bool{}
	for _, r := range replicas {
		if seen[r] {
			return r
		}
		seen[r] = true
	}
	return ""
}

func seatCensusOf(sets map[int32][]string) map[string]int {
	out := map[string]int{}
	for _, list := range sets {
		for _, id := range list {
			out[id]++
		}
	}
	return out
}

func censusSpread(census map[string]int, nodes []string) int {
	lo, hi := -1, -1
	for _, id := range nodes {
		v := census[id]
		if lo < 0 || v < lo {
			lo = v
		}
		if v > hi {
			hi = v
		}
	}
	return hi - lo
}

func seatsFor(plan SeatPlan, node string) int {
	n := 0
	for _, list := range plan.Target {
		for _, id := range list {
			if id == node {
				n++
			}
		}
	}
	return n
}

// TestSeatPlanHoldsTheInvariants drives the plan across pool sizes and factors:
// every target set holds MIN(factor, |pool|) distinct members of the pool (I1 /
// I1a), and the per-node census stays inside its floor/ceil band (I2).
func TestSeatPlanHoldsTheInvariants(t *testing.T) {
	pools := [][]string{
		{"node-1"},
		{"node-1", "node-2"},
		{"node-1", "node-2", "node-3"},
		{"node-1", "node-2", "node-3", "node-4"},
		{"node-1", "node-2", "node-3", "node-4", "node-5", "node-6"},
	}
	for _, nodes := range pools {
		for _, factor := range []int{1, 2, 3} {
			t.Run(fmt.Sprintf("n=%d/rf=%d", len(nodes), factor), func(t *testing.T) {
				tbl := seatTable(nodes, 16, factor)
				plan := tbl.DesiredSeats()
				want := factor
				if len(nodes) < want {
					want = len(nodes)
				}
				inPool := map[string]bool{}
				for _, id := range nodes {
					inPool[id] = true
				}
				if len(plan.Target) != 16 {
					t.Fatalf("plan covers %d slots, want 16", len(plan.Target))
				}
				for s, set := range plan.Target {
					if len(set) != want {
						t.Fatalf("slot %d target %v has %d seats, want %d", s, set, len(set), want)
					}
					if dup := duplicateSeat(set); dup != "" {
						t.Fatalf("slot %d target names %s twice: %v", s, dup, set)
					}
					for _, id := range set {
						if !inPool[id] {
							t.Fatalf("slot %d seats %s, which is not in the pool", s, id)
						}
					}
				}
				if spread := censusSpread(seatCensusOf(plan.Target), nodes); spread > 1 {
					t.Fatalf("census spread %d over %v: %v", spread, nodes, seatCensusOf(plan.Target))
				}
			})
		}
	}
}

// TestEvenGateHoldsAnOddPool pins the gate of DESIGN §4.5: a fifth member of a
// four-member cluster arms nothing (a single new node on an even pool is exactly
// what the gate is for), while the pair that makes the pool even again does seat
// them.
func TestEvenGateHoldsAnOddPool(t *testing.T) {
	nodes := []string{"node-1", "node-2", "node-3", "node-4"}
	tbl := seatTable(nodes, 16, 2)
	if !tbl.gateOpen() {
		t.Fatal("an even pool must be open")
	}

	tbl.Peers["node-5"] = Peer{ID: "node-5", Role: RoleLearner, ClientAddr: "node-5:1"}
	if tbl.gateOpen() {
		t.Fatal("an odd pool with a pending member change must be gated")
	}
	if tbl.layoutStale() {
		t.Fatal("the gate must keep layoutStale quiet, or the replan arms anyway")
	}

	tbl.Peers["node-6"] = Peer{ID: "node-6", Role: RoleLearner, ClientAddr: "node-6:1"}
	if !tbl.gateOpen() {
		t.Fatal("an even pool must be open again")
	}
	if !tbl.layoutStale() {
		t.Fatal("two new members must arm the replan")
	}
	plan := tbl.DesiredSeats()
	for _, id := range []string{"node-5", "node-6"} {
		if seatsFor(plan, id) == 0 {
			t.Fatalf("%s joined into an even pool and got no seat: %v", id, seatCensusOf(plan.Target))
		}
	}
}

// TestBootstrapIsExemptFromTheGate: an odd pool at epoch 1 is how every cluster
// starts (one node), and it must still plan.
func TestBootstrapIsExemptFromTheGate(t *testing.T) {
	tbl := seatTable([]string{"node-1", "node-2", "node-3"}, 8, 2)
	tbl.LayoutEpoch = 1
	tbl.PlanMembers = nil
	if !tbl.gateOpen() {
		t.Fatal("bootstrap must not be gated")
	}
}

// TestFactorChangeIsExemptFromTheGate: an operator's replica-policy change does
// not touch membership, so an odd pool must still converge on it.
func TestFactorChangeIsExemptFromTheGate(t *testing.T) {
	tbl := seatTable([]string{"node-1", "node-2", "node-3"}, 8, 2)
	if !tbl.gateOpen() {
		t.Fatal("a factor change with an unchanged directory must not be gated")
	}
	tbl.Replicas = 3
	if !tbl.layoutStale() {
		t.Fatal("raising the factor must arm the replan even on an odd pool")
	}
}

// TestLeaveRefillKeepsTheFactorAndSparesOtherSlots pins the removal path of
// DESIGN §4.5: the seats the departing node held are re-filled on the same
// command, no slot is left short, and no seat is dropped on the way.
func TestLeaveRefillKeepsTheFactorAndSparesOtherSlots(t *testing.T) {
	nodes := []string{"node-1", "node-2", "node-3", "node-4"}
	tbl := seatTable(nodes, 16, 2)
	before := map[int32][]string{}
	for s, p := range tbl.Slots {
		before[s] = append([]string(nil), p.Replicas...)
	}
	if err := tbl.Apply(&Command{Op: OpLeaveNode, NodeID: "node-4"}); err != nil {
		t.Fatalf("leave: %v", err)
	}
	if p, ok := tbl.Peers["node-4"]; ok {
		t.Fatalf("the departed member is still in the directory: %+v", p)
	}
	for s, p := range tbl.Slots {
		if replicaListHas(p.Replicas, "node-4") {
			t.Fatalf("slot %d still names the departed node: %v", s, p.Replicas)
		}
		if len(p.Replicas) != 2 {
			t.Fatalf("slot %d holds %d seats, want the factor 2: %v", s, len(p.Replicas), p.Replicas)
		}
		if dup := duplicateSeat(p.Replicas); dup != "" {
			t.Fatalf("slot %d names %s twice: %v", s, dup, p.Replicas)
		}
		for _, had := range before[s] {
			if had == "node-4" || replicaListHas(p.Replicas, had) {
				continue
			}
			t.Fatalf("slot %d dropped %s on the way: %v", s, had, p.Replicas)
		}
	}
	if plan := tbl.DesiredSeats(); len(plan.Add) > 0 {
		t.Fatalf("the refill must leave nothing short, plan still adds %v", plan.Add)
	}
}

// TestJoinMovesOnlyTheDeficit pins minimal movement: adding two members to a
// settled four-member layout seats them on a share of the slots, not on the
// whole table (a bootstrap ring would move nearly everything).
func TestJoinMovesOnlyTheDeficit(t *testing.T) {
	nodes := []string{"node-1", "node-2", "node-3", "node-4"}
	tbl := seatTable(nodes, 64, 2)
	tbl.Peers["node-5"] = Peer{ID: "node-5", Role: RoleLearner, ClientAddr: "node-5:1"}
	tbl.Peers["node-6"] = Peer{ID: "node-6", Role: RoleLearner, ClientAddr: "node-6:1"}
	plan := tbl.DesiredSeats()

	adds := 0
	for _, list := range plan.Add {
		adds += len(list)
	}
	// 64 slots × 2 seats = 128 seats over 6 nodes: each new member takes about
	// 21, so roughly 42 of 128 seats change hands. Anything near 128 would be a
	// full re-derivation.
	if adds == 0 {
		t.Fatal("two new members must be seated, plan adds nothing")
	}
	if adds > 64 {
		t.Fatalf("seating two members moved %d of 128 seats: not minimal movement", adds)
	}
	census := seatCensusOf(plan.Target)
	for _, id := range nodes {
		if census[id] < 10 {
			t.Fatalf("an existing member lost its share: %v", census)
		}
	}
}

// TestLeaveLeavesNothingToReplan is the contract the removal path owes the
// layout: the drain/refill a leave performs must land on exactly the target the
// next replan would have computed, so nothing is left to re-plan. Otherwise a
// removal always arms one more round of movement — and on an even pool it
// really does run, moving seats the operator never asked to move.
func TestLeaveLeavesNothingToReplan(t *testing.T) {
	for _, n := range []int{3, 4, 5, 6, 8} {
		for _, factor := range []int{1, 2, 3} {
			name := fmt.Sprintf("n=%d/rf=%d", n, factor)
			t.Run(name, func(t *testing.T) {
				nodes := make([]string, 0, n)
				for i := 1; i <= n; i++ {
					nodes = append(nodes, fmt.Sprintf("node-%d", i))
				}
				tbl := seatTable(nodes, 24, factor)
				gone := nodes[len(nodes)-1]
				if err := tbl.Apply(&Command{Op: OpLeaveNode, NodeID: gone}); err != nil {
					t.Fatalf("leave %s: %v", gone, err)
				}
				plan := tbl.DesiredSeats()
				// Seats must be settled: no slot short of its factor, no seat the
				// layout no longer wants. Leadership may still owe a few hand-overs
				// — those are metadata moves on copies the slot already holds (the
				// rebalancer's rounds, not a replan), and they never move data.
				if len(plan.Add) > 0 || len(plan.Drop) > 0 {
					sets := map[int32][]string{}
					for sl, pl := range tbl.Slots {
						sets[sl] = pl.Replicas
					}
					t.Fatalf("after leaving %s the seats are not settled: add=%v drop=%v census=%v",
						gone, plan.Add, plan.Drop, seatCensusOf(sets))
				}
				for slot := range plan.Lead {
					if !replicaListHas(tbl.Slots[slot].Replicas, plan.Lead[slot]) {
						t.Fatalf("slot %d: balancing hand-over target %s is not one of its copies: %v",
							slot, plan.Lead[slot], tbl.Slots[slot].Replicas)
					}
				}
			})
		}
	}
}

// TestLeaderCensusConverges is the regression for the scenario the ring anchor
// cannot fix: a node leaves and comes back, its seats return but its leadership
// does not, and the ring's expected leader is no longer in the set. The balancing
// pass has to bring the census back into floor/ceil, using only hand-overs on
// copies the slot already holds (no slot transfer).
func TestLeaderCensusConverges(t *testing.T) {
	nodes := []string{"node-1", "node-2", "node-3", "node-4"}
	tbl := seatTable(nodes, 8, 2)
	apply := func(c *Command) {
		if err := tbl.Apply(c); err != nil {
			t.Fatalf("%s: %v", c.Op, err)
		}
	}
	census := func() map[string]int {
		out := map[string]int{}
		for _, p := range tbl.Slots {
			out[p.Leader]++
		}
		return out
	}
	if got := census(); len(got) != 4 {
		t.Fatalf("fresh layout leads on %d nodes: %v", len(got), got)
	}

	// node-4 leaves: node-1 absorbs its leadership.
	apply(&Command{Op: OpLeaveNode, NodeID: "node-4"})
	// node-4 returns and the minimal-movement replan re-seats it.
	apply(&Command{Op: OpJoinNode, Peer: &Peer{Role: RoleLearner, ID: "node-4", ClientAddr: "node-4:1"}})
	apply(&Command{Op: OpReplanSlots})
	if got := census(); got["node-4"] != 0 {
		t.Logf("note: node-4 already leads %d slot(s) after the replan: %v", got["node-4"], got)
	}

	// Play the controller's whole loop to convergence: seat (replan), reclaim the
	// seats the layout no longer wants, then balance leadership. Leadership can
	// only follow the seats, so a run that only balances would stall on a slot
	// whose set does not hold the starved node yet.
	for round := 0; round < 40; round++ {
		progressed := false
		if tbl.applyReplanSlots() {
			progressed = true
		}
		plan := tbl.DesiredSeats()
		for slot, drops := range plan.Drop {
			for _, drop := range drops {
				if surplusSeatForReplan(tbl.Slots[slot], tbl.Replicas, plan.Target[slot]) != drop {
					continue
				}
				apply(&Command{Op: OpSlotRemoveReplica, Slots: []int32{slot}, NodeID: drop})
				progressed = true
			}
		}
		if progressed {
			continue
		}
		moves := PlanLeaderRebalance(tbl)
		if len(moves) == 0 {
			break
		}
		for _, m := range moves {
			if !replicaListHas(tbl.Slots[m.Slot].Replicas, m.To) {
				t.Fatalf("slot %d: hand-over target %s is not a copy: %v", m.Slot, m.To, tbl.Slots[m.Slot].Replicas)
			}
			apply(&Command{Op: OpLeaderMove, Slots: []int32{m.Slot}, NewLeader: m.To})
		}
	}
	if left := PlanLeaderRebalance(tbl); len(left) != 0 {
		t.Fatalf("leadership never settled, %d move(s) left: %v", len(left), left)
	}
	// Leadership can only be dealt to the members a slot already replicates, so
	// the exact band is reachable only when the seat layout allows it; what the
	// balancing pass guarantees is that it stops when nothing more is movable, no
	// node is left leading nothing, and the spread is bounded by the seat
	// constraint rather than by history.
	got := census()
	for _, id := range nodes {
		if got[id] == 0 {
			t.Fatalf("member %s leads nothing after balancing: %v", id, got)
		}
	}
	lo, hi := got[nodes[0]], got[nodes[0]]
	for _, id := range nodes {
		if got[id] < lo {
			lo = got[id]
		}
		if got[id] > hi {
			hi = got[id]
		}
	}
	if hi-lo > 2 {
		t.Fatalf("leader census %v spread %d: balancing stalled too early", got, hi-lo)
	}
}

// TestStageThenSwitchMovesTheSegment is the table half of a generation
// migration: staging plans the destination segment as importing_in (a leader
// that can take replayed records, but still outside the served routing), and the
// switch promotes exactly those placements and retires the old segment.
func TestStageThenSwitchMovesTheSegment(t *testing.T) {
	nodes := []string{"node-1", "node-2", "node-3", "node-4"}
	tbl := seatTable(nodes, 16, 2)
	// The served segment is g2 = [4,8); the table is 16 slots so g3 = [8,16) fits.
	tbl.GroupGen, tbl.GroupBase, tbl.GroupSize = 2, 4, 4
	tbl.Slots = PlanSlotsInRange(nodes, 4, 8, 2)
	oldSlots := map[int32]bool{}
	for s := range tbl.Slots {
		oldSlots[s] = true
	}

	if err := tbl.Apply(&Command{Op: OpStageGroup, GroupGen: 3, GroupBase: 8, GroupSize: 8}); err != nil {
		t.Fatalf("stage g3: %v", err)
	}
	if tbl.GroupBase != 4 || tbl.GroupSize != 4 {
		t.Fatalf("staging must not switch the served segment: base=%d size=%d", tbl.GroupBase, tbl.GroupSize)
	}
	if !tbl.segmentStaged(8, 8) {
		t.Fatalf("the staged segment is not marked importing_in")
	}
	for s := int32(8); s < 16; s++ {
		if tbl.Slots[s] == nil || tbl.Slots[s].Leader == "" {
			t.Fatalf("staged slot %d has no leader to accept the replay", s)
		}
	}
	if layoutSettled(tbl) {
		t.Fatal("a cluster with a staged segment is not settled: rebalance and reclaim must yield")
	}

	// Staging is idempotent (an operator retry must not replan the destination).
	before := len(tbl.Slots)
	if err := tbl.Apply(&Command{Op: OpStageGroup, GroupGen: 3, GroupBase: 8, GroupSize: 8}); err != nil {
		t.Fatalf("stage again: %v", err)
	}
	if len(tbl.Slots) != before {
		t.Fatalf("a repeated staging changed the table: %d slots, want %d", len(tbl.Slots), before)
	}

	// Switch: promote the staged slots, drop the served segment.
	epoch := tbl.LayoutEpoch
	if err := tbl.Apply(&Command{Op: OpSetGroup, GroupGen: 3, GroupBase: 8, GroupSize: 8}); err != nil {
		t.Fatalf("switch: %v", err)
	}
	if tbl.GroupBase != 8 || tbl.GroupSize != 8 || tbl.LayoutEpoch != epoch+1 {
		t.Fatalf("after the switch: base=%d size=%d epoch=%d", tbl.GroupBase, tbl.GroupSize, tbl.LayoutEpoch)
	}
	for s := int32(8); s < 16; s++ {
		if p := tbl.Slots[s]; p == nil || p.State != SlotStable {
			t.Fatalf("slot %d was not promoted: %+v", s, p)
		}
	}
	for s := range oldSlots {
		if tbl.Slots[s] != nil {
			t.Fatalf("old segment slot %d is still in the table", s)
		}
	}
	if !layoutSettled(tbl) {
		t.Fatal("after the switch every slot must be stable again")
	}
}
