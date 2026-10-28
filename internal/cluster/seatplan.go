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

import "sort"

// SeatPlan is one round of layout movement expressed as per-slot differences
// against the current table.
//
// It is the minimal-movement answer of DESIGN §4.5: every stable slot holds
// want = MIN(|candidates|, factor) distinct seats, every node ends up inside
// its floor/ceil band, and the seats that already sit well do not move. The
// bootstrap mod-ring does the opposite — it re-derives the whole table, which is
// why one node joining a 1024-node cluster moves ~94% of the leaders.
//
// Target is the replica set the plan wants for a slot (existing seats first,
// in their order, then the additions). It is what the reclaim round diffs
// against: a seat outside Target is the one that comes off, and only after a
// copy elsewhere is caught up.
//
// Add is what the layout has to seat; Drop names seats the layout no longer
// wants. Drop is an intention, not an action: a set never dips below its factor
// because the removal waits for the replacement (surplusSeatForReplan refuses a
// drop that would take the set below the factor, and never drops the leader).
type SeatPlan struct {
	Target map[int32][]string
	Add    map[int32][]string
	Drop   map[int32][]string
	// LeadBalance marks the entries of Lead that exist only to even out the
	// leader census (rather than to replace a leader that left its set or went
	// unreachable). Those moves may only be executed against a caught-up copy:
	// a balancing hand-over costs a fence, never a slot transfer.
	LeadBalance map[int32]bool
	// Lead is the slot's desired leader when it differs from the current one.
	// Leadership is balanced on its own census (floor/ceil of slots per node),
	// because a cheap layout is one where each node leads its share: a leader
	// that the target set does not hold must move whatever the census says, and
	// otherwise only a node above the band hands one over.
	Lead map[int32]string
}

// Empty reports whether the layout already satisfies the invariants.
func (p SeatPlan) Empty() bool { return len(p.Add) == 0 && len(p.Drop) == 0 && len(p.Lead) == 0 }

// DesiredSeats computes the minimal movement that satisfies DESIGN §4.5 for the
// current table: fill every stable slot to want distinct candidate seats, then
// equalise the per-node seat count into its floor/ceil band (I2). Nothing is
// mutated, and the answer is a pure, deterministic function of the table: slots
// are visited by ascending id, candidates by sorted id, and every tie-break is
// the smaller id — so every node replaying the same table computes the same plan.
//
// Filling and equalising are separate because they move different things. A
// short set (a member left, a slot was never seated) only needs a seat added. An
// uneven census (a member joined) needs a seat to change hands: the surplus node
// gives one up on a slot the plan keeps, and the starved one takes it. The
// transfer is expressed as Add of the receiver on that slot — the receiver joins
// as an ordinary follower, and the giver's seat becomes surplus, which the
// reclaim round takes off once the receiver is caught up. No data moves twice.
//
// A single-copy layout is the one shape where the giver's seat is also the
// leader, and a leader's seat is never surplus (a slot without a writer cannot
// accept appends). There the transfer waits for the ring anchor: the plan seats
// the starved node on the slot the ring already names as its own, the rebalancer
// sees its expected leader now holding a copy and fences the hand-over, and the
// old leader's seat turns surplus the ordinary way. So the ring is not the
// layout any more — it is the tie-breaker that decides WHICH of several
// equivalent transfers happens, which keeps every hand-over on the existing
// fenced path instead of inventing a second leader-change channel.
func (t *Table) DesiredSeats() SeatPlan {
	plan := SeatPlan{Target: map[int32][]string{}, Add: map[int32][]string{}, Drop: map[int32][]string{}, Lead: map[int32]string{}, LeadBalance: map[int32]bool{}}
	nodes := t.planCandidates()
	if len(nodes) == 0 {
		return plan
	}
	want := t.Replicas
	if len(nodes) < want {
		want = len(nodes) // I1a: a pool smaller than the factor serves one copy per node
	}
	inPool := make(map[string]bool, len(nodes))
	for _, id := range nodes {
		inPool[id] = true
	}

	slots := make([]int32, 0, len(t.Slots))
	for s, p := range t.Slots {
		if p.State == SlotStable {
			slots = append(slots, s)
		}
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
	if len(slots) == 0 {
		return plan
	}

	// The seats each slot keeps, in their current order (the failure path takes
	// a slot's backup leader from the front of this list, so the order is not
	// cosmetic), and the census over the candidate pool only.
	kept := make(map[int32][]string, len(slots))
	seats := make(map[string]int, len(nodes))
	for _, id := range nodes {
		seats[id] = 0
	}
	for _, s := range slots {
		p := t.Slots[s]
		for _, r := range p.Replicas {
			if inPool[r] && !containsNode(kept[s], r) {
				kept[s] = append(kept[s], r)
				seats[r]++
			}
		}
	}

	// Phase 1 — fill short sets.
	for _, s := range slots {
		for len(kept[s]) < want {
			pick := ""
			for _, id := range nodes {
				if containsNode(kept[s], id) {
					continue
				}
				if pick == "" || seats[id] < seats[pick] {
					pick = id
				}
			}
			if pick == "" {
				break
			}
			kept[s] = append(kept[s], pick)
			seats[pick]++
			plan.Add[s] = append(plan.Add[s], pick)
		}
	}

	// Phase 1b — trim sets that hold more than the factor wants (a factor drop,
	// or a set that grew while a replacement was being seated). The seat leaves
	// Target, which is exactly what makes it reclaimable; the leader is never
	// trimmed (a slot without a writer cannot accept appends), and the seat that
	// goes is the one on the node carrying the most.
	// The ring is a tie-break only, and only for sets that have to lose a seat:
	// compute it lazily so the common (already correct) table pays nothing.
	var ring map[int32]*Placement
	for _, s := range slots {
		for len(kept[s]) > want {
			if ring == nil {
				ring = t.planSlots(nodes)
			}
			// Which seat gives up. The one the ring does not name goes first:
			// the ring is no longer the layout, but it is the shape the set
			// converged from, so a seat outside it is the outlier a round of
			// growth or a hand-picked migration added. Among equals, the node
			// carrying the most seats goes, and among those the last seat in
			// the set (the most recently granted one). All three rules only
			// break ties — the set size is fixed by want, never by this order.
			pick, pickInRing := "", true
			for i := len(kept[s]) - 1; i >= 0; i-- {
				id := kept[s][i]
				if id == t.Slots[s].Leader {
					continue
				}
				inRing := ring[s] != nil && replicaListHas(ring[s].Replicas, id)
				if pick == "" {
					pick, pickInRing = id, inRing
					continue
				}
				if inRing != pickInRing {
					if !inRing {
						pick, pickInRing = id, false
					}
					continue
				}
				if seats[id] > seats[pick] {
					pick = id
				}
			}
			if pick == "" {
				break
			}
			kept[s] = removeString(kept[s], pick)
			seats[pick]--
		}
	}

	// Phase 2 — equalise the census. floor/ceil are over the total seat count,
	// which is fixed by the slot count and the factor, so the band does not move
	// while seats change hands.
	total := len(slots) * want
	floorSeats := total / len(nodes)
	for pass := 0; pass < len(slots); pass++ {
		// "Rich" is measured against floor, not ceil: a census that reads
		// 6,6,6,5,5,4 has nobody above ceil but still has a seat to give, and
		// the band is only reached by moving it. Seats are conserved, so a
		// give-and-take pair always exists while anybody sits below floor.
		rich, poor := "", ""
		for _, id := range nodes {
			if seats[id] > floorSeats && (rich == "" || seats[id] > seats[rich]) {
				rich = id
			}
			if seats[id] < floorSeats && (poor == "" || seats[id] < seats[poor]) {
				poor = id
			}
		}
		if rich == "" || poor == "" {
			break
		}
		moved := false
		// Prefer a slot where the giver is not the leader: that transfer is
		// pure metadata (a new follower joins, the giver's seat is reclaimable
		// at once). Take the lowest such slot id so the choice is stable.
		for _, s := range slots {
			if !containsNode(kept[s], rich) || t.Slots[s].Leader == rich {
				continue
			}
			if containsNode(kept[s], poor) {
				continue // the receiver already holds a seat here: swapping would name it twice (I1)
			}
			kept[s] = replaceNode(kept[s], rich, poor)
			seats[rich]--
			seats[poor]++
			plan.Add[s] = append(plan.Add[s], poor)
			moved = true
			break
		}
		if moved {
			continue
		}
		// Every seat the giver holds is a leadership: hand the starved node the
		// slot the ring already assigns to it, let the rebalancer move
		// leadership there through the normal fence, and let the giver's seat
		// turn surplus after the hand-over.
		for _, s := range slots {
			if !containsNode(kept[s], rich) || t.ringLeader(nodes, s) != poor {
				continue
			}
			if containsNode(kept[s], poor) {
				continue
			}
			kept[s] = append(kept[s], poor)
			seats[poor]++
			plan.Add[s] = append(plan.Add[s], poor)
			moved = true
			break
		}
		if !moved {
			break // nothing transferable this round: the rebalancer's rounds converge the rest
		}
	}

	// Phase 3 — leadership. Only two shapes need a hand-over: a leader the
	// target set no longer holds (the set is what replicates the slot, so it
	// cannot lead), and a leader clients cannot reach (the failover path owns the
	// moment, but the plan must not pin a slot to a node that is gone). Anything
	// else keeps its leader: balancing the census is the rebalancer's ring job
	// and it runs every round, while the plan only speaks when the seats it just
	// moved made the old leader invalid.
	live := make(map[string]bool, len(nodes))
	for _, id := range t.OnlinePeerIDs() {
		live[id] = true
	}
	for _, s := range slots {
		cur := t.Slots[s].Leader
		if cur == "" || (containsNode(kept[s], cur) && live[cur]) {
			continue
		}
		best := ""
		for _, id := range kept[s] {
			if !live[id] || id == cur {
				continue
			}
			if best == "" || id < best {
				best = id
			}
		}
		if best != "" {
			plan.Lead[s] = best
		}
	}

	// Phase 3b — the leader census. Leadership is the scarce resource on a busy
	// node (one fsync pipeline per led slot), so the band is enforced the same
	// way the seat band is: a node above ceil hands one slot over to a set member
	// below floor. Only members of the slot's own set are eligible, and only
	// while the giver is above the band and the taker below it — conditions that
	// cannot both hold after the move, so this cannot oscillate.
	//
	// This is what the ring anchor cannot do once the replica sets stop matching
	// the ring (which minimal movement makes normal): the ring's expected leader
	// is usually NOT in the set, so it has nothing to say, and leadership stays
	// wherever a failover or a removal put it.
	// The band is over the nodes that can actually hold leadership, not over the
	// candidate pool: an offline member keeps its seats (its data is real) but
	// cannot lead, and counting it would make every live node look over-quota and
	// hand leadership to a node that is not there.
	liveCount := 0
	for _, id := range nodes {
		if live[id] {
			liveCount++
		}
	}
	if liveCount == 0 {
		liveCount = len(nodes)
	}
	leadFloor := len(slots) / liveCount
	leadCeil := leadFloor
	if len(slots)%liveCount != 0 {
		leadCeil++
	}
	leaders := make(map[string]int, len(nodes))
	for _, id := range nodes {
		leaders[id] = 0
	}
	for _, s := range slots {
		// Count every live leader, seated in its own set or not: the census is
		// about how much write load a node carries, and a leader outside its set
		// is still carrying it (that slot is simply about to be moved anyway).
		// Filtering by membership here made the pass believe a node was inside the
		// band while it was over it, and the ring placement then put leadership
		// back — a two-round loop that never settled.
		if l := t.Slots[s].Leader; live[l] {
			leaders[l]++
		}
	}
	var ringForBalance map[int32]*Placement
	for _, s := range slots {
		if plan.Lead[s] != "" {
			continue // already moving for a stronger reason
		}
		cur := t.Slots[s].Leader
		if !live[cur] || leaders[cur] <= leadCeil {
			continue
		}
		// Among eligible takers prefer one the ring also names for this slot: a
		// leader sitting on a ring seat keeps the surplus-reclaim path working
		// (the seat the old leader leaves behind is the one the layout no longer
		// wants), while a leader on a non-ring seat would pin that seat and
		// stall the reclaim.
		if ringForBalance == nil {
			ringForBalance = t.planSlots(nodes)
		}
		taker, takerOnRing := "", false
		for _, id := range kept[s] {
			if id == cur || !live[id] || leaders[id] >= leadFloor {
				continue
			}
			onRing := ringForBalance[s] != nil && replicaListHas(ringForBalance[s].Replicas, id)
			if taker == "" || (onRing && !takerOnRing) ||
				(onRing == takerOnRing && (leaders[id] < leaders[taker] || (leaders[id] == leaders[taker] && id < taker))) {
				taker, takerOnRing = id, onRing
			}
		}
		if taker == "" {
			continue
		}
		plan.Lead[s] = taker
		plan.LeadBalance[s] = true
		leaders[cur]--
		leaders[taker]++
	}

	// Phase 3c — the ring still places a slot when doing so costs the census
	// nothing. A hand-over between two nodes carrying the same number of leaders
	// leaves the census exactly as it was, so the layout can keep converging on
	// the ring (which is what makes a returning member's slots come home) without
	// fighting the band. Once the counts differ, the ring has nothing to say: that
	// is the case that used to make a balancing hand-over and a ring hand-over
	// undo each other every round.
	if ringForBalance == nil {
		ringForBalance = t.planSlots(nodes)
	}
	// Only when the census is inside its band everywhere. If some node is still
	// over the band, the balancing pass owns the layout: letting the ring place
	// slots at the same time re-creates the very loop this was meant to end (ring
	// puts leadership back on the loaded node, balancing takes it off again).
	balanced := true
	for _, id := range nodes {
		if live[id] && (leaders[id] < leadFloor || leaders[id] > leadCeil) {
			balanced = false
			break
		}
	}
	for _, s := range slots {
		if !balanced {
			break
		}
		if plan.Lead[s] != "" {
			continue
		}
		cur := t.Slots[s].Leader
		want := t.ringLeader(nodes, s)
		if cur == "" || want == "" || want == cur || !live[want] {
			continue
		}
		if !containsNode(kept[s], want) || !containsNode(kept[s], cur) {
			continue
		}
		if leaders[cur] != leaders[want] {
			continue
		}
		// The swap must leave BOTH ends inside the band, not merely start inside
		// it: with equal counts sitting at ceil, handing the slot over pushes the
		// receiver over the band, and the balancing pass moves it straight back —
		// that pair was the two-round loop this rule exists to prevent.
		if leaders[want]+1 > leadCeil || leaders[cur]-1 < leadFloor {
			continue
		}
		plan.Lead[s] = want
		plan.LeadBalance[s] = true
	}

	// Target, and the drop list for the reclaim round. A seat is surplus when
	// the plan does not keep it; the leader is never listed (its set cannot drop
	// below the factor while it writes, and surplusSeatForReplan re-checks both).
	for _, s := range slots {
		plan.Target[s] = append([]string(nil), kept[s]...)
		for _, r := range t.Slots[s].Replicas {
			if containsNode(kept[s], r) {
				continue
			}
			plan.Drop[s] = append(plan.Drop[s], r)
		}
	}
	for s, seatsToDrop := range plan.Drop {
		if len(seatsToDrop) == 0 {
			delete(plan.Drop, s)
		}
	}
	return plan
}

// desiredReplicas is the replica set the plan wants for one slot. The reclaim
// round diffs against it: a seat outside it is the surplus one, and only a
// caught-up replacement makes it reclaimable.
func (t *Table) desiredReplicas(plan SeatPlan, slot int32) []string { return plan.Target[slot] }

// gateOpen answers whether automatic layout movement may arm this round
// (DESIGN §4.5, the even-pool gate).
//
// A pool of odd size cannot satisfy both floor and ceil bands, and every single
// membership change would swing the layout twice (once in, once out), so
// member-driven movement waits until the pool is even again. Two cases are not
// member-driven and stay open: bootstrap (epoch 1, where there is no layout to
// move yet) and a directory that has not changed since the last plan — a
// replica-factor or policy change, which the operator asked for explicitly and
// which changes no membership at all. The gate is applied at layoutStale, the
// only automatic arming path; gating one criterion elsewhere would leave the
// others to re-arm the replan immediately.
func (t *Table) gateOpen() bool {
	if t.LayoutEpoch <= 1 {
		return true
	}
	hosts := t.HostingIDs()
	if len(hosts)%2 == 0 {
		return true
	}
	return sameMembers(t.PlanMembers, hosts)
}

// replaceNode swaps one seat for another in place (order preserved).
func replaceNode(list []string, from, to string) []string {
	out := make([]string, len(list))
	copy(out, list)
	for i, id := range out {
		if id == from {
			out[i] = to
			return out
		}
	}
	return out
}
