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
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// SlotState is the lifecycle state of one slot in the assignment table.
type SlotState string

const (
	// SlotStable: normal operation.
	SlotStable SlotState = "stable"
	// SlotMigratingOut: source side of a live migration.
	SlotMigratingOut SlotState = "migrating_out"
	// SlotImportingIn: target side; accepts forwarded writes, not clients.
	SlotImportingIn SlotState = "importing_in"
	// SlotSealingOut: source side of a GENERATION migration, once the destination
	// segment is within sealGap records of the source. The slot still serves reads
	// and still receives the replays, and any record already inside the write
	// fence lands normally — but new client writes are answered 1004 ASK pointing
	// at the destination slot, so the source log stops growing on its own. That is
	// what makes "the source is quiet" a state instead of a sampling guess: the
	// switch is taken when the record gap reaches zero, not when two readings of
	// the log end offset happen to match.
	SlotSealingOut SlotState = "sealing_out"
	// SlotBackingUp: migration committed; old copy kept until retention.
	SlotBackingUp SlotState = "backing_up"
)

// Placement is where one slot lives: its leader, replica set and epoch.
// Epoch increments on every leader change (fencing against stale leaders).
type Placement struct {
	Leader   string    `json:"leader"`
	Replicas []string  `json:"replicas"`
	Epoch    int64     `json:"epoch"`
	State    SlotState `json:"state"`
	// MigratingTo is the target node while State is migrating_out.
	MigratingTo string `json:"migrating_to,omitempty"`
	// SeatGrant labels WHY a node holds a replica seat of this slot: ring =
	// the ring layout gives it this slot (it joined, rejoined, or the ring
	// re-spread after a member left), factor = the replica factor rose and
	// the set had to grow. A ring seat is load the node has to carry, a
	// factor seat is redundancy; the fetch round serves the former first
	// (see HandleMFetchCtx and DESIGN §5).
	SeatGrant map[string]string `json:"seat_grant,omitempty"`
}

// Seat grant kinds (Placement.SeatGrant).
const (
	SeatGrantRing   = "ring"   // the ring layout's own seat: load, not redundancy
	SeatGrantFactor = "factor" // granted because the replica factor rose
)

// stampRingGrants labels every seat of a freshly planned placement as a ring
// grant: a brand-new layout's seats are all the ring's own.
func stampRingGrants(p *Placement) {
	if len(p.Replicas) == 0 {
		return
	}
	p.SeatGrant = make(map[string]string, len(p.Replicas))
	for _, r := range p.Replicas {
		p.SeatGrant[r] = SeatGrantRing
	}
}

// containsNode reports whether a node-id list holds the node.
func containsNode(list []string, id string) bool {
	for _, x := range list {
		if x == id {
			return true
		}
	}
	return false
}

// sameMembers reports whether two node-id lists hold the same members.
func sameMembers(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]struct{}, len(a))
	for _, x := range a {
		seen[x] = struct{}{}
	}
	for _, x := range b {
		if _, ok := seen[x]; !ok {
			return false
		}
	}
	return true
}

// Table is the replicated slot assignment table plus the peer directory.
// It is the entire Raft-replicated state of pushupes.
type Table struct {
	SlotCount int32 `json:"slot_count"`
	Replicas  int   `json:"replicas"`
	// GroupGen / GroupBase / GroupSize / LayoutEpoch describe the slot
	// generation the cluster serves (DESIGN §4.5, §7.2.7): generation g owns
	// [2^g, 2^(g+1)), so GroupBase == GroupSize == 2^g and business routing is
	// slot = size + hash&(size-1). GroupSize == 0 means no generation has been
	// switched on yet (the pre-generation full-table layout); GroupGen is 0
	// then too, and the encoder writes zeros so a v9 snapshot of such a table
	// round-trips as the same thing. LayoutEpoch increments on every
	// generation switch; clients carry it on BatchAppend and are told to
	// refresh their routes when it no longer matches.
	//
	// NOT WIRED YET: PlanSlots still assigns every one of SlotCount slots and
	// the routing call sites still use the flat hash. The fields are replicated
	// and snapshot-encoded so the layout switch has its state in place (see
	// docs/slot-group-layout.md, step D).
	GroupGen    int    `json:"group_gen"`
	GroupBase   int32  `json:"group_base"`
	GroupSize   int32  `json:"group_size"`
	LayoutEpoch uint64 `json:"layout_epoch"`
	// Policy is the cluster-wide replica strategy tier behind Replicas; the
	// controller re-derives Replicas from it (MIN with the data-node count)
	// every round, so the factor tracks both the policy and the membership.
	Policy ReplicaPolicy `json:"policy"`
	// Topology is this cluster's consensus generation: one random id per
	// consensus history, written into the replicated state by the first
	// controller that finds it empty (OpSetTopology) and read from there by
	// every node afterwards. The owner fence compares a data copy's
	// provenance record against it (owner.go), and the layout feed carries it
	// to the data plane, so the value has to be CLUSTER-wide: a per-node id
	// would change when leadership moved between voters, and a plain restart
	// would then declare every copy in the data plane a different generation
	// and refuse to lead it until an operator adopted each node by hand.
	Topology string               `json:"topology,omitempty"`
	Peers    map[string]Peer      `json:"peers"`
	Slots    map[int32]*Placement `json:"slots"`
	// CommitLEO is the per-slot COMMIT FLOOR: the frozen source log end the
	// fence confirmed before the leadership move was committed. It travels
	// with the leader_move command (replicated, so every node has it) and it
	// is monotonic — a later handover of the same slot can only raise it.
	//
	// It exists because the fence's confirmation is about the TARGET's copy at
	// commit time, while clients reach whichever node the table names leader
	// at any later instant. A leader whose local copy still sits below this
	// floor cannot answer a client for a version the previous leader already
	// acknowledged: it must let its fetch catch up first. Serving anyway is
	// exactly the residual 1001 (and, in the worst case, a forked copy).
	CommitLEO map[int32]uint64 `json:"commit_leo,omitempty"`
	// PlanMembers is the member directory the current layout was planned
	// under. A replan that runs while it differs from the directory now grants
	// RING seats (the ring just took the node on); one that runs under the same
	// directory grants FACTOR seats (only the factor grew). See
	// Placement.SeatGrant.
	PlanMembers []string `json:"plan_members,omitempty"`
}

// NewTable makes an empty table with an explicit replica factor. The policy
// defaults to medium; a table that tracks a tier is built with
// NewTableWithPolicy, which derives the factor from the policy.
func NewTable(slotCount int32, replicaFactor int) *Table {
	// A table always serves a segment (there is no "no generation" shape any more):
	// the default is the zero-based one, i.e. the whole table, which is exactly the
	// layout a fresh cluster had before generations existed. A real bootstrap
	// replaces it with the 2^g generation the node was started for (OpPlanSlots).
	return &Table{
		SlotCount: slotCount,
		Replicas:  replicaFactor,
		Policy:    DefaultReplicaPolicy,
		Peers:     map[string]Peer{},
		Slots:     map[int32]*Placement{},
		CommitLEO: map[int32]uint64{},
		GroupGen:  -1,
		GroupBase: 0,
		GroupSize: slotCount,
	}
}

// NewTableWithPolicy makes an empty table seeded with a replica policy tier;
// the replica factor starts at the policy's value for a single-member cluster
// and the controller re-derives it from the member count every round. This is
// the entry point the startup flag seeds a brand-new cluster through.
func NewTableWithPolicy(slotCount int32, policy ReplicaPolicy) *Table {
	if policy == "" {
		policy = DefaultReplicaPolicy
	}
	return &Table{
		SlotCount: slotCount,
		Replicas:  ReplicaCountForPolicy(policy, 1),
		Policy:    policy,
		Peers:     map[string]Peer{},
		Slots:     map[int32]*Placement{},
		CommitLEO: map[int32]uint64{},
		// Same default segment as NewTable: a table always serves one.
		GroupGen:  -1,
		GroupBase: 0,
		GroupSize: slotCount,
	}
}

// tableReplicaShortfall reports whether any stable slot has fewer replicas
// than the configured factor (e.g. after a member joins late).
func tableReplicaShortfall(t *Table) bool {
	for _, p := range t.Slots {
		if p.State == SlotStable && len(p.Replicas) < t.Replicas {
			return true
		}
	}
	return false
}

// Clone returns a deep copy (used to stage Raft command effects).
func (t *Table) Clone() *Table {
	out := NewTableWithPolicy(t.SlotCount, t.Policy)
	out.Replicas = t.Replicas
	// The generation is replicated state like any other field: a clone that
	// dropped it would route the next append into the wrong segment.
	out.GroupGen = t.GroupGen
	out.GroupBase = t.GroupBase
	out.GroupSize = t.GroupSize
	out.LayoutEpoch = t.LayoutEpoch
	// Topology: the cluster generation is part of the replicated state like
	// any other field — a clone that dropped it would fence every copy the
	// holder of the clone leads.
	out.Topology = t.Topology
	for k, v := range t.Peers {
		out.Peers[k] = v
	}
	for s, p := range t.Slots {
		p2 := *p
		p2.Replicas = append([]string(nil), p.Replicas...)
		if p.SeatGrant != nil {
			p2.SeatGrant = make(map[string]string, len(p.SeatGrant))
			for k, v := range p.SeatGrant {
				p2.SeatGrant[k] = v
			}
		}
		out.Slots[s] = &p2
	}
	out.PlanMembers = append([]string(nil), t.PlanMembers...)
	if len(t.CommitLEO) > 0 {
		out.CommitLEO = make(map[int32]uint64, len(t.CommitLEO))
		for s, leo := range t.CommitLEO {
			out.CommitLEO[s] = leo
		}
	}
	return out
}

// validateGroup checks the replicated generation fields. Base is stored only
// so a corrupt snapshot cannot silently route into the wrong range: it must
// equal the size, and the size must be the power of two its generation names.
// The segment has to fit the fixed table (a generation of 2^g occupies
// [2^g, 2^(g+1))), which also keeps the reserved slot-000 out of it.
func (t *Table) validateGroup() error {
	if t.GroupSize == 0 {
		if t.GroupBase != 0 || t.GroupGen != 0 {
			return fmt.Errorf("table: generation fields set with size 0 (gen=%d base=%d)", t.GroupGen, t.GroupBase)
		}
		return nil
	}
	if t.GroupSize < 0 || t.GroupSize&(t.GroupSize-1) != 0 {
		return fmt.Errorf("table: generation size %d is not a power of two", t.GroupSize)
	}
	// A zero-based segment (base 0, the whole table) is what a test table and the
	// development shape serve: it makes "every slot id participates" expressible
	// without a second routing rule. Production generations are 2^g (base == size).
	if t.GroupBase == 0 {
		if t.GroupGen != -1 || t.GroupSize != t.SlotCount {
			return fmt.Errorf("table: zero-based segment must cover the %d-slot table (gen=%d size=%d)",
				t.SlotCount, t.GroupGen, t.GroupSize)
		}
		return nil
	}
	if t.GroupBase != t.GroupSize {
		return fmt.Errorf("table: generation base %d != size %d", t.GroupBase, t.GroupSize)
	}
	if t.GroupGen < 0 || 1<<uint(t.GroupGen) != int(t.GroupSize) {
		return fmt.Errorf("table: generation %d does not match size %d", t.GroupGen, t.GroupSize)
	}
	if t.GroupSize > t.SlotCount/2 {
		return fmt.Errorf("table: generation size %d exceeds half the %d-slot table", t.GroupSize, t.SlotCount)
	}
	return nil
}

// PeerIDs returns the sorted member list.
func (t *Table) PeerIDs() []string {
	ids := make([]string, 0, len(t.Peers))
	for id := range t.Peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// HostingIDs is the layout pool: directory members that may carry slot
// replicas (role = learner, registered). plan_slots/replan_slots spread over
// THIS set and the policy's factor derives from its size: the control plane's
// voters keep their machine to themselves — a raft timing must never share a
// host with a data-plane load, which is the point of splitting the planes.
// planCandidates is the layout pool: hosting members a plan or replan may
// spread slots over, minus the ones marked draining. A draining node stays
// in the directory (its existing seats, read replicas and all, stay until
// the removal or the re-admission resolves them) but no NEW seat lands on
// it — seats added to a leaving node would only be moved out again, twice
// the data motion for zero effect. Same all-marked fallback philosophy as
// OnlinePeerIDs: a pool of nothing is not a plan.
func (t *Table) planCandidates() []string {
	pool := t.HostingIDs()
	keep := pool[:0:0]
	for _, id := range pool {
		if p, ok := t.Peers[id]; ok && p.Draining {
			continue
		}
		keep = append(keep, id)
	}
	if len(keep) == 0 {
		return pool
	}
	return keep
}

func (t *Table) HostingIDs() []string {
	ids := make([]string, 0, len(t.Peers))
	for id, p := range t.Peers {
		if p.Hosts() {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// OnlinePeerIDs lists the peers the cluster may put leadership ON: members
// that are not Offline (self-announced a client address AND not marked down
// by the controller's liveness sweep) and may host slots. This is the single authoritative
// "which nodes can serve clients right now" set: the rebalancer ring and the
// failover leader pick anchor on it, so they can never hand a slot to a node
// clients cannot reach. (plan_slots/replan_slots spread over the FULL
// directory — at bootstrap only one member is registered, and a down member
// loses leadership anyway via mark_down/ring.) Sorted like PeerIDs so the
// layout is a stable function of the table.
func (t *Table) OnlinePeerIDs() []string {
	ids := make([]string, 0, len(t.Peers))
	live := make([]string, 0, len(t.Peers))
	for id, p := range t.Peers {
		if !p.Offline() && p.Hosts() {
			live = append(live, id)
			if !p.Draining {
				ids = append(ids, id)
			}
		}
	}
	if len(ids) == 0 {
		ids = live // a cluster whose every reachable node is draining still
		// needs leaders — drain marks shed nodes from the layout, they do
		// not empty the pool out from under live traffic
	}
	sort.Strings(ids)
	return ids
}

// HasDraining reports whether any directory member carries the drain mark
// (the rebalancer spends a wider per-round budget on handovers while true).
func (t *Table) HasDraining() bool {
	for _, p := range t.Peers {
		if p.Draining {
			return true
		}
	}
	return false
}

// LeaderOf returns the leader node and epoch for a slot.
func (t *Table) LeaderOf(slot int32) (string, int64, bool) {
	p, ok := t.Slots[slot]
	if !ok {
		return "", 0, false
	}
	return p.Leader, p.Epoch, true
}

// ReplicasOf returns the replica set of a slot.
func (t *Table) ReplicasOf(slot int32) []string {
	p, ok := t.Slots[slot]
	if !ok {
		return nil
	}
	return p.Replicas
}

// SlotGroup is the slot generation the table serves (DESIGN §4.5, §7.2.7):
// generation g owns the slot range [2^g, 2^(g+1)), so Base == Size == 2^g.
// Size 0 means no generation has been switched on — routing then falls back to
// the flat hash over the whole table, the pre-generation rule.
type SlotGroup struct {
	Gen   int
	Base  int32
	Size  int32
	Epoch uint64
}

// Physical converts a logical slot id (0 .. Size-1) into the table's slot id.
// The client plane speaks logical ids only: it never learns the base, and the
// base is exactly what tells two generations apart on the same disk layout.
func (g SlotGroup) Physical(logical int32) int32 { return g.Base + logical }

// Logical converts a table slot id back into the generation's logical id. A
// slot outside the segment keeps its raw id — that cannot happen for a slot the
// segment owns, and returning the raw value makes a bug visible instead of
// silently pointing at the wrong row.
func (g SlotGroup) Logical(physical int32) int32 {
	if physical < g.Base || physical >= g.Base+g.Size {
		return physical
	}
	return physical - g.Base
}

// PlanSlotsInRange spreads the slots of one generation segment [from, until)
// over the given nodes: slot s prefers nodes[(s-from)%N] and takes the next
// replicaFactor nodes forward. The walking assignment is relative to the
// segment so a generation's leaders do not depend on where its base sits in
// the table.
func PlanSlotsInRange(nodes []string, from, until int32, replicaFactor int) map[int32]*Placement {
	out := make(map[int32]*Placement, until-from)
	if len(nodes) == 0 || until <= from {
		return out
	}
	rf := replicaFactor
	if rf <= 0 || rf > len(nodes) {
		rf = len(nodes)
	}
	for s := from; s < until; s++ {
		start := int(s-from) % len(nodes)
		replicas := make([]string, 0, rf)
		for i := 0; i < rf; i++ {
			replicas = append(replicas, nodes[(start+i)%len(nodes)])
		}
		out[s] = &Placement{Leader: replicas[0], Replicas: replicas, Epoch: 1, State: SlotStable}
	}
	return out
}

// planSlots lays out the slots this table serves: the generation's segment
// when one is switched on, the whole table otherwise (the pre-generation
// layout every table starts as).
func (t *Table) planSlots(nodes []string) map[int32]*Placement {
	if t.GroupSize <= 0 {
		// Unreachable: every table is built with a segment (NewTable seeds the
		// zero-based one) and validation enforces it. Returning nothing beats
		// inventing a layout if that invariant ever breaks.
		return map[int32]*Placement{}
	}
	return PlanSlotsInRange(nodes, t.GroupBase, t.GroupBase+t.GroupSize, t.Replicas)
}

// segmentStaged reports whether a generation migration has already planned the
// given segment: every slot of it exists and is importing_in, which is the state
// OpStageGroup puts them in. The switch uses this to decide between promoting the
// staged layout (records already replayed into it) and planning a fresh one.
func (t *Table) segmentStaged(base, size int32) bool {
	if size <= 0 {
		return false
	}
	for s := base; s < base+size; s++ {
		p, ok := t.Slots[s]
		if !ok || p.State != SlotImportingIn {
			return false
		}
	}
	return true
}

// ringLeader is the node the walking assignment gives one slot, in the same
// segment-relative frame planSlots uses: the leader rebalancer must agree with
// the plan or every round would hand a slot back to a node the plan does not
// want.
func (t *Table) ringLeader(nodes []string, slot int32) string {
	if len(nodes) == 0 {
		return ""
	}
	off := slot
	if t.GroupSize > 0 {
		off = slot - t.GroupBase
	}
	if off < 0 {
		return ""
	}
	return nodes[int(off)%len(nodes)]
}

// ParseSlotGroup turns "g3" (or "3") into the generation it names. The number
// is the generation index: g0 = 1 slot, g10 = 1024. The segment [2^g, 2^(g+1))
// must fit inside the table, which is why the table is twice the largest
// generation (DESIGN §7.2.7).
func ParseSlotGroup(name string, slotCount int32) (SlotGroup, error) {
	txt := strings.TrimSpace(strings.ToLower(name))
	txt = strings.TrimPrefix(txt, "g")
	g, err := strconv.Atoi(txt)
	if err != nil || g < 0 || g > 10 {
		return SlotGroup{}, fmt.Errorf("slot group %q: want g0..g10", name)
	}
	size := int32(1) << uint(g)
	if size > slotCount/2 {
		return SlotGroup{}, fmt.Errorf("slot group %q: segment size %d does not fit a %d-slot table", name, size, slotCount)
	}
	return SlotGroup{Gen: g, Base: size, Size: size, Epoch: 1}, nil
}

// layoutStale reports whether the layout no longer satisfies DESIGN §4.5: a
// stable slot that does not hold its share of distinct candidate seats, or a
// candidate whose seat count sits outside the floor/ceil band.
//
// It is one question asked of one function — "does DesiredSeats have anything
// to do" — so the arming criterion and the movement it arms can never disagree.
// The old shape-by-shape check was three hand-written criteria; each new seat
// rule (factor 1, a member with no seat at all, a set that shrank) had to add
// its own, and a criterion that the re-layout did not actually fix kept firing.
//
// The even-pool gate is applied HERE because this is the only automatic arming
// path: gating one criterion elsewhere would leave the others to re-arm the
// replan immediately (DESIGN §4.5 偶数闸作用域).
func (t *Table) layoutStale() bool {
	if !t.gateOpen() {
		return false
	}
	return !t.DesiredSeats().Empty()
}

// applyReplanSlots re-derives the slot layout on the current member directory:
// every unassigned slot is planned, and — while the layout is stale (a factor
// change, a member the layout has left out, or a single-copy slot sitting off
// the ring: see layoutStale) — every stable slot ADDS the ring's seats it does
// not hold yet. It reports whether it changed the table, which is also
// the controller's test for whether a replan is worth a Raft entry (the
// controller runs it on a clone of the snapshot before submitting; the same
// function is what OpReplanSlots applies).
//
// Adding is what unblocks the layout: the rebalancer hands leadership back to
// the ring's expected node only when that node is a REPLICA of the slot, so a
// set left stale by a factor change — too small after seats went with a removed
// member, or missing the ring's expected leader altogether — strands its slot
// off the ring for good. Re-seating puts the expected leader back in the set as
// a follower, and it catches up over the ordinary fetch protocol; the
// rebalancer can then fence a hand-over to it.
//
// A re-seat never REMOVES a seat (the mirror direction is the reclaim, which
// runs against real copies — see reclaimRound and surplusSeatForReplan). The
// seats a stale set holds are the ones that have been replicating the slot;
// dropping one the moment the factor moves would take a slot's only caught-up
// copy out of its replica set while the ring's seats were still empty, and the
// failure path picks a slot's new leader from the front of this list
// (backupLeaderFor) — the same order argument that keeps the inherited seats
// AHEAD of the ones this function appends. Convergence to the factor is
// therefore two steps: add here, then reclaim the surplus once the kept copies
// are in sync.
//
// Leaders are never moved: a slot whose current leader leaves the ring layout
// keeps leading it until the rebalancer's fenced hand-over moves it (the
// leader's own copy is never surplus while it leads, see copyIsSurplus). The
// one exception is a slot with NO leader at all — the shape the failure path
// leaves behind when the member that left held a slot's only copy: there is no
// leadership to disturb and no rebalancer to hand the slot over (it skips an
// empty leader), so the slot adopts the ring's expected leader and bumps its
// epoch, which is the leader change's fence.
// Non-stable slots belong to a migration in flight and are never touched.
func (t *Table) applyReplanSlots() bool {
	nodes := t.planCandidates()
	if len(nodes) == 0 {
		return false
	}
	planned := t.planSlots(nodes)
	plan := t.DesiredSeats()
	changed := false
	membersMoved := !sameMembers(t.PlanMembers, nodes)
	for s, p := range planned {
		cur, ok := t.Slots[s]
		if !ok {
			// A slot the table assigns for the first time gets the ring's own
			// seats, so they read as load like any other first plan.
			stampRingGrants(p)
			t.Slots[s] = p
			changed = true
			continue
		}
		if cur.State != SlotStable {
			continue
		}
		// A slot left without a writer: the failure path empties the leader
		// when the member that left held the slot's only copy (a single-copy
		// layout has no other replica to hand leadership to — backupLeaderFor
		// returns ""), and nothing else assigns a leader to an ASSIGNED slot:
		// PlanLeaderRebalance skips an empty leader by contract, and the full
		// plan only ever runs on an empty table. Adopt the ring's expected
		// leader — the seat this same call adds below — so the slot has a
		// writer again; the epoch bump is the leader change's fence, exactly
		// as on every other path that hands a slot to a different node.
		if cur.Leader == "" {
			cur.Leader = p.Leader
			cur.Epoch++
			changed = true
		}
	}
	// Seats come from the minimal-movement plan, not from the ring: the ring
	// re-derives every set, which is right for a first plan and wrong for a
	// membership change (it would move ~K·RF/N seats instead of the deficit).
	// A re-seat never REMOVES a seat — the mirror direction is the reclaim
	// round, which runs against real copies (surplusSeatForReplan) once the
	// kept copies are in sync; dropping a seat here would take the slot's only
	// caught-up copy out of the set while the replacement was still empty.
	slotIDs := make([]int32, 0, len(plan.Add))
	for s := range plan.Add {
		slotIDs = append(slotIDs, s)
	}
	sort.Slice(slotIDs, func(i, j int) bool { return slotIDs[i] < slotIDs[j] })
	for _, s := range slotIDs {
		cur, ok := t.Slots[s]
		if !ok || cur.State != SlotStable {
			continue
		}
		kind := SeatGrantFactor
		if membersMoved {
			kind = SeatGrantRing
		}
		if cur.SeatGrant == nil {
			cur.SeatGrant = map[string]string{}
		}
		for _, add := range plan.Add[s] {
			if replicaListHas(cur.Replicas, add) {
				continue
			}
			cur.Replicas = append(cur.Replicas, add)
			cur.SeatGrant[add] = kind
			changed = true
		}
	}
	if membersMoved {
		t.PlanMembers = append([]string(nil), t.HostingIDs()...) // full pool (see OpPlanSlots)
		changed = true
	}
	return changed
}

// unionReplicas returns the current seats with the plan's missing ones appended.
// The current order is preserved on purpose: the failure path takes a slot's new
// leader from the front of the list, so the seats that have actually been
// replicating the slot must stay ahead of the ones this re-layout just added.
func unionReplicas(cur, want []string) []string {
	out := append([]string(nil), cur...)
	for _, w := range want {
		if !replicaListHas(out, w) {
			out = append(out, w)
		}
	}
	return out
}

// refillAfterLeave re-seats the slots a just-removed member held, exactly as
// far as the factor needs (DESIGN §4.5 删除成员 ③): each short slot takes the
// candidate carrying the fewest seats that is not already in the set (the
// replica set never names a node twice, and the leader is a set member, so the
// "not co-located with the leader" rule is the same check).
//
// Deterministic by construction: the seat census is taken once, slots are
// visited by ascending id and candidates by sorted id, so every node replaying
// the leave command computes the same seats. Only the slots that lost a seat
// are visited — this is a re-fill, not the layout re-plan, and it never
// touches a slot the departed node was not part of.
func (t *Table) refillAfterLeave() {
	nodes := t.planCandidates()
	if len(nodes) == 0 {
		return // nothing to seat on; the slot is re-seated when a data node joins
	}
	want := t.Replicas
	if len(nodes) < want {
		want = len(nodes) // I1a: serve with one copy per node
	}
	seats := make(map[string]int, len(nodes))
	for _, id := range nodes {
		seats[id] = 0
	}
	var short []int32
	stable := 0
	for s, p := range t.Slots {
		for _, r := range p.Replicas {
			if _, ok := seats[r]; ok {
				seats[r]++
			}
		}
		if p.State != SlotStable {
			continue
		}
		stable++
		if len(p.Replicas) < want {
			short = append(short, s)
		}
	}
	sort.Slice(short, func(i, j int) bool { return short[i] < short[j] })
	if len(short) == 0 {
		return
	}

	// The leave has to land on the layout the next replan would compute, or the
	// removal arms one more round of movement — and on an even pool that round
	// really runs, moving seats nobody asked to move. "The replan's layout" for a
	// census is exact, not greedy: the total is fixed at stable × want, so every
	// node lands on floor and the remainder goes to the nodes already carrying
	// the most (that is the smallest movement). A plain "fewest seats first" fill
	// drifts by one on every tie and leaves the table stale.
	total := stable * want
	base, rem := total/len(nodes), total%len(nodes)
	bySeats := append([]string(nil), nodes...)
	sort.SliceStable(bySeats, func(i, j int) bool {
		if seats[bySeats[i]] != seats[bySeats[j]] {
			return seats[bySeats[i]] > seats[bySeats[j]]
		}
		return bySeats[i] < bySeats[j]
	})
	quota := make(map[string]int, len(nodes))
	for _, id := range nodes {
		quota[id] = base
	}
	for i := 0; i < rem; i++ {
		quota[bySeats[i]]++
	}

	// Each node's remaining claim on the freed seats. Spending this counter (not
	// a live "who is emptiest" comparison) is what makes the result exact: a
	// greedy compare re-picks the same node whenever its gap ties with another,
	// which is how a leave ends up one seat short and arms a replan.
	demand := make(map[string]int, len(nodes))
	for _, id := range nodes {
		if d := quota[id] - seats[id]; d > 0 {
			demand[id] = d
		}
	}
	added := make(map[int32][]string, len(short))
	for _, s := range short {
		p := t.Slots[s]
		for len(p.Replicas) < want {
			pick := ""
			for _, id := range nodes {
				if replicaListHas(p.Replicas, id) || demand[id] <= 0 {
					continue
				}
				if pick == "" || demand[id] > demand[pick] || (demand[id] == demand[pick] && id < pick) {
					pick = id
				}
			}
			if pick == "" {
				// Every claim is spent (or the holder already seats this slot):
				// complete the set from the emptiest remaining node so the factor
				// never dips, even if that leaves the census one off.
				for _, id := range nodes {
					if replicaListHas(p.Replicas, id) {
						continue
					}
					if pick == "" || seats[id] < seats[pick] {
						pick = id
					}
				}
			}
			if pick == "" {
				break
			}
			p.Replicas = append(p.Replicas, pick)
			if p.SeatGrant == nil {
				p.SeatGrant = map[string]string{}
			}
			p.SeatGrant[pick] = SeatGrantRing
			added[s] = append(added[s], pick)
			seats[pick]++
			if demand[pick] > 0 {
				demand[pick]--
			}
		}
	}

	// A claim can be unreachable for the slots that are short: the last slot may
	// already seat the only node still owed one, and every established seat the
	// over-quota node holds may be a leadership or a co-seat with the starved one.
	// Even it out here, because the alternative is leaving the census off by one
	// and arming a replan the operator never asked for. Order matters: a seat this
	// refill just added (a new follower, nothing to move) first, then an
	// established follower seat, and only then a leader's seat — that last case
	// hands leadership to another copy of the same slot first (metadata only, the
	// epoch bump is the fence), which is the same hand-over the rebalancer would
	// have performed a round later.
	for pass := 0; pass < len(short); pass++ {
		rich, poor := "", ""
		for _, id := range nodes {
			if seats[id] > quota[id] && (rich == "" || seats[id] > seats[rich]) {
				rich = id
			}
			if seats[id] < quota[id] && (poor == "" || seats[id] < seats[poor]) {
				poor = id
			}
		}
		if rich == "" || poor == "" {
			break
		}
		swap := func(s int32, mayMoveLeader bool) bool {
			p := t.Slots[s]
			if p == nil || p.State != SlotStable {
				return false
			}
			if !replicaListHas(p.Replicas, rich) || replicaListHas(p.Replicas, poor) {
				return false
			}
			if p.Leader == rich {
				if !mayMoveLeader {
					return false
				}
				next := ""
				for _, r := range p.Replicas {
					if r != rich {
						next = r
						break
					}
				}
				if next == "" {
					return false
				}
				p.Leader = next
				p.Epoch++
			}
			for i, r := range p.Replicas {
				if r == rich {
					p.Replicas[i] = poor
					break
				}
			}
			if p.SeatGrant != nil {
				delete(p.SeatGrant, rich)
				p.SeatGrant[poor] = SeatGrantRing
			}
			seats[rich]--
			seats[poor]++
			return true
		}
		ordered := func() []int32 {
			out := make([]int32, 0, len(t.Slots))
			for s := range t.Slots {
				out = append(out, s)
			}
			sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
			return out
		}
		done := false
		// 1) seats this refill added.
		fresh := make([]int32, 0, len(added))
		for s := range added {
			fresh = append(fresh, s)
		}
		sort.Slice(fresh, func(i, j int) bool { return fresh[i] < fresh[j] })
		for _, s := range fresh {
			if swap(s, false) {
				done = true
				break
			}
		}
		// 2) established follower seats, then 3) leader seats.
		if !done {
			for _, s := range ordered() {
				if swap(s, false) {
					done = true
					break
				}
			}
		}
		if !done {
			for _, s := range ordered() {
				if swap(s, true) {
					done = true
					break
				}
			}
		}
		if !done {
			break
		}
	}
}

// surplusSeatForReplan picks the seat a re-layout may drop once the kept copies
// are caught up, or "" when the placement is not oversized. Pure, so the choice
// is directly assertable.
//
// The surplus is a seat the ring plan does not prescribe: the re-layout added
// the plan's seats (applyReplanSlots) but never removes any, so a slot whose set
// outgrew the factor converges on the plan by dropping exactly these. The leader
// is never eligible (a slot without a writer cannot accept appends), and the
// choice is sorted so every node replaying the table agrees on it — the same
// determinism rule the migration's surplusForReclaim follows.
func surplusSeatForReplan(p *Placement, factor int, plan []string) string {
	if p == nil || p.State != SlotStable || len(p.Replicas) <= factor {
		return ""
	}
	rest := make([]string, 0, len(p.Replicas))
	for _, r := range p.Replicas {
		if r == p.Leader || replicaListHas(plan, r) {
			continue
		}
		rest = append(rest, r)
	}
	if len(rest) == 0 {
		return ""
	}
	sort.Strings(rest)
	return rest[0]
}

// LeaderMove is one rebalance step: hand slot s from its current leader to
// To. Pure metadata (a controller-side plan entry, never replicated itself).
type LeaderMove struct {
	Slot int32
	From string
	To   string
	// Balance marks a move whose only purpose is the leader census (DESIGN
	// §4.5 I2): the target already holds a seat, and the move happens only when
	// that copy is CAUGHT UP. A balancing move never stages a new copy and never
	// repairs a diverged one — it costs a fence, not a slot transfer, which is
	// the whole reason it may run on its own schedule.
	Balance bool
}

// PlanLeaderRebalance computes the leader layout the ring policy prescribes
// and lists the stable slots that currently deviate from it. The ring is the
// exact output of PlanSlots over the sorted member ids (DESIGN §默认拓扑:
// leader(s) = nodes[s % N]), so a table this function returns no moves for is
// byte-identical in its leader assignment to a freshly planned one.
//
// The anchor is the RING, not a free "balance the counts" search: after a
// failover the expected leader is back on the layout it had before the node
// died — the behaviour is deterministic and explainable (and survives
// controller restarts: the plan is a pure function of the table). The cost of
// anchoring is that a hand-picked placement a rebalance can express as a move
// will be moved back; that is the accepted trade-off for a converging layout.
//
// The expected leader is the ring over the ONLINE member directory (members
// only — Table.OnlinePeerIDs: a peer with no announced client addr, or
// one the controller has marked down, is not a node clients can reach, and
// leadership must not land on it). During a failure the plan balances
// leadership across the surviving online nodes, and once the failed node
// answers probes again and re-announces (and replan_slots has re-admitted it
// to the replica sets) the same function hands its slots back. A deviating slot is only a
// candidate when its expected leader is a replica of the slot: moving a
// leader onto a node without the data is a full migration, not a rebalance,
// and belongs to the operator. Replica sets still short of the factor are
// left to replan_slots' top-up; the rebalancer never adds members.
// Non-stable slots are owned by a live migration — never touched.
func PlanLeaderRebalance(t *Table) []LeaderMove {
	nodes := t.OnlinePeerIDs()
	// Leadership targets come from the same plan the seats do (DESIGN §4.5):
	// balancing seats against one anchor and leaders against another is how a
	// cluster ends up chasing its own layout every round.
	plan := t.DesiredSeats()
	lead, leadBalance := plan.Lead, plan.LeadBalance
	// The ring half of the plan needs two live hosts to have anything to
	// say (one candidate = it already leads what it leads). The DRAIN half
	// does not: when the pool shrinks to "only the node being drained is
	// online", refusing everything would strand its slots on it forever —
	// the ring's only answer is to hand each slot to the non-draining
	// replica anyway, and a slot whose set holds nobody else waits for the
	// operator (replan tops it up once a peer is admitted). Same
	// fall-back-to-the-possible philosophy as OnlinePeerIDs all-marked.
	drainPool := false
	if len(nodes) < 2 {
		if !t.HasDraining() {
			return nil
		}
		drainPool = true
	}
	var out []LeaderMove
	for s, p := range t.Slots {
		if p.State != SlotStable || p.Leader == "" {
			continue
		}
		if drainPool {
			if d, known := t.Peers[p.Leader]; !known || !d.Draining {
				continue // no ring: only slots on the draining node move
			}
		}
		if d, known := t.Peers[p.Leader]; known && d.Draining {
			// A draining leader is deviated by definition, whatever the
			// ring says: hand it to a live, non-draining member of the
			// slot's own replica set (the set order is the table's, so the
			// pick is deterministic on every node). A set with no such
			// member waits for the operator — the ring cannot seat the
			// slot elsewhere from here.
			next := ""
			for _, r := range p.Replicas {
				if r == p.Leader {
					continue
				}
				if rp, ok := t.Peers[r]; ok && !rp.Offline() && rp.Hosts() && !rp.Draining {
					next = r
					break
				}
			}
			if next != "" {
				out = append(out, LeaderMove{Slot: s, From: p.Leader, To: next})
			}
			continue
		}
		if drainPool {
			continue // non-draining leader, ring has no say at pool size 1
		}
		// The anchor is the seat plan, not the ring: the target replica sets
		// are the ones DesiredSeats laid out, so the leader it wants is a member
		// of the set by construction (the ring's pick often is not any more).
		// Only the plan speaks. The ring used to be the fallback anchor, and it
		// cannot be any more: the ring names a leader for every slot, the plan
		// only names the ones that must move, and the two disagree the moment
		// replica sets stop matching the ring — which is the normal state under
		// minimal movement. Keeping the ring as a fallback made the two chase
		// each other (a balancing hand-over on slot s, then the ring pulling it
		// back next round, forever).
		want := lead[s]
		if want == "" {
			continue
		}
		if p.Leader == want || !replicaListHas(p.Replicas, want) {
			continue
		}
		out = append(out, LeaderMove{Slot: s, From: p.Leader, To: want, Balance: leadBalance[s]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slot < out[j].Slot })
	return out
}

// layoutSettled reports whether every placement is stable: a slot in any
// migration state means a live hand-over owns the layout, and the leader
// rebalancer yields to it rather than starting a competing move mid-flight.
func layoutSettled(t *Table) bool {
	for _, p := range t.Slots {
		if p.State != SlotStable {
			return false
		}
	}
	return true
}

// ---- Replicated commands ---------------------------------------------------

// Command ops. Commands are JSON-encoded and applied identically on every
// node, so all tables converge. Data never appears here — only metadata.
const (
	OpJoinNode  = "join_node"  // add a peer to the directory
	OpLeaveNode = "leave_node" // remove a peer (also drops it from replicas)
	// OpDrainNode toggles a member's drain mark (c.NodeID, c.Drain).
	// Draining is the graceful half of a scale-down: leadership flows off
	// the node through the same handover path a rebalance move uses, so
	// running clients keep answering while the operator prepares the stop.
	OpDrainNode = "drain_node"
	// OpMarkDown / OpMarkUp carry the controller's liveness verdict into the
	// replicated directory. Down marks a peer offline WITHOUT deleting its
	// entry (leave_node deletes it, and the raft-config directory sync then
	// re-seeds it address-less — the pair of them made a failed node flicker
	// in the console) and moves the leadership of its slots to live
	// replicas. The replica set is left intact: when the node answers probes
	// again MarkUp clears the flag, its fetch loop catches the copy up, and
	// the rebalancer hands its ring slots back.
	OpMarkDown   = "mark_down"
	OpMarkUp     = "mark_up"
	OpPlanSlots  = "plan_slots"  // (re)spread all slots over current members
	OpLeaderMove = "leader_move" // explicit leader change for some slots (migration commit)
	OpSlotState  = "slot_state"  // migration state transition for one slot
	OpConfig     = "config"      // replica factor / slot count at bootstrap
	// OpSetPolicy changes the cluster-wide replica policy tier (low / medium /
	// high). The command carries only the policy: the factor it implies is
	// derived at apply time from the member count, so the command stays a
	// deterministic function of the replicated state like every other op.
	OpSetPolicy      = "set_policy"
	OpReplanSlots    = "replan_slots"     // only plan slots that are still unassigned
	OpRegister       = "register"         // patch a peer's self-announced admin/client addrs
	OpSlotAddReplica = "slot_add_replica" // add one member to a slot's replica set
	// OpSlotRemoveReplica removes one member from a slot's replica set
	// (reclaim a surplus copy, e.g. the former source of a committed
	// migration). Non-leader only: dropping the leader would strand the slot.
	OpSlotRemoveReplica = "slot_remove_replica"
	// OpSetTopology records this cluster's consensus generation — one random
	// id per consensus history (Table.Topology). The controller submits it
	// once, before anything plans, so every later reader (the owner fence,
	// the layout feed) reads one cluster-wide value instead of whichever
	// voter happens to be leading.
	OpSetTopology = "set_topology"
	// OpSetGroup switches the served slot generation (DESIGN §4.5, §7.2.7). The
	// controller refuses it while the current generation holds records — the
	// segment migration has to move them first — so a committed OpSetGroup is
	// always a switch from an empty segment to a fresh one. Apply re-plans the
	// new segment and bumps LayoutEpoch, which is what clients compare against.
	OpSetGroup = "set_group"
	// OpStageGroup plans the DESTINATION segment of a generation migration
	// (DESIGN §4.5.8) without switching to it: the slots appear in the table as
	// importing_in — they have a leader and can take replayed records, but the
	// routing still names the current segment, so no client can reach them yet.
	// The switch itself is still OpSetGroup, taken once the replay has landed.
	OpStageGroup = "stage_group"
)

// Command is one replicated metadata mutation.
type Command struct {
	Op     string `json:"op"`
	Peer   *Peer  `json:"peer,omitempty"`
	NodeID string `json:"node_id,omitempty"`
	// For OpLeaderMove / OpSlotState:
	// CommitLEO carries the frozen source tail the commit fence confirmed for
	// these slots (0 = unknown, e.g. an offline source with no writer to
	// fence). It becomes Table.CommitLEO on every node that applies the move.
	CommitLEO   uint64    `json:"commit_leo,omitempty"`
	Slots       []int32   `json:"slots,omitempty"`
	NewLeader   string    `json:"new_leader,omitempty"`
	State       SlotState `json:"state,omitempty"`
	MigratingTo string    `json:"migrating_to,omitempty"`
	// GroupGen / GroupBase / GroupSize carry the slot generation when the
	// FIRST plan (bootstrap) establishes it. A zero GroupSize means "no
	// generation here": a replan never changes the served generation, and a
	// later switch is an OpSetGroup's job.
	GroupGen  int   `json:"group_gen,omitempty"`
	GroupBase int32 `json:"group_base,omitempty"`
	GroupSize int32 `json:"group_size,omitempty"`
	// For OpMarkDown: the controller's per-slot failover pick (slot -> leader),
	// computed from the replicas' reported LEOs — the freshest live replica holds
	// what was acknowledged, and the table itself cannot know that (no offsets in
	// it). Absent or invalid entries fall back to the table-only rule
	// (backupLeaderFor), so the command stays a deterministic function of the
	// replicated state on every node that applies it.
	NewLeaders map[int32]string `json:"new_leaders,omitempty"`
	// For OpMarkDown: per-slot the best durable LEO seen on the live
	// freshness probe — the evidence that a promoted leader must at least
	// hold. Recorded as the slot's commit floor when it exceeds the current
	// one, independent of which node the pick lands on (a slot the table rule
	// takes over still gains its bound). Slots the probe could not reach
	// carry nothing.
	NewLeaderLEO map[int32]uint64 `json:"new_leader_leo,omitempty"`
	// For OpDrainNode:
	Drain      bool `json:"drain,omitempty"`
	ThenRemove bool `json:"then_remove,omitempty"` // drain mark + removal intent
	// For OpConfig:
	SlotCount int32 `json:"slot_count,omitempty"`
	Replicas  int   `json:"replicas,omitempty"`
	// For OpSetTopology: the consensus generation to record (Table.Topology).
	Topology string `json:"topology,omitempty"`
	// For OpSetPolicy: the tier name (low/medium/high).
	Policy string `json:"policy,omitempty"`
}

// Encode marshals a command for Raft.
func (c *Command) Encode() []byte {
	b, _ := json.Marshal(c)
	return b
}

// DecodeCommand unmarshals a Raft command.
func DecodeCommand(b []byte) (*Command, error) {
	var c Command
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// Apply mutates the table per a command; it is the single source of state
// transition logic and must be deterministic across nodes.
func (t *Table) Apply(c *Command) error {
	switch c.Op {
	case OpConfig:
		if c.SlotCount > 0 {
			t.SlotCount = c.SlotCount
		}
		if c.Replicas > 0 {
			t.Replicas = c.Replicas
		}
	case OpSetTopology:
		// First writer wins: a generation is decided once per consensus
		// history. A later submit — a stale controller, or a node that
		// booted with a fresh id of its own — must not overwrite it, or
		// every copy in the data plane would be fenced against a value its
		// provenance records cannot match.
		if t.Topology == "" && c.Topology != "" {
			t.Topology = c.Topology
		}
	case OpSetPolicy:
		policy, err := NormalizeReplicaPolicy(c.Policy)
		if err != nil {
			return fmt.Errorf("set_policy: %w", err)
		}
		t.Policy = policy
		// The factor follows the policy immediately, MIN'd with the members the
		// directory holds; the layout itself converges on the next replan (the
		// same factor-change path a membership change drives, see DESIGN §4).
		t.Replicas = ReplicaCountForPolicy(policy, len(t.HostingIDs()))
	case OpJoinNode:
		if c.Peer == nil || c.Peer.ID == "" {
			return fmt.Errorf("join_node: missing peer")
		}
		// A re-admission of a node already in the directory (the operator
		// bringing it back, or its own startup self-admission) is the
		// declaration that it serves again: the drain and removal marks
		// clear with it. Addresses and role refresh from the announcement.
		if old, existed := t.Peers[c.Peer.ID]; existed {
			c.Peer.Draining = false
			c.Peer.PendingRemove = false
			_ = old
		}
		t.Peers[c.Peer.ID] = *c.Peer
	case OpRegister:
		// Self-announced data-plane addresses (single-port peer config):
		// patch the entry in place. The peer MUST already be in the table
		// (created by the controller's join command) — registration never
		// adds members on its own, membership is Raft-configuration-owned.
		if c.Peer == nil || c.Peer.ID == "" || c.Peer.AdminAddr == "" || c.Peer.ClientAddr == "" {
			return fmt.Errorf("register: incomplete announcement")
		}
		p, ok := t.Peers[c.Peer.ID]
		if !ok {
			// An announcement only ever patches an entry that exists: the
			// announcer keeps announcing on its heartbeat, so a join that lands
			// later is picked up by the next round. Failing here would write an
			// unapplyable entry into the log (every data node's feed stalls on
			// it) for an announcement that beat its own admission — a real
			// cold-boot ordering, since a node announces at 1s while its join
			// offer waits on the 3s adopt interval. Old logs written before the
			// submit-side guard exist too; they must replay without errors.
			return nil
		}
		p.AdminAddr, p.ClientAddr = c.Peer.AdminAddr, c.Peer.ClientAddr
		if c.Peer.Role != "" {
			// The announcement carries the node's -role (voter or learner)
			// and the node's flag is the only authority on it. A role-less
			// form exists only in logs written before roles landed; replay
			// keeps the recorded role for those.
			p.Role = c.Peer.Role
		}
		t.Peers[c.Peer.ID] = p
	case OpDrainNode:
		p, ok := t.Peers[c.NodeID]
		if !ok {
			return fmt.Errorf("drain_node: unknown node %q", c.NodeID)
		}
		if p.Draining == c.Drain && p.PendingRemove == c.ThenRemove {
			return nil // idempotent toggle
		}
		p.Draining = c.Drain
		p.PendingRemove = c.Drain && c.ThenRemove // the intent only rides a drain mark
		t.Peers[c.NodeID] = p
	case OpLeaveNode:
		// Purge the node from the directory AND from every placement that
		// still names it — leader, replica seat and the migration label. A
		// dangling name is not cosmetic: the slot stays owned by a node the
		// cluster does not know, and a slot left in migrating_out towards a
		// removed node never settles (layoutSettled stays false, so the
		// rebalancer yields to a migration that can never commit).
		delete(t.Peers, c.NodeID)
		for _, p := range t.Slots {
			p.Replicas = removeString(p.Replicas, c.NodeID)
			if p.SeatGrant != nil {
				delete(p.SeatGrant, c.NodeID)
			}
			if p.SeatGrant != nil {
				delete(p.SeatGrant, c.NodeID)
			}
			if p.MigratingTo == c.NodeID {
				p.MigratingTo = ""
				if p.State == SlotMigratingOut || p.State == SlotImportingIn {
					p.State = SlotStable // the migration's target is gone: abandon it
				}
			}
			if p.Leader == c.NodeID {
				if len(p.Replicas) > 0 {
					p.Leader = p.Replicas[0]
				} else {
					p.Leader = ""
				}
				p.Epoch++
			}
		}
		// ③ Seats the departing node held are re-filled on the same command
		// (DESIGN §4.5 删除成员). Leaving them short would hold the replica
		// factor down until the next layout round, and a slot that had no
		// second copy left would be serving on one machine in the meantime.
		// Only the slots that named the departed node are touched: no other
		// slot moves, and this is not the layout re-plan, so it does not count
		// against the even-pool gate (the removal itself is an operator
		// action, and waiting for an even pool would strand a dead node).
		t.refillAfterLeave()
	case OpMarkDown:
		// The liveness verdict of the controller, replicated. The directory
		// entry STAYS (addresses and replica-set membership included): the
		// console keeps showing the node — flagged offline — instead of it
		// flickering between deleted and re-seeded-address-less, and the
		// returning node's copy is still a replica the fetch loop can catch
		// up. Only leadership moves: every slot this node led gets a live
		// replica as leader (preferring one that is itself active).
		p, ok := t.Peers[c.NodeID]
		if !ok {
			// The entry is gone (a removal committed between the sweep's
			// snapshot and this apply). A verdict about a node the directory
			// no longer holds says nothing: the leave already purged its
			// leadership and seats, and erroring here would take every data
			// node's layout feed down over a command that has no state to
			// land on (see the same rule in OpRegister).
			return nil
		}
		if !p.Down {
			p.Down = true
			t.Peers[c.NodeID] = p
		}
		for s, pl := range t.Slots {
			if pl.Leader != c.NodeID {
				continue
			}
			if next := t.failoverLeader(pl, c.NodeID, c.NewLeaders[s]); next != "" {
				pl.Leader = next
				pl.Epoch++
				// The freshness evidence becomes the slot's commit floor
				// (monotone): whoever was seated — pick or table rule — is
				// now bounded below by the best log the live probe saw.
				if leo := c.NewLeaderLEO[s]; leo > 0 {
					if t.CommitLEO == nil {
						t.CommitLEO = map[int32]uint64{}
					}
					if leo > t.CommitLEO[s] {
						t.CommitLEO[s] = leo
					}
				}
			}
		}
	case OpMarkUp:
		p, ok := t.Peers[c.NodeID]
		if !ok {
			// Same rule as OpMarkDown: a recovered node the directory no longer
			// holds stays out until it joins again (its own offer re-admits it),
			// and a no-op keeps the feeds alive.
			return nil
		}
		if p.Down {
			p.Down = false
			t.Peers[c.NodeID] = p
		}
	case OpPlanSlots:
		// Placement spreads over the FULL member directory: at bootstrap no
		// peer but oneself has announced addresses yet, and a plan that
		// "respected the online set" here would put every slot on the first
		// voter and converge only through the slow rebalancer. Registration
		// lands within seconds; a member that is genuinely down at plan
		// time loses leadership anyway (mark_down moved it, the ring gate
		// keeps it off the leaders until it returns). The ACTIVE set is the
		// rebalancer's and the failover pick's domain, not the layout's.
		nodes := t.planCandidates()
		if len(nodes) == 0 {
			return fmt.Errorf("plan_slots: no data-plane members (every directory entry is a voter)")
		}
		// A bootstrap plan may carry the generation (Engine.SetBootGroup): apply
		// it BEFORE laying the slots out, so the first plan covers the
		// generation's segment instead of the whole table. Only an empty table
		// adopts it; a replan never does.
		if c.GroupSize > 0 && len(t.Slots) == 0 {
			t.GroupGen, t.GroupBase, t.GroupSize = c.GroupGen, c.GroupBase, c.GroupSize
			if t.LayoutEpoch == 0 {
				t.LayoutEpoch = 1
			}
			if err := t.validateGroup(); err != nil {
				return err
			}
		}
		t.Slots = t.planSlots(nodes)
		for _, p := range t.Slots {
			stampRingGrants(p)
		}
		// The directory record is the FULL pool, never the filtered spread:
		// layoutStale/layoutSettled compare it against HostingIDs to answer
		// "did the member set change", and recording the drain-filtered list
		// under it would make that answer permanently "yes" — every round a
		// new replan, every gate stuck unsettled, the drain's own handover
		// rounds starved. Filtering changes WHO RECEIVES seats, not who the
		// directory HOLDS.
		t.PlanMembers = append([]string(nil), t.HostingIDs()...)
	case OpStageGroup:
		if c.GroupSize <= 0 {
			return fmt.Errorf("stage_group: size %d must be positive", c.GroupSize)
		}
		if c.GroupSize <= t.GroupSize {
			return fmt.Errorf("stage_group: %d is not larger than the served segment %d", c.GroupSize, t.GroupSize)
		}
		nodes := t.planCandidates()
		if len(nodes) == 0 {
			return fmt.Errorf("stage_group: no data-plane members")
		}
		planned := PlanSlotsInRange(nodes, c.GroupBase, c.GroupBase+c.GroupSize, t.Replicas)
		for s, p := range planned {
			if _, exists := t.Slots[s]; exists {
				continue // already staged (a retried command)
			}
			// Importing: the slot has a writer and takes replayed records, but it
			// is not part of the served segment, so client routing cannot find it.
			p.State = SlotImportingIn
			stampRingGrants(p)
			t.Slots[s] = p
		}
	case OpSetGroup:
		if c.GroupSize <= 0 {
			return fmt.Errorf("set_group: size %d must be positive", c.GroupSize)
		}
		if t.GroupGen == c.GroupGen && t.GroupBase == c.GroupBase && t.GroupSize == c.GroupSize {
			return nil // idempotent replay of the switch that already landed
		}
		prevGen, prevBase, prevSize := t.GroupGen, t.GroupBase, t.GroupSize
		t.GroupGen, t.GroupBase, t.GroupSize = c.GroupGen, c.GroupBase, c.GroupSize
		if err := t.validateGroup(); err != nil {
			t.GroupGen, t.GroupBase, t.GroupSize = prevGen, prevBase, prevSize
			return err
		}
		t.LayoutEpoch++
		nodes := t.planCandidates()
		if len(nodes) == 0 {
			return fmt.Errorf("set_group: no data-plane members (every directory entry is a voter)")
		}
		if t.segmentStaged(c.GroupBase, c.GroupSize) {
			// A generated migration staged this segment: its slots already hold the
			// replayed records, so the switch PROMOTES them instead of planning a
			// fresh layout. Re-planning here would hand each slot to whatever node
			// the ring names now and throw away the copies the replay just wrote.
			for s, p := range t.Slots {
				if s >= c.GroupBase && s < c.GroupBase+c.GroupSize && p.State == SlotImportingIn {
					p.State = SlotStable
				}
			}
			// The segment being left stops being part of the table. Its data is
			// reclaimed afterwards, slot by slot, by the retiring half of the
			// migration (a table entry that lingered would keep the old slots in
			// every layout computation: seat plans, reclaim, health scans).
			for s := range t.Slots {
				if prevSize > 0 && s >= prevBase && s < prevBase+prevSize {
					delete(t.Slots, s)
				}
			}
		} else {
			t.Slots = t.planSlots(nodes)
			for _, p := range t.Slots {
				stampRingGrants(p)
			}
		}
		t.PlanMembers = append([]string(nil), t.HostingIDs()...)
	case OpReplanSlots:
		// Converge the layout on the current member directory: fill in any
		// slot that is still unassigned, and add the ring's seats to every
		// stable slot while the layout is stale (applyReplanSlots).
		// The re-layout therefore happens when the factor moves — a policy
		// change (OpSetPolicy; the tiers are fixed numbers, so membership
		// alone never moves the factor) — and when the layout has left a
		// member out: a membership change holds the factor where it is, so
		// without a re-seat such a member holds no seat at all, the ring's
		// expected leader for its slots is
		// not a replica, and the rebalancer can never act on them (see
		// layoutStale). The re-seat seats the new member as the follower of
		// the slots the ring gives it; the ordinary fetch and the rebalance
		// converge the rest — DESIGN §4.
		// Leaders of assigned, stable slots are
		// never moved — a slot with NO leader (the failure path leaves one
		// behind when the departing member held a slot's only copy) adopts
		// the ring's expected leader, which is the seat this same call adds —
		// re-seating is safe because every added replica pulls
		// via fetch before the rebalancer may hand leadership to it, and a
		// dropped seat is reclaimed by its holder (localdrop). A marked-down
		// member KEEPS its replica seats (that is how its copy is still home
		// when it returns); the sets are derived from the full directory for
		// the same reason the plan uses it above.
		if len(t.HostingIDs()) == 0 {
			return fmt.Errorf("replan_slots: no data-plane members")
		}
		t.applyReplanSlots()
	case OpLeaderMove:
		// The new leader must be a directory member. A migration staged
		// before a leave_node and committed after it would otherwise re-seat
		// the removed node as the slot's leader — and append it to the
		// replica set below — leaving a placement that names a node the
		// directory (and the Raft configuration) no longer holds. That is
		// exactly how a real cluster ended up with 238 slots led by a node
		// that had been removed, and with a snapshot no node could restore.
		if _, ok := t.Peers[c.NewLeader]; !ok {
			return fmt.Errorf("leader_move: unknown node %q", c.NewLeader)
		}
		// Handovers INTO a draining node are off the table: a scale-down
		// that moved slots out and then moved one back would ping-pong the
		// data. Moving a slot OFF a draining node rides a different field
		// (the slot's current leader), so this guard never blocks the
		// drain itself — and failover picks route around draining nodes at
		// the submit side (only-last-resort), never relying on this apply.
		if p, ok := t.Peers[c.NewLeader]; ok && p.Draining {
			return fmt.Errorf("leader_move: target %q is draining", c.NewLeader)
		}
		for _, s := range c.Slots {
			p, ok := t.Slots[s]
			if !ok {
				return fmt.Errorf("leader_move: unknown slot %d", s)
			}
			if p.Leader != c.NewLeader {
				p.Leader = c.NewLeader
				p.Epoch++
			}
			p.State = SlotStable
			p.MigratingTo = ""
			// ensure the new leader is in the replica set
			found := false
			for _, r := range p.Replicas {
				if r == c.NewLeader {
					found = true
					break
				}
			}
			if !found {
				p.Replicas = append(p.Replicas, c.NewLeader)
			}
			if c.CommitLEO > 0 {
				if t.CommitLEO == nil {
					t.CommitLEO = map[int32]uint64{}
				}
				if c.CommitLEO > t.CommitLEO[s] {
					t.CommitLEO[s] = c.CommitLEO
				}
			}
		}
	case OpSlotAddReplica:
		// Join a member to a slot's replica set (the node then follows the
		// slot over the ordinary fetch protocol, exactly like a replica a
		// later `replan_slots` top-up would have added). A migration to a
		// node outside the replica set stages this first: the snapshot and
		// catch-up steps only work against a node that follows the slot.
		if c.NodeID == "" {
			return fmt.Errorf("slot_add_replica: missing node")
		}
		// Same invariant as leader_move: only a directory member may be
		// seated (a staged admission whose target was removed in the
		// meantime must not re-create a phantom member), and never a
		// draining one — a migration or seat top-up pointed at a leaving
		// node would re-fill the pool the drain is emptying.
		if p, ok := t.Peers[c.NodeID]; ok && p.Draining {
			return fmt.Errorf("slot_add_replica: target %q is draining", c.NodeID)
		}
		if _, ok := t.Peers[c.NodeID]; !ok {
			return fmt.Errorf("slot_add_replica: unknown node %q", c.NodeID)
		}
		for _, s := range c.Slots {
			p, ok := t.Slots[s]
			if !ok {
				return fmt.Errorf("slot_add_replica: unknown slot %d", s)
			}
			found := false
			for _, r := range p.Replicas {
				if r == c.NodeID {
					found = true
					break
				}
			}
			if !found {
				p.Replicas = append(p.Replicas, c.NodeID)
				// A staged admission seats a hand-over target: the copy it
				// builds is load it is about to take over, so its catch-up
				// leads the factor seats in the fetch round.
				if p.SeatGrant == nil {
					p.SeatGrant = map[string]string{}
				}
				p.SeatGrant[c.NodeID] = SeatGrantRing
			}
		}
	case OpSlotRemoveReplica:
		// Reclaim one member from a slot's replica set — the mirror of
		// slot_add_replica. The branch is a deterministic, idempotent
		// function of the current state (it is replayed on every node and
		// may be retried): an absent member is a no-op, and replaying after
		// a successful removal does nothing. Only the named slot changes,
		// and neither leader nor epoch move — dropping a non-leader replica
		// is not a leadership change. The leader is never removable: a slot
		// without a writer cannot accept appends.
		if c.NodeID == "" {
			return fmt.Errorf("slot_remove_replica: missing node")
		}
		for _, s := range c.Slots {
			p, ok := t.Slots[s]
			if !ok {
				return fmt.Errorf("slot_remove_replica: unknown slot %d", s)
			}
			if p.Leader == c.NodeID {
				return fmt.Errorf("slot_remove_replica: %s leads slot %d", c.NodeID, s)
			}
			found := false
			for _, r := range p.Replicas {
				if r == c.NodeID {
					found = true
					break
				}
			}
			if !found {
				continue // not a replica: nothing to do, no error
			}
			p.Replicas = removeString(p.Replicas, c.NodeID)
			if p.SeatGrant != nil {
				delete(p.SeatGrant, c.NodeID)
			}
		}
	case OpSlotState:
		for _, s := range c.Slots {
			p, ok := t.Slots[s]
			if !ok {
				return fmt.Errorf("slot_state: unknown slot %d", s)
			}
			if c.State != "" {
				p.State = c.State
			}
			p.MigratingTo = c.MigratingTo
		}
	default:
		return fmt.Errorf("unknown op %q", c.Op)
	}
	return nil
}

func removeString(list []string, s string) []string {
	out := list[:0]
	for _, v := range list {
		if v != s {
			out = append(out, v)
		}
	}
	return out
}

// failoverLeader picks the new leader of a slot whose leader just failed over.
//
// preferred is the controller's pick (Command.NewLeaders): the replica whose
// reported LEO was the highest among the live ones. Honouring it matters because
// the alternative — the first live replica in the replica-set order — can be a
// copy that is BEHIND, and the records the dead leader had already acknowledged
// live only in the leader's log: handing those slots to a lagging replica loses
// them silently. The controller is the only node that can see the replicas'
// offsets, so the pick travels in the command and is validated here (pure table
// state, so every node applies the same verdict):
//
//   - a directory member,
//   - a replica of THIS slot,
//   - not the failed node itself,
//   - not itself offline (a second dead node is not a leader).
//
// Anything else falls back to backupLeaderFor, the deterministic table-only
// rule, so a command without a pick (or with one that no longer applies, e.g.
// after a leave_node raced it) still converges.
func (t *Table) failoverLeader(p *Placement, down, preferred string) string {
	if preferred != "" && preferred != down && replicaListHas(p.Replicas, preferred) {
		if peer, ok := t.Peers[preferred]; ok && !peer.Offline() && !peer.Draining {
			return preferred
		}
	}
	return t.backupLeaderFor(p, down)
}

// backupLeaderFor picks the new leader of a slot whose leader just went down:
// the first replica that is not itself offline (a live node clients can
// reach); if every other replica is offline too, fall back to the first other
// replica so leadership at least leaves the failed node. Deterministic: the
// replica list order is the same on every node (the table is replicated), so
// Apply converges. "" when the set holds nobody else.
func (t *Table) backupLeaderFor(p *Placement, down string) string {
	// A draining node ranks last among LIVE candidates: it is reachable and
	// could hold the slot, but seating leadership on a node the operator is
	// removing re-fills the pool the drain is emptying (and every acked
	// write it takes then has to move again). Only chosen when it is all
	// that is alive — leadership stranded on nobody starves the slot.
	for _, r := range p.Replicas {
		if r == down {
			continue
		}
		if peer, ok := t.Peers[r]; ok && !peer.Offline() && !peer.Draining {
			return r
		}
	}
	for _, r := range p.Replicas {
		if r == down {
			continue
		}
		if peer, ok := t.Peers[r]; ok && !peer.Offline() {
			return r
		}
	}
	for _, r := range p.Replicas {
		if r != down {
			return r
		}
	}
	return ""
}

// EncodeTable serialises the whole table for snapshots.
func (t *Table) EncodeTable() []byte {
	b, _ := json.Marshal(t)
	return b
}

// DecodeTable restores a table from a snapshot.
func DecodeTable(b []byte) (*Table, error) {
	var t Table
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, err
	}
	if t.Slots == nil {
		t.Slots = map[int32]*Placement{}
	}
	if t.Peers == nil {
		t.Peers = map[string]Peer{}
	}
	return &t, nil
}
