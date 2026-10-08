package cluster

import (
	"encoding/json"
	"fmt"
	"sort"
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
}

// Table is the replicated slot assignment table plus the peer directory.
// It is the entire Raft-replicated state of pushupes.
type Table struct {
	SlotCount int32 `json:"slot_count"`
	Replicas  int   `json:"replicas"`
	// Policy is the cluster-wide replica strategy tier behind Replicas; the
	// controller re-derives Replicas from it (clamped to the member count)
	// every round, so the factor tracks both the policy and the membership.
	Policy ReplicaPolicy        `json:"policy"`
	Peers  map[string]Peer      `json:"peers"`
	Slots  map[int32]*Placement `json:"slots"`
}

// NewTable makes an empty table with an explicit replica factor. The policy
// defaults to medium; a table that tracks a tier is built with
// NewTableWithPolicy, which derives the factor from the policy.
func NewTable(slotCount int32, replicaFactor int) *Table {
	return &Table{
		SlotCount: slotCount,
		Replicas:  replicaFactor,
		Policy:    DefaultReplicaPolicy,
		Peers:     map[string]Peer{},
		Slots:     map[int32]*Placement{},
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
	for k, v := range t.Peers {
		out.Peers[k] = v
	}
	for s, p := range t.Slots {
		p2 := *p
		p2.Replicas = append([]string(nil), p.Replicas...)
		out.Slots[s] = &p2
	}
	return out
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

// OnlinePeerIDs lists the peers the cluster may put leadership ON: members
// that are not Offline (self-announced a client address AND not marked down
// by the controller's liveness sweep). This is the single authoritative
// "which nodes can serve clients right now" set: the rebalancer ring and the
// failover leader pick anchor on it, so they can never hand a slot to a node
// clients cannot reach. (plan_slots/replan_slots spread over the FULL
// directory — at bootstrap only one member is registered, and a down member
// loses leadership anyway via mark_down/ring.) Sorted like PeerIDs so the
// layout is a stable function of the table.
func (t *Table) OnlinePeerIDs() []string {
	ids := make([]string, 0, len(t.Peers))
	for id, p := range t.Peers {
		if !p.Offline() {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
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

// PlanSlots spreads all slots over the given nodes (walking assignment):
// slot s prefers node s%N and takes the next replicaFactor
// nodes forward. replicaFactor 0 or >N means all nodes.
func PlanSlots(nodes []string, slotCount int32, replicaFactor int) map[int32]*Placement {
	out := make(map[int32]*Placement, slotCount)
	if len(nodes) == 0 {
		return out
	}
	rf := replicaFactor
	if rf <= 0 || rf > len(nodes) {
		rf = len(nodes)
	}
	for s := int32(0); s < slotCount; s++ {
		start := int(s) % len(nodes)
		replicas := make([]string, 0, rf)
		for i := 0; i < rf; i++ {
			replicas = append(replicas, nodes[(start+i)%len(nodes)])
		}
		out[s] = &Placement{Leader: replicas[0], Replicas: replicas, Epoch: 1, State: SlotStable}
	}
	return out
}

// layoutStale reports whether the replica sets no longer match the member
// directory. Three shapes arm the re-layout:
//
//   - a stable slot whose set size is not the derived replica factor: the
//     fingerprint of a factor change that has not been reconciled yet (a member
//     left, so the factor dropped and the sets now hold surplus seats; or a
//     member joined, so the factor rose and they hold too few);
//   - a member the ring plan gives a seat to that holds no replica seat in ANY
//     stable slot: the layout has left a member out entirely. That is the shape
//     of every membership change that does NOT move the factor — the low and
//     medium tiers hold the factor steady against membership (the single-member
//     clamp is the only place it moves), and the high tier's even counts reuse
//     the odd count below them — and it strands the new member exactly as a
//     factor change does: the ring's expected leader for its slots is not a
//     replica, and the rebalancer only ever hands leadership to an existing
//     replica, so it can never act on them (observed: a low-policy cluster
//     bootstrapped as one node and grown to three never moves a single slot off
//     node-1, and a 2-member cluster grown to three never gives node-3 a single
//     seat);
//   - at factor 1, a stable slot whose leader is not the ring's expected
//     leader: a single-copy set has no spare seat to hold the ring's leader as
//     a follower, so the two shapes above can both miss a slot that is merely
//     in the wrong place — a hand-picked migration committed off the ring
//     leaves the set back at the factor once the surplus reclaim has taken the
//     old seat off, with nobody left out.
//
// applyReplanSlots then ADDS the ring's seats every stable slot is missing;
// that re-seat is the only thing that can put the ring's expected leader into a
// set, and the rebalancer needs it there before it may hand the slot over.
func (t *Table) layoutStale() bool {
	for _, p := range t.Slots {
		if p.State == SlotStable && len(p.Replicas) != t.Replicas {
			return true
		}
	}
	nodes := t.PeerIDs()
	if len(nodes) == 0 {
		return false
	}
	planned := PlanSlots(nodes, t.SlotCount, t.Replicas)
	prescribed := make(map[string]bool, len(nodes))
	for _, p := range planned {
		for _, r := range p.Replicas {
			prescribed[r] = true
		}
	}
	seated := make(map[string]bool, len(nodes))
	for _, p := range t.Slots {
		if p.State != SlotStable {
			continue
		}
		for _, r := range p.Replicas {
			seated[r] = true
		}
	}
	for id := range prescribed {
		if !seated[id] {
			return true
		}
	}
	if t.Replicas != 1 {
		return false
	}
	for s, p := range t.Slots {
		if p.State != SlotStable {
			continue
		}
		if want := planned[s]; want != nil && p.Leader != want.Leader {
			return true
		}
	}
	return false
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
	nodes := t.PeerIDs()
	if len(nodes) == 0 {
		return false
	}
	planned := PlanSlots(nodes, t.SlotCount, t.Replicas)
	stale := t.layoutStale()
	changed := false
	for s, p := range planned {
		cur, ok := t.Slots[s]
		if !ok {
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
		if !stale {
			continue
		}
		grown := unionReplicas(cur.Replicas, p.Replicas)
		if len(grown) != len(cur.Replicas) {
			cur.Replicas = grown
			changed = true
		}
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
	if len(nodes) < 2 {
		return nil
	}
	var out []LeaderMove
	for s, p := range t.Slots {
		if p.State != SlotStable || p.Leader == "" {
			continue
		}
		want := nodes[int(s)%len(nodes)]
		if p.Leader == want || !replicaListHas(p.Replicas, want) {
			continue
		}
		out = append(out, LeaderMove{Slot: s, From: p.Leader, To: want})
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
)

// Command is one replicated metadata mutation.
type Command struct {
	Op     string `json:"op"`
	Peer   *Peer  `json:"peer,omitempty"`
	NodeID string `json:"node_id,omitempty"`
	// For OpLeaderMove / OpSlotState:
	Slots       []int32   `json:"slots,omitempty"`
	NewLeader   string    `json:"new_leader,omitempty"`
	State       SlotState `json:"state,omitempty"`
	MigratingTo string    `json:"migrating_to,omitempty"`
	// For OpMarkDown: the controller's per-slot failover pick (slot -> leader),
	// computed from the replicas' reported LEOs — the freshest live replica holds
	// what was acknowledged, and the table itself cannot know that (no offsets in
	// it). Absent or invalid entries fall back to the table-only rule
	// (backupLeaderFor), so the command stays a deterministic function of the
	// replicated state on every node that applies it.
	NewLeaders map[int32]string `json:"new_leaders,omitempty"`
	// For OpConfig:
	SlotCount int32 `json:"slot_count,omitempty"`
	Replicas  int   `json:"replicas,omitempty"`
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
	case OpSetPolicy:
		policy, err := NormalizeReplicaPolicy(c.Policy)
		if err != nil {
			return fmt.Errorf("set_policy: %w", err)
		}
		t.Policy = policy
		// The factor follows the tier immediately, clamped to the members the
		// directory holds; the layout itself converges on the next replan (the
		// same factor-change path a membership change drives, see DESIGN §4).
		t.Replicas = ReplicaCountForPolicy(policy, len(t.Peers))
	case OpJoinNode:
		if c.Peer == nil || c.Peer.ID == "" {
			return fmt.Errorf("join_node: missing peer")
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
			return fmt.Errorf("register: unknown peer %s", c.Peer.ID)
		}
		p.AdminAddr, p.ClientAddr = c.Peer.AdminAddr, c.Peer.ClientAddr
		t.Peers[c.Peer.ID] = p
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
			return fmt.Errorf("mark_down: unknown peer %s", c.NodeID)
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
			}
		}
	case OpMarkUp:
		p, ok := t.Peers[c.NodeID]
		if !ok {
			return fmt.Errorf("mark_up: unknown peer %s", c.NodeID)
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
		nodes := t.PeerIDs()
		if len(nodes) == 0 {
			return fmt.Errorf("plan_slots: no members")
		}
		t.Slots = PlanSlots(nodes, t.SlotCount, t.Replicas)
	case OpReplanSlots:
		// Converge the layout on the current member directory: fill in any
		// slot that is still unassigned, and add the ring's seats to every
		// stable slot while the layout is stale (applyReplanSlots).
		// The re-layout therefore happens when the factor moves — a policy
		// change (OpSetPolicy, any tier) or, under the high tier, the ODD
		// member counts (3 -> 2 copies, 5 -> 3, 7 -> 4...) — and when the
		// layout has left a member out: an even member count under the high
		// tier leaves the derived count where it was, and the low/medium
		// tiers hold the factor steady against membership, so such a member
		// holds no seat at all, the ring's expected leader for its slots is
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
		if len(t.PeerIDs()) == 0 {
			return fmt.Errorf("replan_slots: no members")
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
		// meantime must not re-create a phantom member).
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
		if peer, ok := t.Peers[preferred]; ok && !peer.Offline() {
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
