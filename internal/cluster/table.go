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
	SlotCount int32                `json:"slot_count"`
	Replicas  int                  `json:"replicas"`
	Peers     map[string]Peer      `json:"peers"`
	Slots     map[int32]*Placement `json:"slots"`
}

// NewTable makes an empty table.
func NewTable(slotCount int32, replicaFactor int) *Table {
	return &Table{
		SlotCount: slotCount,
		Replicas:  replicaFactor,
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
	out := NewTable(t.SlotCount, t.Replicas)
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

// ---- Replicated commands ---------------------------------------------------

// Command ops. Commands are JSON-encoded and applied identically on every
// node, so all tables converge. Data never appears here — only metadata.
const (
	OpJoinNode       = "join_node"        // add a peer to the directory
	OpLeaveNode      = "leave_node"       // remove a peer (also drops it from replicas)
	OpPlanSlots      = "plan_slots"       // (re)spread all slots over current members
	OpLeaderMove     = "leader_move"      // explicit leader change for some slots (migration commit)
	OpSlotState      = "slot_state"       // migration state transition for one slot
	OpConfig         = "config"           // replica factor / slot count at bootstrap
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
	// For OpConfig:
	SlotCount int32 `json:"slot_count,omitempty"`
	Replicas  int   `json:"replicas,omitempty"`
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
		delete(t.Peers, c.NodeID)
		for _, p := range t.Slots {
			p.Replicas = removeString(p.Replicas, c.NodeID)
			if p.Leader == c.NodeID {
				if len(p.Replicas) > 0 {
					p.Leader = p.Replicas[0]
				} else {
					p.Leader = ""
				}
				p.Epoch++
			}
		}
	case OpPlanSlots:
		nodes := t.PeerIDs()
		if len(nodes) == 0 {
			return fmt.Errorf("plan_slots: no members")
		}
		t.Slots = PlanSlots(nodes, t.SlotCount, t.Replicas)
	case OpReplanSlots:
		// Fill in any slot that is still unassigned, and top up replica
		// sets that are below the configured factor (late-joining members).
		// Leaders of assigned, stable slots are never moved — adding a
		// replica is safe because the new replica pulls via fetch.
		nodes := t.PeerIDs()
		if len(nodes) == 0 {
			return fmt.Errorf("replan_slots: no members")
		}
		planned := PlanSlots(nodes, t.SlotCount, t.Replicas)
		for s, p := range planned {
			cur, ok := t.Slots[s]
			if !ok {
				t.Slots[s] = p
				continue
			}
			if cur.State != SlotStable {
				continue // migration in flight; do not touch
			}
			if len(cur.Replicas) < t.Replicas && len(p.Replicas) > len(cur.Replicas) {
				cur.Replicas = p.Replicas
			}
		}
	case OpLeaderMove:
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
