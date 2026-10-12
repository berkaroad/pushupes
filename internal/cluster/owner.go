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

// owner.go — the slot-data provenance fence (DESIGN §3).
//
// Two independent recovery paths meet in one hazard: a node's slot WAL says
// "I hold records 1..N", while the replicated table says "this slot is new".
// That happens when the consensus state is lost or replaced while the data
// dir survives (a wiped <data>/cluster/, a rebuilt cluster the old data dirs
// join). Serving such a copy as leader mixes two seq spaces into one slot:
// reads return the old events without complaint, and writes append on top
// of a version floor the client was never told about.
//
// The fence is a per-slot record — slot-<id>/owner.json — written by the
// node WHILE the table has that slot in this node's replica set (leader or
// seat): {topology, leader, epoch, replica, state}. It is evidence of which
// cluster generation the bytes under it belong to, and the node only serves
// as leader of a slot whose record it can reconcile with the live table:
//
//   - the record names this node's topology id (the data dir was not rebuilt
//     out from under the copy),
//   - its epoch never runs ahead of the table's (a consensus rollback is a
//     fork, not a continuation),
//   - its lineage leads somewhere: it was stamped while this node LED the
//     slot at that placement, or it was stamped while this node HELD A SEAT
//     (the fetch protocol vouched the bytes) and the promotion since then
//     only moved leadership.
//
// A slot that fails the check goes PENDING: the leader refuses appends
// (fail/1007 SlotUnproven) and local reads answer empty. Nothing is deleted
// — the evidence stays — until an operator acts on it explicitly:
// adopt (POST /admin/slots/pending/adopt: "this copy IS this generation's",
// whole list or empty=all, pending slots only) or discard (remove the slot
// directory on disk — deliberately NOT an endpoint: deleting a copy the
// cluster still references through another node's seat forks the slot's
// lineage between the surviving replica and a rebuilt leader log).
// Deleting owner.json by
// hand means exactly "the copy is unproven": a WAL with no record is
// pending whenever it predates the process (it was on disk at startup); a
// WAL that appeared mid-life is this generation's own bytes (migration
// push, fetch catch-up, first writes) and is stamped immediately.
//
// Upgrade note: a cluster adopting this feature starts with no records at
// all. Every leader slot it restarts into is pending by the rule above, and
// one explicit call per node — POST /admin/slots/pending/adopt — stamps
// the whole fleet against the live table, which is the operator confirming
// "my consensus history is continuous". The strict default is deliberate:
// guessing adoption from "the dir looks old" is exactly the ambiguity this
// fence exists to refuse.

package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ownerFileName is the per-slot provenance record inside the slot dir.
const ownerFileName = "owner.json"

// topologyFileName is the per-node generation id inside the consensus dir.
const topologyFileName = "topology"

// slotOwner is the decoded owner.json.
type slotOwner struct {
	Topology string `json:"topology"`          // node generation that wrote it
	Leader   string `json:"leader"`            // slot leader the table named then
	Epoch    int64  `json:"epoch"`             // placement epoch the record was stamped at
	Replica  bool   `json:"replica,omitempty"` // this node held a seat then
	State    string `json:"state,omitempty"`   // slot state the table named then
}

// loadOrInitTopology returns the consensus dir's generation id, creating it
// on first start. The id lives and dies with the dir: wiping the consensus
// state (the fence's trigger scenario) replaces the id, so every surviving
// data dir's records mismatch automatically.
func loadOrInitTopology(dir string) (string, error) {
	path := filepath.Join(dir, topologyFileName)
	b, err := os.ReadFile(path)
	if err == nil {
		id := strings.TrimSpace(string(b))
		if id != "" {
			return id, nil
		}
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("cluster: read topology id: %w", err)
	}
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("cluster: generate topology id: %w", err)
	}
	id := hex.EncodeToString(raw)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(id+"\n"), 0o644); err != nil {
		return "", fmt.Errorf("cluster: write topology id: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", fmt.Errorf("cluster: install topology id: %w", err)
	}
	return id, nil
}

// ownerReason classifies why a copy is unproven (admin view + logs).
type ownerReason string

const (
	ownerOK             ownerReason = ""
	ownerMissing        ownerReason = "owner record absent for a copy that predates this process"
	ownerTopoMismatch   ownerReason = "owner record names a different cluster generation (consensus state was rebuilt)"
	ownerEpochBackward  ownerReason = "owner record's epoch is ahead of the table's (consensus state rewound)"
	ownerLineageUnknown ownerReason = "owner record was stamped for a placement that cannot lead here (no seat, foreign leader)"
)

// ownerAction is what the fence loop does with one slot.
type ownerAction int

const (
	ownerNone    ownerAction = iota // nothing on disk to fence or stamp
	ownerStamp                      // copy proven: (re)write the record if stale
	ownerPending                    // copy unproven while we lead it: refuse service
)

// decideOwner is the whole fence rule as a pure function: it decides how a
// node may treat its LOCAL copy of one slot given the replicated placement.
//
//   - hasWAL: the slot dir holds segment files.
//   - bornWAL: the dir already held segments when this process started
//     (a copy that appeared mid-life was written under the table's own
//     protocol and belongs to this generation by construction).
//   - The fence (pending) can only fall on the LEADER: that is the role that
//     writes into the seq space and answers local reads from the copy.
//   - A SEAT stamps its record only when provenance holds (a matching,
//     current record refreshed; or a copy received while this process ran)
//     — a born copy without a record stays unstamped, so the promotion into
//     leader judges it under the leader rules and an unproven copy can
//     never buy provenance by holding a read-fenced seat.
//   - Not a member of the slot: no verdict here — the post-migration
//     countdown owns surplus copies, and its drop deletes the record with
//     the data (the "authoritative copy elsewhere" path).
func decideOwner(topo, self string, p *Placement, owner slotOwner, hasOwner, hasWAL, bornWAL bool) (ownerAction, ownerReason) {
	if !hasWAL {
		return ownerNone, ownerOK
	}
	lead := p.Leader == self
	if !lead && !replicaListHas(p.Replicas, self) {
		return ownerNone, ownerOK
	}
	if !hasOwner {
		if lead && bornWAL {
			return ownerPending, ownerMissing
		}
		if !lead && bornWAL {
			return ownerNone, ownerOK
		}
		return ownerStamp, ownerOK
	}
	proven := owner.Topology == topo && owner.Epoch <= p.Epoch &&
		(owner.Leader == p.Leader || owner.Replica)
	if proven {
		if owner == desiredOwner(topo, self, p) {
			return ownerNone, ownerOK
		}
		return ownerStamp, ownerOK
	}
	if lead {
		switch {
		case owner.Topology != topo:
			return ownerPending, ownerTopoMismatch
		case owner.Epoch > p.Epoch:
			return ownerPending, ownerEpochBackward
		default:
			return ownerPending, ownerLineageUnknown
		}
	}
	return ownerNone, ownerOK
}

// desiredOwner is the record content the table today implies for this node.
func desiredOwner(topo, self string, p *Placement) slotOwner {
	return slotOwner{
		Topology: topo,
		Leader:   p.Leader,
		Epoch:    p.Epoch,
		Replica:  replicaListHas(p.Replicas, self),
		State:    string(p.State),
	}
}

// ---- on-disk record ----

func ownerPath(dir string) string { return filepath.Join(dir, ownerFileName) }

func readOwner(slotDir string) (slotOwner, bool, error) {
	b, err := os.ReadFile(ownerPath(slotDir))
	if os.IsNotExist(err) {
		return slotOwner{}, false, nil
	}
	if err != nil {
		return slotOwner{}, false, err
	}
	var o slotOwner
	if err := json.Unmarshal(b, &o); err != nil {
		return slotOwner{}, false, fmt.Errorf("owner record %s: %w", ownerPath(slotDir), err)
	}
	return o, true, nil
}

func writeOwner(slotDir string, o slotOwner) error {
	b, err := json.Marshal(o)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(slotDir, 0o755); err != nil {
		return err
	}
	tmp := ownerPath(slotDir) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, ownerPath(slotDir))
}

// ---- engine wiring ----

// PendingSlotView is one fenced slot in the admin view.
type PendingSlotView struct {
	Slot         int32  `json:"slot"`
	Reason       string `json:"reason"`
	TableLeader  string `json:"table_leader"`
	TableEpoch   int64  `json:"table_epoch"`
	OwnerLeader  string `json:"owner_leader,omitempty"`
	OwnerEpoch   int64  `json:"owner_epoch,omitempty"`
	OwnerReplica bool   `json:"owner_replica,omitempty"`
	OwnerTopo    string `json:"owner_topology,omitempty"`
}

// ownerRefreshInterval bounds how long a placement change may go unstamped;
// the apply path signals the loop directly, so this is the safety net for a
// WAL appearing without a table change (first write into a fresh seat).
const ownerRefreshInterval = 2 * time.Second

// runOwnerFence maintains owner.json and the pending set (see the file
// comment). It is signal-driven (every applied table change kicks it) with a
// slow ticker behind, and it only ever WRITES when the implied record
// differs, so a steady-state cluster performs no file IO per round.
func (e *Engine) runOwnerFence(ctx context.Context) {
	tick := time.NewTicker(ownerRefreshInterval)
	defer tick.Stop()
	e.refreshOwners()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-e.ownerKick:
		}
		e.refreshOwners()
	}
}

// topologyID names this node's consensus generation for the fence. A node
// without a consensus Node (unit harnesses) gets one fixed stand-in: the id
// only ever has to match its own previous records, never another node's.
func (e *Engine) topologyID() string {
	if e.node != nil {
		return e.node.TopologyID()
	}
	return "default"
}

// refreshOwners re-decides every slot this node is a member of (its own
// leader slots plus the seats it holds). The fence verdict can only fall on
// a leader slot; seats participate so their records stay current — a record
// stamped for a seat is what lets a later promotion distinguish "bytes the
// fetch protocol vouched" from "born copy of unknown lineage". The per-tick
// cost is one small read per member slot and writes only when the implied
// record differs, so a steady-state cluster does no file IO.
func (e *Engine) refreshOwners() {
	e.tableMu.RLock()
	member := make(map[int32]Placement)
	for s, p := range e.table.Slots {
		if p != nil && (p.Leader == e.self || replicaListHas(p.Replicas, e.self)) {
			member[s] = *p
		}
	}
	e.tableMu.RUnlock()

	topo := e.topologyID()
	next := map[int32]ownerReason{}
	for slot, p := range member {
		p := p
		if !e.store.HasWAL(slot) {
			continue // empty copy: nothing to fence, nothing proven yet
		}
		dir := e.store.SlotDir(slot)
		owner, hasOwner, err := readOwner(dir)
		if err != nil {
			e.logger.Warn("owner fence: unreadable record; the slot stays out of service", "slot", slot, "err", err)
			if p.Leader == e.self {
				next[slot] = ownerReason("owner record unreadable: " + err.Error())
			}
			continue
		}
		act, reason := decideOwner(topo, e.self, &p, owner, hasOwner, true, e.bornWAL(slot))
		switch act {
		case ownerPending:
			next[slot] = reason
		case ownerStamp:
			if err := writeOwner(dir, desiredOwner(topo, e.self, &p)); err != nil {
				e.logger.Warn("owner fence: stamp failed", "slot", slot, "err", err)
			}
		}
	}

	e.ownerMu.Lock()
	e.pendingOwners = next
	e.ownerMu.Unlock()
}

// bornWAL reports whether the slot held segments when this process started
// (the snapshot is taken in NewEngine, before any append can land).
func (e *Engine) bornWAL(slot int32) bool {
	e.ownerMu.Lock()
	defer e.ownerMu.Unlock()
	return e.bornSet[slot]
}

// slotPending reports the fence verdict for one slot's write path. The check
// is map-only (no disk IO): the verdict set is rebuilt by refreshOwners on
// every table change and every WAL transition the loop can observe.
func (e *Engine) slotPending(slot int32) bool {
	e.ownerMu.Lock()
	defer e.ownerMu.Unlock()
	_, ok := e.pendingOwners[slot]
	return ok
}

// kickOwnerFence schedules a fence re-decision after a table change
// (non-blocking; one pending kick is enough).
func (e *Engine) kickOwnerFence() {
	select {
	case e.ownerKick <- struct{}{}:
	default:
	}
}

// PendingSlots is the admin view of this node's fenced copies (every entry
// names the LOCAL node — the endpoint is node-local, not controller-routed).
func (e *Engine) PendingSlots() []PendingSlotView {
	e.ownerMu.Lock()
	set := make(map[int32]ownerReason, len(e.pendingOwners))
	for s, r := range e.pendingOwners {
		set[s] = r
	}
	e.ownerMu.Unlock()
	out := make([]PendingSlotView, 0, len(set))
	e.tableMu.RLock()
	for slot, reason := range set {
		v := PendingSlotView{Slot: slot, Reason: string(reason)}
		if p, ok := e.table.Slots[slot]; ok {
			v.TableLeader, v.TableEpoch = p.Leader, p.Epoch
		}
		if o, has, err := readOwner(e.store.SlotDir(slot)); err == nil && has {
			v.OwnerLeader, v.OwnerEpoch, v.OwnerReplica, v.OwnerTopo = o.Leader, o.Epoch, o.Replica, o.Topology
		}
		out = append(out, v)
	}
	e.tableMu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Slot < out[j].Slot })
	return out
}

// AdoptPending is the owner fence's disposition: stamp the fenced copies so
// they re-enter service as this generation's data. ONLY pending slots can be
// adopted — the pending set is the fence's own verdict, and adopting
// anything else would be declaring provenance for bytes nobody disputed
// (which is to say: no-op at best, papering over a fork at worst). An empty
// want list means "every pending slot" (the one-time upgrade action for a
// cluster adopting the fence); named slots that are not pending are skipped
// and reported back, they are never adopted implicitly. Discarding a copy
// is deliberately NOT an endpoint: the operator removes the slot directory
// on disk, and the next fence round clears the verdict on its own.
func (e *Engine) AdoptPending(want []int32) (adopted int, skipped []int32) {
	e.ownerMu.Lock()
	targets := make([]int32, 0, len(e.pendingOwners))
	if len(want) == 0 {
		for s := range e.pendingOwners {
			targets = append(targets, s)
		}
	} else {
		for _, s := range want {
			if _, ok := e.pendingOwners[s]; !ok {
				skipped = append(skipped, s)
				continue
			}
			targets = append(targets, s)
		}
	}
	e.ownerMu.Unlock()
	sort.Slice(targets, func(i, j int) bool { return targets[i] < targets[j] })

	for _, slot := range targets {
		e.tableMu.RLock()
		p, ok := e.table.Slots[slot]
		var copy Placement
		if ok && p != nil {
			copy = *p
		}
		e.tableMu.RUnlock()
		if !ok || copy.Leader != e.self {
			// The table moved under the verdict (a failover committed between
			// the snapshot and here): the slot is no longer ours to adopt;
			// the fence re-decides it on the next round.
			skipped = append(skipped, slot)
			continue
		}
		if err := writeOwner(e.store.SlotDir(slot), desiredOwner(e.topologyID(), e.self, &copy)); err != nil {
			e.logger.Warn("owner fence: adopt stamp failed", "slot", slot, "err", err)
			skipped = append(skipped, slot)
			continue
		}
		e.ownerMu.Lock()
		delete(e.pendingOwners, slot)
		e.ownerMu.Unlock()
		adopted++
	}
	if adopted > 0 {
		e.loggerf("%d slot(s) adopted: local copies declared this generation's", adopted)
	}
	return adopted, skipped
}
