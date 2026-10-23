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
// The replicated table is the provenance authority: a controller seats a
// node on a slot only for copies its fetch protocol has tracked, so a live
// placement already names the generation the bytes belong to. Every slot
// dir in this node's table picture — leader OR seat, WAL present or not —
// carries the per-slot record owner.json: {topology, leader, epoch,
// replica, state}. A placement without data yet gets its record too: the
// record is the copy's birth certificate, and any bytes that appear under
// it afterwards were written under this table.
//
// The promise the console depends on: any change of a slot's leader installs
// the new leader's record on disk WITH the table apply (ApplyCommand's
// synchronous stamp), so leadership arriving here never waits on a fence
// tick, never fences, and never reads as "unproven". The fence round
// (refreshOwners: every table kick plus a 2s tick) re-verifies every member
// slot and rewrites records the layout aged out — zero file IO at steady
// state, since a record is written only when the table-implied content
// differs.
//
// One hazard lives OUTSIDE the table: a whole data dir predating this
// cluster generation — a mispointed -data, or consensus state rebuilt under
// the same node id. The dir answers that with its own admission stamp,
// <data>/admission.json {topology}, written the first time a live table
// applies here. In a dir stamped for a DIFFERENT generation, a member slot
// with no record is unproven: the leader refuses appends (fail/1007
// SlotUnproven, reason foreignDir) and local reads answer empty until an
// operator acts explicitly — adopt (POST /admin/slots/pending/adopt: "this
// dir's copies ARE this generation's"; adopting also refreshes the admission
// stamp, which is exactly what that declaration means) or discard (remove
// the slot directory on disk — deliberately NOT an endpoint: deleting a
// copy the cluster still references through another node's seat forks the
// slot's lineage between the surviving replica and a rebuilt leader log).
//
// Records that exist are still held to the table: a record naming another
// topology id, or running its epoch ahead of the placement's, or claiming a
// lineage this node cannot lead under, fences the leader copy
// (topoMismatch / epochBackward / lineageUnknown). A record matching the
// table is refreshed whenever the implied content differs.

package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	ownerForeignDir     ownerReason = "the data dir was admitted to a different cluster generation; this copy predates the admission"
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
//   - strangerDir: the dir-level admission stamp names a different
//     generation than the live table — the whole data dir predates this
//     admission, the one fact the table itself cannot see.
//   - The fence (pending) can only fall on the LEADER: that is the role that
//     writes into the seq space and answers local reads from the copy.
//   - A member slot WITHOUT a record is proven by its table placement: the
//     controller seated this node for that copy, so the record is written
//     (ownerStamp) — with or without a WAL, leader or seat. The one
//     exception is a stranger dir: there a record-less leader copy that
//     ALREADY HOLDS DATA waits for the operator's adopt (an empty placement
//     is just a seat — no bytes, nothing to dispute, no stamp to risk),
//     and a record-less seat is left unstamped, so a promotion judges it
//     under the same leader rule.
//   - hasWAL: whether the slot dir holds segment files (fence input only —
//     a WAL-less placement still gets its birth certificate when the dir
//     belongs here).
//   - A record that EXISTS is held to the table: matching topology, epoch
//     not ahead of the placement's, and a lineage this node can lead (it
//     named us leader, or seated us). Proven and current ⇒ nothing; proven
//     and aged ⇒ refresh; unproven while leading ⇒ pending.
//   - Not a member of the slot: no verdict here — the post-migration
//     countdown owns surplus copies, and its drop deletes the record with
//     the data (the "authoritative copy elsewhere" path).
func decideOwner(topo, self string, p *Placement, owner slotOwner, hasOwner, hasWAL, strangerDir bool) (ownerAction, ownerReason) {
	lead := p.Leader == self
	if !lead && !replicaListHas(p.Replicas, self) {
		return ownerNone, ownerOK
	}
	if !hasOwner {
		if strangerDir {
			// Only bytes can dispute; an empty placement in a stranger
			// dir is just a seat — nothing predates anything yet.
			if lead && hasWAL {
				return ownerPending, ownerForeignDir
			}
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

// stampPlacement is the table-apply path's synchronous half of the fence
// promise: when a leadership (re)arrival lands here, the owner record is
// installed WITH the apply, not one fence tick later — the new leader never
// serves (or waits) without it. Idempotent by the implied-content check, and
// silent on a stranger dir: adoption there is the operator's call, not the
// table's.
func (e *Engine) stampPlacement(s int32, p *Placement) {
	if p == nil || p.Leader != e.self {
		return
	}
	topo := e.topologyID()
	if e.strangerDir(topo) {
		return
	}
	want := desiredOwner(topo, e.self, p)
	dir := e.store.SlotDir(s)
	// clearFence lifts this slot's local fence verdict NOW, in the apply
	// path. The replicated table names self leader and the on-disk record
	// is (or already was) the matching proof — both halves of what the
	// fence loop would re-derive are in hand, and waiting for its next
	// tick left every append to a freshly promoted slot answering 1007
	// for up to a full tick: a 512-slot re-plan fenced whole seconds of
	// client traffic (the err_1007 storm behind a scale-down).
	clearFence := func() {
		e.ownerMu.Lock()
		delete(e.pendingOwners, s)
		e.ownerMu.Unlock()
	}
	if cur, has, err := readOwner(dir); err == nil && has && cur == want {
		clearFence()
		return
	}
	if err := writeOwner(dir, want); err != nil {
		e.logger.Warn("owner fence: apply-path stamp failed", "slot", s, "err", err)
		return
	}
	clearFence()
}

// stampAppliedSlots is the ApplyCommand hook behind the apply-path promise:
// it stamps every slot whose leadership this commit landed on self (and, for
// a full re-plan, walks the table for exactly those slots). Slots untouched
// by the commit cost nothing; a re-plan costs one small read per leading
// slot, and re-plans are rare by construction.
func (e *Engine) stampAppliedSlots(c *Command) {
	e.tableMu.RLock()
	type rec struct {
		s int32
		p Placement
	}
	want := make([]rec, 0, len(c.Slots))
	if c.Op == OpPlanSlots {
		for s, p := range e.table.Slots {
			if p != nil && p.Leader == e.self {
				want = append(want, rec{s, *p})
			}
		}
	} else if len(c.Slots) > 0 {
		for _, s := range c.Slots {
			if p := e.table.Slots[s]; p != nil && p.Leader == e.self {
				want = append(want, rec{s, *p})
			}
		}
	}
	e.tableMu.RUnlock()
	for _, r := range want {
		p := r.p
		e.stampPlacement(r.s, &p)
	}
}

// ---- dir-level admission stamp ----

// admissionFileName is the data-dir stamp naming the cluster generation this
// dir was first admitted under. It answers the one hazard the replicated
// table cannot see: a whole dir that predates the generation (a mispointed
// -data, or consensus state rebuilt under the same node id).
const admissionFileName = "admission.json"

type admission struct {
	Topology string `json:"topology"`
}

func admissionPath(dataDir string) string { return filepath.Join(dataDir, admissionFileName) }

// strangerDir consults the admission stamp: a missing stamp is written with
// the live topology (this is the dir's first table — the common first boot
// and every re-admission of a dir this generation already owns), a matching
// stamp says "this dir belongs here", and a mismatched one makes the
// record-less member slots of this dir unproven until an adopt declares
// otherwise (AdoptPending refreshes the stamp — that declaration IS its
// meaning). A stamp that cannot be read fails open toward the table: the
// placement, not the dir's paperwork, remains the primary proof.
func (e *Engine) strangerDir(topo string) bool {
	b, err := os.ReadFile(admissionPath(e.store.Dir))
	if os.IsNotExist(err) {
		e.writeAdmissionStamp(topo)
		return false
	}
	if err != nil {
		return false
	}
	var a admission
	if json.Unmarshal(b, &a) != nil || a.Topology == "" {
		e.writeAdmissionStamp(topo)
		return false
	}
	return a.Topology != topo
}

func (e *Engine) writeAdmissionStamp(topo string) {
	b, err := json.Marshal(admission{Topology: topo})
	if err != nil {
		return
	}
	if err := writeFileSticky(admissionPath(e.store.Dir), b, e.store.Dir); err != nil {
		e.logger.Warn("owner fence: admission stamp could not be written", "err", err)
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
	return writeFileSticky(filepath.Join(slotDir, ownerFileName), b, slotDir)
}

// writeFileAtomic lands data at path through a uniquely named temp file in
// dir, so concurrent writers of the same target each rename a COMPLETE file
// (the last one wins — every reader sees one whole record, never two halves
// of different ones). A fixed temp name would interleave: two writers, two
// open(...TRUNC) at the same offset stream, one garbage file — and garbage
// that reaches the target through rename fences a healthy slot forever. This
// race was real: the apply-path stamp and the fence loop write the same
// slots during a hand-over wave.
func writeFileSticky(path string, b []byte, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer func() {
		if err != nil {
			os.Remove(name)
		}
	}()
	if err = tmp.Chmod(0o644); err == nil {
		_, err = tmp.Write(b)
	}
	if err == nil {
		err = tmp.Close()
	} else {
		tmp.Close()
	}
	if err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		os.Remove(name)
	}
	return err
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

// topologyID names this cluster's consensus generation for the fence. The
// authoritative value is the one in the replicated table (Table.Topology):
// one id per consensus history, identical on every node, so a copy stamped
// while one controller led stays proven when another one takes over. Before
// that entry applies — the first rounds of a cold start, and unit harnesses
// with no Node at all — the node's own bootstrap id stands in: it is exactly
// the value the controller is about to submit.
func (e *Engine) topologyID() string {
	e.tableMu.RLock()
	tbl := e.table
	var id string
	if tbl != nil {
		id = tbl.Topology
	}
	e.tableMu.RUnlock()
	if id != "" {
		return id
	}
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
	stranger := e.strangerDir(topo)
	next := map[int32]ownerReason{}
	for slot, p := range member {
		p := p
		dir := e.store.SlotDir(slot)
		owner, hasOwner, err := readOwner(dir)
		if err != nil {
			// A record that PARSES wrong is evidence of nothing — in a dir
			// admitted to this generation the placement is the proof, so
			// the corrupt bytes are re-stamped over and the slot keeps
			// serving (the same verdict its absence would get). An I/O
			// error says nothing about the bytes: hold the slot out of
			// service, like the record's absence in a stranger dir would.
			var syn *json.SyntaxError
			var typeErr *json.UnmarshalTypeError
			if !stranger && (errors.As(err, &syn) || errors.As(err, &typeErr)) {
				e.logger.Warn("owner fence: corrupt record re-stamped from the table", "slot", slot, "err", err)
				if err := writeOwner(dir, desiredOwner(topo, e.self, &p)); err != nil {
					e.logger.Warn("owner fence: repair stamp failed", "slot", slot, "err", err)
				}
				continue
			}
			e.logger.Warn("owner fence: unreadable record; the slot stays out of service", "slot", slot, "err", err)
			if p.Leader == e.self {
				next[slot] = ownerReason("owner record unreadable: " + err.Error())
			}
			continue
		}
		act, reason := decideOwner(topo, e.self, &p, owner, hasOwner, e.store.HasWAL(slot), stranger)
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
// want list means "every pending slot" (the one call per node that clears a
// disputed dir); named slots that are not pending are skipped and reported
// back, they are never adopted implicitly. Adopting also refreshes the
// dir-level admission stamp: declaring the copies this generation's is
// exactly declaring the dir admitted to this generation, and without it the
// next record-less slot would fence again. Discarding a copy is
// deliberately NOT an endpoint: the operator removes the slot directory on
// disk, and the next fence round clears the verdict on its own.
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
		e.writeAdmissionStamp(e.topologyID())
		e.loggerf("%d slot(s) adopted: local copies declared this generation's", adopted)
	}
	return adopted, skipped
}
