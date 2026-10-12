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
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	pushupesv1 "github.com/berkaroad/pushupes/internal/grpcapi/pushupes/v1"
)

// ---- Slot handover: stage, catch up, commit ----------------------------------

// handoverStageTimeout bounds the table command that stages a handover (the
// target joins the replica set). The catch-up itself is not bounded here: the
// target pulls at whatever rate the fetch path gives it, and the controller
// commits on a later round once the target holds the source's log.
const handoverStageTimeout = 5 * time.Second

// ---- Engine-side handover steps ----------------------------------------------

// Every layout change is three phases, the same whatever the reason a copy is
// built (a replica-factor increase, a node re-joining, a leader hand-over):
//
//	1 stage:    make the target a replica (Raft). It then pulls the slot over
//	            the ordinary replica fetch path, like any late-joining seat:
//	            no data moves through this call, no leader changes, no write is
//	            frozen.
//	2 catch up: the fetch path carries the log. A seat that is behind is
//	            excluded from the watermark min (see advanceHW), so a live
//	            writer never waits for it.
//	3 commit:   only a hand-over has something to commit — freeze the source's
//	            log (fence), move the leader (OpLeaderMove, epoch+1) and
//	            reclaim the surplus seat the stage may have added. A copy that
//	            cannot converge (same log end offset, different directory) is
//	            DROPPED and staged again, never repaired in place.
//
// stageMigration and commitMigration are the two steps the controller drives
// across rounds; MigrateSlot runs all three for an operator's one-shot request.

// stageMigration joins the handover target to the slot's replica set so it
// starts pulling the log. Idempotent: an in-set target is left alone. Returns
// true when this call changed the table.
func (e *Engine) stageMigration(ctx context.Context, slot int32, toNode string) (bool, error) {
	if err := e.controllerGuard(); err != nil {
		return false, err
	}
	tbl := e.TableSnapshot()
	p, ok := tbl.Slots[slot]
	if !ok {
		return false, fmt.Errorf("slot %d unassigned", slot)
	}
	if p.Leader == toNode {
		return false, fmt.Errorf("slot %d already led by %s", slot, toNode)
	}
	if _, ok := tbl.Peers[toNode]; !ok {
		return false, fmt.Errorf("unknown target node %q", toNode)
	}
	if replicaListHas(p.Replicas, toNode) {
		return false, nil // already following: the fetch path is already on it
	}
	if err := e.submit(&Command{Op: OpSlotAddReplica, Slots: []int32{slot}, NodeID: toNode}); err != nil {
		return false, fmt.Errorf("add target replica: %w", err)
	}
	if err := e.awaitApplied(ctx, handoverStageTimeout); err != nil {
		return true, err
	}
	e.logger.Info("handover staged: target joined the replica set and pulls over the fetch path",
		"slot", slot, "target", toNode, "op", OpSlotAddReplica)
	return true, nil
}

// handoverReady reports whether a handover may commit: the target holds the
// source's log (same directory digest, nothing behind). diverged reports the
// one case fetching can never repair — the target's log end offset has caught
// up while its directory differs, so the records above the gap sit in its WAL
// unreadable; the caller drops that copy and lets it pull again from empty.
func (e *Engine) handoverReady(ctx context.Context, slot int32, from, to string) (ready, diverged bool, err error) {
	srcAddr := e.peerAddr(from)
	dstAddr := e.peerAddr(to)
	if from == e.self {
		srcAddr = "" // read our own copy locally
	}
	if dstAddr == "" {
		return false, false, fmt.Errorf("no address for target %s", to)
	}
	equivalent, behind, err := e.slotCopyStatus(ctx, slot, srcAddr, dstAddr)
	if err != nil {
		return false, false, err
	}
	if behind {
		return false, false, nil
	}
	return equivalent, !equivalent, nil
}

// commitMigration moves a slot's leadership to a target that holds its log:
// mark the handover (readers are asked to the target; writes queue behind the
// fence), freeze the source, move the leader through Raft (epoch+1) and reclaim
// the surplus seat. A target that is not a replica yet is refused — stage it
// first (stageMigration), which is also what gives it the data.
func (e *Engine) commitMigration(ctx context.Context, slot int32, toNode string) error {
	if err := e.controllerGuard(); err != nil {
		return err
	}
	tbl := e.TableSnapshot()
	p, ok := tbl.Slots[slot]
	if !ok {
		return fmt.Errorf("slot %d unassigned", slot)
	}
	from := p.Leader
	if from == toNode {
		return nil // already committed (idempotent)
	}
	if !replicaListHas(p.Replicas, toNode) {
		return fmt.Errorf("slot %d: %s is not a replica yet (stage the handover first)", slot, toNode)
	}
	// From here on this slot is OURS: the stage below is replicated state that
	// outlives this call (and this process), and the controller round must know
	// the difference between driving it and inheriting it. See
	// reconcileOrphanMigrations.
	e.markMigrationInFlight(slot)
	defer e.clearMigrationInFlight(slot)
	if err := e.submit(&Command{Op: OpSlotState, Slots: []int32{slot}, State: SlotMigratingOut, MigratingTo: toNode}); err != nil {
		return fmt.Errorf("stage: %w", err)
	}
	if err := e.awaitApplied(ctx, handoverStageTimeout); err != nil {
		return err
	}
	// The commit fence. Sampling the target's log end offset and moving the
	// leader right after that sample can race writes accepted in between: those
	// records would be absent from the target when it takes over, and it would
	// reuse their seqs for different records — a fork. So freeze the source's
	// log for the commit window: it blocks new appends (they never fail; they
	// queue), drains the in-flight ones, pushes the frozen tail to the target
	// and answers only once the target confirms it holds the frozen log end
	// offset. The fence stays held until the move (or a rollback) is applied and
	// self-releases on a bound. A failure here aborts cleanly: the source stays
	// leader and the slot returns to stable.
	if _, err := e.fenceSource(ctx, from, slot); err != nil {
		e.rollbackMigration(slot)
		return fmt.Errorf("fence: %w", err)
	}
	if err := e.submit(&Command{Op: OpLeaderMove, Slots: []int32{slot}, NewLeader: toNode}); err != nil {
		e.rollbackMigration(slot)
		return fmt.Errorf("commit: %w", err)
	}
	if err := e.awaitApplied(ctx, handoverStageTimeout); err != nil {
		return err
	}
	epoch := int64(0)
	if cur, ok := e.TableSnapshot().Slots[slot]; ok {
		epoch = cur.Epoch
	}
	e.logger.Info("handover committed: leader moved to the target",
		"slot", slot, "from", from, "to", toNode, "epoch", epoch, "op", OpLeaderMove)

	// Reclaim the surplus copy the stage step created. Admitting an out-of-set
	// target grew the replica set to factor+1; now that the target leads and
	// holds the data, shrink the set back to the configured factor. Runs
	// strictly AFTER the leader move: the source must never be dropped while it
	// is still the slot's writer. Best effort — see reclaimSurplusReplica.
	e.reclaimSurplusReplica(slot, from, toNode)

	// The former source drops its own copy on its own retention countdown: the
	// node starts it the moment it observes this commit in the replicated table
	// (slot stable, it no longer leads the slot) — see localdrop.go. Scheduling
	// it from the controller would (a) ignore who actually holds the copy and
	// (b) risk starting the retention for a handover that has not committed.
	return nil
}

// MigrateSlot drives one handover to completion: stage, wait for the target to
// hold the source's log, commit. This is the operator's entry point (the admin
// endpoint); the rebalance loop runs the same steps across rounds instead of
// blocking, so a slow catch-up costs it a round rather than a request.
func (e *Engine) MigrateSlot(ctx context.Context, slot int32, toNode string) error {
	// Hold the in-flight mark for the whole handover. Its seat is not the ring
	// plan's prescription for this slot (an operator may name any node), so the
	// surplus-seat reclaim would otherwise treat the copy the handover is
	// building as surplus and drop it mid-catch-up — observed as a commit
	// refused with "is not a replica yet". The mark also feeds the orphan
	// reconcile, which returns a slot to stable if this process dies mid-way.
	e.markMigrationInFlight(slot)
	defer e.clearMigrationInFlight(slot)
	if _, err := e.stageMigration(ctx, slot, toNode); err != nil {
		return err
	}
	p, ok := e.TableSnapshot().Slots[slot]
	if !ok {
		return fmt.Errorf("slot %d unassigned", slot)
	}
	from := p.Leader
	if from == toNode {
		return nil
	}
	for {
		ready, diverged, err := e.handoverReady(ctx, slot, from, toNode)
		if err != nil {
			return fmt.Errorf("catch-up: %w", err)
		}
		if diverged && !e.confirmDiverged(slot, from, toNode, time.Now()) {
			// One reading is not evidence: a directory settling behind its log
			// end offset reads like a fork for a moment (confirmDiverged), and
			// dropping an innocent copy costs a whole-slot pull.
			continue
		}
		if diverged {
			// Fetching cannot repair this copy: drop it so it pulls from empty.
			if err := e.peerDropSlot(ctx, e.peerAddr(toNode), slot, toNode); err != nil {
				return fmt.Errorf("drop diverged target copy: %w", err)
			}
		} else if ready {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("catch-up: %w", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	return e.commitMigration(ctx, slot, toNode)
}

// reclaimSurplusReplica shrinks a slot's replica set back to the configured
// replica factor after a migration admitted a target that was outside the set.
//
// Admitting such a target grows the set to factor+1 — that admission is the
// precondition of the snapshot/catch-up steps — so a committed migration would
// otherwise leave the slot one replica too many until an operator ran the
// manual remove-replica admin endpoint. The surplus is reclaimed right here,
// through the same command the manual endpoint submits (OpSlotRemoveReplica),
// so the table stays the single source of truth and neither leader nor epoch
// move.
//
// The former source is the preferred candidate: it holds a full copy but no
// longer leads. The current leader and the migration target are never removed,
// and a set already at the factor is left alone — only a set strictly larger
// than the factor is touched.
//
// Best effort by design: the migration is already committed, so a failed
// reclaim must NOT fail it. It logs and leaves the extra replica in place; a
// later migration or the manual remove-replica endpoint can still reclaim it.
// Nothing here deletes the removed node's on-disk copy.
//
// The decision itself lives in surplusForReclaim, so the part that a test can
// actually observe — whether to reclaim and whom — is a pure function.
func (e *Engine) reclaimSurplusReplica(slot int32, from, toNode string) {
	// One snapshot, no loop. The only path that used to iterate was a
	// successful RemoveReplica: the next pass then re-snapshotted the whole
	// table just to find the set already back at the factor. Reading the
	// placement once is enough — RemoveReplica commits through Raft and this
	// is best effort, so re-checking afterwards would only re-read a value the
	// caller has no decision left to make about.
	tbl := e.TableSnapshot()
	p, ok := tbl.Slots[slot]
	if !ok {
		return // unknown slot: nothing to reclaim
	}
	node, over := surplusForReclaim(p, tbl.Replicas, from, toNode)
	if !over {
		if p.State == SlotStable {
			e.logger.Info("migration step 3/3: no surplus replica (set already at the factor)",
				"slot", slot, "factor", tbl.Replicas)
		}
		return // not stable (a migration owns it), or already at the factor
	}
	if node == "" {
		// Over the factor, yet no member is eligible (every one is the
		// leader or the target): leave it to the operator rather than guess.
		e.logger.Warn("migration step 3/3: set is over the factor but no replica is eligible for reclaim",
			"slot", slot, "from", from, "to", toNode)
		return
	}
	if err := e.RemoveReplica(context.Background(), slot, node); err != nil {
		e.logger.Warn("migration step 3/3: surplus replica reclaim failed; leaving it in the set (migration stays committed)",
			"slot", slot, "node", node, "from", from, "to", toNode, "op", OpSlotRemoveReplica, "error", err)
		return
	}
	e.logger.Info("migration step 3/3: source replica reclaimed (set back at the factor)",
		"slot", slot, "node", node, "from", from, "factor", tbl.Replicas, "op", OpSlotRemoveReplica)
}

// surplusForReclaim decides whether a slot's replica set has a member to drop
// and, if so, which one. Pure and side-effect free, so the decision can be
// asserted directly: the reclaim path commits through Raft, which a test engine
// has no node for, so "did it try, and whom did it pick" is the observable that
// a pure function makes checkable.
//
// Returns ("", false) when the set is already at the factor (or under it), and
// ("", true) when the set is over the factor but no member is eligible — every
// remaining member is the leader or the migration target, which the caller
// reports to the operator rather than guessing.
func surplusForReclaim(p *Placement, factor int, from, toNode string) (node string, over bool) {
	if p == nil || p.State != SlotStable {
		return "", false
	}
	if len(p.Replicas) <= factor {
		return "", false
	}
	return surplusReplica(p, from, toNode), true
}

// surplusReplica picks the member to drop from an oversized replica set: the
// migration source first, then any other non-leader, non-target member (sorted,
// so every node replaying the table agrees on the choice). Empty means none is
// eligible.
func surplusReplica(p *Placement, from, toNode string) string {
	if from != "" && from != p.Leader && from != toNode && replicaListHas(p.Replicas, from) {
		return from
	}
	rest := make([]string, 0, len(p.Replicas))
	for _, r := range p.Replicas {
		if r != p.Leader && r != toNode {
			rest = append(rest, r)
		}
	}
	if len(rest) == 0 {
		return ""
	}
	sort.Strings(rest)
	return rest[0]
}

// replicaListHas reports whether a replica list contains a node.
func replicaListHas(list []string, id string) bool {
	for _, v := range list {
		if v == id {
			return true
		}
	}
	return false
}

// RemoveReplica reclaims one member from a slot's replica set — the mirror of
// the target admission StartMigration performs when a migration targets a node
// outside the replica set.
//
// A committed migration now reclaims its own surplus copy (see
// reclaimSurplusReplica), so this entry point is the OPERATOR's tool: it backs
// the manual `POST /admin/slots/{slot}/remove-replica` endpoint and is the
// fallback when an automatic reclaim could not run (e.g. the controller
// restarted between commit and reclaim) or when a slot carries a surplus for
// any other reason. It is also the rollback for a staged target admission.
//
// It is a controller-only command (the table is Raft-replicated), so it must
// run on the Raft leader. The submitter-side guards below turn a bad request
// into a readable error BEFORE anything is committed; the committed command
// itself stays a deterministic, idempotent function of the table (Table.Apply /
// OpSlotRemoveReplica):
//
//   - the slot must be stable (a migration in flight owns its placement),
//   - the node must be a known member and currently a replica of the slot,
//   - it must not be the slot's leader (that would strand the slot),
//   - at least one replica must remain.
//
// Removing a node from the metadata does NOT touch that node's on-disk data:
// dropping the local copy is the operator's call.
func (e *Engine) RemoveReplica(ctx context.Context, slot int32, node string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Controller-only, and never forwarded: a follower refuses with a
	// *NotControllerError that names the controller (node id + admin
	// address) so the caller can retry against it directly.
	if err := e.controllerGuard(); err != nil {
		return err
	}
	tbl := e.TableSnapshot()
	p, ok := tbl.Slots[slot]
	if !ok {
		return fmt.Errorf("slot %d unassigned", slot)
	}
	if p.State != SlotStable {
		return fmt.Errorf("slot %d is not stable (%s)", slot, p.State)
	}
	if _, ok := tbl.Peers[node]; !ok {
		return fmt.Errorf("unknown node %q", node)
	}
	inSet := false
	for _, r := range p.Replicas {
		if r == node {
			inSet = true
			break
		}
	}
	if !inSet {
		return fmt.Errorf("node %s is not a replica of slot %d", node, slot)
	}
	if p.Leader == node {
		return fmt.Errorf("node %s leads slot %d; move the leader before removing it", node, slot)
	}
	if len(p.Replicas) <= 1 {
		return fmt.Errorf("slot %d would lose its last replica", slot)
	}
	if err := e.submit(&Command{Op: OpSlotRemoveReplica, Slots: []int32{slot}, NodeID: node}); err != nil {
		return fmt.Errorf("remove replica: %w", err)
	}
	return nil
}

// verifyTargetHoldsSource is the fence's last guard before the leader move.
//
// Completeness is the COMMIT GATE's job, not the fence's: the gate proved the
// target's directory equivalent to the source's (handoverReady — equality of
// the aggregate/version/resolvable counts at the same log end offset). By the
// time the fence runs the source has accepted more records, and that in-flight
// tail is exactly what the fence ships, so demanding equality here would abort
// every hand-over. What the fence must still refuse is the FORK direction: a
// target holding versions the source does not have. Taking a slot over with
// such a copy means reusing seqs for different records, so the hand-over is
// aborted here and the gate drops the copy on its next round.
func (e *Engine) verifyTargetHoldsSource(ctx context.Context, slot int32, targetAddr, toNode string) error {
	st, err := e.remoteSlotState(ctx, targetAddr, slot)
	if err != nil {
		return err
	}
	srcAgg, srcVer, srcRes := e.store.SlotDigest(slot)
	if st.Versions <= srcVer && st.Resolvable <= srcRes {
		return nil
	}
	return fmt.Errorf("fence: target %s holds more than this source (target agg=%d versions=%d resolvable=%d, source agg=%d versions=%d resolvable=%d): drop the target copy and stage the handover again",
		toNode, st.Aggregates, st.Versions, st.Resolvable, srcAgg, srcVer, srcRes)
}

// awaitApplied waits until the local FSM applied everything committed.
func (e *Engine) awaitApplied(ctx context.Context, timeout time.Duration) error {
	num := func(key string) uint64 {
		st := e.node.Stats()
		switch v := st[key].(type) {
		case uint64:
			return v
		case int:
			return uint64(v)
		case string:
			n, _ := strconv.ParseUint(v, 10, 64)
			return n
		}
		return 0
	}
	target := num("commit-index")
	deadline := time.Now().Add(timeout)
	for num("applied-index") < target {
		if time.Now().After(deadline) {
			return fmt.Errorf("raft apply timeout")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	return nil
}

// rollbackMigration returns a failed migration to stable.
func (e *Engine) rollbackMigration(slot int32) {
	_ = e.submit(&Command{Op: OpSlotState, Slots: []int32{slot}, State: SlotStable})
}

// ---- Abandoned migrations ------------------------------------------------------

// markMigrationInFlight / clearMigrationInFlight track the slots this
// controller is running StartMigration on. The stage a migration writes
// (migrating_out) is replicated table state and outlives the call — and the
// process — that wrote it, so "which migration is mine" cannot be read off the
// table alone.
func (e *Engine) markMigrationInFlight(slot int32) {
	e.migMu.Lock()
	if e.migrating == nil {
		e.migrating = map[int32]struct{}{}
	}
	e.migrating[slot] = struct{}{}
	e.migMu.Unlock()
}

func (e *Engine) clearMigrationInFlight(slot int32) {
	e.migMu.Lock()
	delete(e.migrating, slot)
	e.migMu.Unlock()
}

// migrationInFlight reports whether this process is currently driving a
// hand-over of the slot. The surplus-seat reclaim consults it: the seat such a
// hand-over is building may be foreign to the ring plan (an operator names the
// target), and dropping it mid-catch-up aborts the commit.
func (e *Engine) migrationInFlight(slot int32) bool {
	e.migMu.Lock()
	defer e.migMu.Unlock()
	_, ok := e.migrating[slot]
	return ok
}

func (e *Engine) migrationsInFlight() map[int32]struct{} {
	e.migMu.Lock()
	defer e.migMu.Unlock()
	out := make(map[int32]struct{}, len(e.migrating))
	for s := range e.migrating {
		out[s] = struct{}{}
	}
	return out
}

// orphanedMigrations lists the slots abandoned mid-hand-over: their placement
// is not stable, yet no migration in this process is driving them.
//
// A migration's state is written to the replicated table by StartMigration's
// first step, and only that goroutine ever clears it again (commit or
// rollback). Lose it and the slot stays non-stable forever: replan_slots
// deliberately skips non-stable slots, and the leader rebalancer yields to
// them (layoutSettled) — so the ring layout for every other slot freezes too,
// and the leadership a failover moved away is never handed back. Observed on a
// cluster restarted while the rebalancer was mid-hand-over: one slot stuck in
// migrating_out, the raft log frozen, and one node left leading 958 of 1680
// slots for good.
//
// Nothing can resume such a migration — its progress (snapshot, catch-up,
// fence) was never persisted — so it is abandoned instead: the slot returns to
// stable and the ordinary machinery (failover, rebalancer, replan_slots) takes
// it from there. Pure, so the decision is directly assertable.
func orphanedMigrations(t *Table, inFlight map[int32]struct{}) []int32 {
	var out []int32
	for s, p := range t.Slots {
		if p.State == SlotStable {
			continue
		}
		if _, mine := inFlight[s]; mine {
			continue
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// reconcileOrphanMigrations abandons the migrations whose controller is gone.
// Runs in the controller round (leader only), so exactly one node ever does it,
// and it is idempotent: the slots it clears are stable from then on.
func (e *Engine) reconcileOrphanMigrations() {
	slots := orphanedMigrations(e.TableSnapshot(), e.migrationsInFlight())
	if len(slots) == 0 {
		return
	}
	e.logger.Warn("abandoned slot migrations (their controller is gone): returning the slots to stable", "slots", slots)
	if err := e.submit(&Command{Op: OpSlotState, Slots: slots, State: SlotStable}); err != nil {
		e.logger.Warn("abandoned slot migrations: could not return the slots to stable; retried next round",
			"slots", slots, "error", err)
	}
}

// remoteLEO asks a peer for its LEO of a slot over the peer plane.
func (e *Engine) remoteLEO(ctx context.Context, addr string, slot int32) (uint64, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return e.peerLeo(cctx, addr, slot)
}

// ---- Target-side handlers (invoked from the peer gRPC server) -----------------

// HandleDropSlot discards this node's local copy of a slot and reopens it empty,
// so the migration source can rebuild it from scratch. The caller is the fence on
// the migration source: the slot is migrating_out (still advertised to this node
// only because it is the target), so no reader depends on the local copy.
//
// Refusing to drop a slot this node LEADS is the safety line: dropping the copy a
// live slot writes to would destroy acknowledged records. A target never leads the
// slot it is receiving.
func (e *Engine) HandleDropSlot(slot int32, from string) error {
	if e.Leads(slot) {
		return fmt.Errorf("drop slot %d: this node leads it (refusing to discard a live writer's log)", slot)
	}
	if err := e.store.DropSlot(slot); err != nil {
		return err
	}
	e.logger.Warn("migration target copy discarded: its streams were inconsistent with its LEO; rebuilding from the source",
		"slot", slot, "from", from)
	return nil
}

// peerDropSlot asks a target to discard its local copy of a slot.
func (e *Engine) peerDropSlot(ctx context.Context, addr string, slot int32, self string) error {
	c, err := e.peerRPC(addr)
	if err != nil {
		return err
	}
	cctx, cancel := context.WithTimeout(ctx, peerRPCTimeout)
	defer cancel()
	_, err = c.DropSlot(cctx, &pushupesv1.DropSlotRequest{Slot: slot, Node: self})
	return err
}

// HandleSlotFlush is the fence's durability probe on the TARGET: flush the slot
// and answer the log end offset that is on disk afterwards. A slot this node
// does not hold answers 0, which never satisfies a frozen tail.
func (e *Engine) HandleSlotFlush(slot int32) (uint64, error) {
	if !e.store.SlotPresent(slot) {
		return 0, nil
	}
	return e.store.FlushSlot(slot)
}

// peerDurableLEO asks a peer to flush one slot and answers the offset it holds
// on disk afterwards.
func (e *Engine) peerDurableLEO(ctx context.Context, addr string, slot int32) (uint64, error) {
	c, err := e.peerRPC(addr)
	if err != nil {
		return 0, err
	}
	cctx, cancel := context.WithTimeout(ctx, peerRPCTimeout)
	defer cancel()
	resp, err := c.SlotLeo(cctx, &pushupesv1.SlotLeoRequest{Slot: slot, Flush: true})
	if err != nil {
		return 0, err
	}
	return resp.DurableLeo, nil
}

// HandleLEO answers a slot-LEO probe.
func (e *Engine) HandleLEO(slot int32) uint64 {
	return e.store.LastSeqOf(slot)
}

// HandleSlotLeo answers the migration catch-up probe: the slot's LEO plus the
// directory summary that tells the caller whether this node's streams are
// consistent with that offset (see SlotDigest).
func (e *Engine) HandleSlotLeo(slot int32) (leo, aggregates, versions, resolvable uint64) {
	leo = e.store.LastSeqOf(slot)
	aggregates, versions, resolvable = e.store.SlotDigest(slot)
	return leo, aggregates, versions, resolvable
}
