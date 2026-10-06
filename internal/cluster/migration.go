package cluster

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	pushupesv1 "pushupes/internal/grpcapi/pushupes/v1"
	"pushupes/internal/lease"
	"pushupes/internal/storage"
)

// ---- Migration stream bounds ---------------------------------------------------

// snapshotChunkBytes bounds one streamed segment chunk: the source reads
// sealed segment files through one reusable window instead of holding whole
// segments (up to 256MiB each) in memory, and the target assembles into a
// .tmp file. It matches the replica fetch per-item payload cap.
const snapshotChunkBytes = 4 << 20

// snapshotStreamTimeout bounds a whole snapshot transfer: a slot may carry
// hundreds of MiB, so this is far above the unary peerRPCTimeout.
const snapshotStreamTimeout = 10 * time.Minute

// ---- Engine-side migration steps ---------------------------------------------

// StartMigration drives a slot migration from the controller node. The six
// steps from the design doc:
//
//	1 stage:    mark the slot migrating_out -> toNode (Raft; source forwards writes)
//	2 snapshot: stream sealed segments to the target
//	3 catch-up: target pulls the remainder over the replica fetch protocol
//	4 forward:  state-driven in SubmitAppend (migrating_out makes writes land on both)
//	5 commit:   leader_move to target via Raft (epoch+1)
//	6 reclaim:  surplus replica (the former source) dropped from the set
//	7 cleanup:  the former source drops its own copy after a retention delay
//	            (the source arms that countdown itself, once it observes the
//	            commit below — see localdrop.go)
//
// Only the controller runs this; data transfer is direct source<->target. A
// non-controller refuses with a *NotControllerError naming the controller
// (node id + admin address) — it never forwards the command to the leader.
func (e *Engine) StartMigration(ctx context.Context, slot int32, toNode string) error {
	if err := e.controllerGuard(); err != nil {
		return err
	}
	e.tableMu.RLock()
	p, ok := e.table.Slots[slot]
	e.tableMu.RUnlock()
	if !ok {
		return fmt.Errorf("slot %d unassigned", slot)
	}
	from := p.Leader
	if from == toNode {
		return fmt.Errorf("slot %d already led by %s", slot, toNode)
	}
	if _, ok := e.TableSnapshot().Peers[toNode]; !ok {
		return fmt.Errorf("unknown target node %q", toNode)
	}
	// A target that is not a replica yet has no copy to catch up from and no
	// fetch session covering the slot, so the snapshot/catch-up steps below
	// (which are driven by the target pulling from the source) cannot work.
	// Join it to the replica set first: from that moment it behaves exactly
	// like a replica that joined late — it follows the slot and pulls the
	// remainder over the same fetch protocol — and the six steps proceed
	// unchanged.
	//
	// From here on this slot is OURS: the stage below is replicated state that
	// outlives this call (and this process), and the controller round must
	// know the difference between driving it and inheriting it. See
	// reconcileOrphanMigrations.
	e.markMigrationInFlight(slot)
	defer e.clearMigrationInFlight(slot)

	inSet := false
	for _, r := range p.Replicas {
		if r == toNode {
			inSet = true
		}
	}
	if !inSet {
		if err := e.submit(&Command{Op: OpSlotAddReplica, Slots: []int32{slot}, NodeID: toNode}); err != nil {
			return fmt.Errorf("add target replica: %w", err)
		}
		e.logger.WithFields(map[string]any{"slot": slot, "target": toNode, "op": OpSlotAddReplica}).
			Info("migration step 1/3: target joined the replica set")
	} else {
		e.logger.WithFields(map[string]any{"slot": slot, "target": toNode}).
			Info("migration step 1/3: target already a replica (no admission needed)")
	}

	// step 1
	if err := e.submit(&Command{Op: OpSlotState, Slots: []int32{slot}, State: SlotMigratingOut, MigratingTo: toNode}); err != nil {
		return fmt.Errorf("stage: %w", err)
	}
	if err := e.awaitApplied(ctx, 5*time.Second); err != nil {
		return err
	}

	// step 2: the source streams its sealed segments (peer plane).
	srcAddr := e.peerAddr(from)
	if srcAddr == "" {
		e.rollbackMigration(slot)
		return fmt.Errorf("no address for source %s", from)
	}
	// Fast path — a target that already holds an EQUIVALENT copy (same
	// directory digest, not behind the source's LEO) needs no segment
	// transfer at all. The ordinary reason such a target exists is the
	// leader rebalance (balance.go): the target is the slot's in-set
	// replica catching up over the fetch protocol, and handing leadership
	// back must not re-ship the source's whole WAL (a slot is up to 256MiB
	// of sealed segments, and a restarted node's share of the ring is
	// slotCount/N slots). When the source IS this node its copy is read
	// locally; any probe error falls through to the full snapshot path
	// (conservative: a wrong skip would be data loss, a wrong snapshot is
	// only slow). awaitCaughtUp and the commit fence's
	// ensureTargetConsistent re-verify after the skip — the safety net is
	// the fence, not this check.
	srcProbe := srcAddr
	if from == e.self {
		srcProbe = "" // read our own copy locally (the source IS us)
	}
	if equivalent, err := e.slotCopyEquivalent(ctx, slot, srcProbe, e.peerAddr(toNode)); err == nil && equivalent {
		e.logger.WithFields(map[string]any{"slot": slot, "target": toNode, "op": "skip_snapshot"}).
			Info("migration step 2/3: target already holds an equivalent copy; skipping the segment snapshot")
	} else if err := e.snapshotTo(ctx, srcAddr, slot, toNode); err != nil {
		e.rollbackMigration(slot)
		return fmt.Errorf("snapshot: %w", err)
	}

	// step 3+4: wait until the target has caught up to the source LEO.
	if err := e.awaitCaughtUp(ctx, from, toNode, slot); err != nil {
		e.rollbackMigration(slot)
		return fmt.Errorf("catch-up: %w", err)
	}

	// step 4b: commit fence. awaitCaughtUp samples the target LEO and a
	// leader move submitted right after that sample can race writes accepted
	// in between: those records would be absent from the target when it takes
	// over, and the target would reuse their seqs for different records — a
	// fork. So freeze the source's log for the commit window: the source blocks
	// new appends (they never fail; they queue), drains the in-flight ones,
	// pushes the frozen tail to the target and only answers once the target
	// confirms it holds the frozen LEO. The fence stays held until the move (or
	// a rollback) is applied, and self-releases on a bound. A failure here
	// aborts the migration cleanly: the source stays leader and the slot
	// returns to stable (rollbackMigration).
	if _, err := e.fenceSource(ctx, from, slot); err != nil {
		e.rollbackMigration(slot)
		return fmt.Errorf("fence: %w", err)
	}

	// step 5
	if err := e.submit(&Command{Op: OpLeaderMove, Slots: []int32{slot}, NewLeader: toNode}); err != nil {
		e.rollbackMigration(slot)
		return fmt.Errorf("commit: %w", err)
	}
	if err := e.awaitApplied(ctx, 5*time.Second); err != nil {
		return err
	}
	epoch := int64(0)
	if p, ok := e.TableSnapshot().Slots[slot]; ok {
		epoch = p.Epoch
	}
	e.logger.WithFields(map[string]any{"slot": slot, "leader": toNode, "epoch": epoch, "op": OpLeaderMove}).
		Info("migration step 2/3: leader moved to the target")

	// step 6: reclaim the surplus copy the admission step created. Admitting
	// an out-of-set target grew the replica set to factor+1; now that the
	// target leads and has caught up (awaitCaughtUp above), shrink the set
	// back to the configured factor. Runs strictly AFTER the leader move: the
	// source must never be dropped while it is still the slot's writer.
	// Best effort — see reclaimSurplusReplica.
	e.reclaimSurplusReplica(slot, from, toNode)

	// step 7: the former source drops its own copy. That countdown is NOT
	// started here: the source node starts it the moment it observes this
	// commit in the replicated table (slot stable, it no longer leads the
	// slot) — see syncMigrationState/localdrop.go. Scheduling it from the
	// controller would (a) ignore who actually holds the copy and (b) risk
	// starting the retention for a migration that has not committed yet.
	e.logger.WithFields(map[string]any{"slot": slot, "from": from, "to": toNode}).Info("Slot migration committed")
	return nil
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
			e.logger.WithFields(map[string]any{"slot": slot, "factor": tbl.Replicas}).
				Info("migration step 3/3: no surplus replica (set already at the factor)")
		}
		return // not stable (a migration owns it), or already at the factor
	}
	if node == "" {
		// Over the factor, yet no member is eligible (every one is the
		// leader or the target): leave it to the operator rather than guess.
		e.logger.WithFields(map[string]any{"slot": slot, "from": from, "to": toNode}).
			Warn("migration step 3/3: set is over the factor but no replica is eligible for reclaim")
		return
	}
	if err := e.RemoveReplica(context.Background(), slot, node); err != nil {
		e.logger.WithError(err).WithFields(map[string]any{
			"slot": slot, "node": node, "from": from, "to": toNode, "op": OpSlotRemoveReplica,
		}).Warn("migration step 3/3: surplus replica reclaim failed; leaving it in the set (migration stays committed)")
		return
	}
	e.logger.WithFields(map[string]any{
		"slot": slot, "node": node, "from": from, "factor": tbl.Replicas, "op": OpSlotRemoveReplica,
	}).Info("migration step 3/3: source replica reclaimed (set back at the factor)")
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

// snapshotTo asks the source to push sealed segments over the peer plane;
// when we ARE the source it pushes directly to the target.
func (e *Engine) snapshotTo(ctx context.Context, srcAddr string, slot int32, toNode string) error {
	if srcAddr == e.node.cfg.PeerAddr {
		return e.pushSealedSegments(ctx, slot, toNode)
	}
	sctx, cancel := context.WithTimeout(ctx, peerRPCTimeout)
	defer cancel()
	return e.peerTriggerSnapshot(sctx, srcAddr, slot, toNode)
}

// pushSealedSegments streams all sealed segments of a slot to the target in
// bounded chunks, reading each WAL file through one reusable buffer — the
// source's memory stays O(chunk), not O(slot).
func (e *Engine) pushSealedSegments(ctx context.Context, slot int32, toNode string) error {
	sl, err := e.store.Slot(slot)
	if err != nil {
		return err
	}
	segs := sl.SealedSegments()
	if len(segs) == 0 {
		return nil // catch-up via fetch covers everything
	}
	sctx, cancel := context.WithTimeout(ctx, snapshotStreamTimeout)
	defer cancel()
	stream, err := e.peerOpenPush(sctx, e.peerAddr(toNode))
	if err != nil {
		return err
	}
	fail := func(err error) error {
		_, _ = stream.CloseAndRecv()
		return err
	}
	buf := make([]byte, snapshotChunkBytes)
	for _, seg := range segs {
		f, err := os.Open(seg.Path)
		if err != nil {
			return fail(err)
		}
		name := filepath.Base(seg.Path)
		st, err := f.Stat()
		if err != nil {
			f.Close()
			return fail(err)
		}
		if err := stream.Send(&pushupesv1.PushSegmentsRequest{Slot: slot, Name: name, Size: uint64(st.Size())}); err != nil {
			f.Close()
			return err // stream already dead; CloseAndRecv would mask the cause
		}
		var sent int64
		for sent < st.Size() {
			n, err := f.Read(buf)
			if n > 0 {
				if serr := stream.Send(&pushupesv1.PushSegmentsRequest{Slot: slot, Data: buf[:n]}); serr != nil {
					f.Close()
					return serr
				}
				sent += int64(n)
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				return fail(fmt.Errorf("segment %s: %w", name, err))
			}
		}
		f.Close()
	}
	_, err = stream.CloseAndRecv()
	return err
}

// awaitCaughtUp polls the target's LEO until it reaches the source's LEO.
// The replica fetch loop on the target keeps pulling; migration just waits.
func (e *Engine) awaitCaughtUp(ctx context.Context, from, toNode string, slot int32) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		srcLEO, err := e.remoteLEO(ctx, e.peerAddr(from), slot)
		if err != nil {
			return err
		}
		tgtLEO, err := e.remoteLEO(ctx, e.peerAddr(toNode), slot)
		if err != nil {
			return err
		}
		if tgtLEO >= srcLEO {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("target LEO %d < source LEO %d (timeout)", tgtLEO, srcLEO)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
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
	e.logger.WithField("slots", slots).Warn("abandoned slot migrations (their controller is gone): returning the slots to stable")
	if err := e.submit(&Command{Op: OpSlotState, Slots: slots, State: SlotStable}); err != nil {
		e.logger.WithError(err).WithField("slots", slots).
			Warn("abandoned slot migrations: could not return the slots to stable; retried next round")
	}
}

// remoteLEO asks a peer for its LEO of a slot over the peer plane.
func (e *Engine) remoteLEO(ctx context.Context, addr string, slot int32) (uint64, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return e.peerLeo(cctx, addr, slot)
}

// ---- Target-side handlers (invoked from the peer gRPC server) -----------------

// chunkSource is the receive half of a PushSegments stream, extracted as an
// interface so the importer can be unit-tested without a live gRPC server.
type chunkSource interface {
	Recv() (*pushupesv1.PushSegmentsRequest, error)
}

// writeSegments consumes a PushSegments chunk stream and lands the sealed
// segment files into the importing slot directory. Each file streams into a
// "<name>.tmp" — memory stays O(chunk), never O(segment). The WAL header
// (magic/version + slot id) is assembled from the first WALHeaderLen bytes
// (chunks may split anywhere), validated before anything is written to the
// file, and a file whose existing copy already matches the announced size is
// drained, not rewritten (retry-friendly). Incomplete totals are rejected
// and the .tmp removed.
func (e *Engine) writeSegments(recv chunkSource) error {
	var slot int32
	seenSlot := false
	var dir string

	var cur *os.File // open .tmp; nil while draining a duplicate file
	var path, tmpPath string
	var want, got uint64
	var hdr [storage.WALHeaderLen]byte
	var hdrGot int
	var hdrOK bool

	abort := func() {
		if cur != nil {
			cur.Close()
			_ = os.Remove(tmpPath)
			cur = nil
		}
	}
	finalize := func() error {
		if cur == nil {
			return nil
		}
		if got != want {
			abort()
			return fmt.Errorf("segment %s: got %d of %d bytes", filepath.Base(path), got, want)
		}
		if err := cur.Close(); err != nil {
			cur = nil
			_ = os.Remove(tmpPath)
			return err
		}
		cur = nil
		return os.Rename(tmpPath, path)
	}

	for {
		chunk, err := recv.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			abort()
			return err
		}
		// The chunk's bytes alias the receive buffer (the peer codec hands them
		// over without copying), so the lease must outlive the writes and be
		// released on every exit path out of this iteration.
		if cerr := func() error {
			defer lease.Release(chunk)
			if !seenSlot {
				slot, seenSlot = chunk.Slot, true
				dir = e.store.SlotDir(slot)
				if err := os.MkdirAll(dir, 0o755); err != nil {
					abort()
					return err
				}
			} else if chunk.Slot != slot {
				abort()
				return fmt.Errorf("snapshot stream: slot %d != %d", chunk.Slot, slot)
			}

			if chunk.Name != "" { // descriptor: a new file begins
				if err := finalize(); err != nil {
					abort()
					return err
				}
				path = filepath.Join(dir, filepath.Base(chunk.Name))
				tmpPath = path + ".tmp"
				want, got, hdrGot, hdrOK = chunk.Size, 0, 0, false
				if st, err := os.Stat(path); err == nil && st.Size() == int64(chunk.Size) {
					cur = nil // duplicate: drain this file's chunks untouched
					return nil
				}
				cur, err = os.Create(tmpPath)
				if err != nil {
					abort()
					return err
				}
			}
			if cur == nil || len(chunk.Data) == 0 {
				return nil // draining a duplicate
			}
			data := chunk.Data

			// assemble + validate the WAL header before the first write
			if !hdrOK {
				n := copy(hdr[hdrGot:], data)
				hdrGot += n
				data = data[n:]
				if hdrGot < storage.WALHeaderLen {
					return nil // header split across chunks: accumulate
				}
				slotID, err := storage.HeaderSlotID(hdr[:])
				if err != nil {
					abort()
					return fmt.Errorf("segment %s: %w", filepath.Base(path), err)
				}
				if slotID != slot {
					abort()
					return fmt.Errorf("segment %s: header slot %d != %d", filepath.Base(path), slotID, slot)
				}
				hdrOK = true
				if _, err := cur.Write(hdr[:]); err != nil {
					abort()
					return err
				}
				got += storage.WALHeaderLen
			}
			if len(data) == 0 {
				return nil
			}
			if _, err := cur.Write(data); err != nil {
				abort()
				return err
			}
			got += uint64(len(data))
			return nil
		}(); cerr != nil {
			abort()
			return cerr
		}
	}
	if err := finalize(); err != nil {
		abort()
		return err
	}
	if !seenSlot {
		return nil // empty stream: nothing imported
	}
	// reload the slot so catch-up (replica fetch) continues from disk state
	return e.store.ReloadSlot(slot)
}

// HandleSnapshotCommand triggers this node (as source) to push its sealed
// segments of a slot to the target.
func (e *Engine) HandleSnapshotCommand(ctx context.Context, slot int32, toNode string) error {
	return e.pushSealedSegments(ctx, slot, toNode)
}

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
	e.logger.WithFields(map[string]any{"slot": slot, "from": from}).
		Warn("migration target copy discarded: its streams were inconsistent with its LEO; rebuilding from the source")
	return nil
}

// ensureTargetConsistent makes sure the migration target's local streams agree
// with the source's before any increment is shipped to it.
//
// A target can be at a high LEO while one of its aggregates is short or absent:
// records appended above that stream never entered its directory, so they sit in
// its WAL unreadable. An increment cannot repair that (the seqs the missing
// versions need are already spent on other aggregates, and only counter+1 may be
// written), and the LEO comparison the caller relies on cannot detect it either.
// So the copy is dropped and re-pulled from scratch.
//
// The comparison must be against the SOURCE, not the target against itself. A
// target whose aggregate is missing entirely reports versions == resolvable (both
// sides just omit it), so an internal-consistency check calls it healthy — the
// sums hide the omission. Comparing the two digests catches a target that is
// missing an aggregate or has a shorter stream for one.
func (e *Engine) ensureTargetConsistent(ctx context.Context, slot int32, targetAddr, toNode string) error {
	st, err := e.remoteSlotState(ctx, targetAddr, slot)
	if err != nil {
		return err
	}
	srcAgg, srcVer, srcRes := e.store.SlotDigest(slot)
	if st.Aggregates == srcAgg && st.Versions == srcVer && st.Resolvable == srcRes {
		return nil
	}
	e.logger.WithFields(map[string]any{
		"slot": slot, "target": toNode, "target_leo": st.LEO,
		"target_digest": fmt.Sprintf("agg=%d versions=%d resolvable=%d", st.Aggregates, st.Versions, st.Resolvable),
		"source_digest": fmt.Sprintf("agg=%d versions=%d resolvable=%d", srcAgg, srcVer, srcRes),
	}).Warn("migration target's streams do not match this source; discarding its copy and rebuilding")

	if err := e.peerDropSlot(ctx, targetAddr, slot, toNode); err != nil {
		return fmt.Errorf("fence: drop inconsistent target copy: %w", err)
	}
	// Rebuild: sealed segments first (the bulk), then the live tail from seq 1 so
	// every record arrives in seq order and the directory adopts each version.
	// The source's log is frozen by the caller's fence, so this is a bounded job.
	if err := e.pushSealedSegments(ctx, slot, toNode); err != nil {
		return fmt.Errorf("fence: rebuild target segments: %w", err)
	}
	if err := e.pushFrames(ctx, targetAddr, slot, 1, e.store.LastSeqOf(slot)); err != nil {
		return fmt.Errorf("fence: rebuild target tail: %w", err)
	}
	after, err := e.remoteSlotState(ctx, targetAddr, slot)
	if err != nil {
		return err
	}
	if after.Aggregates != srcAgg || after.Versions != srcVer || after.Resolvable != srcRes {
		return fmt.Errorf("fence: rebuilt target %s still differs from source (target agg=%d versions=%d resolvable=%d, source agg=%d versions=%d resolvable=%d)",
			toNode, after.Aggregates, after.Versions, after.Resolvable, srcAgg, srcVer, srcRes)
	}
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
