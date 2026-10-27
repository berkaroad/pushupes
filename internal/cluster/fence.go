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
	"sync"
	"time"

	"github.com/berkaroad/pushupes/internal/config"
	"github.com/berkaroad/pushupes/internal/data"
)

// ---- Migration write fence ---------------------------------------------------
//
// A migration must not move the leader onto a target that does not yet hold
// every record the source has accepted. After the move the target assigns its
// own seqs, so a record the source accepted but never shipped would be
// replaced at the same seq by a DIFFERENT record on the new leader — a real
// fork of the log ("diverged on replay") that the follower fetch loop cannot
// heal and that silently drops an acknowledged write.
//
// The gap is the commit window: the commit gate samples the target's LEO and the
// leader move is submitted afterwards. Writes accepted in between are not
// guaranteed to have reached the target, so the moved-to leader can start
// writing at seqs the source already used for other records.
//
// The fix is a short write fence around that window, on the SOURCE:
//
//	(i)   new appends to the slot BLOCK (they are never rejected);
//	(ii)  in-flight appends are drained, so the source's LEO is frozen;
//	(iii) the frozen tail is pushed to the target over the ordinary forward
//	      path and the target's LEO is polled until it confirms the frozen
//	      offset (the same progress/LEO channel the fetch loop uses);
//	(iv)  only then is the leader move submitted; the fence is released as
//	      soon as the source applies the move (or an abort) and the blocked
//	      writers are then redirected to the new leader.
//
// The fence is always bounded: the drain and the catch-up wait each have a
// deadline, the controller bounds the RPC, and a watchdog force-releases the
// fence past config.FenceMaxHold so a lost controller can never wedge a slot's
// writes. Any timeout aborts the migration cleanly (rollback to stable) and the
// source keeps leading.

// slotFence is the per-slot write gate. One instance exists per slot for the
// life of the engine (preallocated) so a writer can never race the creation of
// the fence: a writer always registers its "hold" under mu, and taking the
// fence sets closed under the same mu, so the drain sees every in-flight write.
type slotFence struct {
	mu     sync.Mutex
	closed bool
	hold   int // writers inside the critical section
	// drained is closed when hold reaches 0 while closed; nil when the fence
	// was taken with no writers in flight.
	drained chan struct{}
	// done is closed when the fence is released; blocked writers wait on it.
	done chan struct{}
	// deadline is the watchdog bound for the current hold.
	deadline time.Time
	// gateArmed is set while a committed fence's release is being gated on the
	// migration target applying the leader move, so repeated table changes do
	// not spawn a second gate (see holdFenceUntilTargetLeader).
	gateArmed bool
	// heldSince stamps when the fence was taken, so release can log the total
	// window a client append could be blocked behind the fence.
	heldSince time.Time
	// committedTo names the target of a COMMITTED leader move. The fence's
	// release is gated on the TARGET applying the move, which says nothing
	// about THIS node's own applied table; a writer woken by that release
	// re-reads a table that still names this node the leader and would land
	// the record here and acknowledge it — an ack whose seq the new leader
	// has already spent on a different record (the client then meets 1001 at
	// a leader that never received it, and this copy forks into a rebuild;
	// the dg15 trace: an acknowledgement 23ms after the fence release, one
	// round before the client's next write was refused). Writers therefore
	// wait here for the local demotion, bounded, instead of serving.
	committedTo string
	// committedAt stamps when committedTo was taken: the stamp is a WAIT hint,
	// not durable state, and expires (a rolled-back plan never applies a
	// leader change, and a slot must not stay held for the rest of its life).
	committedAt time.Time
}

// leave deregisters one writer; the last one out closes a pending drain.
func (f *slotFence) leave() {
	f.mu.Lock()
	f.hold--
	if f.closed && f.hold == 0 && f.drained != nil {
		close(f.drained)
		f.drained = nil
	}
	f.mu.Unlock()
}

// fenceFor returns the (preallocated) fence of a slot, or nil when the slot is
// outside the store's range.
// awaitLocalDemotion blocks a writer whose slot has a COMMITTED leader move
// that this node has not applied yet — bounded, and only while the local table
// still names this node the leader. It never fails the write: once the
// demotion lands (normally within one table apply) the caller's ordinary
// routing redirects to the committed leader, and nothing was acknowledged
// against a copy the handover had already left behind.
func (e *Engine) awaitLocalDemotion(slot int32) {
	f := e.fenceFor(slot)
	if f == nil {
		return
	}
	f.mu.Lock()
	pending := f.committedTo
	stamped := f.committedAt
	f.mu.Unlock()
	if pending == "" {
		return
	}
	if !stamped.IsZero() && time.Since(stamped) > 3*time.Second {
		f.mu.Lock()
		if f.committedTo == pending {
			f.committedTo = ""
		}
		f.mu.Unlock()
		return
	}
	// Short bound: the demotion is one table apply away, and a writer parked
	// here still holds its fence registration — a long bound would eat into
	// the NEXT fence's drain budget on an actively rebalanced slot.
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		// Cheap one-slot read: this runs on the write path (TableSnapshot
		// clones all 1024 placements and must not be used here).
		p, ok := e.slotPlacement(slot)
		// Only a moved leader clears the stamp. "still names this node the
		// leader" covers BOTH a node that has not applied the stage yet
		// (stable+self — indistinguishable from a rollback in the table) and
		// one that applied the stage but not the commit; in either case an
		// acknowledgement here is a record the new leader never inherits. The
		// TTL above bounds the cost when a plan really was rolled back.
		if !ok || p.Leader != e.self {
			f.mu.Lock()
			if f.committedTo == pending {
				f.committedTo = ""
			}
			f.mu.Unlock()
			return
		}
		if time.Now().After(deadline) {
			e.logger.Warn("slot handover: local demotion has not applied yet; serving the write here",
				"slot", slot, "committed_to", pending, "op", "handover_wait")
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (e *Engine) fenceFor(slot int32) *slotFence {
	if slot < 0 || int(slot) >= len(e.fences) {
		return nil
	}
	return &e.fences[slot]
}

// enterWriteFence registers a writer for a slot. It returns immediately unless
// a fence is held, in which case it BLOCKS until the fence is released — it
// never returns an error, so a client write is never failed by a fence. The
// returned func releases the registration and must be called once the append
// is done (the drain waits for it).
func (e *Engine) enterWriteFence(slot int32) func() {
	f := e.fenceFor(slot)
	if f == nil {
		return func() {}
	}
	for {
		f.mu.Lock()
		if !f.closed {
			f.hold++
			f.mu.Unlock()
			return f.leave
		}
		ch := f.done
		f.mu.Unlock()
		// Bounded by the fence watchdog: a held fence is always released.
		<-ch
	}
}

// acquireSlotFence takes the fence for a slot: it flags the slot closed so new
// appends block, then waits (bounded by config.FenceDrainTimeout) for the in-flight
// appends to drain so the caller can sample a frozen LEO. On a drain timeout
// the fence is released and an error returned; the migration then aborts.
func (e *Engine) acquireSlotFence(ctx context.Context, slot int32) error {
	f := e.fenceFor(slot)
	if f == nil {
		return fmt.Errorf("fence: slot %d out of range", slot)
	}
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return fmt.Errorf("fence: slot %d already fenced", slot)
	}
	f.closed = true
	f.gateArmed = false
	f.heldSince = time.Now()
	f.done = make(chan struct{})
	f.deadline = time.Now().Add(config.FenceMaxHold)
	if f.hold == 0 {
		f.drained = nil
	} else {
		f.drained = make(chan struct{})
	}
	drained := f.drained
	f.mu.Unlock()

	e.fenceSetAdd(slot)
	e.armFenceWatchdog(slot, f)

	if drained == nil {
		return nil // nothing in flight: the LEO is already frozen
	}
	dctx, cancel := context.WithTimeout(ctx, config.FenceDrainTimeout)
	defer cancel()
	select {
	case <-drained:
		return nil
	case <-dctx.Done():
		e.releaseSlotFence(slot)
		return fmt.Errorf("fence: slot %d timed out draining in-flight appends", slot)
	}
}

// releaseSlotFence lets blocked writers through and wakes them. The total hold
// (from acquire to release) is what a client append can be blocked behind, so
// it is logged as the fence's write-block cost.
func (e *Engine) releaseSlotFence(slot int32) {
	f := e.fenceFor(slot)
	if f == nil {
		return
	}
	f.mu.Lock()
	held := time.Duration(0)
	if f.closed {
		if !f.heldSince.IsZero() {
			held = time.Since(f.heldSince)
		}
		f.closed = false
		f.gateArmed = false
		f.heldSince = time.Time{}
		if f.done != nil {
			close(f.done)
			f.done = nil
		}
		f.drained = nil
	}
	f.mu.Unlock()
	if held > 0 {
		e.logger.Info("migration write fence released: blocked appends resumed",
			"slot", slot, "ms", held.Milliseconds(), "op", "fence_release")
	}
	e.fenceSetDel(slot)
}

// holdFenceUntilTargetLeader defers the release of a COMMITTED fence until the
// migration target has applied the leader move. The source keeps blocking
// writers (as the fence always does — they are never failed) until the target's
// own table names the target as the slot's leader at an epoch not older than
// the one the source committed, or until config.FenceTargetGateTimeout elapses. Only
// then is the fence released, so a writer woken here is redirected to a target
// that will serve it instead of bouncing back here (the MOVED/ASK ping-pong
// that failed clients with a small redirect budget).
//
// Idempotent per fence hold (gateArmed) and stale-safe: the goroutine releases
// only the exact hold it was armed for, and the fence watchdog still
// force-releases past config.FenceMaxHold, so a slow or lost target can never wedge
// the slot's writes.
func (e *Engine) holdFenceUntilTargetLeader(slot int32, from, target string, epoch int64) {
	// from is the leader the fence is moving AWAY from; kept in the signature
	// because the gate's proof is about BOTH homes (see the NOTE in the body).
	f := e.fenceFor(slot)
	if f == nil {
		return
	}
	f.mu.Lock()
	if !f.closed || f.gateArmed {
		f.mu.Unlock()
		return
	}
	f.gateArmed = true
	since := f.heldSince
	f.mu.Unlock()

	go func() {
		start := time.Now()
		deadline := start.Add(config.FenceTargetGateTimeout)
		ready := false
		for {
			if addr := e.peerAddr(target); addr != "" {
				ctx, cancel := context.WithTimeout(context.Background(), config.FenceTargetGateRPCTimeout)
				leader, ep, err := e.peerSlotLeader(ctx, addr, slot)
				cancel()
				if err == nil && leader == target && ep >= epoch {
					ready = true
					break
				}
			}
			if !time.Now().Before(deadline) {
				break
			}
			time.Sleep(config.FenceTargetGatePoll)
		}
		// NOTE (dg8 investigation): this node IS the old leader — reaching
		// this gate means its own table already applied the move, so there
		// is no self-demotion to wait for here. The residual hole the gate
		// cannot close is a foreign-version frame that lands as a NEW frame
		// on the successor's WAL during the handover window and breaks the
		// version chain (withholds one version, strands every later claim).
		// That shape is internally consistent per-home and invisible to the
		// gate; it is caught by the storage-layer chain-break detector
		// (Slot.TailStalled) on the replication apply path instead.
		_ = from
		// The gate proves the TARGET applied the move; it says nothing about
		// THIS node. Releasing while the local table still names this node the
		// leader wakes blocked writers that re-read that stale row, land the
		// record here and acknowledge it — an ack the new leader never
		// receives (the residual 1001, and a forked copy). So also wait for
		// the local demotion, bounded by the same gate deadline.
		for e.Leads(slot) && time.Now().Before(deadline) {
			time.Sleep(config.FenceTargetGatePoll)
		}
		if e.Leads(slot) {
			e.logger.Warn("migration write fence: local demotion has not applied; releasing anyway (write path holds each writer until it does)",
				"slot", slot, "target", target, "op", "fence_gate")
		}
		f.mu.Lock()
		stale := !f.closed || !f.heldSince.Equal(since)
		f.mu.Unlock()
		if stale {
			return // a newer fence (or none) owns the slot now
		}
		args := []any{
			"slot", slot, "target", target, "epoch", epoch,
			"waited_ms", time.Since(start).Milliseconds(), "target_ready", ready, "op", "fence_gate",
		}
		if ready {
			e.logger.Info("migration write fence: target applied the leader move; releasing the fence", args...)
		} else {
			e.logger.Warn("migration write fence: target did not confirm the leader move before the gate deadline; releasing so writes cannot be wedged", args...)
		}
		e.releaseSlotFence(slot)
	}()
}

// armFenceWatchdog force-releases a fence held past its bound. It captures the
// fence object and its deadline, so a later re-fence on the same slot (with a
// later deadline) is never released early by a stale watchdog.
func (e *Engine) armFenceWatchdog(slot int32, f *slotFence) {
	go func() {
		t := time.NewTimer(config.FenceMaxHold)
		defer t.Stop()
		<-t.C
		f.mu.Lock()
		stale := f.closed && !time.Now().Before(f.deadline)
		f.mu.Unlock()
		if stale {
			e.logger.Warn("migration write fence held past its bound; releasing so a lost controller cannot wedge the slot's writes", "slot", slot)
			e.releaseSlotFence(slot)
		}
	}()
}

func (e *Engine) fenceSetAdd(slot int32) {
	e.fenceMu.Lock()
	if e.fenceSet == nil {
		e.fenceSet = map[int32]bool{}
	}
	e.fenceSet[slot] = true
	e.fenceMu.Unlock()
}

func (e *Engine) fenceSetDel(slot int32) {
	e.fenceMu.Lock()
	delete(e.fenceSet, slot)
	e.fenceMu.Unlock()
}

// fencedSlots returns a snapshot of the currently fenced slots.
func (e *Engine) fencedSlots() []int32 {
	e.fenceMu.Lock()
	defer e.fenceMu.Unlock()
	out := make([]int32, 0, len(e.fenceSet))
	for s := range e.fenceSet {
		out = append(out, s)
	}
	return out
}

// HandleFenceSlot is the source-side half of the migration commit fence. It
// requires that this node still leads the slot, takes the fence (blocking new
// appends and draining the in-flight ones), freezes the LEO, ships the frozen
// tail to the migration target over the existing forward path and waits for the
// target to confirm it holds that LEO. It returns the frozen LEO; the fence
// REMAINS HELD (the controller commits the leader move next, and the source
// releases the fence when it applies that move or any rollback). On an error
// the fence is released and the migration aborts.
func (e *Engine) HandleFenceSlot(ctx context.Context, slot int32, target string) (uint64, error) {
	if !e.Leads(slot) {
		return 0, fmt.Errorf("fence: not the leader of slot %d", slot)
	}
	// target is the controller's committed migration target. The source's
	// own table may not have applied the stage when this RPC lands (the
	// controller awaits only its own Raft apply), and "no target visible
	// here" must not silently skip shipping the frozen tail — that is how
	// handovers used to commit over a target that never received the tail
	// the source acked after fencing (stranded writes; the tail a rebuilt
	// copy can serve vs the LEO the gate confirmed). With the controller's
	// instruction the fence runs on authority, not on a lagging view; an
	// empty target falls back to the local table (the in-process commit
	// path, where the stage applied HERE by construction).
	if target == "" {
		target = e.migrationTargetOf(slot)
	}
	start := time.Now()
	if err := e.acquireSlotFence(ctx, slot); err != nil {
		return 0, err
	}
	// From here on the move may commit: a writer woken by the fence release
	// must wait for THIS node to apply the demotion before it may serve the
	// slot again (see slotFence.committedTo).
	if f := e.fenceFor(slot); f != nil && target != "" {
		f.mu.Lock()
		f.committedTo = target
		f.committedAt = time.Now()
		f.mu.Unlock()
	}
	ok := false
	defer func() {
		if !ok {
			e.releaseSlotFence(slot)
		}
	}()
	leo := e.store.LastSeqOf(slot)
	if err := e.pushFencedTail(ctx, slot, leo, target); err != nil {
		return 0, err
	}
	// Split-view guard. The fence only runs on the node that BELIEVES it leads
	// (the guard at the top of this function), and the frozen LEO is that
	// copy's. A peer whose own table still names IT the leader — a lagging
	// demotion, a plan whose source id and the applied table disagreed — keeps
	// acknowledging client writes at seqs this frozen tail does not cover, and
	// those acks are then lost with the demoted copy: the client later hits
	// 1001 at a leader that never received them (the dg14 class: every acks we
	// could attribute during a handover happened while the slot was
	// migrating_out and NOT yet fenced). Proof before commit: ask every other
	// replica what it believes. Any peer that names itself leader at an epoch
	// at least ours AND holds a longer log than the frozen tail means this
	// handover would cut off acknowledged records — fail it, the controller
	// rolls back and the next round retries once the view has converged.
	if err := e.verifyNoCompetingLeader(ctx, slot, leo, target); err != nil {
		return 0, err
	}
	ok = true
	e.logger.Info("migration write fence held: new appends blocked, target confirmed the frozen LEO",
		"slot", slot, "leo", leo, "ms", time.Since(start).Milliseconds(), "op", "fence")
	return leo, nil
}

// pushFencedTail ships the records the target is missing (up to the frozen
// LEO) and waits until the target reports it holds them. It reuses the existing
// replication/forward path and the existing LEO probe — no new data channel.
//
// The target's streams must first be CONSISTENT with its own LEO (DESIGN.md
// §1.1 rule 3). A target can sit at a high LEO while one aggregate's stream is
// short: records appended above it never entered the target's directory, so they
// are in its WAL but no reader can serve them. Shipping an increment cannot
// repair that — the seqs those missing versions would need are already spent on
// other aggregates (rule 1) and only counter+1 may be written (rule 2) — and the
// catch-up gate compares LEOs, so it would report "caught up" about a target
// that cannot serve everything the new leader will be asked for.
//
// So an inconsistent target is REBUILT: its local copy is dropped and re-pulled
// from scratch, while the source still holds the full slot and the fence keeps
// this slot frozen. A consistent target takes the ordinary incremental path.
func (e *Engine) pushFencedTail(ctx context.Context, slot int32, leo uint64, target string) error {
	to := target
	if to == "" {
		to = e.migrationTargetOf(slot)
	}
	if to == "" {
		// A fence RPC arrives while the source's OWN table may not yet have
		// applied the migrating_out the controller just committed through
		// Raft (the controller only awaits its own apply). Shipping "nothing"
		// on that lag used to be a silent nil — the commit would then move
		// leadership over a target that never received the frozen tail: the
		// acked records stranded on the demoted source (the slot LEO the
		// handover confirmed vs the 0 a rebuilt copy can serve), and the
		// demoted source keeps accepting redirected writes no one ever
		// fetches from it. A commit is terminal for the log handoff, so this
		// must fail LOUD: the controller aborts, rolls the slot back to
		// stable, and the next rebalance round fences a table that has since
		// caught up.
		return fmt.Errorf("fence: slot %d is not migrating in this node's table; the handover must wait for the stage to apply here (retryable)", slot)
	}
	addr := e.peerAddr(to)
	if addr == "" {
		return fmt.Errorf("fence: no address for migration target %s", to)
	}
	if err := e.verifyTargetHoldsSource(ctx, slot, addr, to); err != nil {
		return err
	}
	deadline := time.Now().Add(config.FenceCatchUpTimeout)
	for {
		tgtLEO, err := e.remoteLEO(ctx, addr, slot)
		if err != nil {
			return err
		}
		if tgtLEO >= leo {
			// The target is ABOVE the frozen source tail: it appended as a
			// leader (or from another writer) before this fence existed, so
			// its seqs past the frozen tail are its own records. Committing
			// now would fork the log — the source's acknowledged records at
			// those seqs would never be inherited, and the client would meet
			// a leader whose copy lacks them. Fail the handover; the
			// controller rolls back and retries once the view converges.
			if tgtLEO > leo {
				return fmt.Errorf("fence: target %s log end %d is above the frozen source tail %d — it wrote as a leader before the fence; committing would fork slot %d",
					to, tgtLEO, leo, slot)
			}
			// Held, not yet durable: the leadership may only move once the
			// frozen tail is ON DISK at the target (see confirmTargetDurable).
			return e.confirmTargetDurable(ctx, addr, slot, leo, to)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("fence: target %s LEO %d < frozen source LEO %d (catch-up timeout)", to, tgtLEO, leo)
		}
		if err := e.pushFrames(ctx, addr, slot, tgtLEO+1, leo); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// confirmTargetDurable requires the target to hold the frozen tail durably
// before the leader move commits. A committed move is terminal and the source
// stops being recoverable once it leaves the replica set, so on a single-copy
// layout the last acknowledged records would otherwise live only in the
// target's page cache: a target that then died would lose writes the cluster
// acknowledged. It flushes, re-asks, and gives up (failing the hand-over) on the
// bound — the retention on the source's copy is what covers the retry.
// verifyNoCompetingLeader proves, before a handover commits, that no OTHER
// replica of the slot still acts as its leader with a longer log than the
// frozen tail. See the call site in HandleFenceSlot for why this is the one
// hole the fence itself cannot close: the fence runs on the copy that
// believes it leads, while a peer holding a stale view (a lagging demotion, a
// plan whose source and the applied table disagreed) keeps acknowledging
// writes at seqs the frozen tail does not cover — and those acks die with the
// demoted copy, surfacing as a 1001 at a leader that never received them.
//
// Unreachable peers are not treated as competitors (a peer we cannot reach
// may be dead, and a dead node acknowledges nothing); the guard is aimed at
// the live split view, which is the one that loses acknowledged records.
func (e *Engine) verifyNoCompetingLeader(ctx context.Context, slot int32, frozenLEO uint64, target string) error {
	tbl := e.TableSnapshot()
	p, ok := tbl.Slots[slot]
	if !ok {
		return nil
	}
	myEpoch := p.Epoch
	for _, replica := range p.Replicas {
		if replica == e.self || replica == target {
			continue
		}
		pe, known := tbl.Peers[replica]
		if !known || pe.Offline() {
			continue
		}
		addr := e.peerAddr(replica)
		if addr == "" {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, config.FenceTargetGateRPCTimeout)
		leader, epoch, err := e.peerSlotLeader(cctx, addr, slot)
		cancel()
		if err != nil {
			e.logger.Warn("migration fence: competing-leader check could not reach a replica; proceeding (a dead peer acknowledges nothing)",
				"slot", slot, "peer", replica, "err", err, "op", "fence_quorum")
			continue
		}
		if leader != replica || epoch < myEpoch {
			continue
		}
		cctx2, cancel2 := context.WithTimeout(ctx, config.FenceTargetGateRPCTimeout)
		peerLEO, lerr := e.remoteLEO(cctx2, addr, slot)
		cancel2()
		if lerr != nil {
			e.logger.Warn("migration fence: competing leader found but its log end could not be read; proceeding",
				"slot", slot, "peer", replica, "epoch", epoch, "err", lerr, "op", "fence_quorum")
			continue
		}
		if peerLEO > frozenLEO {
			return fmt.Errorf("fence: slot %d has a competing leader %s at epoch %d whose log end %d is above the frozen tail %d — its acknowledged writes would be cut off; aborting the handover",
				slot, replica, epoch, peerLEO, frozenLEO)
		}
	}
	return nil
}

func (e *Engine) confirmTargetDurable(ctx context.Context, addr string, slot int32, leo uint64, to string) error {
	deadline := time.Now().Add(config.FenceDurableTimeout)
	var last uint64
	for {
		dur, err := e.peerDurableLEO(ctx, addr, slot)
		if err != nil {
			return err
		}
		last = dur
		if dur >= leo {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("fence: target %s durable LEO %d < frozen source LEO %d (flush timeout)", to, last, leo)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// pushFrames reads records [fromSeq, toSeq] from the local slot and pushes each
// to a peer at its own seq over the migration Replicate RPC (idempotent: the
// peer ignores a record it already holds at that seq).
func (e *Engine) pushFrames(ctx context.Context, toAddr string, slot int32, fromSeq, toSeq uint64) error {
	seq := fromSeq
	for seq <= toSeq {
		_, next, payload, err := e.store.ReadSlotBytes(slot, seq, toSeq+1, config.FencePushBatch)
		if err != nil {
			return err
		}
		if len(payload) == 0 {
			return nil
		}
		rest := payload
		s := seq
		for len(rest) > 0 {
			_, consumed, err := data.DecodeRecordMeta(rest)
			if err != nil {
				return err
			}
			if err := e.peerReplicate(ctx, toAddr, slot, s, rest[:consumed]); err != nil {
				return err
			}
			rest = rest[consumed:]
			s++
		}
		if next <= seq {
			return nil // no progress: avoid a spin
		}
		seq = next
	}
	return nil
}

// fenceSource asks the migration source to take the commit fence and returns
// the frozen LEO the target has confirmed. When the controller IS the source it
// runs in-process.
func (e *Engine) fenceSource(ctx context.Context, from string, slot int32, target string) (uint64, error) {
	if from == e.self {
		return e.HandleFenceSlot(ctx, slot, target)
	}
	addr := e.peerAddr(from)
	if addr == "" {
		return 0, fmt.Errorf("fence: no address for source %s", from)
	}
	fctx, cancel := context.WithTimeout(ctx, config.FenceRPCTimeout)
	defer cancel()
	return e.peerFence(fctx, addr, slot, target)
}
