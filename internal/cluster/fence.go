package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"pushupes/internal/data"
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
// The gap is the commit window: awaitCaughtUp samples the target's LEO and the
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
// fence past fenceMaxHold so a lost controller can never wedge a slot's
// writes. Any timeout aborts the migration cleanly (rollback to stable) and the
// source keeps leading.
const (
	// fenceDrainTimeout bounds waiting for in-flight appends to finish while
	// the fence is being taken. A write already accepted holds the fence for
	// its whole local append (including an acks=all watermark wait), so this
	// must exceed a healthy wait and stays far below the 10s client deadline.
	fenceDrainTimeout = 2 * time.Second
	// fenceCatchUpTimeout bounds how long the source holds the fence waiting
	// for the target to confirm the frozen tail. The common case is "already
	// caught up" (the best-effort mirror and the fetch loop keep the target
	// within a few ms) and returns almost immediately; this is the fallback.
	fenceCatchUpTimeout = 3 * time.Second
	// fenceMaxHold is the source-side watchdog: a fence is force-released
	// after this long even if no controller action ever arrives, so a crashed
	// controller cannot block a slot's writes forever.
	fenceMaxHold = 8 * time.Second
	// fenceRPCTimeout is the controller-side bound on the FenceSlot call: it
	// covers the drain + catch-up on the source plus one RPC margin.
	fenceRPCTimeout = fenceDrainTimeout + fenceCatchUpTimeout + time.Second
	// fencePushBatch caps one tail-push read (frames are streamed in batches).
	fencePushBatch = 4 << 20

	// fenceTargetGateTimeout bounds how long the source keeps a COMMITTED
	// fence's writers blocked while waiting for the migration target to apply
	// the leader move (see holdFenceUntilTargetLeader). The target applies it
	// within a Raft round trip (a few ms); this is the fallback when the target
	// is slow or unreachable, and stays well inside the 10s client deadline and
	// the fence's own watchdog (fenceMaxHold).
	fenceTargetGateTimeout = 1 * time.Second
	// fenceTargetGatePoll is the interval between two target-readiness probes.
	fenceTargetGatePoll = 2 * time.Millisecond
	// fenceTargetGateRPCTimeout bounds one readiness probe (a hung target must
	// not stretch the gate past its deadline).
	fenceTargetGateRPCTimeout = 500 * time.Millisecond
)

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
// appends block, then waits (bounded by fenceDrainTimeout) for the in-flight
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
	f.deadline = time.Now().Add(fenceMaxHold)
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
	dctx, cancel := context.WithTimeout(ctx, fenceDrainTimeout)
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
		e.logger.WithFields(map[string]any{"slot": slot, "ms": held.Milliseconds(), "op": "fence_release"}).
			Info("migration write fence released: blocked appends resumed")
	}
	e.fenceSetDel(slot)
}

// holdFenceUntilTargetLeader defers the release of a COMMITTED fence until the
// migration target has applied the leader move. The source keeps blocking
// writers (as the fence always does — they are never failed) until the target's
// own table names the target as the slot's leader at an epoch not older than
// the one the source committed, or until fenceTargetGateTimeout elapses. Only
// then is the fence released, so a writer woken here is redirected to a target
// that will serve it instead of bouncing back here (the MOVED/ASK ping-pong
// that failed clients with a small redirect budget).
//
// Idempotent per fence hold (gateArmed) and stale-safe: the goroutine releases
// only the exact hold it was armed for, and the fence watchdog still
// force-releases past fenceMaxHold, so a slow or lost target can never wedge
// the slot's writes.
func (e *Engine) holdFenceUntilTargetLeader(slot int32, target string, epoch int64) {
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
		deadline := start.Add(fenceTargetGateTimeout)
		ready := false
		for {
			if addr := e.peerAddr(target); addr != "" {
				ctx, cancel := context.WithTimeout(context.Background(), fenceTargetGateRPCTimeout)
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
			time.Sleep(fenceTargetGatePoll)
		}
		f.mu.Lock()
		stale := !f.closed || !f.heldSince.Equal(since)
		f.mu.Unlock()
		if stale {
			return // a newer fence (or none) owns the slot now
		}
		fields := map[string]any{
			"slot": slot, "target": target, "epoch": epoch,
			"waited_ms": time.Since(start).Milliseconds(), "target_ready": ready, "op": "fence_gate",
		}
		if ready {
			e.logger.WithFields(fields).Info("migration write fence: target applied the leader move; releasing the fence")
		} else {
			e.logger.WithFields(fields).Warn("migration write fence: target did not confirm the leader move before the gate deadline; releasing so writes cannot be wedged")
		}
		e.releaseSlotFence(slot)
	}()
}

// armFenceWatchdog force-releases a fence held past its bound. It captures the
// fence object and its deadline, so a later re-fence on the same slot (with a
// later deadline) is never released early by a stale watchdog.
func (e *Engine) armFenceWatchdog(slot int32, f *slotFence) {
	go func() {
		t := time.NewTimer(fenceMaxHold)
		defer t.Stop()
		<-t.C
		f.mu.Lock()
		stale := f.closed && !time.Now().Before(f.deadline)
		f.mu.Unlock()
		if stale {
			e.logger.WithField("slot", slot).
				Warn("migration write fence held past its bound; releasing so a lost controller cannot wedge the slot's writes")
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
func (e *Engine) HandleFenceSlot(ctx context.Context, slot int32) (uint64, error) {
	if !e.Leads(slot) {
		return 0, fmt.Errorf("fence: not the leader of slot %d", slot)
	}
	start := time.Now()
	if err := e.acquireSlotFence(ctx, slot); err != nil {
		return 0, err
	}
	ok := false
	defer func() {
		if !ok {
			e.releaseSlotFence(slot)
		}
	}()
	leo := e.store.LastSeqOf(slot)
	if err := e.pushFencedTail(ctx, slot, leo); err != nil {
		return 0, err
	}
	ok = true
	e.logger.WithFields(map[string]any{"slot": slot, "leo": leo, "ms": time.Since(start).Milliseconds(), "op": "fence"}).
		Info("migration write fence held: new appends blocked, target confirmed the frozen LEO")
	return leo, nil
}

// pushFencedTail ships the records the target is missing (up to the frozen
// LEO) and waits until the target reports it holds them. It reuses the existing
// replication/forward path and the existing LEO probe — no new data channel.
func (e *Engine) pushFencedTail(ctx context.Context, slot int32, leo uint64) error {
	to := e.migrationTargetOf(slot)
	if to == "" {
		return nil // no longer migrating: nothing to ship
	}
	addr := e.peerAddr(to)
	if addr == "" {
		return fmt.Errorf("fence: no address for migration target %s", to)
	}
	deadline := time.Now().Add(fenceCatchUpTimeout)
	for {
		tgtLEO, err := e.remoteLEO(ctx, addr, slot)
		if err != nil {
			return err
		}
		if tgtLEO >= leo {
			return nil
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

// pushFrames reads records [fromSeq, toSeq] from the local slot and pushes each
// to a peer at its own seq over the migration Replicate RPC (idempotent: the
// peer ignores a record it already holds at that seq).
func (e *Engine) pushFrames(ctx context.Context, toAddr string, slot int32, fromSeq, toSeq uint64) error {
	seq := fromSeq
	for seq <= toSeq {
		_, next, payload, err := e.store.ReadSlotBytes(slot, seq, toSeq+1, fencePushBatch)
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
func (e *Engine) fenceSource(ctx context.Context, from string, slot int32) (uint64, error) {
	if from == e.self {
		return e.HandleFenceSlot(ctx, slot)
	}
	addr := e.peerAddr(from)
	if addr == "" {
		return 0, fmt.Errorf("fence: no address for source %s", from)
	}
	fctx, cancel := context.WithTimeout(ctx, fenceRPCTimeout)
	defer cancel()
	return e.peerFence(fctx, addr, slot)
}
