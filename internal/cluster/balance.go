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
	"sync"
	"time"

	"github.com/berkaroad/pushupes/internal/config"
)

// Leader rebalance ("回切"): after a node dies, the failover sweep moves all
// of its slots' leadership to surviving replicas; those nodes now lead their
// own slots PLUS the dead node's share. When the node comes back, replan_slots
// re-joins it to the replica sets (it pulls its data over the ordinary fetch
// protocol) but never takes leadership back — RunController's invariant is
// "leaders of assigned, stable slots are never moved". This loop restores the
// ring layout: it moves each deviating slot's leader back onto the ring's
// expected node (PlanLeaderRebalance, anchored on PlanSlots), reusing the
// committed hot-migration path (fence + epoch bump + MOVED redirect) rather
// than inventing a second leader-change channel.
//
// The layout anchor is the RING, not a free "equalise the counts" search:
// after a failover the table returns to exactly the layout the cluster had
// before the node died — deterministic, explainable, and a pure function of
// the table, so a controller restart changes nothing. (The trade-off, accepted
// in design: a hand-picked placement that the ring would express as a move is
// moved back by the rebalancer.)
//
// Cadence and safety gates, in order:
//
//   - only the controller (Raft leader) runs rounds; every move is a
//     controller-guarded StartMigration, so a stale controller cannot act;
//   - a round is skipped while ANY slot is not stable — a human-initiated
//     migrate owns the layout and the rebalancer yields to it;
//   - a slot moves only when its ring-expected node already HOLDS a copy that
//     is equivalent to the source's (same directory digest, not behind). That
//     is both the gate and what makes the move cheap: StartMigration's fast
//     path skips the segment snapshot for such a target, so a hand-back is
//     pure metadata plus a millisecond fence — a restart of one node never
//     moves slot data around. The gate distinguishes "still catching up"
//     (wait) from "diverged" (same LEO, different directory — a copy whose
//     aggregate directory stopped adopting versions, which fetching can
//     never repair): a diverged target is handed to the ordinary migration
//     path, whose ensureTargetConsistent drops and rebuilds it. Refusing
//     both would strand such a slot off the ring forever;
//   - moves run strictly serially, at most `batch` per round: each one's
//     commit fence freezes exactly one slot, and serial rounds keep the
//     frozen-slot count at one, so rebalance latency noise is bounded to a
//     single slot's writes at a time;
//   - a failed move is logged and simply re-planned next round; a slow target
//     that keeps failing catch-up costs nothing but a round, which is the
//     natural backoff — there is no failure counter to maintain.

// SetRebalanceConfig wires the -rebalance-interval knob. An interval <= 0
// disables the rebalancer; startup validation rejects negative values, so 0 is
// the operator's "off" switch.
func (e *Engine) SetRebalanceConfig(interval time.Duration) {
	e.rebalanceInterval = interval
}

// rebalanceMayRun reports whether a controller round may start a hand-over now.
// The start-up grace window keeps the round off the layout while the data nodes
// are still coming up: the layout has not moved (liveness verdicts are held for
// those nodes), and staying out of that window means no commit fence lands
// before the cluster is whole. A drain is the operator asking for motion, so it
// outranks the wait.
func (e *Engine) rebalanceMayRun(tbl *Table, draining bool) bool {
	if draining {
		return true
	}
	return len(e.startupAwaiting(tbl, time.Now())) == 0
}

func (e *Engine) rebalanceEnabled() bool {
	return e.rebalanceInterval > 0
}

// RunRebalancer is the leader-rebalance loop: one round every
// rebalanceInterval while this node holds the Raft leadership. Launched from
// Engine.Start like RunController.
func (e *Engine) RunRebalancer(ctx context.Context) {
	// The switch is off: this loop owes nothing — it returns until a drain
	// marks the table, and whoever marks it (or inherits a marked table as
	// controller) wakes the drain driver instead (wakeDrainRunner, run from
	// the controller tick). No idle watcher here, no busy poll. An ENABLED
	// cluster always keeps its loop: the ring-return rounds are its own job.
	if !e.rebalanceEnabled() && !e.TableSnapshot().HasDraining() {
		return
	}
	// Running while the switch is off means this loop IS the drain driver —
	// claim the seat once so the wake path cannot start a second one
	// alongside; release on the way out (when the drain ends this returns,
	// and the next mark wakes a runner instead).
	if !e.rebalanceEnabled() {
		if !e.claimDrainDriver() {
			return // a runner already holds the wheel for this drain
		}
		defer e.releaseDrainDriver()
	}
	for {
		draining := e.TableSnapshot().HasDraining()
		if !e.rebalanceEnabled() && !draining {
			// The drain this loop drove is done (removed or withdrawn):
			// hand the wheel back (the deferred release) and go dark —
			// an operator who turned rebalancing off must not get it
			// running forever the moment the scale-down completes.
			return
		}
		interval := e.rebalanceInterval
		if draining && (!e.rebalanceEnabled() || interval > config.DrainTick) {
			// A drain outranks the configured pace: handover rounds run
			// back to back at config.DrainTick until the node is empty. With the
			// switch off the configured interval is 0 — a bare `>` test
			// against it would NewTimer(0) straight into a busy loop, so
			// the disabled case takes config.DrainTick unconditionally.
			interval = config.DrainTick
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if e.node == nil || !e.node.IsLeader() {
			continue
		}
		limit := config.RebalanceCommitsPerRound
		if draining {
			limit = config.DrainCommitsPerRound // a scale-down in progress sets the pace
		}
		// Start-up grace: while the window is open, no hand-over starts. The
		// layout has not moved yet (the verdicts that would move it are held —
		// see Engine.startupAwaiting), so the round has nothing to do anyway;
		// holding it keeps the window free of commit fences. A drain is an
		// operator asking for motion and outranks the wait.
		if e.rebalanceMayRun(e.TableSnapshot(), draining) {
			e.rebalanceRound(ctx, limit)
		}
		e.finishPendingRemovals(ctx)
		// Surplus-seat reclaim: the other half of a re-layout. A re-layout only
		// ADDS seats (Table.applyReplanSlots), so a set left oversized by a
		// factor change converges here, in one budgeted sweep per round and only
		// once the copies that stay are in sync (see reclaimRound).
		e.reclaimRound(ctx)
	}
}

// rebalanceRound runs one controller round and returns how many layout
// transitions it executed. A transition is one step of the handover state
// machine (see migration.go): stage a target that is not a replica yet, drop a
// copy that can never converge, or commit a target that already holds the
// slot's log. The limit bounds steps per round; one step per round makes the
// loop strictly serial, which is what keeps the number of slots frozen by a
// commit fence at one.
func (e *Engine) rebalanceRound(ctx context.Context, limit int) int {
	if limit <= 0 {
		return 0
	}
	tbl := e.TableSnapshot()
	if len(tbl.Peers) < 2 || !layoutSettled(tbl) {
		return 0 // single-node cluster, or a live handover owns the layout: yield to it
	}
	moves := PlanLeaderRebalance(tbl)
	if len(moves) == 0 {
		return 0
	}
	done := 0
	for _, m := range moves {
		if done >= limit || ctx.Err() != nil {
			break
		}
		srcAddr := tbl.Peers[m.From].PeerAddr
		dstAddr := tbl.Peers[m.To].PeerAddr
		if srcAddr == "" || dstAddr == "" {
			continue // a peer that has not registered yet: next round
		}
		// Re-read the placement immediately before acting: the round's plan was
		// computed from a snapshot, and a human migrate may have staged or
		// committed this slot since.
		cur, ok := e.TableSnapshot().Slots[m.Slot]
		if !ok || cur.State != SlotStable || cur.Leader == m.To {
			continue
		}
		if !e.alive(dstAddr) {
			continue // the move-back target must be reachable (its liveness is
			// also what failover is about to re-shape: do not race it)
		}
		// A draining node that is ALSO dead cannot be fenced or asked for
		// its log end — and does not need either: nothing writes through a
		// dead process. Its slots hand over by plain table command (the
		// epoch bump is the fence, exactly as on failover), so a DELETE of
		// a crashed node drains instead of waiting on a handover that can
		// never get a source-side answer.
		if pe, ok := e.TableSnapshot().Peers[m.From]; ok && pe.Draining && pe.Offline() {
			if err := e.submit(&Command{Op: OpLeaderMove, Slots: []int32{m.Slot}, NewLeader: m.To}); err != nil {
				continue
			}
			e.logger.Info("rebalance: dead drain source; slot handed over by table command",
				"slot", m.Slot, "from", m.From, "to", m.To, "op", OpLeaderMove)
			done++
			continue
		}
		// 1) stage: a target outside the replica set has no copy to catch up
		// from. Joining it to the set is the cheap half of the handover and it
		// is what starts the fetch; the commit waits for a later round. No
		// backoff here: nothing was asked of a peer yet and the command is
		// idempotent.
		if !replicaListHas(cur.Replicas, m.To) {
			if _, err := e.stageMigration(ctx, m.Slot, m.To); err != nil {
				e.loggerf("rebalance: staging slot %d to %s failed: %v (retried next round)", m.Slot, m.To, err)
				continue
			}
			e.logger.Info("rebalance: handover staged; the target now pulls the slot over the fetch path",
				"slot", m.Slot, "target", m.To, "op", OpSlotAddReplica)
			done++
			continue
		}
		// 2) gate: the target must hold the source's log. BEHIND means the
		// fetch path still has work — wait, and do not accumulate backoff:
		// nothing failed. A copy with the same (or a higher) log end offset but
		// a DIFFERENT directory can never converge by fetching (the records
		// above the gap sit in its WAL unreadable), so it is dropped here and
		// pulls again from empty.
		ready, diverged, err := e.handoverReady(ctx, m.Slot, m.From, m.To)
		if err != nil {
			continue // peer-plane error: it will either heal or be caught next round
		}
		if diverged {
			// One reading is not evidence: confirm it next round first, so a
			// directory still settling behind its log end offset is not
			// mistaken for a fork (see confirmDiverged).
			if !e.confirmDiverged(m.Slot, m.From, m.To, time.Now()) {
				continue
			}
			// The drop is heavy (the copy is pulled from scratch), so it backs
			// off: without that, a target that keeps diverging would be dropped
			// every round, forever.
			if !e.rebalanceRetryReady(m.Slot, m.From, m.To, time.Now()) {
				continue
			}
			e.recordRebalanceAttempt(m.Slot, m.From, m.To, time.Now())
			e.logger.Warn("rebalance: target holds the source's log end offset but a different directory; dropping it to pull again",
				"slot", m.Slot, "from", m.From, "to", m.To, "op", "rebalance_repair")
			if err := e.peerDropSlot(ctx, dstAddr, m.Slot, m.To); err != nil {
				continue
			}
			done++
			continue
		}
		e.clearDiverged(m.Slot) // a consistent or merely-behind reading: forget any fork
		if !ready {
			continue // still catching up: the cheap hand-back waits for it
		}
		// 3) commit: the fence freezes this slot for the length of a leader
		// move. A commit that keeps failing backs off.
		if !e.rebalanceRetryReady(m.Slot, m.From, m.To, time.Now()) {
			continue
		}
		e.recordRebalanceAttempt(m.Slot, m.From, m.To, time.Now())
		if err := e.commitMigration(ctx, m.Slot, m.To); err != nil {
			e.loggerf("rebalance: committing slot %d to %s failed: %v (retried with backoff)", m.Slot, m.To, err)
			continue
		}
		e.logger.Info("leader rebalanced back to the ring layout",
			"slot", m.Slot, "from", m.From, "to", m.To, "op", "rebalance")
		done++
	}
	return done
}

// ---- Retry backoff for hand-overs that do not stick --------------------------

// A hand-over that fails costs real work. A target whose copy is DIVERGED (the
// source's LEO with a different directory) is rebuilt — the source re-ships the
// whole slot under the commit fence — and a move that aborts is re-planned. Both
// used to happen again the very next round, i.e. a full slot snapshot every
// config.RebalanceInterval for as long as the target cannot converge. Failed
// attempts therefore back off per slot: the first retry is the next round (as
// before), and each further one doubles up to a cap. The record is keyed on the
// move's endpoints, so a failover (a different from/to) gets a fresh try — it
// is a different move, not a retry.

type rebalanceAttempt struct {
	from, to  string
	attempts  int
	notBefore time.Time
}

// rebalanceRetryDelay is the wait after the given attempt number: 2s, 4s, 8s,
// ... capped. Pure, so the escalation is assertable on its own.
func rebalanceRetryDelay(attempts int) time.Duration {
	d := config.RebalanceRetryBase
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= config.RebalanceRetryMax {
			return config.RebalanceRetryMax
		}
	}
	return d
}

// confirmDiverged records this reading and reports whether the copy has been
// reading as diverged long enough to drop it. The window lives on the engine so
// tests can pin the confirmation without waiting.
func (e *Engine) confirmDiverged(slot int32, from, to string, now time.Time) bool {
	e.rebMu.Lock()
	defer e.rebMu.Unlock()
	if e.divSeen == nil {
		e.divSeen = map[int32]rebalanceAttempt{}
	}
	cur, ok := e.divSeen[slot]
	if ok && cur.from == from && cur.to == to && now.Sub(cur.notBefore) >= e.divergedConfirm {
		delete(e.divSeen, slot)
		return true
	}
	e.divSeen[slot] = rebalanceAttempt{from: from, to: to, notBefore: now}
	return false
}

// clearDiverged forgets a copy's diverged reading: a round has just read it as
// consistent or merely behind, so a later fork confirms from scratch.
func (e *Engine) clearDiverged(slot int32) {
	e.rebMu.Lock()
	delete(e.divSeen, slot)
	e.rebMu.Unlock()
}

func (e *Engine) rebalanceRetryReady(slot int32, from, to string, now time.Time) bool {
	e.rebMu.Lock()
	defer e.rebMu.Unlock()
	return retryReady(e.rebRetry, slot, from, to, now)
}

// recordRebalanceAttempt counts one attempt of this move and sets when the next
// one may start.
func (e *Engine) recordRebalanceAttempt(slot int32, from, to string, now time.Time) {
	e.rebMu.Lock()
	defer e.rebMu.Unlock()
	recordAttempt(&e.rebRetry, slot, from, to, now)
}

// seatRetryReady / recordSeatAttempt are the same gate for the surplus-seat
// reclaim (reclaimRound), keyed on the seat being dropped: a copy whose kept
// peers never converge would otherwise cost one digest round trip per seat per
// round, forever.
func (e *Engine) seatRetryReady(slot int32, seat string, now time.Time) bool {
	e.rebMu.Lock()
	defer e.rebMu.Unlock()
	return retryReady(e.seatRetry, slot, seat, "", now)
}

func (e *Engine) recordSeatAttempt(slot int32, seat string, now time.Time) {
	e.rebMu.Lock()
	defer e.rebMu.Unlock()
	recordAttempt(&e.seatRetry, slot, seat, "", now)
}

// retryReady reports whether the attempt recorded for this (slot, from, to) key
// may be tried now. Read-only, so a slot that is merely WAITING (a copy still
// catching up) does not accumulate backoff.
func retryReady(m map[int32]rebalanceAttempt, slot int32, from, to string, now time.Time) bool {
	cur, ok := m[slot]
	if !ok || cur.from != from || cur.to != to {
		return true // nothing recorded, or a different move
	}
	return !now.Before(cur.notBefore)
}

// recordAttempt counts one attempt of the key and sets when the next one of the
// SAME key may start. A different key (a different move, a different seat) is a
// fresh try, not a retry.
func recordAttempt(m *map[int32]rebalanceAttempt, slot int32, from, to string, now time.Time) {
	if *m == nil {
		*m = map[int32]rebalanceAttempt{}
	}
	cur, ok := (*m)[slot]
	if !ok || cur.from != from || cur.to != to {
		cur = rebalanceAttempt{from: from, to: to}
	}
	cur.attempts++
	cur.notBefore = now.Add(rebalanceRetryDelay(cur.attempts))
	(*m)[slot] = cur
}

// ---- Surplus-seat reclaim: the other half of a re-layout ---------------------

// reclaimRound drops the seats a re-layout left redundant: one seat per slot,
// and the round is capped like the hand-overs (a batch per tick, serial), so
// convergence costs one Raft entry per seat instead of a burst.
//
// A re-layout only ADDS seats (Table.applyReplanSlots), because removing one
// takes a copy out of the slot's replica set and the failure path picks the
// slot's new leader from the front of that set (backupLeaderFor) — the copies a
// stale set holds are the ones that have actually been replicating the slot.
// The surplus therefore comes off here, against real copies: a seat is dropped
// only when every seat that STAYS already holds a copy equivalent to the
// leader's (same directory digest, not behind). Until then the extra seat is
// what keeps the slot's acknowledged records available if the leader dies, so
// waiting is not a delay but the point.
//
// Gates, in order (same shape as the hand-overs):
//   - controller only, and a round yields while ANY slot is not stable (a live
//     migration owns the layout);
//   - the surplus is a seat the ring plan does not prescribe — the plan is the
//     same one the re-layout adds (Table.PeerIDs, the full directory), so a
//     member that is merely DOWN keeps its seat and is never reclaimed;
//   - the leader is never a candidate (Table.surplusSeatForReplan);
//   - a seat whose kept peers are unreachable, or diverged rather than merely
//     behind, is skipped with backoff: it will either heal or wait for the
//     operator (the hand-over path owns repairing a diverged copy).
//
// reclaimCandidate is one surplus seat a sweep may retire: the slot, the seat to
// drop, and the placement the decision was made from.
type reclaimCandidate struct {
	slot      int32
	drop      string
	placement *Placement
}

// probeParked reports whether a probe of this candidate would touch a peer that
// already failed one this round. It mirrors keptCopiesInSync's peer set exactly:
// the leader plus the seats that stay.
func probeParked(parked map[string]bool, p *Placement, drop string) bool {
	if parked[p.Leader] {
		return true
	}
	for _, r := range p.Replicas {
		if r != drop && parked[r] {
			return true
		}
	}
	return false
}

// reclaimRound retires the surplus replica seats a re-layout left behind: a
// re-layout only ADDS seats (Table.applyReplanSlots), so a set left oversized by
// a factor change converges here. One call sweeps every surplus slot it can
// within config.ReclaimBudget rather than one seat per round, with three guards so the
// sweep cannot turn into a stall:
//
//   - a deadline (config.ReclaimBudget): the round stops on time and the next one
//     continues where it left off;
//   - a per-peer circuit breaker: a peer that fails a probe is parked for the
//     rest of the round, so one unreachable node costs one timeout instead of
//     one per seat it holds — the failure mode a serial one-seat-per-round loop
//     was accidentally protected from;
//   - a bounded probe pool plus one table command per node: the copy checks run
//     config.ReclaimProbeConcurrency at a time, and the removals for one seat travel in
//     a single command (OpSlotRemoveReplica carries a slot list), so a
//     1024-seat reclaim costs a handful of Raft entries, not thousands.
//
// The safety rule is unchanged: a seat only comes off once every seat that STAYS
// holds a copy equivalent to the leader's (keptCopiesInSync), so the dropped
// seat is redundant by construction. The layout must be settled — a live
// hand-over owns the placement rules and the sweep yields to it.
func (e *Engine) reclaimRound(ctx context.Context) int {
	tbl := e.TableSnapshot()
	if len(tbl.Peers) < 2 || !layoutSettled(tbl) {
		return 0
	}
	planned := PlanSlots(tbl.HostingIDs(), tbl.SlotCount, tbl.Replicas)
	now := time.Now()
	var cands []reclaimCandidate
	for s, p := range tbl.Slots {
		want := planned[s]
		if want == nil {
			// No hosting member plans this slot: the pool is empty (every
			// member is control-plane only). Nothing comes off — a dropped
			// seat must be redundant against the seats that STAY, and with no
			// hosting member there is no such seat. The bytes outlive the
			// layout; the slot is re-seated when a data-plane node arrives.
			continue
		}
		drop := surplusSeatForReplan(p, tbl.Replicas, want.Replicas)
		if drop == "" {
			continue
		}
		if e.migrationInFlight(s) {
			continue // a handover is building a copy of this slot: its seat is not surplus
		}
		if !e.seatRetryReady(s, drop, now) {
			continue
		}
		cands = append(cands, reclaimCandidate{slot: s, drop: drop, placement: p})
	}
	if len(cands) == 0 {
		return 0
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].slot < cands[j].slot })

	// Phase 1: prove the surplus redundant, in a bounded pool.
	deadline := time.Now().Add(config.ReclaimBudget)
	redundant := make([]bool, len(cands))
	var (
		mu   sync.Mutex
		down = map[string]bool{} // peers parked for the rest of this round
		wg   sync.WaitGroup
		sem  = make(chan struct{}, config.ReclaimProbeConcurrency)
	)
	for i := range cands {
		if ctx.Err() != nil || time.Now().After(deadline) {
			break // out of budget: the next round takes the rest
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(i int, c reclaimCandidate) {
			defer wg.Done()
			defer func() { <-sem }()
			mu.Lock()
			parked := probeParked(down, c.placement, c.drop)
			mu.Unlock()
			if parked {
				return // a peer this probe needs is unreachable this round
			}
			inSync, behind, err := e.keptCopiesInSync(ctx, c.slot, c.placement, c.drop)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err != nil:
				// Park exactly the peers the probe touched — the leader and the
				// copies that stay. The seat's own node is deliberately NOT parked:
				// another candidate may still be able to drop a seat from it this
				// round, and removing a seat needs no contact with the node.
				down[c.placement.Leader] = true
				for _, r := range c.placement.Replicas {
					if r != c.drop {
						down[r] = true
					}
				}
				e.loggerf("surplus seat reclaim: probe of slot %d failed: %v (peers parked for this round)", c.slot, err)
			case behind:
				// the seats that stay are still fetching: the surplus is not redundant yet
			case !inSync:
				// A kept copy reports the leader's LEO with a different directory.
				// Fetching cannot repair that (the rebalancer's repair path owns it
				// when it hands this slot's leadership over), and probing it again
				// every round buys nothing: back off.
				e.recordSeatAttempt(c.slot, c.drop, time.Now())
				e.logger.Warn("surplus seat kept: a copy that stays has the leader's LEO but a different directory",
					"slot", c.slot, "seat", c.drop, "op", "seat_reclaim")
			default:
				redundant[i] = true
			}
		}(i, cands[i])
	}
	wg.Wait()

	// Phase 2: one removal command per seat, carrying every redundant slot.
	bySeat := map[string][]int32{}
	for i, c := range cands {
		if redundant[i] {
			bySeat[c.drop] = append(bySeat[c.drop], c.slot)
		}
	}
	seats := make([]string, 0, len(bySeat))
	for n := range bySeat {
		seats = append(seats, n)
	}
	sort.Strings(seats)
	done := 0
	for _, n := range seats {
		if ctx.Err() != nil {
			break
		}
		slots := bySeat[n]
		sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
		removed, err := e.RemoveReplicaBatch(ctx, n, slots)
		if err != nil {
			for _, s := range slots {
				e.recordSeatAttempt(s, n, time.Now())
			}
			e.loggerf("surplus seat reclaim: dropping %s from %d slots failed: %v (retried with backoff)", n, len(slots), err)
			continue
		}
		if removed == 0 {
			continue // every slot in the batch stopped qualifying
		}
		e.logger.Info("surplus replicas reclaimed: the sets are back at the factor",
			"seat", n, "slots", removed, "factor", tbl.Replicas, "op", OpSlotRemoveReplica)
		done += removed
	}
	return done
}

// keptCopiesInSync reports whether every seat the reclaim would KEEP for the
// slot holds a copy equivalent to the leader's — the condition that makes the
// dropped seat redundant. It answers the same way as the hand-over gate
// (slotCopyStatus), including the wait-vs-repair distinction the caller needs:
//
//   - (true, false, nil):  every kept seat matches the leader: drop the surplus;
//   - (false, true, nil):  a kept seat is still fetching (below the leader's
//     LEO): wait;
//   - (false, false, nil): a kept seat has the leader's LEO with a different
//     directory: only a rebuild repairs it, so back off;
//   - an error is a peer-plane failure and says nothing about the copies.
func (e *Engine) keptCopiesInSync(ctx context.Context, slot int32, p *Placement, drop string) (inSync, behind bool, err error) {
	src := e.peerAddr(p.Leader)
	if p.Leader == e.self {
		src = "" // read our own copy locally
	} else if src == "" {
		return false, false, fmt.Errorf("slot %d: no address for leader %s", slot, p.Leader)
	}
	for _, r := range p.Replicas {
		if r == drop || r == p.Leader {
			continue
		}
		dst := e.peerAddr(r)
		if dst == "" {
			return false, false, fmt.Errorf("slot %d: no address for replica %s", slot, r)
		}
		equivalent, belowLeader, err := e.slotCopyStatus(ctx, slot, src, dst)
		if err != nil {
			return false, false, err
		}
		if belowLeader {
			return false, true, nil
		}
		if !equivalent {
			return false, false, nil
		}
	}
	return true, false, nil
}

// slotCopyStatus compares the two copies and separates the two ways they can
// fail to agree:
//
//   - equivalent: identical digest and the target is not behind;
//   - behind: the target's LEO is below the source's — its fetch is still
//     catching up, and waiting is the right answer;
//   - neither: the target reports the source's LEO (or more) but a different
//     directory. That is a copy whose aggregate directory stopped adopting
//     versions, so the records above the gap sit in its WAL unreadable: the
//     fetch loop cannot repair it (the seqs the missing versions need are
//     already spent elsewhere and only counter+1 may be appended), and
//     waiting leaves it wrong forever. Callers must rebuild it instead —
//     see rebalanceRound, and ensureTargetConsistent, which does the drop
//     and re-pull.
//
// An error is a peer-plane failure and says nothing about the copies.
func (e *Engine) slotCopyStatus(ctx context.Context, slot int32, srcAddr, dstAddr string) (equivalent, behind bool, err error) {
	src, err := e.slotStateAt(ctx, slot, srcAddr)
	if err != nil {
		return false, false, err
	}
	dst, err := e.remoteSlotState(ctx, dstAddr, slot)
	if err != nil {
		return false, false, err
	}
	if dst.LEO < src.LEO {
		return false, true, nil
	}
	if dst.Aggregates == src.Aggregates && dst.Versions == src.Versions && dst.Resolvable == src.Resolvable {
		return true, false, nil
	}
	return false, false, nil // diverged: same LEO (or more), different directory
}

// slotStateAt reads one node's copy of a slot over the peer plane; an empty
// addr means this node's own copy, read locally.
func (e *Engine) slotStateAt(ctx context.Context, slot int32, addr string) (slotState, error) {
	if addr == "" {
		leo, agg, ver, res := e.HandleSlotLeo(slot)
		return slotState{LEO: leo, Aggregates: agg, Versions: ver, Resolvable: res}, nil
	}
	return e.remoteSlotState(ctx, addr, slot)
}

// finishPendingRemovals closes the scale-down the moment its drain is done:
// a node marked for removal that leads nothing and holds nothing in flight
// is removed for real, in this same round — no second DELETE from the
// operator, no lost intent when the controller changed hands (the intent
// rode the replicated table). Removal failures (a raft config that refused,
// a lost leadership) just retry next tick.
// wakeDrainRunner starts the on-demand drain loop if one is needed and not
// already running: the switch is off (the ordinary loop returned long ago),
// the table carries a drain mark, and no runner is up. It paces at config.DrainTick
// — the same tempo a marked drain holds the enabled loop to — and exits by
// itself once the table has no draining node left (removed, or withdrawn by
// a re-admission). Cheap to call every controller tick: the guard reads an
// in-memory snapshot and a running runner is a flag check.
// claimDrainDriver / releaseDrainDriver guard the single drain driver (see
// the Engine field comment). Claim is cheap and lock-free-adjacent: a
// controller tick calls wake every second, and a held seat is one mutex.
func (e *Engine) claimDrainDriver() bool {
	e.drainRunMu.Lock()
	defer e.drainRunMu.Unlock()
	if e.drainDriverBusy {
		return false
	}
	e.drainDriverBusy = true
	return true
}

func (e *Engine) releaseDrainDriver() {
	e.drainRunMu.Lock()
	e.drainDriverBusy = false
	e.drainRunMu.Unlock()
}

func (e *Engine) wakeDrainRunner() {
	if e.rebalanceEnabled() || !e.TableSnapshot().HasDraining() {
		return
	}
	if !e.claimDrainDriver() {
		return // the disabled RunRebalancer loop (or an earlier wake) drives it
	}
	go func() {
		defer e.releaseDrainDriver()
		ctx := context.Background()
		for e.TableSnapshot().HasDraining() {
			if e.node != nil && e.node.IsLeader() {
				e.rebalanceRound(ctx, config.DrainCommitsPerRound)
				e.finishPendingRemovals(ctx)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(config.DrainTick):
			}
		}
	}()
}

func (e *Engine) finishPendingRemovals(ctx context.Context) {
	tbl := e.TableSnapshot()
	for id, p := range tbl.Peers {
		if !p.PendingRemove {
			continue
		}
		if leading, inflight := e.drainProgress(tbl, id); leading != 0 || inflight != 0 {
			continue // still draining: the handover rounds above own this
		}
		if err := e.finishRemoval(ctx, id); err != nil {
			e.logger.Warn("rebalance: drained node could not be removed yet",
				"node", id, "err", err)
			continue
		}
		e.logger.Info("rebalance: drained node removed", "node", id)
	}
}
