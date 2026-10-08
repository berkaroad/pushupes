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
	"time"
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
const (
	// DefaultRebalanceInterval is how often the controller re-checks the
	// ring layout. The layout only ever needs healing after a failover or a
	// late-joined replica catches up; a fast tick matters because the batch
	// (not the tick) is the throughput knob the operator thinks about.
	DefaultRebalanceInterval = 2 * time.Second
	// DefaultRebalanceBatch caps the hand-overs one round may execute. With
	// 1680 slots the worst case (a node that led 560 slots died) converges
	// in 20 rounds at the default batch; moves stay strictly serial within
	// a round, so one commit fence freezes exactly one slot at a time and a
	// round may run longer than the tick (the ticker simply skips ahead).
	// A smaller batch trades convergence time for a longer tail of the
	// failover layout.
	DefaultRebalanceBatch = 28
)

// SetRebalanceConfig wires the -rebalance-interval / -rebalance-batch knobs.
// An interval or batch <= 0 disables the rebalancer; startup validation
// rejects negative values, so 0 is the operator's "off" switch.
func (e *Engine) SetRebalanceConfig(interval time.Duration, batch int) {
	e.rebalanceInterval = interval
	e.rebalanceBatch = batch
}

func (e *Engine) rebalanceEnabled() bool {
	return e.rebalanceInterval > 0 && e.rebalanceBatch > 0
}

// RunRebalancer is the leader-rebalance loop: one round every
// rebalanceInterval while this node holds the Raft leadership. Launched from
// Engine.Start like RunController.
func (e *Engine) RunRebalancer(ctx context.Context) {
	for {
		if !e.rebalanceEnabled() {
			return
		}
		timer := time.NewTimer(e.rebalanceInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if e.node == nil || !e.node.IsLeader() {
			continue
		}
		e.rebalanceRound(ctx, e.rebalanceBatch)
		// Surplus-seat reclaim: the other half of a re-layout. A re-layout only
		// ADDS seats (Table.applyReplanSlots), so a set left oversized by a
		// factor change converges here — one seat per round, and only once the
		// copies that stay are in sync (see reclaimRound).
		e.reclaimRound(ctx, e.rebalanceBatch)
	}
}

// rebalanceRound runs one controller round and returns the number of
// hand-overs it committed. Exported-to-package test surface: the gates are
// observable through the return value even when the moves themselves abort on
// unreachable peers (the engine harness has no data plane).
func (e *Engine) rebalanceRound(ctx context.Context, limit int) int {
	if limit <= 0 {
		return 0
	}
	tbl := e.TableSnapshot()
	if len(tbl.Peers) < 2 || !layoutSettled(tbl) {
		return 0 // single-node cluster, or a live migration owns the layout: yield to it
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
		// Re-read the placement immediately before acting: the round's plan
		// was computed from a snapshot, and a human migrate may have staged
		// or committed this slot since. Staging over a live migration would
		// hijack its target, and a committed move may already be the one we
		// were going to make (leader == ring leader: nothing to do).
		cur, ok := e.TableSnapshot().Slots[m.Slot]
		if !ok || cur.State != SlotStable || cur.Leader == m.To {
			continue
		}
		if !e.alive(dstAddr) {
			continue // the move-back target must be reachable (its liveness
			// is also what failover is about to re-shape: do not race it)
		}
		// A move that keeps not sticking is throttled before anything is
		// probed or shipped: without this a target that cannot converge (a
		// diverged copy) is REPAIRED once per round — a full slot snapshot
		// every two seconds, forever.
		if !e.rebalanceRetryReady(m.Slot, m.From, m.To, time.Now()) {
			continue
		}
		// Gate the move on the target's copy. Two different refusals live
		// here, and they must not be conflated:
		//
		//   - the target is BEHIND (its fetch is still catching up): wait.
		//     Handing leadership over now would make the hand-back a full
		//     snapshot migration, and the point of the ring layout is that
		//     the data does NOT have to move twice;
		//
		//   - the target is DIVERGED (same or higher LEO, different
		//     directory): wait forever, which is what this used to do. Its
		//     aggregate directory stopped adopting versions at a gap, so the
		//     records above that point sit in its WAL unreadable and no
		//     amount of fetching repairs them — the copy is structurally
		//     short while its LEO says "in sync". Skipping it leaves the ring
		//     permanently unbalanced for that slot (observed: 5 slots stuck
		//     at a settled 559/563/558, rebalance round after round, with no
		//     log line saying why).
		//
		// So a diverged target is handed to the ordinary migration path
		// instead: StartMigration's ensureTargetConsistent drops such a copy
		// and rebuilds it from this source before moving leadership, which is
		// the only thing that repairs it. The move is expensive — a real
		// rebuild under the fence — and that is the honest price of healing a
		// replica the leader is still counting as in-sync.
		equivalent, behind, err := e.slotCopyStatus(ctx, m.Slot, srcAddr, dstAddr)
		if err != nil {
			continue // peer-plane error: it will either heal or be caught next round
		}
		if behind {
			continue // still catching up: the cheap hand-back waits for it
		}
		if !equivalent {
			e.logger.Warn("rebalance: target copy has the source's LEO but a different directory; rebuilding it before handing leadership back",
				"slot", m.Slot, "from", m.From, "to", m.To, "op", "rebalance_repair")
		}
		e.recordRebalanceAttempt(m.Slot, m.From, m.To, time.Now())
		if err := e.StartMigration(ctx, m.Slot, m.To); err != nil {
			e.loggerf("rebalance: moving slot %d back to %s failed: %v (retried with backoff)", m.Slot, m.To, err)
			continue
		}
		e.logger.Info("leader rebalanced back to the ring layout",
			"slot", m.Slot, "from", m.From, "to", m.To, "op", "rebalance")
		done++
	}
	return done
}

// slotCopyEquivalent reports whether the target's copy of a slot is equivalent
// to the source's: the same directory digest (aggregate count, total versions,
// resolvable versions) and not behind the source's LEO. Both nodes are reached
// over the peer plane; an empty srcAddr means THIS node is the source and reads
// its own store.
//
// The digest — not just the LEO — is what makes the comparison safe to skip
// a snapshot with. A caught-up in-set replica pulls every record from the
// current leader's log by seq (idempotent appendAtSeq; a fork is quarantined
// by the fetch loop, so a diverged copy can never report matching sums). A
// same-LEO-but-short-stream copy (DESIGN §1.1 rule 3's gap: bytes in the WAL
// above a stream's tail) fails the digest and takes the ordinary
// snapshot/rebuild path. This is ensureTargetConsistent's comparison, used
// before the fence instead of under it: the fence window must stay O(tail).
//
// Callers that must tell "not caught up yet" from "never will be" want
// slotCopyStatus instead — this wrapper is for the migration fast path, which
// only asks whether the snapshot can be skipped.
func (e *Engine) slotCopyEquivalent(ctx context.Context, slot int32, srcAddr, dstAddr string) (bool, error) {
	equivalent, _, err := e.slotCopyStatus(ctx, slot, srcAddr, dstAddr)
	return equivalent, err
}

// ---- Retry backoff for hand-overs that do not stick --------------------------

// A hand-over that fails costs real work. A target whose copy is DIVERGED (the
// source's LEO with a different directory) is rebuilt — the source re-ships the
// whole slot under the commit fence — and a move that aborts is re-planned. Both
// used to happen again the very next round, i.e. a full slot snapshot every
// DefaultRebalanceInterval for as long as the target cannot converge. Failed
// attempts therefore back off per slot: the first retry is the next round (as
// before), and each further one doubles up to a cap. The record is keyed on the
// move's endpoints, so a failover (a different from/to) gets a fresh try — it
// is a different move, not a retry.
const (
	rebalanceRetryBase = DefaultRebalanceInterval
	rebalanceRetryMax  = time.Minute
)

type rebalanceAttempt struct {
	from, to  string
	attempts  int
	notBefore time.Time
}

// rebalanceRetryDelay is the wait after the given attempt number: 2s, 4s, 8s,
// ... capped. Pure, so the escalation is assertable on its own.
func rebalanceRetryDelay(attempts int) time.Duration {
	d := rebalanceRetryBase
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= rebalanceRetryMax {
			return rebalanceRetryMax
		}
	}
	return d
}

// rebalanceRetryReady reports whether this move may be attempted now. Read-only:
// it never counts an attempt, so a slot that is merely WAITING (its target still
// catching up) does not accumulate backoff.
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
func (e *Engine) reclaimRound(ctx context.Context, limit int) int {
	if limit <= 0 {
		return 0
	}
	tbl := e.TableSnapshot()
	if len(tbl.Peers) < 2 || !layoutSettled(tbl) {
		return 0
	}
	planned := PlanSlots(tbl.PeerIDs(), tbl.SlotCount, tbl.Replicas)
	slots := make([]int32, 0, 8)
	for s, p := range tbl.Slots {
		if surplusSeatForReplan(p, tbl.Replicas, planned[s].Replicas) != "" {
			slots = append(slots, s)
		}
	}
	if len(slots) == 0 {
		return 0
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
	done := 0
	for _, s := range slots {
		if done >= limit || ctx.Err() != nil {
			break
		}
		// Re-read the placement: the round's plan came from a snapshot and a
		// hand-over or a migration may have moved this slot since.
		cur, ok := e.TableSnapshot().Slots[s]
		if !ok {
			continue
		}
		drop := surplusSeatForReplan(cur, tbl.Replicas, planned[s].Replicas)
		if drop == "" {
			continue
		}
		if !e.seatRetryReady(s, drop, time.Now()) {
			continue
		}
		inSync, behind, err := e.keptCopiesInSync(ctx, s, cur, drop)
		if err != nil {
			continue // peer-plane error: it will either heal or be caught next round
		}
		if behind {
			continue // the copies that stay are still fetching: the surplus is not redundant yet
		}
		if !inSync {
			// A kept copy reports the leader's LEO with a different directory.
			// Fetching cannot repair that (the rebalancer's repair path owns it
			// when it hands this slot's leadership over), and probing it again
			// every round buys nothing: back off.
			e.recordSeatAttempt(s, drop, time.Now())
			e.logger.Warn("surplus seat kept: a copy that stays has the leader's LEO but a different directory",
				"slot", s, "seat", drop, "op", "seat_reclaim")
			continue
		}
		if err := e.RemoveReplica(ctx, s, drop); err != nil {
			e.recordSeatAttempt(s, drop, time.Now())
			e.loggerf("surplus seat reclaim: dropping %s from slot %d failed: %v (retried with backoff)", drop, s, err)
			continue
		}
		e.logger.Info("surplus replica reclaimed: the set is back at the factor",
			"slot", s, "seat", drop, "factor", tbl.Replicas, "op", OpSlotRemoveReplica)
		done++
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
