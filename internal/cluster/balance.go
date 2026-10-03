package cluster

import (
	"context"
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
//     moves slot data around;
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
	// late-joined replica catches up, so a slow tick is cheap and quiet.
	DefaultRebalanceInterval = 15 * time.Second
	// DefaultRebalanceBatch caps the hand-overs one round may execute. With
	// 1680 slots the worst case (a node that led 560 slots died) converges
	// in ~70 rounds ≈ 17 minutes at the default interval; a smaller batch
	// trades convergence time for a longer tail of the failover layout.
	DefaultRebalanceBatch = 8
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
		// Gate the move on the target already holding an equivalent copy.
		// Until the replica's fetch catches up, a hand-back would be a full
		// snapshot migration — and the point of the ring layout is that the
		// data does NOT have to move twice.
		equivalent, err := e.slotCopyEquivalent(ctx, m.Slot, srcAddr, dstAddr)
		if err != nil || !equivalent {
			continue
		}
		if err := e.StartMigration(ctx, m.Slot, m.To); err != nil {
			e.loggerf("rebalance: moving slot %d back to %s failed: %v (retried next round)", m.Slot, m.To, err)
			continue
		}
		e.logger.WithFields(map[string]any{
			"slot": m.Slot, "from": m.From, "to": m.To, "op": "rebalance",
		}).Info("leader rebalanced back to the ring layout")
		done++
	}
	return done
}

// slotCopyEquivalent reports whether the target's copy of a slot is
// equivalent to the source's copy: the same directory digest (aggregate
// count, total versions, resolvable versions) and not behind the source's
// LEO. Both nodes are reached over the peer plane; an empty srcAddr means
// THIS node is the source and reads its own store.
//
// The digest — not just the LEO — is what makes the comparison safe to skip
// a snapshot with. A caught-up in-set replica pulls every record from the
// current leader's log by seq (idempotent appendAtSeq; a fork is quarantined
// by the fetch loop, so a diverged copy can never report matching sums). A
// same-LEO-but-short-stream copy (DESIGN §1.1 rule 3's gap: bytes in the WAL
// above a stream's tail) fails the digest and takes the ordinary
// snapshot/rebuild path. This is ensureTargetConsistent's comparison, used
// before the fence instead of under it: the fence window must stay O(tail).
func (e *Engine) slotCopyEquivalent(ctx context.Context, slot int32, srcAddr, dstAddr string) (bool, error) {
	src, err := e.slotStateAt(ctx, slot, srcAddr)
	if err != nil {
		return false, err
	}
	dst, err := e.remoteSlotState(ctx, dstAddr, slot)
	if err != nil {
		return false, err
	}
	if dst.LEO < src.LEO {
		return false, nil
	}
	return dst.Aggregates == src.Aggregates && dst.Versions == src.Versions && dst.Resolvable == src.Resolvable, nil
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
