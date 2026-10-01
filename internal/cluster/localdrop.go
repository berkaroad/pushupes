package cluster

import (
	"fmt"
	"time"
)

// Post-migration local cleanup, as seen from the outside.
//
// A COMMITTED migration eventually drops the FORMER SOURCE node's local copy
// of the slot when — and only when — that copy has become surplus: the node
// neither leads the slot nor belongs to its replica set any more. The source
// keeps it for a retention window (DESIGN §6: "S 确认新 epoch 生效后隔离本地槽
// 数据，保留到到期防回滚"), then deletes it (store.DropSlot).
//
// What arms the countdown is the node LEAVING the slot's replica set, not the
// hand-over itself. The two differ exactly in the case that used to be wrong:
//
//   - a migration to a node OUTSIDE the replica set admits that node (the set
//     grows to factor+1), and the post-commit reclaim removes the former
//     source — the source leaves the set, so its copy is surplus and the
//     countdown is armed;
//   - a migration to a node that is ALREADY a replica (an in-set leader
//     hand-over) removes nobody: the former source stays in the set and is
//     still part of the replication factor. Its copy is NOT surplus, so no
//     countdown is armed, nothing is published as "queued" and the data is
//     never dropped. Arming on the hand-over itself (rather than on the
//     membership change) is what made the console grey out a node that still
//     held a required copy.
//
// While a countdown runs, the schedule is published (PendingDrop/PendingDrops)
// so the admin plane can say which of a slot's copies is queued to be dropped
// and when. It ends when the timer fires, when the copy is actually dropped, or
// when the table puts this node back on the slot (rollback, a later failover,
// or a re-join) — in which case the data is kept.
//
// The window is the -drop-after knob, and the cleanup CANNOT be switched off: a
// surplus copy left on disk forever is how a node fills up and takes the
// cluster down. A configured 0 therefore means "use the default" (see
// ResolveDropRetention).
//
// Two properties the scheduling has to keep, both learned the hard way:
//
//   - a schedule belongs to ONE hand-over. Reusing an older deadline for a
//     newer hand-over deletes the newer copy the moment the older window
//     expires — visibly "immediately" when the two line up. Every arming
//     therefore takes a fresh deadline and bumps a generation, which is what
//     makes any already-running timer harmless;
//   - the table is the authority at FIRE time, not at arming time. Whether the
//     copy is still surplus is re-read when the timer wakes up.
//
// Restart semantics: the countdown lives in memory only. A node that restarts
// mid-window loses the timer and keeps the copy — a later hand-over that makes
// it surplus again (or the operator) reclaims it. Restarting therefore never
// deletes anything.

// DefaultDropRetention is how long a migrated slot's former source keeps its
// local copy before dropping it. Long enough to survive an immediately noticed
// bad migration, short enough that surplus copies do not linger; operators
// tune it with -drop-after.
const DefaultDropRetention = 30 * time.Second

// ResolveDropRetention turns a configured -drop-after value into the window
// actually used, and rejects the one value that cannot mean anything:
//
//   - negative: an error (rejected at startup, nothing is applied);
//   - 0: the default window — a Go flag's zero value must mean "not set", and
//     there is deliberately NO way to disable the cleanup: the former source's
//     copy is dead weight, and leaving it on disk forever runs the node out of
//     space;
//   - positive: that window.
func ResolveDropRetention(d time.Duration) (time.Duration, error) {
	switch {
	case d < 0:
		return 0, fmt.Errorf("drop retention %s must not be negative (positive = the retention window, 0 = the default %s; there is no way to disable the automatic post-migration cleanup)", d, DefaultDropRetention)
	case d == 0:
		return DefaultDropRetention, nil
	default:
		return d, nil
	}
}

// SetDropRetention sets the post-migration retention window. 0 means "the
// default" (see ResolveDropRetention); there is no off switch.
func (e *Engine) SetDropRetention(d time.Duration) {
	w, err := ResolveDropRetention(d)
	if err != nil {
		w = DefaultDropRetention // never applies a nonsensical window
	}
	e.dropMu.Lock()
	e.dropAfter = w
	e.dropMu.Unlock()
}

// DropRetention reports the configured post-migration retention window.
func (e *Engine) DropRetention() time.Duration {
	e.dropMu.Lock()
	defer e.dropMu.Unlock()
	return e.dropAfter
}

// pendingDrop is one queued cleanup: the deadline the console shows, plus the
// generation that identifies THIS schedule. The generation is what makes a
// superseded timer harmless — when its goroutine wakes up it finds its
// generation is no longer current and returns without touching the data of the
// newer window.
type pendingDrop struct {
	deadline time.Time
	gen      uint64
}

// startDropCountdown arms the retention wait for one slot whose local copy has
// just become surplus (this node left the slot's replica set).
//
// It ALWAYS takes a fresh deadline. A schedule belongs to one hand-over: reusing
// a deadline armed by an earlier one is precisely how a copy gets deleted
// "immediately" — the stale window expires moments after the new hand-over
// commits, and the timer that belongs to the old window fires against the new
// copy. Bumping the generation invalidates any timer still running for the
// previous window.
//
// The wait is a plain timer, deliberately NOT tied to any request or caller
// context: the retention exists to protect a rollback, and a request that has
// already been answered must not cancel it.
func (e *Engine) startDropCountdown(slot int32) {
	// A copy the table still expects this node to hold is never scheduled:
	// the countdown exists for SURPLUS copies only.
	if !e.copyIsSurplus(slot) || !e.store.SlotPresent(slot) {
		e.cancelPendingDrop(slot)
		return
	}
	e.dropMu.Lock()
	if e.pendingDrops == nil {
		e.pendingDrops = map[int32]pendingDrop{}
	}
	e.dropGen++
	gen := e.dropGen
	d := e.dropAfter
	e.pendingDrops[slot] = pendingDrop{deadline: time.Now().Add(d), gen: gen}
	e.dropMu.Unlock()
	go e.dropAfterRetention(slot, gen, d)
}

// dropAfterRetention waits out the retention window and then drops this node's
// local copy of the slot — unless the schedule was superseded or cancelled in
// the meantime, or the table has put this node back on the slot, in which case
// the data is kept (that is what the retention is for) and the schedule is
// called off.
func (e *Engine) dropAfterRetention(slot int32, gen uint64, delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	<-timer.C

	e.dropMu.Lock()
	cur, ok := e.pendingDrops[slot]
	current := ok && cur.gen == gen
	e.dropMu.Unlock()
	if !current {
		return // superseded by a newer hand-over, or cancelled: not this timer's copy
	}

	// The table is the authority at fire time. A copy this node still owes the
	// slot (it leads it again, or it is back in the replica set) is kept.
	if !e.copyIsSurplus(slot) {
		e.forgetPendingDrop(slot, gen)
		return
	}
	if err := e.store.DropSlot(slot); err != nil {
		e.logger.WithError(err).WithField("slot", slot).Warn("post-migration drop failed")
	} else {
		e.logger.WithField("slot", slot).Info("Post-migration source copy dropped")
	}
	e.forgetPendingDrop(slot, gen)
}

// copyIsSurplus reports whether this node's local copy of slot is genuinely
// surplus per the (replicated) table: the slot is known, this node does not
// lead it, and this node is not one of its replicas. Only such a copy may ever
// be dropped — a member of the replication factor never is.
func (e *Engine) copyIsSurplus(slot int32) bool {
	e.tableMu.RLock()
	p, ok := e.table.Slots[slot]
	e.tableMu.RUnlock()
	if !ok {
		return false // unknown slot: nothing to reclaim, keep whatever is on disk
	}
	if p.Leader == e.self {
		return false
	}
	return !replicaListHas(p.Replicas, e.self)
}

// onSlot reports whether the table still expects this node to hold slot (it
// leads it, or it is one of its replicas).
func (e *Engine) onSlot(slot int32) bool {
	e.tableMu.RLock()
	p, ok := e.table.Slots[slot]
	e.tableMu.RUnlock()
	if !ok {
		return false
	}
	return p.Leader == e.self || replicaListHas(p.Replicas, e.self)
}

// cancelPendingDrop forgets slot's schedule unconditionally, invalidating any
// timer still running for it: the copy it was about to delete is not surplus
// any more (the node is on the slot again), or nothing was queued at all.
func (e *Engine) cancelPendingDrop(slot int32) {
	e.dropMu.Lock()
	delete(e.pendingDrops, slot)
	e.dropMu.Unlock()
}

// forgetPendingDrop forgets slot's schedule only if it is still the given
// generation — so a timer that already ran cannot wipe a schedule a newer
// hand-over armed in the meantime.
func (e *Engine) forgetPendingDrop(slot int32, gen uint64) {
	e.dropMu.Lock()
	if cur, ok := e.pendingDrops[slot]; ok && cur.gen == gen {
		delete(e.pendingDrops, slot)
	}
	e.dropMu.Unlock()
}

// pendingSlotIDs lists the slots this node currently has a cleanup queued for.
// There are at most a handful (one per in-flight hand-over), so the table walk
// in syncMigrationState can afford to ask about each of them.
func (e *Engine) pendingSlotIDs() []int32 {
	e.dropMu.Lock()
	defer e.dropMu.Unlock()
	out := make([]int32, 0, len(e.pendingDrops))
	for s := range e.pendingDrops {
		out = append(out, s)
	}
	return out
}

// pendingDropLocked reads one entry, dropping it once its deadline is behind
// us: at that point the retention window is over and nothing is queued any
// more, whatever the cleanup itself ended up doing. Expiring lazily keeps the
// map bounded without a sweeper.
func (e *Engine) pendingDropLocked(slot int32, now time.Time) (time.Time, bool) {
	p, ok := e.pendingDrops[slot]
	if !ok {
		return time.Time{}, false
	}
	if !now.Before(p.deadline) {
		delete(e.pendingDrops, slot)
		return time.Time{}, false
	}
	return p.deadline, true
}

// PendingDrop reports when THIS node will automatically drop its local copy of
// slot, and whether such a cleanup is queued at all (false means nothing is
// queued; the returned time is then the zero time).
//
// It is per-node by nature: only the node that holds the copy to delete knows
// about its own cleanup, which is why the console reads it per node, next to
// the other per-slot gauges.
func (e *Engine) PendingDrop(slot int32) (time.Time, bool) {
	now := time.Now()
	e.dropMu.Lock()
	defer e.dropMu.Unlock()
	return e.pendingDropLocked(slot, now)
}

// PendingDrops lists this node's queued cleanups as a per-slot array aligned
// with the slot index — 0 means "nothing queued", otherwise the unix second at
// which the local copy is due to be dropped. Same shape as the other per-slot
// gauges the console polls.
func (e *Engine) PendingDrops() []int64 {
	out := make([]int64, e.store.SlotCount)
	now := time.Now()
	e.dropMu.Lock()
	defer e.dropMu.Unlock()
	for slot := range e.pendingDrops {
		if slot < 0 || int(slot) >= len(out) {
			continue
		}
		t, ok := e.pendingDropLocked(slot, now)
		if !ok {
			continue
		}
		out[slot] = t.Unix()
	}
	return out
}

// replicaSnapshot copies this node's replica-set membership as observed on the
// PREVIOUS table walk (slot -> this node is one of the slot's replicas). The
// difference between two walks is what tells a hand-over that left this node
// surplus from one that did not, without depending on who happened to be the
// migration source.
func (e *Engine) replicaSnapshot() map[int32]bool {
	e.dropMu.Lock()
	defer e.dropMu.Unlock()
	out := make(map[int32]bool, len(e.replicaOf))
	for k, v := range e.replicaOf {
		out[k] = v
	}
	return out
}

// setReplicaMembership replaces the membership snapshot.
func (e *Engine) setReplicaMembership(m map[int32]bool) {
	e.dropMu.Lock()
	e.replicaOf = m
	e.dropMu.Unlock()
}
