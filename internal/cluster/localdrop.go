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
	"fmt"
	"time"

	"github.com/berkaroad/pushupes/internal/config"
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
// The countdown itself is ONE sweeper per node, not one timer per queued slot:
// a factor decrease leaves thousands of copies surplus at once, and a goroutine
// plus a timer for each of them is a load the node never needed. The sweeper
// sleeps until the EARLIEST queued deadline (woken when a schedule is armed or
// cancelled) and then drops everything that is due, so a schedule still fires at
// its own deadline, to the millisecond, exactly as the per-slot timers did.
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
		return 0, fmt.Errorf("drop retention %s must not be negative (positive = the retention window, 0 = the default %s; there is no way to disable the automatic post-migration cleanup)", d, config.DropAfter)
	case d == 0:
		return config.DropAfter, nil
	default:
		return d, nil
	}
}

// SetDropRetention sets the post-migration retention window. 0 means "the
// default" (see ResolveDropRetention); there is no off switch.
func (e *Engine) SetDropRetention(d time.Duration) {
	w, err := ResolveDropRetention(d)
	if err != nil {
		w = config.DropAfter // never applies a nonsensical window
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
	e.wakeDropSweeperLocked()
	e.dropMu.Unlock()
}

// wakeDropSweeperLocked arms the node's single drop sweeper (once) and nudges it
// to re-read the queue, because the schedule just added — or the one just
// cancelled — may hold a deadline earlier than the one it is sleeping for. The
// nudge is a one-slot channel: a pending wake already covers every reason.
func (e *Engine) wakeDropSweeperLocked() {
	if e.dropWake == nil {
		e.dropWake = make(chan struct{}, 1)
	}
	if !e.dropSweeper {
		e.dropSweeper = true
		e.dropSweeperN++
		go e.runDropSweeper()
	}
	select {
	case e.dropWake <- struct{}{}:
	default:
	}
}

// runDropSweeper is the node's ONLY retention timer. It sleeps until the earliest
// queued deadline and drops what is due, then repeats; with nothing queued it
// parks until a schedule is armed. A schedule therefore fires at its own
// deadline rather than on a fixed tick, which keeps the retention exact for any
// configured window.
func (e *Engine) runDropSweeper() {
	for {
		e.dropMu.Lock()
		var next time.Time
		for _, p := range e.pendingDrops {
			if next.IsZero() || p.deadline.Before(next) {
				next = p.deadline
			}
		}
		wake := e.dropWake
		e.dropMu.Unlock()

		if next.IsZero() {
			<-wake // nothing queued: wait for a schedule to be armed
			continue
		}
		if d := time.Until(next); d > 0 {
			timer := time.NewTimer(d)
			select {
			case <-timer.C:
			case <-wake:
				// A schedule was armed or cancelled: recompute instead of
				// sleeping out a deadline that may no longer be the earliest.
				timer.Stop()
				continue
			}
		}
		e.dropDueCopies()
	}
}

// dropDueCopies drops every queued copy whose retention window has elapsed. The
// table is the authority at fire time, exactly as it was for the per-slot timer:
// a copy this node owes the slot again (it leads it, or it is back in the set)
// is kept and its schedule is called off.
func (e *Engine) dropDueCopies() {
	now := time.Now()
	type dueDrop struct {
		slot int32
		gen  uint64
	}
	var due []dueDrop
	e.dropMu.Lock()
	for slot, p := range e.pendingDrops {
		if now.Before(p.deadline) {
			continue
		}
		delete(e.pendingDrops, slot)
		due = append(due, dueDrop{slot: slot, gen: p.gen})
	}
	e.dropMu.Unlock()
	for _, d := range due {
		e.dropRetainedCopy(d.slot, d.gen)
	}
}

// dropRetainedCopy drops one copy whose window elapsed, unless a newer hand-over
// re-armed the slot in the meantime (the schedule belongs to ONE hand-over: the
// newer window owns the data now) or the table has put this node back on the
// slot. Both re-checks are the same ones the per-slot timer made when it woke.
func (e *Engine) dropRetainedCopy(slot int32, gen uint64) {
	e.dropMu.Lock()
	cur, ok := e.pendingDrops[slot]
	e.dropMu.Unlock()
	if ok && cur.gen != gen {
		return // superseded by a newer hand-over: not this schedule's copy
	}
	if !e.copyIsSurplus(slot) {
		e.forgetPendingDrop(slot, gen)
		return
	}
	// TOCTOU: the surplus check above reads the applied table, and leadership
	// can land on this node between that read and the drop (the callers that
	// change the table run on other goroutines). Dropping here would destroy
	// the copy of a slot this node now LEADS — the empty-leader state that
	// answers version 0 to every client. Re-check under the same table lock
	// the write path uses, immediately before the destructive call.
	if e.Leads(slot) {
		e.forgetPendingDrop(slot, gen)
		e.logger.Warn("post-migration drop called off: this node leads the slot again",
			"slot", slot, "op", "drop_race")
		return
	}
	if err := e.store.DropSlot(slot); err != nil {
		e.logger.Warn("post-migration drop failed", "slot", slot, "error", err)
	} else {
		e.logger.Info("Post-migration source copy dropped", "slot", slot)
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
	_, had := e.pendingDrops[slot]
	delete(e.pendingDrops, slot)
	if had {
		e.wakeDropSweeperLocked() // the earliest deadline may have moved out
	}
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

// pendingDropLocked reads one entry for the admin plane. An entry whose deadline
// is behind us reports "nothing queued" (the window is over whatever the cleanup
// is doing) but is NOT deleted here: the node's sweeper owns that transition, and
// a reader stealing the entry would leave the copy on disk forever.
func (e *Engine) pendingDropLocked(slot int32, now time.Time) (time.Time, bool) {
	p, ok := e.pendingDrops[slot]
	if !ok {
		return time.Time{}, false
	}
	if !now.Before(p.deadline) {
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
// gauges the console polls: the cleanup queue is shown per (slot, node), so the
// console can mark the replica whose copy is on its way out.
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
