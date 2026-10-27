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

package storage

import (
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// This file holds the store's single fsync owner. Every segment fsync of every
// slot runs on this one goroutine, one at a time, with no slot write lock held
// across the syscall. Three properties follow, and each one is asserted by a
// test in flusher_test.go:
//
//	I1 the slot write lock is never held across a syscall: appends and replica
//	   applies keep making progress while the device is slow.
//	I2 at most one fsync is in flight (inflight never exceeds 1).
//	I3 a slot's data is fsynced within the flush policy's bound: the queue is
//	   drained oldest-unflushed-first, so oldest_unflushed_age_s stays bounded.
//
// A unit of work is ONE segment of one slot (not a whole slot): a slow fsync
// therefore delays a single 256MiB segment, not every segment of that slot, and
// not the rest of the queue.

// flushUnit is one segment's outstanding fsync. Everything the syscall needs
// was snapshotted under the slot's write lock in prepareFlushLocked, so the
// flusher never touches slot state outside the lock's windows.
type flushUnit struct {
	slot     int32
	seg      *Segment
	file     *os.File // handle snapshot; syncing it needs no lock and it stays open while queued
	size     int64    // file size this fsync covers, settled into seg.syncedSize
	newBytes int64    // of size, the bytes not covered by an earlier fsync (durable from here)
	recs     int64    // records this fsync covers, settled into seg.unflushed/pendingFlush
	enq      time.Time
	cov      *flushCov // the round's coverage claim (nil for a sealed-segment enqueue)
}

// flushCov claims one prepare round's coverage: the slot's log end at the
// moment the round snapshotted its segments. The claim lands on the slot's
// durable watermark once EVERY unit of the round has settled an fsync — every
// record appended before the snapshot then sits on disk in every segment it
// touched. A round whose unit the queue dropped to dedup stays unclaimed: the
// in-flight fsync it merged behind flushes those bytes anyway, and the NEXT
// round's snapshot (a higher log end) lands the claim. Claims are monotonic,
// so the watermark only ever asserts what a settled fsync proved.
type flushCov struct {
	seq  uint64
	left int
}

// flusher owns every fsync in the store.
type flusher struct {
	st *Store

	mu     sync.Mutex
	units  []*flushUnit // sorted by enq: oldest-dirty-first
	inQ    map[*Segment]bool
	frozen map[int32]bool // slots detached for Close/Drop: no new units

	// Seal index materialization (a freshly sealed segment's .idx tail, .cidx
	// and .aidx) runs on its own worker instead of under the slot's write lock:
	// the roll hands the segment over and returns. sealQ/sealBusy share mu with
	// units/frozen so detach can drop a slot's queued jobs and pick up its
	// in-flight one under the same lock.
	sealQ    []*sealJob
	sealBusy map[int32]chan struct{}
	sealWake chan struct{}
	sealStop chan struct{}
	sealDone chan struct{}

	// Drain budget accounting: passes counts drain rounds, budgetRounds the
	// rounds that ran out of budget with work left, queueHigh the deepest the
	// queue ever got. All three are read by FlushStats (and its tests).
	passes       atomic.Uint64
	budgetRounds atomic.Uint64
	queueHigh    atomic.Uint64

	// flushSem caps how many fsyncs run at once (I2). Its capacity is the
	// configured flush parallelism (default 1 = the historical one-at-a-time
	// pipeline). Queued units are executed by the persistent worker pool below;
	// a forced flush (explicit Flush/Close) takes a permit on the caller's
	// goroutine, so it obeys the same cap and never waits for a worker that
	// stopped running.
	flushSem    chan struct{}
	parallelism int

	// dispatch hands queued units to the worker pool; workers take a unit,
	// fsync it, and settle it. Unbuffered: a send blocks until a worker is free,
	// which is the pool's backpressure.
	dispatch chan *flushUnit
	poolDone sync.WaitGroup

	// active counts units per slot that a worker has taken and not yet settled.
	// detach waits on activeCond until the slot's count reaches zero, so a
	// Close/Drop cannot delete files under an fsync that is still running.
	active     map[int32]int
	activeCond *sync.Cond

	wake chan struct{}
	stop chan struct{}
	done chan struct{}

	// stopOnce makes shutdown idempotent: a second Close waits for the same
	// exit instead of closing the same channel twice.
	stopOnce sync.Once

	// inflight is how many fsyncs are in the syscall right now: 0..parallelism
	// (invariant I2 with the configured cap).
	inflight atomic.Int64

	count       atomic.Uint64 // fsyncs performed
	svcNS       atomic.Uint64 // syscall service time
	svcMaxNS    atomic.Uint64
	waitNS      atomic.Uint64 // queue wait before the syscall
	waitMaxNS   atomic.Uint64
	fileSizeSum atomic.Uint64 // segment file size each fsync covered
	fileSizeMax atomic.Uint64
	newBytesSum atomic.Uint64 // of that, bytes this fsync was the first to cover
	newBytesMax atomic.Uint64
	svcBucket   [fsyncBuckets]atomic.Uint64
	waitBucket  [fsyncBuckets]atomic.Uint64
}

func newFlusher(st *Store, parallelism int) *flusher {
	if parallelism < 1 {
		parallelism = 1
	}
	f := &flusher{
		st:          st,
		parallelism: parallelism,
		flushSem:    make(chan struct{}, parallelism),
		dispatch:    make(chan *flushUnit),
		active:      map[int32]int{},
		inQ:         map[*Segment]bool{},
		frozen:      map[int32]bool{},
		wake:        make(chan struct{}, 1),
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
		sealBusy:    map[int32]chan struct{}{},
		sealWake:    make(chan struct{}, 1),
		sealStop:    make(chan struct{}),
		sealDone:    make(chan struct{}),
	}
	f.activeCond = sync.NewCond(&f.mu)
	for i := 0; i < parallelism; i++ {
		f.poolDone.Add(1)
		go f.worker()
	}
	return f
}

// worker executes queued units: fsync under a permit, then settle. The pool is
// only used when the flush parallelism is above one; with the default the round
// runs its units on the loop's own goroutine, exactly as before.
func (f *flusher) worker() {
	defer f.poolDone.Done()
	for u := range f.dispatch {
		_ = f.syncUnit(u)
		f.decActive(u.slot)
	}
}

// decActive clears one in-flight unit of a slot (workers and the serial round
// both call it) and wakes any detach waiting for that slot to quiesce.
func (f *flusher) decActive(slot int32) {
	f.mu.Lock()
	if f.active[slot] > 0 {
		f.active[slot]--
		if f.active[slot] == 0 {
			f.activeCond.Broadcast()
		}
	}
	f.mu.Unlock()
}

// kick nudges the loop: a slot became dirty, crossed its record threshold, or a
// forced flush was queued.
func (f *flusher) kick() {
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

// enqueue appends prepared units, keeping the queue ordered oldest-first so the
// slot whose data has waited longest is served first (invariant I3).
func (f *flusher) enqueue(units []*flushUnit) {
	if len(units) == 0 {
		return
	}
	f.mu.Lock()
	for _, u := range units {
		if f.inQ[u.seg] {
			continue // already queued: that unit's fsync covers this write too
		}
		f.inQ[u.seg] = true
		f.units = append(f.units, u)
	}
	if n := uint64(len(f.units)); n > 0 {
		addMax(&f.queueHigh, n)
	}
	sort.SliceStable(f.units, func(i, j int) bool { return f.units[i].enq.Before(f.units[j].enq) })
	f.mu.Unlock()
	f.kick()
}

// enqueueSealed hands one just-sealed segment over, if it still owes an fsync.
func (f *flusher) enqueueSealed(sl *Slot, seg *Segment) {
	if seg.File == nil || seg.sizeBytes <= seg.syncedSize {
		return
	}
	f.enqueue([]*flushUnit{{
		slot: sl.ID, seg: seg, file: seg.File,
		size: seg.sizeBytes, recs: seg.unflushed, enq: time.Now(),
	}})
}

// flushSlotNow is the explicit path (Slot.Flush / Store.Flush / Store.Close):
// sync this slot's outstanding segments now. It runs on the caller's goroutine
// under a flush permit, so it obeys the same parallelism cap, and it never waits for the
// loop — a retired or busy flusher cannot make an explicit flush hang.
func (f *flusher) flushSlotNow(sl *Slot) error {
	sl.mu.Lock()
	units, _, _ := sl.prepareFlushLocked(time.Now(), true)
	sl.mu.Unlock()
	var firstErr error
	for _, u := range units {
		if err := f.syncUnit(u); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// detach stops queueing work for a slot and waits until no fsync of it can be
// in flight. Callers (Slot.Close, Slot.Drop, Store.DropSlot) call it before
// closing or deleting the segment files.
func (f *flusher) detach(slot int32) {
	f.mu.Lock()
	f.frozen[slot] = true
	kept := f.units[:0]
	for _, u := range f.units {
		if u.slot == slot {
			delete(f.inQ, u.seg)
			continue
		}
		kept = append(kept, u)
	}
	f.units = kept
	// Queued seal jobs are dropped (their indexes are rebuilt at the next
	// start); the in-flight one is waited for below.
	jkept := f.sealQ[:0]
	for _, j := range f.sealQ {
		if j.sl.ID == slot {
			continue
		}
		jkept = append(jkept, j)
	}
	f.sealQ = jkept
	busy := f.sealBusy[slot]
	f.mu.Unlock()

	// The seal worker reads the segment and settles under the slot lock, so it
	// must be finished before the caller closes files or takes that lock. Do
	// this without holding a flush permit: the job's own index fsyncs take one.
	if busy != nil {
		<-busy
	}

	// Then wait until no unit of this slot is being fsynced (a worker may hold
	// one it took before the freeze), and only then quiesce the device: taking
	// every permit waits out whatever fsync is running on any slot, so the
	// caller can close or delete the files.
	f.mu.Lock()
	for f.active[slot] > 0 {
		f.activeCond.Wait()
	}
	f.mu.Unlock()
	f.quiesce()
}

// quiesce blocks until no fsync is in flight, then releases the permits. Taking
// all of them is what proves the device queue is empty at that instant.
func (f *flusher) quiesce() {
	for i := 0; i < f.parallelism; i++ {
		f.flushSem <- struct{}{}
	}
	for i := 0; i < f.parallelism; i++ {
		<-f.flushSem
	}
}

// thaw re-allows work for a slot (used by tests and by a slot that came back).
func (f *flusher) thaw(slot int32) {
	f.mu.Lock()
	delete(f.frozen, slot)
	f.mu.Unlock()
	f.kick()
}

// run is the single fsync thread.
func (f *flusher) run() {
	defer close(f.done)
	var timer *time.Timer
	var timerC <-chan time.Time
	for {
		select {
		case <-f.stop:
			f.drainAll()
			return
		default:
		}
		next := f.pass(time.Now())
		// Arm (or disarm) the policy timer: only an interval-policy slot with
		// unflushed data gives the loop a deadline.
		switch {
		case next.IsZero():
			if timer != nil {
				timer.Stop()
				timer, timerC = nil, nil
			}
		case timer == nil:
			timer = time.NewTimer(time.Until(next))
			timerC = timer.C
		default:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(time.Until(next))
		}
		// Sync queued units before parking, one fsync at a time and at most one
		// round's budget (drainRoundMaxUnits / drainRoundMaxAge). Budget-bound
		// rounds yield between slices so appends, forced flushes and the seal
		// worker get the device, but the policy pass does NOT run between them:
		// a pass iterates every dirty slot under its lock and re-sorts the
		// queue, and doing that per round turned a deep queue into a sweep loop
		// that starved the appenders. The pass runs once the queue is empty (or
		// on the next kick/timer), exactly as before the budget existed.
		for f.drain() {
			select {
			case <-f.stop:
				f.drainAll()
				return
			default:
			}
			runtime.Gosched()
		}
		select {
		case <-f.stop:
			f.drainAll()
			return
		case <-f.wake:
		case <-timerC:
			timer, timerC = nil, nil
		}
	}
}

// pass applies the flush policy to every dirty slot: a slot whose interval
// elapsed (or whose record threshold is crossed) gives up its outstanding
// segments as units. It returns the earliest deadline still ahead of the loop.
func (f *flusher) pass(now time.Time) time.Time {
	f.st.dirtyMu.Lock()
	ids := make([]int32, 0, len(f.st.dirty))
	for id := range f.st.dirty {
		ids = append(ids, id)
	}
	f.st.dirtyMu.Unlock()

	var next time.Time
	for _, id := range ids {
		f.mu.Lock()
		frozen := f.frozen[id]
		f.mu.Unlock()
		if frozen {
			continue
		}
		sl := f.st.slots[id].Load()
		if sl == nil {
			f.st.clearDirty(id)
			continue
		}
		sl.mu.Lock()
		units, due, fired := sl.prepareFlushLocked(now, false)
		pendingNow := sl.pendingFlush > 0
		sl.mu.Unlock()
		if len(units) > 0 {
			f.enqueue(units)
		} else if fired || !pendingNow {
			// The policy fired with nothing left owing, or nothing is pending at
			// all: drop the mark. A concurrent append re-marks the slot and kicks
			// this loop, so nothing slips.
			f.st.clearDirty(id)
		}
		if !due.IsZero() && (next.IsZero() || due.Before(next)) {
			next = due
		}
	}
	return next
}

// drain runs queued fsyncs, oldest first, for at most one round's budget
// (drainRoundMaxUnits units or drainRoundMaxAge of wall clock) and reports
// whether the queue still holds work. The bound exists so one sweep cannot hold
// flush permits and the device for the whole queue: between rounds the loop yields,
// so newly arriving units and forced flushes are not queued behind an unbounded
// fsync chain and appends' pwrites interleave with the fsync stream.
func (f *flusher) drain() bool {
	start := time.Now()
	done := 0
	for {
		f.mu.Lock()
		if len(f.units) == 0 {
			f.mu.Unlock()
			f.passes.Add(1)
			return false
		}
		u := f.units[0]
		f.units = f.units[1:]
		f.active[u.slot]++
		f.mu.Unlock()
		if f.parallelism > 1 {
			f.dispatch <- u // a worker fsyncs and settles it
		} else {
			_ = f.syncUnit(u)
			f.decActive(u.slot)
		}
		done++
		if done >= drainRoundMaxUnits || time.Since(start) >= drainRoundMaxAge {
			f.mu.Lock()
			more := len(f.units) > 0
			f.mu.Unlock()
			f.passes.Add(1)
			if more {
				f.budgetRounds.Add(1)
			}
			return more
		}
	}
}

// drainAll runs the queue to empty, ignoring the round budget: shutdown needs
// every queued fsync to land before the files go away.
func (f *flusher) drainAll() {
	for f.drain() {
	}
}

// syncUnit runs one segment's fsync outside every slot lock, books its cost,
// and settles the counters the prepare phase zeroed. The syscall itself runs
// under a flush permit, so however many callers there are (the round, the
// worker pool, an explicit flush elsewhere) at most `parallelism` fsyncs are in
// the kernel at once.
func (f *flusher) syncUnit(u *flushUnit) error {
	f.inflight.Add(1)
	wait := time.Since(u.enq)
	t0 := time.Now()
	f.flushSem <- struct{}{}
	err := fsyncFile(u.file)
	<-f.flushSem
	svc := time.Since(t0)
	f.inflight.Add(-1)

	f.count.Add(1)
	f.svcNS.Add(uint64(svc))
	f.waitNS.Add(uint64(wait))
	addMax(&f.svcMaxNS, uint64(svc))
	addMax(&f.waitMaxNS, uint64(wait))
	f.fileSizeSum.Add(uint64(u.size))
	addMax(&f.fileSizeMax, uint64(u.size))
	if u.newBytes > 0 {
		f.newBytesSum.Add(uint64(u.newBytes))
		addMax(&f.newBytesMax, uint64(u.newBytes))
	}
	bucketAdd(&f.svcBucket, svc)
	bucketAdd(&f.waitBucket, wait)

	if sl := f.st.slots[u.slot].Load(); sl != nil {
		sl.settleFlush(u, err)
	}
	f.mu.Lock()
	delete(f.inQ, u.seg)
	f.mu.Unlock()
	return err
}

// fsyncProbe lets a test observe every fsync the flusher performs (it runs
// instead of the syscall): the single-fsync invariant and the phase-settlement
// window are both asserted through it. Production leaves it nil.
var fsyncProbe func() error

// fsyncFile is the flusher's one syscall site.
func fsyncFile(f *os.File) error {
	if fsyncProbe != nil {
		return fsyncProbe()
	}
	return f.Sync()
}

// addMax stores v if it exceeds the current maximum.
func addMax(dst *atomic.Uint64, v uint64) {
	for {
		old := dst.Load()
		if v <= old || dst.CompareAndSwap(old, v) {
			return
		}
	}
}

// bucketAdd books a duration into the shared ms histogram.
func bucketAdd(b *[fsyncBuckets]atomic.Uint64, d time.Duration) {
	ms := float64(d) / 1e6
	i := 0
	for i < fsyncBuckets-1 && ms >= fsyncBucketBoundsMs[i] {
		i++
	}
	b[i].Add(1)
}

// shutdown stops the loop after draining the queue, then retires the seal
// worker (a queued seal job is dropped: its indexes rebuild at the next start).
// Idempotent.
func (f *flusher) shutdown() {
	f.stopOnce.Do(func() {
		close(f.stop)
		<-f.done
		close(f.dispatch)
		f.poolDone.Wait()
		close(f.sealStop)
		<-f.sealDone
	})
}

// syncThroughMu runs one fsync under the same mutex the flush loop uses, so the
// seal worker's index writes keep the process-wide "one fsync at a time"
// invariant (I2) without holding any slot lock.
func (f *flusher) syncThroughMu(file *os.File) error {
	f.flushSem <- struct{}{}
	defer func() { <-f.flushSem }()
	return fsyncFile(file)
}

// flushDigest is the flusher's contribution to FlushStats.
type flushDigest struct {
	queueUnits int
	inflight   int64
	count      uint64
	waitAvgMS  float64
	waitMaxMS  float64
	// fileSize is the segment file each fsync covered (it explains a long
	// service time); newBytes is what that fsync was the first to cover.
	fileSizeAvg float64
	fileSizeMax uint64
	newBytesAvg float64
	newBytesMax uint64
	svcP50MS    float64
	svcP99MS    float64
	waitP50MS   float64
	waitP99MS   float64
	svcBucket   []uint64
	waitBucket  []uint64

	// Drain budget: rounds run, rounds that ran out of budget with work left,
	// and the deepest the queue got. A budgetRounds share near zero means the
	// budget never bound; a large one says the queue is routinely deeper than
	// one round drains.
	passes       uint64
	budgetRounds uint64
	queueHigh    uint64
}

func (f *flusher) digest() flushDigest {
	f.mu.Lock()
	q := len(f.units)
	f.mu.Unlock()
	d := flushDigest{
		queueUnits:   q,
		inflight:     f.inflight.Load(),
		count:        f.count.Load(),
		fileSizeMax:  f.fileSizeMax.Load(),
		newBytesMax:  f.newBytesMax.Load(),
		passes:       f.passes.Load(),
		budgetRounds: f.budgetRounds.Load(),
		queueHigh:    f.queueHigh.Load(),
	}
	if d.count > 0 {
		d.waitAvgMS = float64(f.waitNS.Load()) / float64(d.count) / 1e6
		d.waitMaxMS = float64(f.waitMaxNS.Load()) / 1e6
		d.fileSizeAvg = float64(f.fileSizeSum.Load()) / float64(d.count)
		d.newBytesAvg = float64(f.newBytesSum.Load()) / float64(d.count)
	}
	d.svcBucket = make([]uint64, fsyncBuckets)
	d.waitBucket = make([]uint64, fsyncBuckets)
	for i := range d.svcBucket {
		d.svcBucket[i] = f.svcBucket[i].Load()
		d.waitBucket[i] = f.waitBucket[i].Load()
	}
	d.svcP50MS, d.svcP99MS = bucketPercentiles(d.svcBucket)
	d.waitP50MS, d.waitP99MS = bucketPercentiles(d.waitBucket)
	return d
}

// bucketPercentiles approximates p50/p99 from a cumulative ms histogram: the
// value returned is the upper bound of the bucket the percentile falls in.
func bucketPercentiles(b []uint64) (p50, p99 float64) {
	var total uint64
	for _, n := range b {
		total += n
	}
	if total == 0 {
		return 0, 0
	}
	at := func(frac float64) float64 {
		want := uint64(float64(total) * frac)
		if want == 0 {
			want = 1
		}
		var acc uint64
		for i, n := range b {
			acc += n
			if acc >= want {
				if i == len(b)-1 {
					return -1 // open-ended top bucket (>= 1024ms)
				}
				return fsyncBucketBoundsMs[i]
			}
		}
		return -1
	}
	return at(0.50), at(0.99)
}
