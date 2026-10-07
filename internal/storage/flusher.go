package storage

import (
	"os"
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
//	   drained oldest-dirty-first, so oldest_dirty_age_s stays bounded.
//
// A unit of work is ONE segment of one slot (not a whole slot): a slow fsync
// therefore delays a single 256MiB segment, not every segment of that slot, and
// not the rest of the queue.

// flushUnit is one segment's outstanding fsync. Everything the syscall needs
// was snapshotted under the slot's write lock in prepareFlushLocked, so the
// flusher never touches slot state outside the lock's windows.
type flushUnit struct {
	slot int32
	seg  *Segment
	file *os.File // handle snapshot; syncing it needs no lock and it stays open while queued
	size int64    // file size this fsync covers, settled into seg.syncedSize
	recs int64    // records this fsync covers, settled into seg.unflushed/pendingFlush
	enq  time.Time
}

// flusher owns every fsync in the store.
type flusher struct {
	st *Store

	mu     sync.Mutex
	units  []*flushUnit // sorted by enq: oldest-dirty-first
	inQ    map[*Segment]bool
	frozen map[int32]bool // slots detached for Close/Drop: no new units

	// flushMu is what makes every fsync in the process exclusive, wherever it
	// runs: the loop holds it around a queued unit's syscall, and the forced
	// path (explicit Flush/Close) holds it while it syncs on the caller's
	// goroutine. There is no cross-goroutine wait for a queued item, so no
	// shutdown or detach path can block on a loop that has stopped running.
	flushMu sync.Mutex

	wake chan struct{}
	stop chan struct{}
	done chan struct{}

	// stopOnce makes shutdown idempotent: a second Close waits for the same
	// exit instead of closing the same channel twice.
	stopOnce sync.Once

	inflight atomic.Int64 // invariant I2: 0 or 1

	count      atomic.Uint64 // fsyncs performed
	svcNS      atomic.Uint64 // syscall service time
	svcMaxNS   atomic.Uint64
	waitNS     atomic.Uint64 // queue wait before the syscall
	waitMaxNS  atomic.Uint64
	bytesSum   atomic.Uint64 // bytes covered by each fsync
	bytesMax   atomic.Uint64
	svcBucket  [fsyncBuckets]atomic.Uint64
	waitBucket [fsyncBuckets]atomic.Uint64
}

func newFlusher(st *Store) *flusher {
	f := &flusher{
		st:     st,
		inQ:    map[*Segment]bool{},
		frozen: map[int32]bool{},
		wake:   make(chan struct{}, 1),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	return f
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
// under flushMu, so it is still one fsync at a time, and it never waits for the
// loop — a retired or busy flusher cannot make an explicit flush hang.
func (f *flusher) flushSlotNow(sl *Slot) error {
	f.flushMu.Lock()
	defer f.flushMu.Unlock()
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
	// flushMu first: whatever fsync is running (queued or forced) finishes
	// before the caller's files go away.
	f.flushMu.Lock()
	defer f.flushMu.Unlock()
	f.mu.Lock()
	defer f.mu.Unlock()
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
		// Sync everything queued before parking: one fsync at a time, oldest
		// dirty first. An enqueue that lands after this returns re-kicks the
		// loop, so the wake below cannot be lost.
		f.drain()
		select {
		case <-f.stop:
			f.drain()
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

// drain performs every queued fsync, one at a time, until the queue is empty.
func (f *flusher) drain() {
	for {
		f.mu.Lock()
		if len(f.units) == 0 {
			f.mu.Unlock()
			return
		}
		u := f.units[0]
		f.units = f.units[1:]
		f.mu.Unlock()
		f.flushMu.Lock()
		_ = f.syncUnit(u)
		f.flushMu.Unlock()
	}
}

// syncUnit runs one segment's fsync outside every slot lock, books its cost,
// and settles the counters the prepare phase zeroed.
// syncUnit runs one unit's fsync and books it. Caller holds flushMu: that is
// what makes the syscall exclusive whether it came from the loop or from an
// explicit flush on another goroutine.
func (f *flusher) syncUnit(u *flushUnit) error {
	f.inflight.Add(1)
	wait := time.Since(u.enq)
	t0 := time.Now()
	err := fsyncFile(u.file)
	svc := time.Since(t0)
	f.inflight.Add(-1)

	f.count.Add(1)
	f.svcNS.Add(uint64(svc))
	f.waitNS.Add(uint64(wait))
	addMax(&f.svcMaxNS, uint64(svc))
	addMax(&f.waitMaxNS, uint64(wait))
	f.bytesSum.Add(uint64(u.size))
	addMax(&f.bytesMax, uint64(u.size))
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

// shutdown stops the loop after draining the queue. Idempotent.
func (f *flusher) shutdown() {
	f.stopOnce.Do(func() { close(f.stop) })
	<-f.done
}

// flushDigest is the flusher's contribution to FlushStats.
type flushDigest struct {
	queueUnits int
	inflight   int64
	count      uint64
	svcAvgMS   float64
	svcMaxMS   float64
	waitAvgMS  float64
	waitMaxMS  float64
	bytesAvg   float64
	bytesMax   uint64
	svcP50MS   float64
	svcP99MS   float64
	waitP50MS  float64
	waitP99MS  float64
	svcBucket  []uint64
	waitBucket []uint64
}

func (f *flusher) digest() flushDigest {
	f.mu.Lock()
	q := len(f.units)
	f.mu.Unlock()
	d := flushDigest{
		queueUnits: q,
		inflight:   f.inflight.Load(),
		count:      f.count.Load(),
		bytesMax:   f.bytesMax.Load(),
	}
	if d.count > 0 {
		d.svcAvgMS = float64(f.svcNS.Load()) / float64(d.count) / 1e6
		d.svcMaxMS = float64(f.svcMaxNS.Load()) / 1e6
		d.waitAvgMS = float64(f.waitNS.Load()) / float64(d.count) / 1e6
		d.waitMaxMS = float64(f.waitMaxNS.Load()) / 1e6
		d.bytesAvg = float64(f.bytesSum.Load()) / float64(d.count)
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
