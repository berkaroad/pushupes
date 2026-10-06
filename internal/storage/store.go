package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pushupes/internal/data"
)

// Store owns all local slot WALs and the aggregate->slot routing.
type Store struct {
	Dir          string
	SlotCount    int32
	SegmentBytes int64
	FlushPolicy  FlushPolicy

	slots []atomic.Pointer[Slot] // lock-free reads; mu guards lazy open/reload
	mu    sync.RWMutex
	stop  chan struct{}
	done  chan struct{}

	// dirtyMu guards the set of slots with unflushed records; flushLoop
	// consults only this set, so an idle node costs O(active) lock
	// acquisitions per tick instead of O(slot count).
	dirtyMu sync.Mutex
	dirty   map[int32]bool

	// wakeBus is the store-wide advance signal: every slot's
	// advanceNotifyLocked closes the current bus handle and installs a
	// fresh one, so a long-poll can watch ALL slots with a SINGLE channel
	// instead of selecting across one handle per slot. A spurious wake
	// (another slot advanced) costs one cheap pending-slot rescan; a
	// per-slot fan-out in the leader costs O(followed slots) per append.
	wakeMu  sync.Mutex
	wakeBus chan struct{}

	// per-slot counters of records made durable locally (both client
	// appends and replica/migration applies); the basis for write-rate
	// reporting in the admin API.
	writes []atomic.Uint64
}

// OpenStore loads every existing slot directory under dir and creates the
// missing ones lazily on first write. Recovery is parallel: each slot's WAL
// scan is independent, and with thousands of slots a serial scan would dominate
// cold start (one disk walk + index rebuild per slot).
// OpenStore opens (or creates) the store, repairing each slot's segment
// indexes on the way: a start is where an index that is missing, stale or
// damaged is rewritten. A running node never re-indexes history.
func OpenStore(dir string, slotCount int32, segmentBytes int64, flush FlushPolicy) (*Store, error) {
	return openStoreMode(dir, slotCount, segmentBytes, flush, slotOpenLoadAndRepair)
}

// openStoreMode is OpenStore with an explicit index mode; slotOpenLoad is what
// a reload or a lazy slot open uses (it never writes an index for an existing
// segment).
func openStoreMode(dir string, slotCount int32, segmentBytes int64, flush FlushPolicy, mode slotOpenMode) (*Store, error) {
	if slotCount <= 0 {
		slotCount = data.DefaultSlotCount
	}
	if segmentBytes <= 0 {
		segmentBytes = DefaultSegmentBytes
	}
	st := &Store{
		Dir:          dir,
		SlotCount:    slotCount,
		SegmentBytes: segmentBytes,
		FlushPolicy:  flush,
		slots:        make([]atomic.Pointer[Slot], slotCount),
		writes:       make([]atomic.Uint64, slotCount),
		dirty:        map[int32]bool{},
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
		wakeBus:      make(chan struct{}),
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	// Phase 1: which slot dirs have data? (one readdir, cheap)
	type task struct {
		id   int32
		path string
	}
	var tasks []task
	for i := int32(0); i < slotCount; i++ {
		path := st.slotDir(i)
		if fi, err := os.Stat(path); err == nil && fi.IsDir() {
			if entries, err := os.ReadDir(path); err == nil && len(entries) > 0 {
				tasks = append(tasks, task{i, path})
			}
		}
	}
	// Phase 2: open/rebuild them with a bounded worker pool. The first
	// error wins; callers treat a failed open as fatal startup anyway.
	workers := runtime.GOMAXPROCS(0)
	if workers < 4 {
		workers = 4
	}
	var (
		wg      sync.WaitGroup
		errOnce sync.Once
		openErr error
	)
	ch := make(chan task)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range ch {
				slot, err := openSlot(t.path, t.id, segmentBytes, flush, mode)
				if err != nil {
					errOnce.Do(func() { openErr = err })
					continue
				}
				slot.store = st
				st.slots[t.id].Store(slot)
			}
		}()
	}
	for _, t := range tasks {
		ch <- t
	}
	close(ch)
	wg.Wait()
	if openErr != nil {
		return nil, openErr
	}
	go st.flushLoop()
	return st, nil
}

func (st *Store) slotDir(slot int32) string {
	return filepath.Join(st.Dir, fmt.Sprintf("slot-%03d", slot))
}

// SlotDir exposes a slot's WAL directory.
func (st *Store) SlotDir(slot int32) string { return st.slotDir(slot) }

// DropSlot closes and removes a slot's WAL (post-migration cleanup).
func (st *Store) DropSlot(slotID int32) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if slotID < 0 || slotID >= st.SlotCount {
		return fmt.Errorf("slot %d out of range", slotID)
	}
	if s := st.slots[slotID].Load(); s != nil {
		s.Close()
		st.slots[slotID].Store(nil)
	}
	st.clearDirty(slotID)
	return os.RemoveAll(st.slotDir(slotID))
}

// SlotPresent reports whether this node holds a local copy of the slot: the
// slot is currently open, or its directory still holds files on disk (a slot
// is opened lazily, so after a restart a copy this node never touched is only
// visible on disk). It is the "do I actually have something to lose" test the
// post-migration cleanup uses before scheduling a drop.
func (st *Store) SlotPresent(slotID int32) bool {
	if slotID < 0 || slotID >= st.SlotCount {
		return false
	}
	if s := st.slots[slotID].Load(); s != nil {
		return true
	}
	entries, err := os.ReadDir(st.slotDir(slotID))
	return err == nil && len(entries) > 0
}

// SlotDiskBytes is a slot's on-disk footprint: the sum of its segment files.
// A copy this process holds answers from its own accounting (it knows the tail
// this process wrote); one it has not opened — the directory appeared after
// startup (a migration push before its reload) — is stat'ed from disk, because
// reporting 0 there would silently shrink whatever watches the number.
// Deliberately the same accounting as Slot.TotalSize (segment files only: the
// auxiliary files are small and not what an operator is watching).
func (st *Store) SlotDiskBytes(slotID int32) int64 {
	if slotID < 0 || slotID >= st.SlotCount {
		return 0
	}
	if s := st.slots[slotID].Load(); s != nil {
		return s.TotalSize()
	}
	entries, err := os.ReadDir(st.slotDir(slotID))
	if err != nil {
		return 0
	}
	var n int64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".wal") {
			continue
		}
		if info, err := e.Info(); err == nil {
			n += info.Size()
		}
	}
	return n
}

// ReloadSlot reopens a slot from disk (after migration segments were pushed
// into its directory).
func (st *Store) ReloadSlot(slotID int32) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if slotID < 0 || slotID >= st.SlotCount {
		return fmt.Errorf("slot %d out of range", slotID)
	}
	if s := st.slots[slotID].Load(); s != nil {
		s.Close()
	}
	slot, err := OpenSlot(st.slotDir(slotID), slotID, st.SegmentBytes, st.FlushPolicy)
	if err != nil {
		return err
	}
	slot.store = st
	st.slots[slotID].Store(slot)
	return nil
}

// SlotOf routes an aggregate ID to its slot.
func (st *Store) SlotOf(aggregateID string) int32 {
	return data.SlotOf(aggregateID, int(st.SlotCount))
}

// Slot returns slot i, creating its WAL lazily.
func (st *Store) Slot(i int32) (*Slot, error) {
	if i < 0 || i >= st.SlotCount {
		return nil, fmt.Errorf("slot %d out of range [0,%d)", i, st.SlotCount)
	}
	if s := st.slots[i].Load(); s != nil {
		return s, nil
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	s := st.slots[i].Load()
	if s == nil {
		slot, err := OpenSlot(st.slotDir(i), i, st.SegmentBytes, st.FlushPolicy)
		if err != nil {
			return nil, err
		}
		slot.store = st
		st.slots[i].Store(slot)
		return slot, nil
	}
	return s, nil
}

// SlotIfLoaded returns the slot if this node already has it open, and nil when
// it is cold. Unlike Slot() it never opens one, so it neither creates the slot
// directory nor walks the segments it holds — the accessor for read-only admin
// views (a console listing slots must not materialise them on whatever node it
// happens to ask).
func (st *Store) SlotIfLoaded(i int32) (*Slot, error) {
	if i < 0 || i >= st.SlotCount {
		return nil, fmt.Errorf("slot %d out of range [0,%d)", i, st.SlotCount)
	}
	return st.slots[i].Load(), nil
}

// Append routes by aggregate_id and applies the business rules.
func (st *Store) Append(rec *data.EventRecord) (*AppendOutcome, error) {
	slotID := st.SlotOf(rec.AggregateID)
	slot, err := st.Slot(slotID)
	if err != nil {
		return nil, err
	}
	out, err := slot.Append(rec)
	if err == nil && out.Status == data.StatusSuccess {
		st.writes[slotID].Add(1)
		st.markDirty(slotID)
	}
	return out, err
}

// AppendAtSeq writes a leader-assigned record into a specific slot (replica
// and migration catch-up path).
func (st *Store) AppendAtSeq(slotID int32, seq uint64, rec *data.EventRecord) error {
	slot, err := st.Slot(slotID)
	if err != nil {
		return err
	}
	newly, err := slot.appendAtSeq(seq, rec)
	if err != nil {
		return err
	}
	if newly {
		st.writes[slotID].Add(1)
		st.markDirty(slotID)
	}
	return nil
}

// AppendFrameAtSeq lands a leader-encoded record frame at a fixed seq — the
// follower replication and migration catch-up path — without decoding or
// re-encoding it.
func (st *Store) AppendFrameAtSeq(slotID int32, seq uint64, frame []byte) error {
	slot, err := st.Slot(slotID)
	if err != nil {
		return err
	}
	newly, err := slot.appendFrameAtSeq(seq, frame)
	if err != nil {
		return err
	}
	if newly {
		st.writes[slotID].Add(1)
		st.markDirty(slotID)
	}
	return nil
}

// markDirty registers a slot as holding unflushed records (flushLoop only
// visits these; the set is emptied once FlushDue reports nothing pending).
func (st *Store) markDirty(slotID int32) {
	st.dirtyMu.Lock()
	st.dirty[slotID] = true
	st.dirtyMu.Unlock()
}

func (st *Store) clearDirty(slotID int32) {
	st.dirtyMu.Lock()
	delete(st.dirty, slotID)
	st.dirtyMu.Unlock()
}

// WriteCount returns the number of records made durable in one slot since
// the process started.
func (st *Store) WriteCount(slotID int32) uint64 {
	if slotID < 0 || int(slotID) >= len(st.writes) {
		return 0
	}
	return st.writes[slotID].Load()
}

// WriteCounts snapshots the per-slot durable write counters.
func (st *Store) WriteCounts() []uint64 {
	out := make([]uint64, len(st.writes))
	for i := range st.writes {
		out[i] = st.writes[i].Load()
	}
	return out
}

// ReadAggregate reads up to limit records of one aggregate from a version,
// capped at uptoSeq (0 = LEO). Returns records, their seqs, and the next
// version to read.
func (st *Store) ReadAggregate(aggregateID string, fromVersion uint32, limit, uptoSeq uint64) ([]*data.EventRecord, []uint64, error) {
	slot, err := st.Slot(st.SlotOf(aggregateID))
	if err != nil {
		return nil, nil, err
	}
	recs, seqs, err := slot.AggregateVersion(aggregateID, fromVersion, limit, uptoSeq)
	return recs, seqs, err
}

// RecordByCommand looks a record up by idempotency key (within its slot).
func (st *Store) RecordByCommand(aggregateID, commandID string) (*data.EventRecord, uint64, error) {
	slot, err := st.Slot(st.SlotOf(aggregateID))
	if err != nil {
		return nil, 0, err
	}
	return slot.RecordByCommand(commandID)
}

// TailVersionOf returns the latest version of one aggregate that is visible
// at uptoSeq (0 = the slot's durable LEO), i.e. the version a ReadStream
// bounded by the same seq would report as its last record. It is the bulk
// probe behind ReadTails: a resume scan wants one number per stream instead
// of a whole record range.
//
// The bound is applied version by version, stopping at the first version whose
// seq exceeds uptoSeq, so a partially visible tail reports exactly the same
// value a bounded ReadStream would return rather than an unclipped directory
// count. 0 means "no records visible".
func (st *Store) TailVersionOf(aggregateID string, uptoSeq uint64) (uint32, error) {
	slot, err := st.Slot(st.SlotOf(aggregateID))
	if err != nil {
		return 0, err
	}
	return slot.TailVersion(aggregateID, uptoSeq)
}

// LastSeq returns the durable LEO for one slot (0 when the slot is unknown).
func (st *Store) LastSeqOf(slotID int32) uint64 {
	s, err := st.Slot(slotID)
	if err != nil {
		return 0
	}
	return s.LastSeq()
}

// WakeChan returns the current wake handle for one slot: it closes on the
// next LEO advance (any append or replica apply). Take it *after* your last
// read of the slot — advances from then on are caught either by a re-read or
// by the handle firing. Used by the multi-fetch long-poll to decide WHICH
// parked slots actually moved after the store bus fires.
func (st *Store) WakeChan(slotID int32) <-chan struct{} {
	s, err := st.Slot(slotID)
	if err != nil {
		return nil
	}
	return *s.Wake()
}

// ScanFetchState answers a whole batch of follower positions in one call:
// moved[i] reports whether slots[i] holds anything at or beyond froms[i], and
// wakes[i] is the handle to park on. It exists because a fetch session reports
// EVERY slot it follows (thousands) every round, and per-slot accessors put two
// method calls, error paths and a lock-free lookup between the loop and the two
// atomic loads it actually needs.
//
// The handle is read before the position (no-lost-wake discipline): an append
// landing in between closes the handle handed back here, so the caller's wait
// fires immediately rather than sleeping to the deadline. Slots that this node
// does not have read as "no data, no wake".
func (st *Store) ScanFetchState(slots []int32, froms []uint64, moved []bool, wakes []<-chan struct{}) {
	n := len(slots)
	if len(froms) < n {
		n = len(froms)
	}
	if len(moved) < n {
		n = len(moved)
	}
	if len(wakes) < n {
		n = len(wakes)
	}
	for i := 0; i < n; i++ {
		s := slots[i]
		if s < 0 || int(s) >= len(st.slots) {
			moved[i], wakes[i] = false, nil
			continue
		}
		sp := st.slots[s].Load()
		if sp == nil {
			// Slots are opened lazily: a position report for a slot this node
			// has never written must still yield a wake handle, or the caller
			// would park nothing and poll in a hot loop. This is the cold path
			// (once per slot); every later round reads the loaded pointer.
			var err error
			if sp, err = st.Slot(s); err != nil {
				moved[i], wakes[i] = false, nil
				continue
			}
		}
		h := sp.wake.Load()
		if h != nil {
			wakes[i] = *h
		} else {
			wakes[i] = nil
		}
		moved[i] = sp.seqCounter.Load() >= froms[i]
	}
}

// WakeBus returns the store-wide advance signal: ANY slot appending closes
// the current handle. A leader long-polling hundreds of slots selects on
// this one channel instead of rebuilding a per-slot case set every round;
// on a fire it non-blockingly checks the parked slots' own handles to find
// which one moved (spurious fires for slots nobody follows cost O(parked)
// channel reads, not O(followed) locks).
func (st *Store) WakeBus() <-chan struct{} {
	st.wakeMu.Lock()
	defer st.wakeMu.Unlock()
	return st.wakeBus
}

// fireWakeBus closes the current bus handle and installs a fresh one.
func (st *Store) fireWakeBus() {
	st.wakeMu.Lock()
	close(st.wakeBus)
	st.wakeBus = make(chan struct{})
	st.wakeMu.Unlock()
}

// WaitForSeq blocks on one slot until its LEO reaches wantSeq or the
// deadline passes (single-slot long-poll helper, kept for tests).
func (st *Store) WaitForSeq(slotID int32, wantSeq uint64, deadline time.Time) uint64 {
	s, err := st.Slot(slotID)
	if err != nil {
		return 0
	}
	return s.WaitForSeq(wantSeq, deadline)
}

// ReadSlotBytes returns the record byte ranges for fromSeq <= seq < untilSeq
// (untilSeq 0 = LEO+1) capped at maxBytes, plus the concatenated payload.
// This is the unit of work for replica fetch and migration catch-up. Payload
// bytes are read through the segment's already-open handle (one preallocated
// buffer, size-to-fit ranges) — reopening each segment file per range showed
// up as epoll/read syscall pressure on the leader under fetch fan-out.
func (st *Store) ReadSlotBytes(slotID int32, fromSeq, untilSeq uint64, maxBytes int64) ([]data.ByteRange, uint64, []byte, error) {
	slot, err := st.Slot(slotID)
	if err != nil {
		return nil, 0, nil, err
	}
	ranges, next, err := slot.ReadRange(fromSeq, untilSeq, maxBytes)
	if err != nil {
		return ranges, next, nil, err
	}
	var payload []byte
	if len(ranges) > 0 {
		var total int64
		for _, r := range ranges {
			total += r.End - r.Start
		}
		// Ranges are read straight into the payload slice: the extra
		// staging buffer (and its memcpy of every fetched byte) is gone.
		payload = make([]byte, total)
		off := int64(0)
		for _, r := range ranges {
			n := r.End - r.Start
			if err := st.slotReadRange(slot, r, payload[off:off+n]); err != nil {
				return ranges, next, nil, err
			}
			off += n
		}
	}
	return ranges, next, payload, nil
}

// slotReadRange reads [Start,End) of one ByteRange through the open segment
// handle whose base seq covers r. Falls back to reading the file by name if
// the in-memory segment list is behind (should not happen: ReadRange and the
// payload read run under the same slot RLock via Slot.SegmentFile).
func (st *Store) slotReadRange(slot *Slot, r data.ByteRange, buf []byte) error {
	seg := slot.SegmentFile(r.Segment)
	if seg == nil {
		b, err := os.ReadFile(filepath.Join(slot.Dir, r.Segment))
		if err != nil {
			return err
		}
		copy(buf, b[r.Start:r.End])
		return nil
	}
	_, err := seg.File.ReadAt(buf, r.Start)
	return err
}

// Flush flushes every loaded slot.
func (st *Store) Flush() error {
	slots := make([]*Slot, 0, len(st.slots))
	for i := range st.slots {
		if s := st.slots[i].Load(); s != nil {
			slots = append(slots, s)
		}
	}
	for _, s := range slots {
		if err := s.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (st *Store) flushLoop() {
	defer close(st.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-st.stop:
			return
		case now := <-ticker.C:
			st.dirtyMu.Lock()
			ids := make([]int32, 0, len(st.dirty))
			for id := range st.dirty {
				ids = append(ids, id)
			}
			st.dirtyMu.Unlock()
			for _, id := range ids {
				s := st.slots[id].Load()
				if s == nil {
					st.clearDirty(id)
					continue
				}
				if _, pending := s.FlushDue(now); !pending && !s.HasPending() {
					// double-check closes the window where a concurrent append
					// marked the slot dirty during FlushDue; at worst one
					// policy cycle slips (Close full-flushes on shutdown).
					st.clearDirty(id)
				}
			}
		}
	}
}

// Close stops the flush loop and flushes everything.
func (st *Store) Close() error {
	close(st.stop)
	<-st.done
	return st.Flush()
}

// SlotDigest answers the same summary for a slot id, opening nothing: an
// unloaded slot is reported as empty rather than being brought into memory.
func (st *Store) SlotDigest(slotID int32) (aggregates, versions, resolvable uint64) {
	sl, err := st.SlotIfLoaded(slotID)
	if err != nil || sl == nil {
		return 0, 0, 0
	}
	return sl.SlotDigest()
}
