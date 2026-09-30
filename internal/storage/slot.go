package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"pushupes/internal/data"
)

// FlushPolicy controls when a slot WAL is fsynced (mirrors the design doc).
type FlushPolicy struct {
	IntervalMessages int64         // fsync after N unflushed records (0 = never)
	Interval         time.Duration // fsync after T since last flush (0 = never)
}

// DefaultFlushPolicy fsyncs every 1000 records or every 5 seconds.
func DefaultFlushPolicy() FlushPolicy {
	return FlushPolicy{IntervalMessages: 1000, Interval: 5 * time.Second}
}

// AppendOutcome is the result of a slot-level append, carrying the business
// rules: success (appended), exists (idempotent duplicate by command_id,
// returns the stored record), or fail (version conflict / bad request).
type AppendOutcome struct {
	Status         string // data.StatusSuccess / StatusExists / StatusFail
	ErrID          int
	Seq            uint64
	Record         *data.EventRecord // stored record on success/exists
	CurrentVersion uint32            // aggregate's current max version on fail
}

// Slot is one of the fixed 128 slots: an append-only WAL of segments plus
// three in-memory indexes:
//
//	aggs:     aggregate_id -> latest version + seq range in the arena (rule 2)
//	cmdIndex: command_id hash -> seq of its record     (rule 1, idempotency)
//	seqChunks: every aggregate's seqs, dense and chunked (range reads by version)
//
// All writes are serialised by the slot lock; seq is the slot-global record
// sequence, dense from 1.
type Slot struct {
	ID       int32
	Dir      string
	segments []*Segment // ordered, contiguous by seq; last one writable

	// seqCounter is the last assigned seq (0 = empty). Written under s.mu;
	// exposed lock-free via LastSeq/atomic for the fetch hot path.
	seqCounter atomic.Uint64

	aggs      map[string]aggEntry // aggregate directory (see slotaggs.go)
	cmdIndex  cmdTable
	seqChunks [][]uint64 // arena: each aggregate's seqs, ascending
	seqLen    int        // total seqs in the arena

	segmentBytes int64
	flush        FlushPolicy
	pendingFlush int64
	lastFlush    time.Time

	mu sync.RWMutex
	// cond signals seqCounter advances (single-slot long-poll wait).
	cond *sync.Cond
	// wake is the slot's own advance signal: closed on every LEO advance
	// (see Store.WakeChan). The leader's long-poll watch selects across
	// deadline/context/store-bus and uses these handles to decide WHICH
	// parked slots actually moved — the bus alone would force a locked
	// rescan of every followed slot per append. Swapped under s.mu; read
	// lock-free via Store.WakeChan.
	wake atomic.Pointer[chan struct{}]
	// store back-reference: advances also fire the store-wide wake bus
	// (see Store.WakeBus). nil for standalone (test) slots.
	store *Store
}

// OpenSlot loads (or creates) the WAL for one slot directory and rebuilds
// indexes by scanning every segment body.
func OpenSlot(dir string, slotID int32, segmentBytes int64, flush FlushPolicy) (*Slot, error) {
	if segmentBytes <= 0 {
		segmentBytes = DefaultSegmentBytes
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Slot{
		ID:           slotID,
		Dir:          dir,
		segmentBytes: segmentBytes,
		flush:        flush,
		lastFlush:    time.Now(),
		aggs:         make(map[string]aggEntry),
	}
	s.cond = sync.NewCond(&s.mu)
	w := make(chan struct{})
	s.wake.Store(&w)
	paths, err := SegmentFilesOf(dir)
	if err != nil {
		return nil, err
	}
	var expected uint64 = 1
	for _, p := range paths {
		seg, err := LoadSegment(p)
		if err != nil {
			return nil, fmt.Errorf("slot %d load %s: %w", slotID, p, err)
		}
		if seg.SlotID != slotID {
			seg.Close()
			return nil, fmt.Errorf("slot %d: %s has header slot %d", slotID, p, seg.SlotID)
		}
		if seg.BaseSeq != expected {
			seg.Close()
			return nil, fmt.Errorf("slot %d: %s seq gap (base %d, expected %d)", slotID, p, seg.BaseSeq, expected)
		}
		if seg.RecordCnt > 0 {
			// Index rebuild: walk the frame headers only — the maps need the
			// aggregate, version and command id, never the bodies.
			if err := seg.ScanHeaders(seg.BaseSeq, func(seq uint64, meta data.RecordMeta) bool {
				s.indexMetaLocked(seq, meta)
				return true
			}); err != nil {
				seg.Close()
				return nil, fmt.Errorf("slot %d scan %s: %w", slotID, p, err)
			}
			expected = seg.LastSeq + 1
		}
		seg.Seal() // history segments sealed; the tail is unsealed below
		s.segments = append(s.segments, seg)
	}
	// The writable tail: reopen the last loaded segment, or create one.
	if len(s.segments) == 0 {
		seg, err := CreateSegment(dir, slotID, 1)
		if err != nil {
			return nil, err
		}
		s.segments = append(s.segments, seg)
		s.seqCounter.Store(0)
	} else {
		last := s.segments[len(s.segments)-1]
		last.writable = true // resume appending where the log stopped
		if last.RecordCnt > 0 {
			s.seqCounter.Store(last.LastSeq)
		} else {
			s.seqCounter.Store(last.BaseSeq - 1)
		}
	}
	return s, nil
}

// indexRecordLocked updates the in-memory indexes for one stored record.
// Caller holds the write lock.
func (s *Slot) indexRecordLocked(seq uint64, rec *data.EventRecord) {
	s.indexMetaLocked(seq, data.RecordMeta{
		AggregateID: rec.AggregateID,
		Version:     rec.Version,
		CommandHash: data.HashCommandID(rec.CommandID),
	})
}

// indexMetaLocked indexes a record from its header alone — the replicated
// path never materialises event bodies, so it indexes from RecordMeta.
func (s *Slot) indexMetaLocked(seq uint64, m data.RecordMeta) {
	e := s.aggs[m.AggregateID]
	if m.Version > e.version {
		e.version = m.Version
	}
	s.cmdIndex.put(m.CommandHash, seq)
	// Only a contiguous append extends the aggregate's seq range; a record
	// arriving out of order still updates its version, exactly as before.
	if m.Version == uint32(e.n+1) {
		if e.n == 0 {
			e.off = s.seqLen
		}
		s.appendSeqLocked(seq)
		e.n++
	}
	s.aggs[m.AggregateID] = e
}

// tail returns the writable segment, rolling a new one when it passed the
// size threshold.
func (s *Slot) tail() (*Segment, error) {
	seg := s.segments[len(s.segments)-1]
	if seg.SizeBytes() >= s.segmentBytes {
		seg.Seal()
		if err := seg.Flush(); err != nil {
			return nil, err
		}
		next, err := CreateSegment(s.Dir, s.ID, seg.LastSeq+1)
		if err != nil {
			return nil, err
		}
		s.segments = append(s.segments, next)
		seg = next
	}
	return seg, nil
}

// Append applies the business rules and writes the record when they pass.
func (s *Slot) Append(rec *data.EventRecord) (*AppendOutcome, error) {
	if err := rec.Validate(); err != nil {
		return &AppendOutcome{Status: data.StatusFail, ErrID: data.ErrIDBadRequest}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Rule 1: idempotency by command_id within the slot. The index holds
	// hashes only, so a candidate is confirmed against the record it points at
	// — the record the EXISTS answer returns anyway.
	var stored *data.EventRecord
	seq, found, err := s.cmdIndex.lookup(data.HashCommandID(rec.CommandID), func(seq uint64) (bool, error) {
		got, err := s.readBySeqLocked(seq)
		if err != nil || got == nil {
			return false, err
		}
		if got.CommandID != rec.CommandID {
			return false, nil // a different command sharing the hash
		}
		stored = got
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	if found {
		return &AppendOutcome{Status: data.StatusExists, Seq: seq, Record: stored}, nil
	}

	// Rule 2: version must be exactly current+1 (or 1 for a new aggregate).
	cur := s.aggs[rec.AggregateID].version
	if rec.Version != cur+1 {
		return &AppendOutcome{Status: data.StatusFail, ErrID: data.ErrIDVersionConflict, CurrentVersion: cur}, nil
	}

	seg, err := s.tail()
	if err != nil {
		return nil, err
	}
	seq = s.seqCounter.Load() + 1
	if err := seg.AppendRecord(seq, rec); err != nil {
		return nil, err
	}
	s.seqCounter.Store(seq)
	s.indexRecordLocked(seq, rec)
	s.pendingFlush++
	s.advanceNotifyLocked()
	return &AppendOutcome{Status: data.StatusSuccess, Seq: seq, Record: rec}, nil
}

// AppendAtSeq appends a record under an externally fixed seq — used by
// followers and by migration catch-up, where the leader assigned the seq.
// Records must arrive in seq order; duplicates are ignored (idempotent replay).
func (s *Slot) AppendAtSeq(seq uint64, rec *data.EventRecord) error {
	_, err := s.appendAtSeq(seq, rec)
	return err
}

// appendAtSeq is AppendAtSeq plus a "newly written" flag for write-rate
// accounting: an idempotent replay of an existing seq does not count.
func (s *Slot) appendAtSeq(seq uint64, rec *data.EventRecord) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq <= s.seqCounter.Load() {
		if existing, err := s.readBySeqLocked(seq); err == nil && existing != nil {
			if existing.CommandID == rec.CommandID && existing.Version == rec.Version {
				return false, nil // already replicated
			}
			return false, fmt.Errorf("slot %d: seq %d diverged on replay", s.ID, seq)
		}
		return false, fmt.Errorf("slot %d: seq %d below counter %d", s.ID, seq, s.seqCounter.Load())
	}
	if seq != s.seqCounter.Load()+1 {
		return false, fmt.Errorf("slot %d: seq %d not contiguous (counter %d)", s.ID, seq, s.seqCounter.Load())
	}
	seg, err := s.tail()
	if err != nil {
		return false, err
	}
	if err := seg.Append(seq, rec); err != nil {
		return false, err
	}
	s.seqCounter.Store(seq)
	s.indexRecordLocked(seq, rec)
	s.pendingFlush++
	s.advanceNotifyLocked()
	return true, nil
}

// AppendFrameAtSeq appends a leader-encoded record frame under an externally
// fixed seq — the follower replication and migration catch-up path. The frame
// is validated and indexed from its header, then written to the WAL verbatim.
func (s *Slot) AppendFrameAtSeq(seq uint64, frame []byte) error {
	_, err := s.appendFrameAtSeq(seq, frame)
	return err
}

// appendFrameAtSeq is AppendFrameAtSeq plus the "newly written" flag used for
// write-rate accounting (an idempotent replay does not count as a write).
func (s *Slot) appendFrameAtSeq(seq uint64, frame []byte) (bool, error) {
	meta, _, err := data.DecodeRecordMeta(frame)
	if err != nil {
		return false, fmt.Errorf("slot %d: bad record frame: %w", s.ID, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq <= s.seqCounter.Load() {
		if existing, err := s.readBySeqLocked(seq); err == nil && existing != nil {
			// The header-only metadata keeps just the command hash, so confirm
			// the frame is that very record by reading its command id (this is
			// the replay path; a mismatch is a real divergence).
			cmd, cerr := data.CommandIDBytes(frame)
			if cerr == nil && existing.CommandID == string(cmd) && existing.Version == meta.Version {
				return false, nil // already replicated
			}
			return false, fmt.Errorf("slot %d: seq %d diverged on replay", s.ID, seq)
		}
		return false, fmt.Errorf("slot %d: seq %d below counter %d", s.ID, seq, s.seqCounter.Load())
	}
	if seq != s.seqCounter.Load()+1 {
		return false, fmt.Errorf("slot %d: seq %d not contiguous (counter %d)", s.ID, seq, s.seqCounter.Load())
	}
	seg, err := s.tail()
	if err != nil {
		return false, err
	}
	if err := seg.AppendFrame(seq, frame); err != nil {
		return false, err
	}
	s.seqCounter.Store(seq)
	s.indexMetaLocked(seq, meta)
	s.pendingFlush++
	s.advanceNotifyLocked()
	return true, nil
}

// readBySeqLocked fetches one record by seq; caller holds s.mu (read or write).
func (s *Slot) readBySeqLocked(seq uint64) (*data.EventRecord, error) {
	if seq == 0 || seq > s.seqCounter.Load() {
		return nil, data.ErrRecordNotFound
	}
	for _, seg := range s.segments {
		if seg.RecordCnt == 0 {
			continue
		}
		if seq < seg.BaseSeq || seq > seg.BaseSeq+uint64(seg.RecordCnt)-1 {
			continue
		}
		var found *data.EventRecord
		err := seg.ScanFrom(seq, func(q uint64, rec *data.EventRecord) bool {
			if q == seq {
				r := *rec
				found = &r
				return false
			}
			return false
		})
		if err != nil {
			return nil, err
		}
		if found != nil {
			return found, nil
		}
	}
	return nil, data.ErrRecordNotFound
}

// Wake returns the current wake handle pointer. Lock-free read; the handle
// is swapped under s.mu on every advance (advanceNotifyLocked).
func (s *Slot) Wake() *chan struct{} {
	return s.wake.Load()
}

// LastSeq is the slot's high-end seq (LEO). Lock-free: the fetch hot path
// calls it once per reported position per round; the value is written under
// s.mu and observed eventually (appenders see their own write via the lock).
func (s *Slot) LastSeq() uint64 {
	return s.seqCounter.Load()
}

// advanceNotifyLocked fires the store-level wake signal for this slot and
// wakes single-slot Cond waiters: close the current wake handle (waiters
// holding it observe the advance) and install a fresh one for the next.
// Callers hold s.mu (write).
func (s *Slot) advanceNotifyLocked() {
	close(*s.wake.Load())
	nw := make(chan struct{})
	s.wake.Store(&nw)
	if s.store != nil {
		s.store.fireWakeBus()
	}
	s.cond.Broadcast()
}

// WaitForSeq blocks until the slot's LEO reaches wantSeq (> current) or the
// deadline passes; returns the LEO observed at wake-up. Backs the leader's
// long-poll fetch: callers learn about new records immediately after an
// append instead of re-polling.
func (s *Slot) WaitForSeq(wantSeq uint64, deadline time.Time) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.seqCounter.Load() < wantSeq {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		// timed wait: wake the waiter at the deadline even without writes
		timer := time.AfterFunc(remaining, func() { s.cond.Broadcast() })
		s.cond.Wait()
		timer.Stop()
	}
	return s.seqCounter.Load()
}

// CurrentVersion returns the aggregate's last stored version (0 = unknown).
func (s *Slot) CurrentVersion(aggregateID string) uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.aggs[aggregateID].version
}

// RecordByCommand looks a record up by its idempotency key. The index stores
// hashes, so a hit is confirmed by the record's own command id.
func (s *Slot) RecordByCommand(commandID string) (*data.EventRecord, uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var rec *data.EventRecord
	seq, found, err := s.cmdIndex.lookup(data.HashCommandID(commandID), func(seq uint64) (bool, error) {
		got, err := s.readBySeqLocked(seq)
		if err != nil || got == nil {
			return false, err
		}
		if got.CommandID != commandID {
			return false, nil
		}
		rec = got
		return true, nil
	})
	if err != nil || !found {
		return nil, 0, err
	}
	return rec, seq, nil
}

// AggregateVersion reads records of one aggregate starting at a version
// (1-based, contiguous) up to limit records or maxBytes decoded records,
// stopping at uptoSeq (exclusive; 0 = no cap). Records at seq > uptoSeq have
// not reached the high watermark and must not be observed.
func (s *Slot) AggregateVersion(aggregateID string, fromVersion uint32, limit, uptoSeq uint64) ([]*data.EventRecord, []uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e := s.aggs[aggregateID]
	if fromVersion == 0 {
		fromVersion = 1
	}
	if fromVersion > uint32(e.n) {
		return nil, nil, nil
	}
	var out []*data.EventRecord
	var outSeqs []uint64
	for v := fromVersion; v <= uint32(e.n); v++ {
		if limit > 0 && uint64(len(out)) >= limit {
			break
		}
		seq := s.seqAtLocked(e.off + int(v-1))
		if uptoSeq > 0 && seq > uptoSeq {
			break
		}
		rec, err := s.readBySeqLocked(seq)
		if err != nil {
			return out, outSeqs, err
		}
		if rec == nil {
			return out, outSeqs, fmt.Errorf("slot %d: index/seq %d desync", s.ID, seq)
		}
		out = append(out, rec)
		outSeqs = append(outSeqs, seq)
	}
	return out, outSeqs, nil
}

// LastVersionOf returns the highest stored version for an aggregate.
func (s *Slot) LastVersionOf(aggregateID string) uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return uint32(s.aggs[aggregateID].n)
}

// ReadRange returns byte ranges covering records fromSeq <= seq < untilSeq
// across segments (untilSeq 0 = to LEO). Used by replica fetch and migration.
func (s *Slot) ReadRange(fromSeq, untilSeq uint64, maxBytes int64) ([]data.ByteRange, uint64, error) {
	if untilSeq == 0 {
		// Lock-free empty fast path: past the atomic LEO there is nothing
		// to fetch, and steady followers ask for exactly that for every
		// idle slot every round (the fetch hot path's dominant call).
		if leo := s.seqCounter.Load(); fromSeq > leo {
			return nil, fromSeq, nil
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if untilSeq == 0 {
		untilSeq = s.seqCounter.Load() + 1
	}
	if fromSeq >= untilSeq {
		return nil, fromSeq, nil
	}
	var ranges []data.ByteRange
	var total int64
	next := fromSeq
	for _, seg := range s.segments {
		if seg.BaseSeq+uint64(seg.RecordCnt) <= fromSeq {
			continue
		}
		if seg.BaseSeq >= untilSeq {
			break
		}
		remain := maxBytes - total
		if remain <= 0 {
			break
		}
		rs, nx, err := seg.ReadRange(fromSeq, untilSeq, remain)
		if err != nil {
			return ranges, next, err
		}
		for _, r := range rs {
			remain -= r.End - r.Start
		}
		ranges = append(ranges, rs...)
		total = maxBytes - remain
		if nx > next {
			next = nx
			fromSeq = nx
		}
		if len(rs) == 0 || next >= untilSeq || total >= maxBytes {
			break
		}
	}
	return ranges, next, nil
}

// SealedSegments lists frozen segments (safe to copy wholesale for migration).
func (s *Slot) SealedSegments() []*Segment {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*Segment
	n := len(s.segments)
	for i, seg := range s.segments {
		if i == n-1 {
			continue // tail is the active segment
		}
		out = append(out, seg)
	}
	return out
}

// Flush fsyncs all segments.
func (s *Slot) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seg := range s.segments {
		if err := seg.Flush(); err != nil {
			return err
		}
	}
	s.pendingFlush = 0
	s.lastFlush = time.Now()
	return nil
}

// FlushDue fsyncs if the flush policy is satisfied. It reports (flushed,
// stillPending): a slot that holds unflushed records but has not reached its
// policy yet stays pending so the store keeps revisiting it.
func (s *Slot) FlushDue(now time.Time) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingFlush == 0 {
		return false, false
	}
	if s.flush.IntervalMessages > 0 && s.pendingFlush >= s.flush.IntervalMessages {
		_ = s.flushAll()
		return true, false
	}
	if s.flush.Interval > 0 && now.Sub(s.lastFlush) >= s.flush.Interval {
		_ = s.flushAll()
		return true, false
	}
	return false, true
}

func (s *Slot) flushAll() error {
	for _, seg := range s.segments {
		if err := seg.Flush(); err != nil {
			return err
		}
	}
	s.pendingFlush = 0
	s.lastFlush = time.Now()
	return nil
}

// TotalSize is the bytes occupied by the slot WAL.
func (s *Slot) TotalSize() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var n int64
	for _, seg := range s.segments {
		n += seg.SizeBytes()
	}
	return n
}

// SegmentCount is the number of WAL segments.
func (s *Slot) SegmentCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.segments)
}

// HasPending reports unflushed records (used by the store's dirty-set
// bookkeeping to avoid dropping a slot that just took a new append).
func (s *Slot) HasPending() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingFlush > 0
}

// SegmentFile returns the open segment whose file basename is name.
func (s *Slot) SegmentFile(name string) *Segment {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, seg := range s.segments {
		if filepath.Base(seg.Path) == name {
			return seg
		}
	}
	return nil
}

// Close flushes and releases the slot.
func (s *Slot) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for _, seg := range s.segments {
		if err := seg.Flush(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := seg.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Drop removes the slot's WAL files (source side after migration commit).
func (s *Slot) Drop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seg := range s.segments {
		seg.Close()
	}
	s.segments = nil
	return os.RemoveAll(s.Dir)
}
