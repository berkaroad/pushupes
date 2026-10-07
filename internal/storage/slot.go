package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"pushupes/internal/data"
)

// ErrSeqDivergence marks a replay of a seq that the local log already holds
// with DIFFERENT bytes — a real fork of the log, not an idempotent replay.
// It is a typed sentinel so callers that move replicated data (the replica
// fetch loop) can classify it: a single slot's fork must not be mistaken for a
// transport failure and must not fail the whole multiplexed fetch session.
var ErrSeqDivergence = errors.New("diverged on replay")

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
//	blms:     command_id hash bloom filter              (rule 1, idempotency)
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

	aggs map[string]aggEntry // aggregate directory (see slotaggs.go)
	blms *bloomSet
	// blmsMark is where the newest segment's filters start in blms.filters;
	// blmsSkipInsert suppresses inserts while a segment whose filter comes off
	// disk has its metadata applied.
	blmsMark       int
	blmsSkipInsert bool
	// blmsWatermark is the highest seq already placed into blms. Indexing runs
	// in ascending seq order (writes append at the newest seq, recovery walks
	// segments and their frames in order), so anything at or below it is
	// already in the filter and must not be placed again — recovery replays a
	// whole WAL on every start, and re-placing costs real bytes-per-entry.
	blmsWatermark uint64

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
// slotOpenMode says what opening a slot may do to its index files. Repairing
// an index is startup work: a running node records what happens, it does not
// re-index history — that belongs to the next start.
type slotOpenMode int

const (
	slotOpenLoad slotOpenMode = iota
	slotOpenLoadAndRepair
)

// OpenSlot opens a slot for a running node: an existing index is loaded and
// continued, a missing one is left alone.
func OpenSlot(dir string, slotID int32, segmentBytes int64, flush FlushPolicy) (*Slot, error) {
	return openSlot(dir, slotID, segmentBytes, flush, slotOpenLoad)
}

// OpenSlotRepairing opens a slot at process startup, where a segment without a
// usable index is walked and then indexed. That is what makes later starts
// cheap without re-indexing while the node serves traffic.
func OpenSlotRepairing(dir string, slotID int32, segmentBytes int64, flush FlushPolicy) (*Slot, error) {
	return openSlot(dir, slotID, segmentBytes, flush, slotOpenLoadAndRepair)
}

func openSlot(dir string, slotID int32, segmentBytes int64, flush FlushPolicy, mode slotOpenMode) (*Slot, error) {
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
		blms:         &bloomSet{},
	}
	s.cond = sync.NewCond(&s.mu)
	w := make(chan struct{})
	s.wake.Store(&w)
	paths, err := SegmentFilesOf(dir)
	if err != nil {
		return nil, err
	}
	var expected uint64 = 1
	for i, p := range paths {
		isTail := i == len(paths)-1
		// Recovery order of preference: the segment's index, then a walk.
		seg, load, err := LoadSegmentIndexed(p, dir)
		if err != nil {
			return nil, fmt.Errorf("slot %d load %s: %w", slotID, p, err)
		}
		if seg == nil {
			segIndexUnusable.Add(1)
			if seg, err = LoadSegment(p); err != nil {
				return nil, fmt.Errorf("slot %d load %s: %w", slotID, p, err)
			}
		}
		if seg.SlotID != slotID {
			seg.Close()
			return nil, fmt.Errorf("slot %d: %s has header slot %d", slotID, p, seg.SlotID)
		}
		if seg.BaseSeq != expected {
			seg.Close()
			return nil, fmt.Errorf("slot %d: %s seq gap (base %d, expected %d)", slotID, p, seg.BaseSeq, expected)
		}
		s.blmsMark = len(s.blms.filters)
		var segIdx *segCmdIndex
		if !isTail && seg.RecordCnt > 0 {
			// A sealed segment's command bloom either comes off its index file
			// or is rebuilt at startup from the records indexed just below.
			ix, ierr := openSegCmdIndex(s.Dir, s.ID, seg.BaseSeq)
			if ierr != nil {
				seg.Close()
				return nil, fmt.Errorf("slot %d command index %s: %w", slotID, p, ierr)
			}
			if ix != nil && ix.Bloom() != nil && ix.Valid() {
				segIdx = ix
				s.blmsSkipInsert = true
			} else if ix != nil {
				ix.Close()
			}
		}
		if seg.RecordCnt > 0 {
			from := seg.BaseSeq
			if load != nil {
				// The index holds the metadata for these records; no frame is
				// touched for them.
				// One entry at a time out of the loader's buffer: the index of a
				// 262k record segment is never materialized, so a start does not
				// hold 40 bytes per record per segment.
				for i := 0; i < load.Count(); i++ {
					e, ok := load.Entry(i)
					if !ok {
						break // openSegIndex validated every entry first
					}
					s.indexMetaLocked(e.seq, data.RecordMeta{
						AggregateID: e.aggID,
						Version:     e.version,
						CommandHash: e.hash,
					})
				}
				segIndexUsed.Add(1)
				from = seg.BaseSeq + uint64(load.Count())
				load.Release()
			}
			// Startup indexes every segment (that is the repair); while running,
			// only a tail that already has an index keeps writing it, and a
			// missing one is left to the next start.
			if mode == slotOpenLoadAndRepair || (isTail && load != nil) {
				_ = seg.openIndexWriter()
			}
			// Everything past the index coverage is replayed from the frames
			// (usually nothing, at most one index block).
			if from <= seg.LastSeq {
				if err := seg.ScanHeaders(from, func(seq uint64, meta data.RecordMeta) bool {
					s.indexMetaLocked(seq, meta)
					seg.IndexRecord(seq, meta)
					return true
				}); err != nil {
					seg.Close()
					return nil, fmt.Errorf("slot %d scan %s: %w", slotID, p, err)
				}
				segIndexReplayed.Add(seg.LastSeq - from + 1)
			}
			if load == nil && mode == slotOpenLoadAndRepair && seg.idx != nil {
				// Startup: leave the walked segment with a complete index
				// (sparse positions come from the walk, entries from the scan
				// above), so the next start does not walk it again.
				seg.indexSparseFromMemory()
				if err := seg.idx.flush(); err != nil {
					seg.Close()
					return nil, fmt.Errorf("slot %d index %s: %w", slotID, p, err)
				}
				segIndexBuilt.Add(1)
			}
			expected = seg.LastSeq + 1
		}
		switch {
		case segIdx != nil:
			s.blms.filters = append(s.blms.filters, segIdx.Bloom().filters...)
			segIdx.Close()
			s.blmsSkipInsert = false
			// The segment's whole bloom just came off disk, so every seq it
			// covers is already placed. Advance the watermark past them or the
			// index replay below would place each hash a second time.
			if seg.RecordCnt > 0 && seg.LastSeq > s.blmsWatermark {
				s.blmsWatermark = seg.LastSeq
			}
		case !isTail && seg.RecordCnt > 0:
			// No usable bloom for a sealed segment. Rebuilding it needs the
			// records just indexed, which is startup work; running, the set is
			// marked incomplete so it never claims a command is absent.
			s.blmsSkipInsert = false
			if mode == slotOpenLoadAndRepair {
				if err := writeSegCmdIndex(seg, s.blms.filters[s.blmsMark:]); err != nil {
					segIndexUnusable.Add(1)
					s.blms.unsafe = true
				} else {
					segCmdBuilt.Add(1)
				}
			} else {
				segIndexUnusable.Add(1)
				s.blms.unsafe = true
			}
		default:
			s.blmsSkipInsert = false
		}
		if mode == slotOpenLoadAndRepair || (isTail && load != nil) {
			// An empty segment (a header-only file from an earlier start) has
			// nothing to index, but it does need the index files so records
			// appended to it later extend them instead of being replayed by the
			// next start.
			_ = seg.openIndexWriter()
		}
		seg.Seal() // history segments sealed; the tail is unsealed below
		if !isTail && mode == slotOpenLoadAndRepair {
			// Sealed history is never appended to again: its index is complete,
			// so give back the handle.
			_ = seg.indexFlushAndClose()
			// Move the segment's per-aggregate seqs out of memory. The index
			// that keeps them readable is written first (a start is where
			// indexes are written; running, the seqs stay in memory instead).
			if seg.RecordCnt > 0 {
				need := true
				var covered map[uint64]bool
				if ix, err := openSegAggIndex(s.Dir, s.ID, seg.BaseSeq); err == nil && ix != nil {
					if ix.Valid() {
						need = false
						covered = ix.Covered()
					}
					ix.Close()
				}
				if need {
					if err := s.sealAggSeqsLocked(seg); err != nil {
						segIndexUnusable.Add(1)
					} else {
						segAggBuilt.Add(1)
					}
				} else {
					// The index is there: the seqs IT covers need not stay in
					// memory just because loading the segment rebuilt them.
					// Aggregates the file does not list keep theirs.
					s.dropSealedSeqsLocked(seg, covered)
				}
			}
		}
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
//
// The two numbers in the entry — version (what the aggregate claims its tail
// is) and sealedN+n (the seqs it can actually resolve) — are the pair every read
// path requires to be equal: AggregateVersion and TailVersion both refuse to
// answer when they disagree, so a split makes the aggregate permanently
// unreadable on this node (see deinv_test.go).
//
// They must therefore MOVE TOGETHER, and exactly one condition moves them: the
// record is the stream's NEXT version. That is the only case in which the
// directory may both claim a version and count a seq.
//
// Every other version leaves the directory untouched, for two different reasons:
//
//   - a version BEYOND the next one: the record itself is still written to the
//     WAL by the caller, but the directory must not claim a version it cannot
//     resolve. Replicas legitimately receive such a frame — the migration fence
//     pushes from a target-reported LEO, and the target's view of its own log can
//     be ahead of the aggregate streams it holds — and a replica knows it is
//     behind without being able to know what it is missing. Claiming the version
//     strands every read of that aggregate; withholding it keeps the aggregate
//     readable at the last version it can actually serve, and the gap closes
//     when the missing records arrive.
//
//   - a version we ALREADY cover: a replay. Its seq is already recorded, so
//     counting it again would inflate the pair from the other side. No caller
//     passes an older seq for an aggregate that has a hole — writes append at
//     the slot's newest seq and recovery walks seqs in ascending order — so
//     there is nothing to backfill here.
//
// An earlier version of this function raised version unconditionally while
// recording the seq only for contiguous ones, which is exactly how a live
// cluster ended up with an aggregate at version 104 with 59 resolvable seqs.
// The asymmetry is what made it possible: version was driven by "any record
// with a higher number", the seq count by "the contiguous one" — two scales
// that could only ever disagree by accident of which write path ran.
func (s *Slot) indexMetaLocked(seq uint64, m data.RecordMeta) {
	e := s.aggs[m.AggregateID]
	if !s.blmsSkipInsert {
		// A seq at or below the watermark was already indexed (the recovery
		// paths replay a whole WAL through here, and every caller walks seqs
		// in ascending order), so its hash is already in the filter. Asking
		// the filter itself cannot answer this: a bloom "maybe" is also true
		// for colliding hashes, and skipping on it would drop a stored command
		// from the filter for good. The watermark is exact.
		s.blms.insertPlaced(m.CommandHash, seq <= s.blmsWatermark)
		if seq > s.blmsWatermark {
			s.blmsWatermark = seq
		}
	}
	if m.Version == uint32(e.sealedN+e.n)+1 {
		e.version = m.Version
		e.appendSeq(seq)
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
		// The sealed segment's index is complete: make it durable and stop.
		if err := seg.indexFlushAndClose(); err != nil {
			return nil, err
		}
		// Sort its commands by hash into <baseSeq>.cidx, the on-disk side of
		// the command lookup. Best effort: a segment without it is still
		// correct, and the next start writes it.
		if err := writeSegCmdIndex(seg, s.blms.filters[s.blmsMark:]); err != nil {
			segIndexUnusable.Add(1)
		}
		s.blmsMark = len(s.blms.filters)
		// Its per-aggregate seqs move to disk: sealed seqs never change again,
		// and they are what the in-memory arena would otherwise keep forever.
		if err := s.sealAggSeqsLocked(seg); err != nil {
			segIndexUnusable.Add(1) // seqs stay in memory: correct, just heavier
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

	// Rule 1: idempotency by command_id within the slot. The bloom filter
	// answers "definitely not stored" without touching anything; only a "maybe"
	// costs a lookup, and a candidate is always confirmed against the record it
	// points at — the record the EXISTS answer returns anyway.
	hash := data.HashCommandID(rec.CommandID)
	if s.blms.maybe(hash) {
		stored, seq, found, err := s.lookupCommandLocked(rec.CommandID, hash)
		if err != nil {
			return nil, err
		}
		if found {
			return &AppendOutcome{Status: data.StatusExists, Seq: seq, Record: stored}, nil
		}
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
	seq := s.seqCounter.Load() + 1
	if err := seg.AppendRecord(seq, rec); err != nil {
		return nil, err
	}
	s.seqCounter.Store(seq)
	s.indexRecordLocked(seq, rec)
	seg.IndexRecord(seq, data.RecordMeta{
		AggregateID: rec.AggregateID,
		Version:     rec.Version,
		CommandHash: data.HashCommandID(rec.CommandID),
	})
	s.noteAppendPending()
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
			return false, fmt.Errorf("slot %d: seq %d %w", s.ID, seq, ErrSeqDivergence)
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
	seg.IndexRecord(seq, data.RecordMeta{
		AggregateID: rec.AggregateID,
		Version:     rec.Version,
		CommandHash: data.HashCommandID(rec.CommandID),
	})
	s.noteAppendPending()
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
			return false, fmt.Errorf("slot %d: seq %d %w", s.ID, seq, ErrSeqDivergence)
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
	seg.IndexRecord(seq, meta)
	s.noteAppendPending()
	s.advanceNotifyLocked()
	return true, nil
}

// aggHash hashes an aggregate id for the segment aggregate index. It is the
// same FNV-1a string hash the command ids use: both are only ever used as a
// bucket selector whose candidates are confirmed against a stored record.
func aggHash(aggregateID string) uint64 { return data.HashCommandID(aggregateID) }

// sealAggSeqsLocked writes the sealed segment's part of every aggregate's seq
// list to <baseSeq>.aidx and then drops exactly the seqs that index now covers.
// An all-or-nothing move per aggregate: an aggregate whose seqs could not be
// written (or whose earliest live seq lies in an EARLIER segment that has no
// index) keeps them in memory — it costs memory, but dropping them would make
// those versions unreadable.
func (s *Slot) sealAggSeqsLocked(seg *Segment) error {
	covered, err := s.writeSegAggSeqsLocked(seg)
	if err != nil {
		return err
	}
	s.dropSealedSeqsLocked(seg, covered)
	return nil
}

// writeSegAggSeqsLocked writes the sealed segment's part of every aggregate's
// seq list to <baseSeq>.aidx, so that nothing is dropped before it is on disk.
// It returns the set of aggregate hashes the index now covers: those, and only
// those, may leave memory. An aggregate whose oldest live seq predates this
// segment is skipped — the .aidx resolves a version by the aggregate's FIRST
// version, so a segment can only cover an aggregate whose coverage is already a
// contiguous prefix; the earlier segment's index has to exist first (a start
// writes it). Caller holds s.mu.
func (s *Slot) writeSegAggSeqsLocked(seg *Segment) (map[uint64]bool, error) {
	if seg.RecordCnt == 0 {
		return nil, nil
	}
	// Each record's byte offset inside the segment, addressed by its ordinal in
	// it: one frame walk over the sealed segment (the same walk the command index
	// needs) turns an aggregate's seqs into offsets, so a sealed read is one
	// pread instead of a walk from the nearest sparse hint — which at 1 MiB
	// granularity is about a thousand frames, and cost a millisecond per record.
	ents, err := collectSegCmdEntries(seg)
	if err != nil || len(ents) == 0 {
		return nil, errSegAggNoOffsets // seqs stay in memory; the next start retries
	}
	offs := make([]uint32, seg.RecordCnt)
	for _, e := range ents {
		if ord := e.seq - seg.BaseSeq; int64(ord) < seg.RecordCnt {
			offs[ord] = uint32(e.off)
		}
	}
	var lists []segAggList
	covered := map[uint64]bool{}
	for id, e := range s.aggs {
		if e.n == 0 {
			continue
		}
		// The aggregate's list is ascending, so the sealed prefix is a binary
		// search, and seq-baseSeq is the record's ordinal in the segment.
		k := sort.Search(e.n, func(i int) bool { return e.seqAt(i) > seg.LastSeq })
		if k == 0 {
			continue
		}
		first := e.seqAt(0)
		if first < seg.BaseSeq {
			continue
		}
		pairs := make([]uint32, 0, k*2)
		ok := true
		for i := 0; i < k; i++ {
			ord := e.seqAt(i) - seg.BaseSeq
			if int64(ord) >= seg.RecordCnt || offs[ord] == 0 {
				ok = false
				break
			}
			pairs = append(pairs, uint32(ord), offs[ord])
		}
		if !ok {
			continue
		}
		lists = append(lists, segAggList{
			hash:     aggHash(id),
			firstVer: uint32(e.sealedN + 1),
			firstOrd: uint32(first - seg.BaseSeq),
			pairs:    pairs,
		})
		covered[aggHash(id)] = true
	}
	if len(lists) == 0 {
		return covered, nil
	}
	if seg.aggIx != nil { // the file is about to be rewritten
		_ = seg.aggIx.Close()
		seg.aggIx = nil
	}
	return covered, writeSegAggIndex(seg, lists)
}

// dropSealedSeqsLocked removes from the arena exactly the seqs the sealed
// segment's aggregate index now covers: an aggregate absent from `covered` was
// not written (its earliest live seq belongs to an earlier segment with no
// index yet) and must keep its seqs in memory, or those versions would resolve
// to nothing. It runs at seal and again at every start: a restart must not
// rebuild in memory what the disk already holds.
func (s *Slot) dropSealedSeqsLocked(seg *Segment, covered map[uint64]bool) {
	if len(covered) == 0 {
		return
	}
	for id, e := range s.aggs {
		if e.n == 0 || !covered[aggHash(id)] {
			continue
		}
		k := sort.Search(e.n, func(i int) bool { return e.seqAt(i) > seg.LastSeq })
		if k == 0 {
			continue
		}
		e.dropFirst(k)
		e.sealedN += k
		s.aggs[id] = e
	}
}

// sealedVersionSeqLocked resolves version v of an aggregate that lives in a
// sealed segment: newest segment first, its aggregate index locates the entry
// covering v, and a candidate is confirmed against the record it points at.
// Caller holds s.mu.
func (s *Slot) sealedVersionSeqLocked(aggregateID string, v uint32) (uint64, *data.EventRecord, error) {
	var buf [4 << 10]byte
	hash := aggHash(aggregateID)
	for i := len(s.segments) - 1; i >= 0; i-- {
		seg := s.segments[i]
		if seg.RecordCnt == 0 {
			continue
		}
		ix := seg.aggIx
		if ix == nil {
			opened, err := openSegAggIndex(s.Dir, s.ID, seg.BaseSeq)
			if err != nil {
				return 0, nil, err
			}
			if opened == nil {
				continue // no index for this segment: nothing to search
			}
			ix, seg.aggIx = opened, opened
		}
		var seq uint64
		var hit *data.EventRecord
		found := ix.Lookup(hash, func(e segAggEntry) bool {
			if v < e.firstVer || v >= e.firstVer+e.count {
				return false
			}
			ord, off, ok := ix.Pair(e, int(v-e.firstVer))
			if !ok {
				return false
			}
			rec, _, rerr := readRecordAt(seg.File, int64(off), buf[:])
			if rerr != nil || rec.AggregateID != aggregateID || rec.Version != v {
				return false
			}
			seq, hit = seg.BaseSeq+uint64(ord), &rec
			return true
		})
		if found {
			return seq, hit, nil
		}
	}
	return 0, nil, errAggSeqNotFound
}

var errAggSeqNotFound = errors.New("aggregate version not found in the sealed indexes")

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
	hash := data.HashCommandID(commandID)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.blms.maybe(hash) {
		return nil, 0, nil // definitely not stored
	}
	rec, seq, found, err := s.lookupCommandLocked(commandID, hash)
	if err != nil || !found {
		return nil, 0, err
	}
	return rec, seq, nil
}

// lookupCommandLocked finds a stored command: the sealed segments' sorted
// command indexes (newest first), then the writable segment, whose own index is
// still in write order and therefore scanned. Every candidate is confirmed
// against the record it points at, so a hash collision or a damaged index can
// only cost time. Caller holds s.mu.
func (s *Slot) lookupCommandLocked(commandID string, hash uint64) (*data.EventRecord, uint64, bool, error) {
	if commandID == "" {
		return nil, 0, false, nil
	}
	var buf [4 << 10]byte
	for i := len(s.segments) - 1; i >= 0; i-- {
		seg := s.segments[i]
		if seg.RecordCnt == 0 {
			continue
		}
		if seg.Writable() {
			rec, seq, err := s.lookupTailCommandLocked(seg, commandID, hash, buf[:])
			if err != nil || rec != nil {
				return rec, seq, rec != nil, err
			}
			continue
		}
		ix, err := openSegCmdIndex(s.Dir, s.ID, seg.BaseSeq)
		if err != nil {
			return nil, 0, false, err
		}
		if ix == nil {
			continue // no index for this segment: nothing to search
		}
		var hit *data.EventRecord
		var hitSeq uint64
		found, err := ix.LookupCmd(uint32(hash>>32), func(e segCmdEntry) bool {
			got, _, rerr := readRecordAt(seg.File, e.off, buf[:])
			if rerr != nil || got.CommandID != commandID {
				return false
			}
			r := got
			hit, hitSeq = &r, e.seq
			return true
		})
		ix.Close()
		if err != nil {
			return nil, 0, false, err
		}
		if found {
			return hit, hitSeq, true, nil
		}
	}
	return nil, 0, false, nil
}

// lookupTailCommandLocked searches the writable segment, whose command index is
// still in write order: it flushes whatever the writer buffers, scans that
// segment's entries and confirms each candidate against the record at its seq.
// Caller holds s.mu.
func (s *Slot) lookupTailCommandLocked(seg *Segment, commandID string, hash uint64, buf []byte) (*data.EventRecord, uint64, error) {
	if seg.idx != nil {
		seg.idx.flushBlock() // entries still buffered are not on disk yet
	}
	var hit *data.EventRecord
	var hitSeq uint64
	sawEntries := false
	err := scanIdxEntries(segIdxPath(s.Dir, seg.BaseSeq), segIdxMagic, s.ID, seg.BaseSeq, segIdxEntryBytes, func(raw []byte) bool {
		sawEntries = true
		if binary.BigEndian.Uint64(raw[0:8]) != hash {
			return true // keep scanning
		}
		seq := seg.BaseSeq + uint64(binary.BigEndian.Uint32(raw[8:12]))
		got, err := s.readBySeqLocked(seq)
		if err != nil || got == nil || got.CommandID != commandID {
			return true
		}
		hit, hitSeq = got, seq
		return false
	})
	if err != nil {
		return nil, 0, err
	}
	if hit != nil || sawEntries || seg.idx != nil {
		return hit, hitSeq, nil
	}
	// The writable segment has records but no index file at all (deleted, or
	// data written before the index existed). Walking its frame headers keeps
	// the answer correct; a start writes the missing index and the walk stops
	// being needed.
	segIndexUnusable.Add(1)
	err = seg.ScanHeaders(seg.BaseSeq, func(seq uint64, meta data.RecordMeta) bool {
		if meta.CommandHash != hash {
			return true
		}
		got, rerr := s.readBySeqLocked(seq)
		if rerr != nil || got == nil || got.CommandID != commandID {
			return true
		}
		hit, hitSeq = got, seq
		return false
	})
	if err != nil {
		return nil, 0, err
	}
	return hit, hitSeq, nil
}

// Wake returns the current wake handle pointer. Lock-free read; the handle
// is swapped under s.mu on every advance (advanceNotifyLocked).
func (s *Slot) AggregateVersion(aggregateID string, fromVersion uint32, limit, uptoSeq uint64) ([]*data.EventRecord, []uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e := s.aggs[aggregateID]
	if fromVersion == 0 {
		fromVersion = 1
	}
	total := uint32(e.sealedN) + uint32(e.n)
	// The directory's claimed latest version and the number of seqs this slot
	// can actually resolve must agree. When they do not, returning "as many as
	// we have" would be a SILENT short read — the caller would see a truncated
	// stream with no error and believe it complete. Fail loudly instead: the
	// sealed index and the in-memory seq list disagree, which is a bug, not an
	// empty range.
	if e.version != total {
		return nil, nil, fmt.Errorf("slot %d: aggregate %s desync: latest version %d but only %d versions resolvable",
			s.ID, aggregateID, e.version, total)
	}
	if fromVersion > total {
		return nil, nil, nil
	}
	// Versions up to sealedN live in the sealed segments' aggregate indexes, the
	// rest in the arena. Reading the newest versions — the hot path — never
	// touches the disk either way.
	liveFirst := uint32(e.sealedN) + 1
	var out []*data.EventRecord
	var outSeqs []uint64
	for v := fromVersion; v <= total; v++ {
		if limit > 0 && uint64(len(out)) >= limit {
			break
		}
		var (
			seq uint64
			rec *data.EventRecord
		)
		if v >= liveFirst {
			seq = e.seqAt(int(v - liveFirst))
		} else {
			got, gotRec, err := s.sealedVersionSeqLocked(aggregateID, v)
			if err != nil {
				return out, outSeqs, err
			}
			seq, rec = got, gotRec
		}
		if uptoSeq > 0 && seq > uptoSeq {
			break
		}
		var err error
		if rec == nil {
			rec, err = s.readBySeqLocked(seq)
			if err != nil {
				return out, outSeqs, err
			}
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
	e := s.aggs[aggregateID]
	return uint32(e.sealedN + e.n)
}

// TailVersion returns the highest version of an aggregate that is visible at
// uptoSeq (0 = the slot's durable LEO) — the last_version a ReadStream bounded
// by the same seq would report for that stream, and 0 when nothing is visible.
//
// It walks versions upward exactly like AggregateVersion and stops at the first
// version whose seq exceeds the bound, so a partially visible tail reports the
// version a bounded read would actually return instead of the directory's
// unclipped count. The desync check matches AggregateVersion: a directory whose
// claimed latest version disagrees with the number of resolvable seqs is a bug,
// and answering a tail from it would be a silent mis-report.
func (s *Slot) TailVersion(aggregateID string, uptoSeq uint64) (uint32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e := s.aggs[aggregateID]
	total := uint32(e.sealedN) + uint32(e.n)
	// Same desync gate as AggregateVersion: a directory whose claimed latest
	// version disagrees with the number of resolvable seqs is a bug, and
	// answering a tail from it would be a silent mis-report.
	if e.version != total {
		return 0, fmt.Errorf("slot %d: aggregate %s desync: latest version %d but only %d versions resolvable",
			s.ID, aggregateID, e.version, total)
	}
	if total == 0 {
		return 0, nil
	}
	if uptoSeq == 0 {
		// 0 means "no bound": the caller wants the durable LEO, which the
		// slot tracks itself. The seqs below are all <= LEO by construction.
		return total, nil
	}
	// Walk down from the tail rather than up from 1: a resume probe asks
	// about streams with long histories, and the answer is almost always
	// "the whole thing is visible".
	liveFirst := uint32(e.sealedN) + 1
	for v := total; v >= 1; v-- {
		var seq uint64
		if v >= liveFirst {
			seq = e.seqAt(int(v - liveFirst))
		} else {
			got, _, err := s.sealedVersionSeqLocked(aggregateID, v)
			if err != nil {
				return 0, err
			}
			seq = got
		}
		if seq <= uptoSeq {
			return v, nil
		}
	}
	return 0, nil
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
	if err := s.flushSegments(); err != nil {
		return err
	}
	s.markFlushedLocked()
	return nil
}

// markFlushedLocked records that the slot holds nothing unflushed: the pending
// counter resets and the store drops the slot's dirty mark, so the flush-stats
// view cannot show a slot as waiting after its records are durable.
// Caller holds s.mu for writing.
func (s *Slot) markFlushedLocked() {
	s.pendingFlush = 0
	s.lastFlush = time.Now()
	if s.store != nil {
		s.store.noteFlushed(s.ID)
	}
}

// flushSegments fsyncs every segment of this slot and books the cost. The
// caller holds s.mu for writing, so this fsync blocks the slot's appends: that
// is how a slow disk reaches writers, and why the cost is recorded.
func (s *Slot) flushSegments() error {
	t0 := time.Now()
	for _, seg := range s.segments {
		if err := seg.Flush(); err != nil {
			return err
		}
	}
	if s.store != nil {
		s.store.recordFsync(time.Since(t0))
	}
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
	if err := s.flushSegments(); err != nil {
		return err
	}
	s.markFlushedLocked()
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

// noteAppendPending records one more unflushed append. The caller holds s.mu
// for writing. Crossing the record-count threshold kicks the store's flush
// loop: for a count-only policy the NEXT append — never the clock — closes the
// policy, and the store arms no timer for such a slot (see DueAt), so this
// kick is the slot's only path to its fsync.
func (s *Slot) noteAppendPending() {
	s.pendingFlush++
	if s.flush.IntervalMessages > 0 && s.pendingFlush == s.flush.IntervalMessages && s.store != nil {
		s.store.kickFlush()
	}
}

// DueAt reports when the flush policy's INTERVAL clause next demands an fsync:
// the zero time when nothing is pending, no interval is configured, or the
// slot's policy is purely record-counted (the next append — not the clock —
// closes that one, so the store arms no timer for it and waits for the append
// to kick the sweep).
func (s *Slot) DueAt(now time.Time) time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.pendingFlush == 0 || s.flush.Interval <= 0 {
		return time.Time{}
	}
	d := s.lastFlush.Add(s.flush.Interval)
	if d.Before(now) {
		return now
	}
	return d
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

// SlotDigest summarizes a slot's aggregate directory for a consistency check:
// how many aggregates it holds, the sum of their claimed latest versions, and
// the sum of the seqs the directory can actually resolve (DESIGN.md §1.1 rule 3).
//
// The two sums are the point. A node can sit at a high LEO while one aggregate's
// stream is short — records appended above it never entered the directory — so
// comparing LEOs alone says "in sync" about a node that cannot serve everything
// it claims. When versions != resolvable, some claimed version has no seq behind
// it and the slot is structurally inconsistent.
//
// An unloaded slot reports zeroes: it holds nothing to be inconsistent about.
func (s *Slot) SlotDigest() (aggregates, versions, resolvable uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, e := range s.aggs {
		aggregates++
		versions += uint64(e.version)
		resolvable += uint64(e.sealedN) + uint64(e.n)
	}
	return aggregates, versions, resolvable
}
