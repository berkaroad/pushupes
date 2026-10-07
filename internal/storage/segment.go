// Package storage implements the append-only slot WAL: each of the fixed
// slots owns a sequence of segment files (rolled at a size threshold), with
// in-memory sparse indexes rebuilt by scanning the log at startup.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"pushupes/internal/data"
)

// Segment file format:
//
//	Header (20 bytes): magic "ESWL"(4) fmtVer(1) reserved(3) slotID(4be) baseSeq(8be)
//	Body:  recLen(4be) record*   (see data.EventRecord encoding)
//
// A record at byte offset off has slot seq = seg.BaseSeq + (records before it).
// Segments are contiguous: the next segment's baseSeq is this segment's last
// seq + 1, which is what lets scans track seq incrementally.
const (
	WALMagic     = "ESWL"
	WALFormatVer = int8(1)
	WALHeaderLen = 20
	// indexIntervalB is how sparsely seq -> file offset is remembered in memory
	// and in the segment's .spx file. It is a seek hint: a lookup lands at the
	// nearest earlier entry and walks frames forward, so the interval bounds
	// how many frames a seek decodes — including the replica-fetch read path,
	// which seeks from the sparse hint to the slot's tail on every round.
	//
	// One entry costs 16 B, so the interval sets what the table holds: one
	// entry per interval of data, i.e. 16 B per 64 KiB, which is 0.25 B per
	// 1 KiB record and ~253 MiB on a 1 TiB node. A coarser interval trades
	// that memory back for a longer walk per seek.
	indexIntervalB = int64(64 << 10) // one entry per 64KiB
)

// DefaultSegmentBytes is the 256MiB roll threshold from the design.
const DefaultSegmentBytes = int64(256 * 1024 * 1024)

// A configured segment size must be a whole number of SegmentBytesMultiple
// bytes, between MinSegmentBytes and MaxSegmentBytes. The floor exists because
// a smaller segment buys nothing — per-slot state, index files and file
// descriptors all scale with the segment count, while the memory a segment
// holds is bounded by its size either way. The ceiling exists because one
// segment's index files, its seal-time frame walk and the per-aggregate seq
// list it keeps in memory are all proportional to it.
const (
	SegmentBytesMultiple = int64(64 << 20)
	MinSegmentBytes      = SegmentBytesMultiple
	MaxSegmentBytes      = int64(2 << 30)
)

// ValidateSegmentBytes reports whether n is a usable segment size. The service
// entry point applies it; the storage layer itself takes whatever it is given,
// so tests can run on tiny segments.
func ValidateSegmentBytes(n int64) error {
	const mib = int64(1 << 20)
	if n < MinSegmentBytes {
		return fmt.Errorf("segment-bytes %d (%d MiB) is below the %d MiB minimum", n, n/mib, MinSegmentBytes/mib)
	}
	if n > MaxSegmentBytes {
		return fmt.Errorf("segment-bytes %d (%d MiB) is above the %d MiB maximum", n, n/mib, MaxSegmentBytes/mib)
	}
	if n%SegmentBytesMultiple != 0 {
		lo := n / SegmentBytesMultiple * SegmentBytesMultiple
		hi := lo + SegmentBytesMultiple
		return fmt.Errorf("segment-bytes %d (%d MiB) is not a multiple of %d MiB (nearest valid: %d MiB or %d MiB)",
			n, n/mib, SegmentBytesMultiple/mib, lo/mib, hi/mib)
	}
	return nil
}

// indexEntry maps a seq to its byte position inside one segment.
type indexEntry struct {
	seq uint64
	pos int64
}

// Segment is one append-only WAL segment file of a slot.
type Segment struct {
	SlotID    int32
	BaseSeq   uint64
	Path      string
	File      *os.File
	dataStart int64
	sizeBytes int64
	RecordCnt int64
	LastSeq   uint64 // seq of the last record (valid when RecordCnt > 0)

	index           []indexEntry
	bytesSinceIndex int64
	writable        bool

	unflushed     int64
	lastFlushTime time.Time
	// syncedSize is the file size covered by the last completed fsync:
	// sizeBytes > syncedSize is exactly "this segment owes an fsync".
	syncedSize int64

	scatterBuf   []byte   // scratch for EncodeScatter headers
	scatterParts [][]byte // reusable segment list handed to pwritev

	idx *segIndexWriter // index files (see segidx.go); nil when none

	// aggIx caches this segment's opened aggregate index (segaidx.go). Without
	// it a versioned read of sealed history re-opens and re-reads the directory
	// once per version, which is the difference between ~0.1ms and ~1.5ms.
	// Guarded by the slot's mutex, like every other segment field.
	aggIx *segAggIndex
}

func headerBuf(slotID int32, baseSeq uint64) []byte {
	h := make([]byte, WALHeaderLen)
	copy(h[0:4], WALMagic)
	h[4] = byte(WALFormatVer)
	binary.BigEndian.PutUint32(h[8:12], uint32(slotID))
	binary.BigEndian.PutUint64(h[12:20], baseSeq)
	return h
}

func parseHeader(h []byte) (int32, uint64, error) {
	if len(h) < WALHeaderLen {
		return 0, 0, errors.New("short header")
	}
	if string(h[0:4]) != WALMagic || h[4] != byte(WALFormatVer) {
		return 0, 0, errors.New("bad WAL header magic/version")
	}
	slot := int32(binary.BigEndian.Uint32(h[8:12]))
	base := binary.BigEndian.Uint64(h[12:20])
	return slot, base, nil
}

// HeaderSlotID validates a WAL segment header and returns its slot id.
// Used by the migration target before accepting a streamed segment file.
func HeaderSlotID(payload []byte) (int32, error) {
	slot, _, err := parseHeader(payload)
	return slot, err
}

// CreateSegment makes a new writable segment file named by its base seq.
func CreateSegment(dir string, slotID int32, baseSeq uint64) (*Segment, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, segmentName(baseSeq))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if _, err := f.Write(headerBuf(slotID, baseSeq)); err != nil {
		f.Close()
		return nil, err
	}
	seg := &Segment{
		SlotID:        slotID,
		BaseSeq:       baseSeq,
		Path:          path,
		File:          f,
		dataStart:     WALHeaderLen,
		sizeBytes:     WALHeaderLen,
		syncedSize:    WALHeaderLen,
		writable:      true,
		lastFlushTime: time.Now(),
	}
	// Best effort: a segment without an index is still a correct segment, the
	// next startup writes the index after walking it.
	_ = seg.openIndexWriter()
	return seg, nil
}

func segmentName(baseSeq uint64) string {
	return fmt.Sprintf("%016d.wal", baseSeq)
}

// OpenSegment opens an existing segment file for read/write.
func OpenSegment(path string) (*Segment, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	h := make([]byte, WALHeaderLen)
	if _, err := io.ReadFull(f, h); err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	slot, base, err := parseHeader(h)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Segment{
		SlotID:    slot,
		BaseSeq:   base,
		Path:      path,
		File:      f,
		dataStart: WALHeaderLen,
		sizeBytes: st.Size(),
	}, nil
}

// Seal freezes the segment so no further appends land in it.
func (s *Segment) Seal() { s.writable = false }

// Writable reports whether the segment accepts appends.
func (s *Segment) Writable() bool { return s.writable }

// SizeBytes is the current file size including the header.
func (s *Segment) SizeBytes() int64 { return s.sizeBytes }

// Encode-buffer pool: the leader-side append path needs one contiguous frame
// buffer per record, which for large event bodies is a large allocation each
// time. Buckets are powers of two from 8 KiB to 2 MiB; anything bigger falls
// back to a plain allocation.
const (
	poolClassShift = 13 // 8 KiB minimum class
	poolClassMax   = 21 // 2 MiB maximum class
	poolClassCount = poolClassMax - poolClassShift + 1
)

var encodeBufPools [poolClassCount]sync.Pool

// getEncodeBuf returns a buffer of exactly n bytes, recycled from a
// power-of-two size class when one is free. The buffer is written once and
// handed to a synchronous pwrite, so pooling it is safe.
func getEncodeBuf(n int) []byte {
	if n <= 0 {
		n = 1
	}
	c := bits.Len(uint(n - 1)) // ceil(log2(n))
	if c < poolClassShift {
		c = poolClassShift
	}
	if c > poolClassMax {
		return make([]byte, n)
	}
	if b := encodeBufPools[c-poolClassShift].Get(); b != nil {
		return b.([]byte)[:n]
	}
	// Length exactly n, capacity one full size class: the caller sees a
	// buffer of the size it asked for, and putEncodeBuf can still classify it.
	return make([]byte, 1<<c)[:n]
}

// putEncodeBuf recycles a buffer previously returned by getEncodeBuf.
func putEncodeBuf(buf []byte) {
	if buf == nil {
		return
	}
	c := bits.Len(uint(cap(buf) - 1))
	if c < poolClassShift || c > poolClassMax || cap(buf) != 1<<c {
		return // not a pooled size class: let it be collected
	}
	encodeBufPools[c-poolClassShift].Put(buf[:cap(buf)])
}

// framePos validates seq order, records the sparse index entry and returns the
// offset the frame is to be written at. commitFrame must follow a successful
// write; a failed write leaves the counters untouched (a torn tail is what
// recovery expects to find either way).
func (s *Segment) framePos(seq uint64) (int64, error) {
	if !s.writable {
		return 0, errors.New("segment is sealed")
	}
	want := s.BaseSeq
	if s.RecordCnt > 0 {
		want = s.LastSeq + 1
	}
	if seq != want {
		return 0, fmt.Errorf("segment %d: seq %d out of order (want %d)", s.BaseSeq, seq, want)
	}
	pos := s.sizeBytes
	if s.RecordCnt == 0 || s.bytesSinceIndex >= indexIntervalB {
		s.index = append(s.index, indexEntry{seq: seq, pos: pos})
		s.bytesSinceIndex = 0
	}
	return pos, nil
}

// commitFrame accounts for n bytes written for seq at the segment tail.
func (s *Segment) commitFrame(seq uint64, n int64) {
	s.sizeBytes += n
	s.RecordCnt++
	s.LastSeq = seq
	s.bytesSinceIndex += n
	s.unflushed++
	// Index the frame only now that it landed: framePos runs before the write,
	// and a sparse entry for a frame that never reached the file would point
	// past the data on the next load.
	if s.idx != nil {
		if last := len(s.index) - 1; last >= 0 && s.index[last].seq == seq {
			s.idx.addSparse(seq, s.index[last].pos)
		}
	}
}

// writeFrame appends one already-encoded frame (recLen(4) + record) at the
// segment tail, maintaining the sparse index. The bytes land verbatim.
func (s *Segment) writeFrame(seq uint64, frame []byte) error {
	pos, err := s.framePos(seq)
	if err != nil {
		return err
	}
	if _, err := s.File.WriteAt(frame, pos); err != nil {
		return err
	}
	s.commitFrame(seq, int64(len(frame)))
	return nil
}

// Append encodes rec under the given seq at the segment tail.
func (s *Segment) Append(seq uint64, rec *data.EventRecord) error {
	frame := rec.EncodeBinary(getEncodeBuf(rec.EncodedSize()))
	err := s.writeFrame(seq, frame)
	putEncodeBuf(frame)
	return err
}

// AppendRecord appends rec under the given seq without assembling its whole
// frame in memory: the fixed-width headers go into a reusable scratch buffer
// and each event body is handed to the kernel as its own segment (pwritev), so
// a large body is never copied on the way to the page cache. Filesystems that
// refuse scatter/gather writes are detected once and served by the contiguous
// path from then on — the bytes that land are identical either way.
func (s *Segment) AppendRecord(seq uint64, rec *data.EventRecord) error {
	pos, err := s.framePos(seq)
	if err != nil {
		return err
	}
	total := rec.EncodedSize()
	if !scatterDisabled.Load() {
		if len(s.scatterBuf) < rec.ScatterSize() {
			s.scatterBuf = make([]byte, rec.ScatterSize())
		}
		s.scatterParts, _ = rec.EncodeScatter(s.scatterBuf, s.scatterParts)
		switch err := pwritevAll(s.File, s.scatterParts, pos); {
		case err == nil:
			s.commitFrame(seq, int64(total))
			return nil
		case !errors.Is(err, errScatterUnsupported):
			return err
		}
		scatterDisabled.Store(true) // latch: later appends skip the probe
	}
	frame := rec.EncodeBinary(getEncodeBuf(total))
	defer putEncodeBuf(frame)
	if _, err := s.File.WriteAt(frame, pos); err != nil {
		return err
	}
	s.commitFrame(seq, int64(total))
	return nil
}

// AppendFrame appends a leader-encoded frame verbatim: follower replication
// and migration catch-up land the bytes the leader produced, with no
// decode-to-record and re-encode round trip in between.
func (s *Segment) AppendFrame(seq uint64, frame []byte) error {
	return s.writeFrame(seq, frame)
}

// Flush fsyncs the segment file.
func (s *Segment) Flush() error {
	if s.File == nil {
		return nil
	}
	if err := s.File.Sync(); err != nil {
		return err
	}
	if s.idx != nil {
		// Push the buffered entries out; no fsync — the index is allowed to lag
		// the WAL, and startup replays whatever it missed.
		s.idx.flushBlock()
		s.idx.flushSparse()
	}
	s.unflushed = 0
	s.syncedSize = s.sizeBytes
	s.lastFlushTime = time.Now()
	return nil
}

// Unflushed counts records written since the last fsync.
func (s *Segment) Unflushed() int64 { return s.unflushed }

// LastFlushTime is when the segment was last fsynced.
func (s *Segment) LastFlushTime() time.Time { return s.lastFlushTime }

// indexPos returns the byte position of the last index entry with seq <=
// target, clamped into the segment's range.
func (s *Segment) indexPos(target uint64) int64 {
	if len(s.index) == 0 {
		return s.dataStart
	}
	i := sort.Search(len(s.index), func(i int) bool { return s.index[i].seq > target })
	if i == 0 {
		return s.index[0].pos
	}
	return s.index[i-1].pos
}

// indexSeqAt returns the seq recorded in the index entry covering off.
func (s *Segment) indexSeqAt(off int64) uint64 {
	if len(s.index) == 0 {
		return s.BaseSeq
	}
	i := sort.Search(len(s.index), func(i int) bool { return s.index[i].pos > off })
	if i == 0 {
		return s.index[0].seq
	}
	return s.index[i-1].seq
}

// readRecordAt decodes the length-prefixed record starting at off.
func readRecordAt(f *os.File, off int64, buf []byte) (data.EventRecord, int, error) {
	h := buf[:4]
	if _, err := f.ReadAt(h, off); err != nil {
		return data.EventRecord{}, 0, err
	}
	recLen := int(binary.BigEndian.Uint32(h))
	if recLen < 30 || recLen > 64<<20 {
		return data.EventRecord{}, 0, errors.New("bad record length")
	}
	need := 4 + recLen
	if need > len(buf) {
		buf = make([]byte, need)
	}
	body := buf[:need]
	if _, err := f.ReadAt(body, off); err != nil {
		return data.EventRecord{}, 0, err
	}
	rec, consumed, err := data.DecodeRecord(body)
	if err != nil {
		return rec, 0, err
	}
	if consumed != need {
		return rec, 0, errors.New("record length mismatch")
	}
	return rec, need, nil
}

// ScanFrom iterates records with seq >= fromSeq (within this segment),
// invoking fn(seq, rec) in order until it returns false.
func (s *Segment) ScanFrom(fromSeq uint64, fn func(uint64, *data.EventRecord) bool) error {
	if s.RecordCnt == 0 || fromSeq > s.LastSeq {
		return nil
	}
	if fromSeq < s.BaseSeq {
		fromSeq = s.BaseSeq
	}
	off := s.indexPos(fromSeq)
	seq := s.indexSeqAt(off)
	buf := make([]byte, 64<<10)
	for off < s.sizeBytes {
		rec, consumed, err := readRecordAt(s.File, off, buf)
		if err != nil {
			return err
		}
		if seq >= fromSeq {
			if !fn(seq, &rec) {
				return nil
			}
		}
		off += int64(consumed)
		seq++
	}
	return nil
}

// ScanHeaders walks this segment's records from fromSeq in one windowed pass,
// handing out frame-header metadata only (data.DecodeRecordMeta: seq,
// aggregate, version, command id — event bodies are neither copied nor
// decoded). The index rebuild uses this instead of the full-record ScanFrom:
// at 30 GiB the old path spent its time on per-record preads and on the
// EventRecord/Events allocations it then threw away.
func (s *Segment) ScanHeaders(fromSeq uint64, fn func(uint64, data.RecordMeta) bool) error {
	if s.RecordCnt == 0 || fromSeq > s.LastSeq {
		return nil
	}
	if fromSeq < s.BaseSeq {
		fromSeq = s.BaseSeq
	}
	pos := s.indexPos(fromSeq)
	seq := s.indexSeqAt(pos)
	if pos >= s.sizeBytes {
		return nil
	}
	w := newFrameWalker(s.File, pos, s.sizeBytes)
	for w.pos < s.sizeBytes {
		frame, _, err := w.next()
		if err != nil {
			return err // LoadSegment validated the segment: no torn tail here
		}
		if seq >= fromSeq {
			meta, _, err := data.DecodeRecordMeta(frame)
			if err != nil {
				return fmt.Errorf("segment %s seq %d: %w", s.Path, seq, err)
			}
			if !fn(seq, meta) {
				return nil
			}
		}
		seq++
	}
	return nil
}

// ReadRange returns contiguous byte ranges (for zero-copy fetch/migration)
// covering records with fromSeq <= seq < untilSeq. total bytes capped by
// maxBytes (the last record that crosses the cap is skipped, not truncated).
//
// The walk runs through the shared window reader: the sparse index entry it
// starts from can sit a whole indexIntervalB behind fromSeq, and the per-frame
// form of this walk (a 4-byte pread per record to find the next frame) made
// every replica fetch pay hundreds to thousands of syscalls on the leader —
// the dominant cost of the replication path. One window read covers the same
// frames; the frames before fromSeq are still skipped, just without a syscall
// each.
func (s *Segment) ReadRange(fromSeq, untilSeq uint64, maxBytes int64) ([]data.ByteRange, uint64, error) {
	if s.RecordCnt == 0 {
		return nil, fromSeq, nil
	}
	if fromSeq < s.BaseSeq {
		fromSeq = s.BaseSeq
	}
	if fromSeq > s.LastSeq {
		return nil, fromSeq, nil
	}
	start := s.indexPos(fromSeq)
	seq := s.indexSeqAt(start)
	var ranges []data.ByteRange
	var total int64
	next := fromSeq
	w := newFrameWalkerPooled(s.File, start, s.sizeBytes)
	defer w.release()
	for {
		frame, frameOff, err := w.next()
		if err == errShortTail || err == errBadRecordLen {
			break // torn tail: stop at the last complete frame
		}
		if err != nil {
			return ranges, next, err
		}
		if untilSeq > 0 && seq >= untilSeq {
			break
		}
		recLen := int64(len(frame))
		if seq < fromSeq {
			// sparse index started before fromSeq: skip forward without
			// collecting
			seq++
			continue
		}
		if total > 0 && total+recLen > maxBytes {
			break
		}
		if len(ranges) > 0 && ranges[len(ranges)-1].End == frameOff {
			ranges[len(ranges)-1].End = frameOff + recLen
		} else {
			ranges = append(ranges, data.ByteRange{
				Segment: filepath.Base(s.Path), Start: frameOff, End: frameOff + recLen,
			})
		}
		total += recLen
		next = seq + 1
		seq++
		if total >= maxBytes {
			break
		}
	}
	if w.err != nil {
		return ranges, next, w.err
	}
	return ranges, next, nil
}

// TruncateTo drops all records with seq >= fromSeq from this segment. fromSeq
// must be > BaseSeq; dropping the whole segment is the caller's decision.
// Returns the new last seq, or errSegmentFullyDropped.
var errSegmentFullyDropped = errors.New("segment fully dropped")

func (s *Segment) TruncateTo(fromSeq uint64) error {
	if fromSeq <= s.BaseSeq {
		return errSegmentFullyDropped
	}
	if s.RecordCnt == 0 || fromSeq > s.LastSeq {
		return nil
	}
	// find the byte offset of record fromSeq
	off := s.indexPos(fromSeq)
	seq := s.indexSeqAt(off)
	buf := make([]byte, 64<<10)
	for off < s.sizeBytes && seq < fromSeq {
		h := buf[:4]
		if _, err := s.File.ReadAt(h, off); err != nil {
			return err
		}
		recLen := int64(binary.BigEndian.Uint32(h))
		if recLen < 30 || off+4+recLen > s.sizeBytes {
			break
		}
		off += 4 + recLen
		seq++
	}
	if err := s.File.Truncate(off); err != nil {
		return err
	}
	return s.rebuildIndex(off)
}

// rebuildIndex resets the sparse index by scanning the body up to size limit.
func (s *Segment) rebuildIndex(limit int64) error {
	if err := s.File.Truncate(limit); err != nil {
		return err
	}
	defer func() {}()
	s.sizeBytes = limit
	s.index = s.index[:0]
	s.bytesSinceIndex = 0
	s.RecordCnt = 0
	s.LastSeq = 0
	var seq uint64 = s.BaseSeq
	buf := make([]byte, 64<<10)
	off := int64(s.dataStart)
	for off < s.sizeBytes {
		h := buf[:4]
		if _, err := s.File.ReadAt(h, off); err != nil {
			return err
		}
		recLen := int64(binary.BigEndian.Uint32(h))
		if recLen < 30 || off+4+recLen > s.sizeBytes {
			return errors.New("rebuild found torn record")
		}
		if s.RecordCnt == 0 || s.bytesSinceIndex >= indexIntervalB {
			s.index = append(s.index, indexEntry{seq: seq, pos: off})
			s.bytesSinceIndex = 0
		}
		s.bytesSinceIndex += 4 + recLen
		s.RecordCnt++
		s.LastSeq = seq
		off += 4 + recLen
		seq++
	}
	if s.idx != nil {
		// The WAL shrank: the index must stop promising records that are gone.
		if err := s.idx.truncateToSeq(s.LastSeq, s.index); err != nil {
			return err
		}
	}
	return nil
}

// LoadSegment opens a segment and rebuilds its sparse index, scanning and
// truncating any torn tail at the end of the file.
// minRecordLen / maxRecordLen bound a plausible frame length prefix; anything
// else is a torn tail (the same bounds the per-record readers use).
const (
	minRecordLen = 30
	maxRecordLen = 64 << 20
)

var (
	errShortTail    = errors.New("storage: record runs past the end of the segment")
	errBadRecordLen = errors.New("storage: implausible record length")
)

// frameWalker streams one segment's frames through a reused window: one pread
// per window instead of one per record. That matters at scale — a 30 GiB node
// holds tens of millions of records, and the per-record preads in the recovery
// walks (LoadSegment's boundary scan plus the index rebuild) cost more than the
// bytes themselves did.
type frameWalker struct {
	f    *os.File
	pos  int64 // file offset of the next unparsed frame
	end  int64 // segment size
	buf  []byte
	base int64 // file offset of buf[0]
	n    int   // valid bytes in buf
	done int64 // end offset of the last complete frame
	err  error // first real read error (EOF is not one)
	// pooled marks buf as coming from frameWindowPool: it must go back
	// through release when the walk ends.
	pooled bool
}

const frameWindow = 1 << 20

func newFrameWalker(f *os.File, from, end int64) *frameWalker {
	return &frameWalker{f: f, pos: from, end: end, buf: make([]byte, frameWindow), done: from}
}

// frameWindowPool serves the walker of ReadRange, which runs once per slot per
// replica-fetch round: a fresh 1MiB window per call there is pure garbage.
// Only the pooled walker's buffer goes back (release); the startup walks keep
// their own.
var frameWindowPool = sync.Pool{New: func() any { return make([]byte, frameWindow) }}

func newFrameWalkerPooled(f *os.File, from, end int64) *frameWalker {
	w := &frameWalker{f: f, pos: from, end: end, done: from, pooled: true}
	w.buf = frameWindowPool.Get().([]byte)
	return w
}

// release returns the pooled window. The walker is unusable afterwards.
func (w *frameWalker) release() {
	if w.pooled {
		frameWindowPool.Put(w.buf[:frameWindow])
		w.pooled = false
	}
}

// lastComplete is the offset just past the last whole frame seen.
func (w *frameWalker) lastComplete() int64 { return w.done }

// refill reads a window starting at the next unparsed frame. It reports false
// when the file has nothing more to give (the reader is then at EOF).
func (w *frameWalker) refill() bool {
	n, err := w.f.ReadAt(w.buf, w.pos)
	w.base, w.n = w.pos, n
	if n > 0 {
		return true
	}
	// A read that returns nothing is the end of the file, except when the
	// kernel reported a real failure: that one is kept so a caller whose
	// contract forbids a silent short read can surface it (the recovery walk
	// treats it as any other stop).
	if err != nil && !errors.Is(err, io.EOF) && w.err == nil {
		w.err = err
	}
	return false
}

// next returns the next complete frame (valid until the next call, except for
// frames larger than the window, which come back in their own slice), its file
// offset, or an error: errShortTail / errBadRecordLen mark a torn tail, which
// LoadSegment truncates.
func (w *frameWalker) next() ([]byte, int64, error) {
	for w.pos < w.end {
		if w.pos < w.base || w.pos >= w.base+int64(w.n) {
			if !w.refill() {
				return nil, 0, errShortTail
			}
			continue
		}
		start := int(w.pos - w.base)
		avail := w.n - start
		if avail < 4 {
			if !w.refill() || w.n < 4 {
				return nil, 0, errShortTail
			}
			continue
		}
		recLen := int(binary.BigEndian.Uint32(w.buf[start:]))
		if recLen < minRecordLen || recLen > maxRecordLen {
			return nil, 0, errBadRecordLen
		}
		need := int64(4 + recLen)
		if need > int64(len(w.buf)) {
			// A frame bigger than the window: read it on its own.
			frame := make([]byte, need)
			if _, err := w.f.ReadAt(frame, w.pos); err != nil {
				return nil, 0, errShortTail
			}
			off := w.pos
			w.pos += need
			w.done = w.pos
			return frame, off, nil
		}
		if int64(avail) < need {
			// Try to get the whole frame into the window; if the file ends
			// first, the frame is torn.
			if !w.refill() || w.n < 4+recLen {
				return nil, 0, errShortTail
			}
			continue
		}
		off := w.pos
		frame := w.buf[start : start+int(need)]
		w.pos += need
		w.done = w.pos
		return frame, off, nil
	}
	return nil, 0, errShortTail
}

func LoadSegment(path string) (*Segment, error) {
	s, err := OpenSegment(path)
	if err != nil {
		return nil, err
	}
	if err := s.walkAndIndex(s.BaseSeq, int64(s.dataStart)); err != nil {
		s.File.Close()
		return nil, err
	}
	return s, nil
}

// walkAndIndex accounts records from a frame position to the end of the file:
// one windowed pass that finds the last complete record (a torn tail is
// truncated), counts records and rebuilds the sparse index, without a pread per
// record. It is what a full recovery uses, and what resumes a segment from the
// last position its index knows.
func (s *Segment) walkAndIndex(fromSeq uint64, fromPos int64) error {
	w := newFrameWalker(s.File, fromPos, s.sizeBytes)
	seq := fromSeq
	for {
		frame, off, err := w.next()
		if err == errShortTail || err == errBadRecordLen {
			break // torn tail: truncate at the last complete record
		}
		if err != nil {
			return err
		}
		if s.RecordCnt == 0 || s.bytesSinceIndex >= indexIntervalB {
			s.index = append(s.index, indexEntry{seq: seq, pos: off})
			s.bytesSinceIndex = 0
		}
		s.bytesSinceIndex += int64(len(frame))
		s.RecordCnt++
		s.LastSeq = seq
		seq++
	}
	if limit := w.lastComplete(); limit != s.sizeBytes {
		// torn tail: truncate to the last complete record
		if err := s.File.Truncate(limit); err != nil {
			return err
		}
		s.sizeBytes = limit
	}
	return nil
}

// LoadSegmentIndexed restores a segment from its index files. The sparse index
// gives the last frame position it knows, and one walk from there to the end of
// the file settles the record count, the last seq and any torn tail — so the
// pass is bounded by one index interval, not by the segment's size. It returns
// (nil, nil, nil) when no usable index is present, leaving the caller to walk.
func LoadSegmentIndexed(path, dir string) (*Segment, *segIndexLoad, error) {
	s, err := OpenSegment(path)
	if err != nil {
		return nil, nil, err
	}
	load, err := openSegIndex(dir, s.SlotID, s.BaseSeq, -1, s.sizeBytes)
	if err != nil {
		s.File.Close()
		return nil, nil, err
	}
	if load == nil || len(load.sparse) == 0 {
		load.Release()
		s.File.Close()
		return nil, nil, nil
	}
	last := load.sparse[len(load.sparse)-1]
	if last.seq < s.BaseSeq || last.pos < int64(s.dataStart) || last.pos >= s.sizeBytes {
		// The index points outside the file: distrust it, let the caller walk.
		load.Release()
		s.File.Close()
		segIndexDamaged.Add(1)
		return nil, nil, nil
	}
	s.index = load.sparse
	s.RecordCnt = int64(last.seq - s.BaseSeq) // the record at last.pos is counted by the walk
	s.LastSeq = last.seq - 1
	s.bytesSinceIndex = 0
	if err := s.walkAndIndex(last.seq, last.pos); err != nil {
		load.Release()
		s.File.Close()
		return nil, nil, err
	}
	if cnt := int(s.RecordCnt); load.Count() > cnt {
		// The WAL holds fewer records than the index claims (a truncated
		// segment): keep only what exists. The entries are dense, so the count
		// alone bounds them.
		load.flat.count = cnt
		segIndexDamaged.Add(1)
	}
	return s, load, nil
}

// indexSparseFromMemory feeds the segment's in-memory sparse index into its
// index files. Used when a walked segment is indexed at startup, where the walk
// already produced the positions.
func (s *Segment) indexSparseFromMemory() {
	if s.idx == nil {
		return
	}
	for _, e := range s.index {
		s.idx.addSparse(e.seq, e.pos)
	}
}

// IndexRecord adds one stored record to the segment's index (no-op when the
// segment has none, and when indexing has been given up on).
func (s *Segment) IndexRecord(seq uint64, m data.RecordMeta) {
	if s.idx == nil {
		return
	}
	s.idx.add(m.CommandHash, seq, m.AggregateID, m.Version)
}

// openIndexWriter starts (or resumes) this segment's index. Failure leaves the
// segment without one: the WAL stays authoritative and the next startup writes
// the index, so this never blocks writes.
func (s *Segment) openIndexWriter() error {
	if s.idx != nil {
		return nil
	}
	w, err := openSegIndexWriter(filepath.Dir(s.Path), s.SlotID, s.BaseSeq)
	if err != nil {
		segIndexUnusable.Add(1)
		return err
	}
	s.idx = w
	return nil
}

// indexFlushAndClose makes the index durable and stops indexing this segment
// (a sealed segment keeps its index; a new segment gets its own).
func (s *Segment) indexFlushAndClose() error {
	if s.idx == nil {
		return nil
	}
	err := s.idx.close()
	s.idx = nil
	return err
}

// Close makes the index durable and releases both file handles.
func (s *Segment) Close() error {
	err := s.indexFlushAndClose()
	if s.File != nil {
		if cerr := s.File.Close(); cerr != nil && err == nil {
			err = cerr
		}
		s.File = nil
	}
	return err
}

// Remove deletes the segment and its index files.
func (s *Segment) Remove() error {
	s.Close()
	if s.aggIx != nil {
		_ = s.aggIx.Close()
		s.aggIx = nil
	}
	dir := filepath.Dir(s.Path)
	for _, p := range []string{segIdxPath(dir, s.BaseSeq), segAgxPath(dir, s.BaseSeq), segSpxPath(dir, s.BaseSeq)} {
		_ = os.Remove(p)
	}
	return os.Remove(s.Path)
}

// SegmentFilesOf lists WAL files in dir sorted by base seq.
func SegmentFilesOf(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".wal") {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names) // %016d zero-padded: lexical == numeric
	paths := make([]string, len(names))
	for i, n := range names {
		paths[i] = filepath.Join(dir, n)
	}
	return paths, nil
}

func baseSeqFromName(name string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSuffix(filepath.Base(name), ".wal"), 10, 64)
}
