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
	WALMagic       = "ESWL"
	WALFormatVer   = int8(1)
	WALHeaderLen   = 20
	indexIntervalB = int64(4096) // sparse index: one entry per ~4KiB
)

// DefaultSegmentBytes is the 256MiB roll threshold from the design.
const DefaultSegmentBytes = int64(256 * 1024 * 1024)

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

	scatterBuf   []byte   // scratch for EncodeScatter headers
	scatterParts [][]byte // reusable segment list handed to pwritev
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
	return &Segment{
		SlotID:        slotID,
		BaseSeq:       baseSeq,
		Path:          path,
		File:          f,
		dataStart:     WALHeaderLen,
		sizeBytes:     WALHeaderLen,
		writable:      true,
		lastFlushTime: time.Now(),
	}, nil
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
	s.unflushed = 0
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
	off := s.indexPos(fromSeq)
	seq := s.indexSeqAt(off)
	var ranges []data.ByteRange
	var total int64
	next := fromSeq
	buf := make([]byte, 64<<10)
	for off < s.sizeBytes {
		if untilSeq > 0 && seq >= untilSeq {
			break
		}
		if seq < fromSeq {
			// sparse index started before fromSeq: skip forward without collecting
			h := buf[:4]
			if _, err := s.File.ReadAt(h, off); err != nil {
				return ranges, next, err
			}
			recLen := int64(binary.BigEndian.Uint32(h))
			if recLen < 30 || off+4+recLen > s.sizeBytes {
				break
			}
			off += 4 + recLen
			seq++
			continue
		}
		h := buf[:4]
		if _, err := s.File.ReadAt(h, off); err != nil {
			return ranges, next, err
		}
		recLen := int64(binary.BigEndian.Uint32(h))
		if recLen < 30 || off+4+recLen > s.sizeBytes {
			break // torn tail
		}
		if total > 0 && total+4+recLen > maxBytes {
			break
		}
		if len(ranges) > 0 && ranges[len(ranges)-1].End == off {
			ranges[len(ranges)-1].End = off + 4 + recLen
		} else {
			ranges = append(ranges, data.ByteRange{
				Segment: filepath.Base(s.Path), Start: off, End: off + 4 + recLen,
			})
		}
		total += 4 + recLen
		next = seq + 1
		off += 4 + recLen
		seq++
		if total >= maxBytes {
			break
		}
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
}

const frameWindow = 1 << 20

func newFrameWalker(f *os.File, from, end int64) *frameWalker {
	return &frameWalker{f: f, pos: from, end: end, buf: make([]byte, frameWindow), done: from}
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
	_ = err
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
	// One windowed pass: it both finds the last complete record (a torn tail is
	// truncated) and rebuilds the sparse index, without a pread per record.
	w := newFrameWalker(s.File, int64(s.dataStart), s.sizeBytes)
	var seq uint64 = s.BaseSeq
	for {
		frame, off, err := w.next()
		if err == errShortTail || err == errBadRecordLen {
			break // torn tail: truncate at the last complete record
		}
		if err != nil {
			s.File.Close()
			return nil, err
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
			s.File.Close()
			return nil, err
		}
		s.sizeBytes = limit
	}
	return s, nil
}

// Close releases the file handle.
func (s *Segment) Close() error {
	if s.File != nil {
		err := s.File.Close()
		s.File = nil
		return err
	}
	return nil
}

// Remove deletes the segment file.
func (s *Segment) Remove() error {
	s.Close()
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
