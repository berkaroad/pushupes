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

// writeFrame appends one already-encoded frame (recLen(4) + record) at the
// segment tail, maintaining the sparse index. The bytes land verbatim.
func (s *Segment) writeFrame(seq uint64, frame []byte) error {
	if !s.writable {
		return errors.New("segment is sealed")
	}
	want := s.BaseSeq
	if s.RecordCnt > 0 {
		want = s.LastSeq + 1
	}
	if seq != want {
		return fmt.Errorf("segment %d: seq %d out of order (want %d)", s.BaseSeq, seq, want)
	}
	pos := s.sizeBytes
	if s.RecordCnt == 0 || s.bytesSinceIndex >= indexIntervalB {
		s.index = append(s.index, indexEntry{seq: seq, pos: pos})
		s.bytesSinceIndex = 0
	}
	if _, err := s.File.WriteAt(frame, pos); err != nil {
		return err
	}
	s.sizeBytes += int64(len(frame))
	s.RecordCnt++
	s.LastSeq = seq
	s.bytesSinceIndex += int64(len(frame))
	s.unflushed++
	return nil
}

// Append encodes rec under the given seq at the segment tail.
func (s *Segment) Append(seq uint64, rec *data.EventRecord) error {
	frame := rec.EncodeBinary(getEncodeBuf(rec.EncodedSize()))
	err := s.writeFrame(seq, frame)
	putEncodeBuf(frame)
	return err
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
func LoadSegment(path string) (*Segment, error) {
	s, err := OpenSegment(path)
	if err != nil {
		return nil, err
	}
	// find the last complete record boundary
	limit := int64(s.dataStart)
	buf := make([]byte, 4)
	off := int64(s.dataStart)
	for off < s.sizeBytes {
		if _, err := s.File.ReadAt(buf, off); err != nil {
			break
		}
		recLen := int64(binary.BigEndian.Uint32(buf))
		if recLen < 30 || recLen > 64<<20 || off+4+recLen > s.sizeBytes {
			break
		}
		off += 4 + recLen
		limit = off
	}
	if limit != s.sizeBytes {
		// torn tail: truncate to the last complete record
		if err := s.File.Truncate(limit); err != nil {
			s.File.Close()
			return nil, err
		}
		s.sizeBytes = limit
	}
	off = s.dataStart
	var seq uint64 = s.BaseSeq
	for off < s.sizeBytes {
		if _, err := s.File.ReadAt(buf, off); err != nil {
			s.File.Close()
			return nil, err
		}
		recLen := int64(binary.BigEndian.Uint32(buf))
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
