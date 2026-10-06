// wal.go — the consensus layer's own write-ahead log.
//
// One directory holds a series of append-only segment files; each segment is a
// flat run of framed records. The WAL is generic on purpose: it stores
// (type, payload) opaque records and knows nothing about Raft entries or
// hard-state. The raft log (log.go) is the only caller and owns the payload
// format.
//
// Durability is group commit: Append buffers an encoded record and returns its
// LSN; a background flusher writes the buffer, fdatasync()s it, then wakes
// every caller whose LSN is now durable. N concurrent appends therefore share
// one fsync instead of paying one each; a caller that must be durable before it
// is acknowledged waits with Wait(lsn).
//
// Crash recovery scans segments in order and truncates at the first record
// whose framing or CRC is bad, dropping only the tail — records already
// durably written before the torn point are never lost as a whole.
//
// Frame layout (all big-endian):
//
//	len u32 | type u8 | crc32c u32 | payload[len]
//
// crc32c covers type + len + payload.
package raft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// WAL record types. The payload is opaque to the WAL.
const (
	RecordEntry     byte = 1 // a log entry (command, no-op or conf)
	RecordHardState byte = 2 // term + voteFor
	RecordConf      byte = 3 // the static voter set (first record only)
	RecordTruncate  byte = 4 // drop every in-memory entry at or above this index
)

const (
	recordHeaderLen = 9
	walFileSuffix   = ".wal"
	walLockName     = "LOCK"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func crc32c(b []byte) uint32 { return crc32.Checksum(b, crcTable) }

// Record is one recovered WAL record.
type Record struct {
	LSN     uint64
	Type    byte
	Payload []byte
}

// WALOptions tunes the WAL. Zero values fall back to defaults.
type WALOptions struct {
	SegmentBytes  int64         // rotate a segment once it reaches this size
	FlushInterval time.Duration // group-commit window
	// OnDurable, when set, is called (from the flusher goroutine) with the
	// highest LSN now on disk after every successful fsync. It must not block
	// for long: durability callers use it to release deferred acknowledgements.
	OnDurable func(lsn uint64)
}

func (o *WALOptions) withDefaults() {
	if o.SegmentBytes <= 0 {
		o.SegmentBytes = 64 << 20
	}
	if o.FlushInterval <= 0 {
		o.FlushInterval = 200 * time.Microsecond
	}
}

type walWaiter struct {
	lsn uint64
	ch  chan struct{}
}

type pendingRec struct {
	lsn   uint64
	frame []byte
}

// walSegment describes a *closed* segment (the open one is tracked by the
// WAL's seg* fields and is never compacted).
type walSegment struct {
	seq      uint64
	path     string
	firstLSN uint64
	lastLSN  uint64
	size     int64
}

// WAL is an append-only, group-committed, crash-recoverable record log.
type WAL struct {
	dir  string
	opts WALOptions
	lock *os.File
	seed []Record // records recovered at open; replayed by the caller

	mu        sync.Mutex
	seg       *os.File
	segSeq    uint64
	segSize   int64
	segFirst  uint64
	segLast   uint64
	segments  []walSegment // closed segments only
	nextLSN   uint64       // highest LSN assigned
	durable   uint64       // highest LSN fsync'd
	pending   []pendingRec
	waiters   []walWaiter
	flushes   uint64 // number of fsync batches (for group-commit accounting)
	onDurable func(uint64)
	err       error
	closed    bool

	dirty     chan struct{}
	closeReq  chan struct{}
	doneCh    chan struct{}
	closeOnce sync.Once
}

// OpenWAL locks dir and recovers every record written so far. The returned
// records are the replay seed; they are not re-appended.
func OpenWAL(dir string, opts WALOptions) (*WAL, error) {
	opts.withDefaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, walLockName), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("raft: wal %s is locked by another process (or a previous run did not release it): %w", dir, err)
	}

	w := &WAL{
		dir:      dir,
		opts:     opts,
		lock:     lock,
		dirty:    make(chan struct{}, 1),
		closeReq: make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	if err := w.recover(); err != nil {
		w.releaseLock()
		return nil, err
	}
	go w.flushLoop()
	return w, nil
}

// Dir is the WAL directory.
func (w *WAL) Dir() string { return w.dir }

// Records returns the records recovered at open (replay seed).
func (w *WAL) Records() []Record { return w.seed }

// LastLSN is the highest LSN assigned so far (0 when empty).
func (w *WAL) LastLSN() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextLSN
}

// flushCount reports how many fsync batches the WAL has run — the group-commit
// coalescing ratio is appends/flushes.
func (w *WAL) flushCount() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushes
}

// ---- recovery ----

func walSegFiles(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), walFileSuffix) {
			continue
		}
		out = append(out, filepath.Join(dir, e.Name()))
	}
	sort.Strings(out) // zero-padded ordinals: lexicographic == numeric
	return out, nil
}

func segSeqOf(path string) uint64 {
	base := strings.TrimSuffix(filepath.Base(path), walFileSuffix)
	n, err := strconv.ParseUint(base, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func (w *WAL) recover() error {
	files, err := walSegFiles(w.dir)
	if err != nil {
		return err
	}
	var recs []Record
	var closed []walSegment
	var globalLSN uint64
	for i, path := range files {
		segs, lastLSN, truncAt, ok, err := scanSegment(path, globalLSN)
		if err != nil {
			return err
		}
		globalLSN += uint64(len(segs))
		recs = append(recs, segs...)
		isLast := i == len(files)-1
		if isLast {
			// The last file becomes the open segment.
			w.segSeq = segSeqOf(path)
			w.segSize = truncAt
			if len(segs) > 0 {
				w.segFirst = segs[0].LSN
			}
			w.segLast = lastLSN
			f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
			if err != nil {
				return err
			}
			w.seg = f
			if !ok {
				if err := f.Truncate(truncAt); err != nil {
					return err
				}
			}
			continue
		}
		closed = append(closed, walSegment{seq: segSeqOf(path), path: path, size: truncAt, lastLSN: lastLSN})
		if !ok {
			// A bad record is only legal at the very tail: drop this and
			// every later segment, and stop.
			if err := os.Truncate(path, truncAt); err != nil {
				return err
			}
			for _, later := range files[i+1:] {
				if rerr := os.Remove(later); rerr != nil && !os.IsNotExist(rerr) {
					return rerr
				}
			}
			break
		}
	}
	w.segments = closed
	w.seed = recs
	w.nextLSN = globalLSN
	w.durable = globalLSN
	if w.seg == nil {
		f, err := w.newSegment()
		if err != nil {
			return err
		}
		w.seg = f
	}
	return nil
}

// scanSegment reads every valid record from one segment. base is the number of
// records before this segment (so LSNs stay global). ok=false means a
// bad/partial record was found; truncAt is the byte offset of the last valid
// record.
func scanSegment(path string, base uint64) (recs []Record, lastLSN uint64, truncAt int64, ok bool, err error) {
	data, rerr := os.ReadFile(path)
	if rerr != nil {
		return nil, 0, 0, false, rerr
	}
	off := int64(0)
	for off < int64(len(data)) {
		if int64(len(data))-off < recordHeaderLen {
			return recs, base + uint64(len(recs)), off, false, nil
		}
		hdr := data[off : off+recordHeaderLen]
		plen := int64(binary.BigEndian.Uint32(hdr[0:4]))
		typ := hdr[4]
		want := binary.BigEndian.Uint32(hdr[5:9])
		if plen < 0 || off+recordHeaderLen+plen > int64(len(data)) {
			return recs, base + uint64(len(recs)), off, false, nil
		}
		payload := data[off+recordHeaderLen : off+recordHeaderLen+plen]
		h := crc32.New(crcTable)
		h.Write([]byte{typ})
		h.Write(hdr[0:4])
		h.Write(payload)
		if h.Sum32() != want {
			return recs, base + uint64(len(recs)), off, false, nil
		}
		lsn := base + uint64(len(recs)) + 1
		recs = append(recs, Record{LSN: lsn, Type: typ, Payload: append([]byte(nil), payload...)})
		off += recordHeaderLen + plen
	}
	return recs, base + uint64(len(recs)), off, true, nil
}

// caller must hold no lock issues: newSegment reads segSeq under mu.
func (w *WAL) newSegment() (*os.File, error) {
	w.mu.Lock()
	seq := w.segSeq + 1
	if seq == 0 {
		seq = 1
	}
	w.segSeq = seq
	w.segSize = 0
	w.segFirst = 0
	w.segLast = 0
	w.mu.Unlock()

	path := filepath.Join(w.dir, fmt.Sprintf("%020d%s", seq, walFileSuffix))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syncDir(w.dir); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// ---- append / flush ----

func encodeRecord(typ byte, payload []byte) []byte {
	var hdr [recordHeaderLen]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	hdr[4] = typ
	h := crc32.New(crcTable)
	h.Write(hdr[4:5])
	h.Write(hdr[0:4])
	h.Write(payload)
	binary.BigEndian.PutUint32(hdr[5:9], h.Sum32())
	frame := make([]byte, 0, recordHeaderLen+len(payload))
	frame = append(frame, hdr[:]...)
	frame = append(frame, payload...)
	return frame
}

// Append buffers one record and returns its LSN. It is durable once Wait(lsn)
// returns nil.
func (w *WAL) Append(typ byte, payload []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, fmt.Errorf("raft: wal closed")
	}
	if w.err != nil {
		return 0, w.err
	}
	lsn := w.nextLSN + 1
	w.nextLSN = lsn
	w.pending = append(w.pending, pendingRec{lsn: lsn, frame: encodeRecord(typ, payload)})
	select {
	case w.dirty <- struct{}{}:
	default:
	}
	return lsn, nil
}

// Wait blocks until the record at lsn is durable (or a write error happened).
func (w *WAL) Wait(lsn uint64) error {
	w.mu.Lock()
	if w.err != nil {
		err := w.err
		w.mu.Unlock()
		return err
	}
	if w.durable >= lsn {
		w.mu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	w.waiters = append(w.waiters, walWaiter{lsn: lsn, ch: ch})
	w.mu.Unlock()

	<-ch
	w.mu.Lock()
	err := w.err
	w.mu.Unlock()
	return err
}

// Flush forces the group commit window closed and waits until everything
// appended so far is durable.
func (w *WAL) Flush() error {
	w.mu.Lock()
	if w.closed || w.err != nil {
		err := w.err
		w.mu.Unlock()
		return err
	}
	target := w.nextLSN
	w.mu.Unlock()
	select {
	case w.dirty <- struct{}{}:
	default:
	}
	if target == 0 {
		return nil
	}
	return w.Wait(target)
}

// flushLoop runs the group-commit window as a one-shot timer that exists only
// while there is work: an append opens the window, the window's expiry closes
// it with a single fsync over everything buffered meanwhile, and once pending
// is empty the loop parks on the dirty signal holding no timer at all. An
// idle WAL therefore never wakes the scheduler — a standing ticker would
// deliver one netpoll-breaking timer every FlushInterval (5000/s at the
// default 200µs) whether or not anything was appended, and at rest that alone
// was most of the process's CPU.
func (w *WAL) flushLoop() {
	defer close(w.doneCh)
	var window *time.Timer
	var windowC <-chan time.Time
	for {
		select {
		case <-w.dirty:
			if window == nil {
				window = time.NewTimer(w.opts.FlushInterval)
				windowC = window.C
			}
		case <-windowC:
			window = nil
			windowC = nil
			w.flush()
		case <-w.closeReq:
			if window != nil {
				window.Stop()
			}
			w.flush()
			return
		}
	}
}

func (w *WAL) flush() {
	w.mu.Lock()
	if len(w.pending) == 0 {
		w.mu.Unlock()
		return
	}
	recs := w.pending
	w.pending = nil
	w.flushes++
	w.mu.Unlock()

	err := w.writePending(recs)

	w.mu.Lock()
	if err != nil {
		w.failLocked(err)
		w.mu.Unlock()
		return
	}
	w.durable = recs[len(recs)-1].lsn
	w.wakeupLocked()
	fn := w.onDurable
	durable := w.durable
	w.mu.Unlock()
	if fn != nil {
		fn(durable)
	}
}

// SetOnDurable installs the durability callback (see WALOptions.OnDurable).
func (w *WAL) SetOnDurable(fn func(lsn uint64)) {
	w.mu.Lock()
	w.onDurable = fn
	w.mu.Unlock()
}

// writePending writes records into the current segment, rotating between
// records only (never inside one), and fsyncs each segment it writes.
func (w *WAL) writePending(recs []pendingRec) error {
	i := 0
	for i < len(recs) {
		w.mu.Lock()
		seg := w.seg
		size := w.segSize
		segBytes := w.opts.SegmentBytes
		w.mu.Unlock()

		if size >= segBytes {
			if err := w.rotate(); err != nil {
				return err
			}
			continue
		}

		var buf []byte
		first := recs[i].lsn
		last := first
		for i < len(recs) {
			f := recs[i].frame
			if len(buf) > 0 && size+int64(len(buf)+len(f)) > segBytes {
				break
			}
			buf = append(buf, f...)
			last = recs[i].lsn
			i++
		}
		if _, err := seg.Write(buf); err != nil {
			return err
		}
		if err := fdatasync(seg); err != nil {
			return err
		}
		w.mu.Lock()
		w.segSize += int64(len(buf))
		if w.segFirst == 0 {
			w.segFirst = first
		}
		w.segLast = last
		w.mu.Unlock()
	}
	return nil
}

func (w *WAL) rotate() error {
	w.mu.Lock()
	old := w.seg
	closed := walSegment{seq: w.segSeq, path: old.Name(), firstLSN: w.segFirst, lastLSN: w.segLast, size: w.segSize}
	if w.segSize > 0 {
		w.segments = append(w.segments, closed)
	}
	w.mu.Unlock()

	if err := old.Sync(); err != nil {
		return err
	}
	if err := old.Close(); err != nil {
		return err
	}
	f, err := w.newSegment()
	if err != nil {
		return err
	}
	w.mu.Lock()
	w.seg = f
	w.mu.Unlock()
	return nil
}

func (w *WAL) failLocked(err error) {
	if w.err == nil {
		w.err = err
	}
	w.wakeupLocked()
}

func (w *WAL) wakeupLocked() {
	kept := w.waiters[:0]
	for _, wt := range w.waiters {
		if w.durable >= wt.lsn || w.err != nil {
			close(wt.ch)
			continue
		}
		kept = append(kept, wt)
	}
	w.waiters = kept
}

// Compact removes whole closed segments entirely covered by records below
// keepFromLSN. The open segment is always retained.
func (w *WAL) Compact(keepFromLSN uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if keepFromLSN <= 1 {
		return nil
	}
	kept := w.segments[:0]
	for _, s := range w.segments {
		if s.lastLSN == 0 || s.lastLSN >= keepFromLSN {
			kept = append(kept, s)
			continue
		}
		if err := os.Remove(s.path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	w.segments = kept
	return nil
}

// Close flushes, syncs and releases the directory lock.
func (w *WAL) Close() error {
	var err error
	w.closeOnce.Do(func() {
		close(w.closeReq)
		<-w.doneCh
		w.mu.Lock()
		seg := w.seg
		werr := w.err
		w.closed = true
		w.mu.Unlock()
		if seg != nil {
			if serr := seg.Sync(); serr != nil && werr == nil {
				werr = serr
			}
			if cerr := seg.Close(); cerr != nil && werr == nil {
				werr = cerr
			}
		}
		w.releaseLock()
		err = werr
	})
	return err
}

func (w *WAL) releaseLock() {
	if w.lock == nil {
		return
	}
	_ = syscall.Flock(int(w.lock.Fd()), syscall.LOCK_UN)
	w.lock.Close()
	w.lock = nil
}

// ---- small helpers ----

func fdatasync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
