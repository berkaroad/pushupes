package storage

// Per-segment index files.
//
// The WAL is the source of truth and these files are derived from it: a node
// can always rebuild them by walking the frame headers. So the one rule the
// format has to enforce is that a damaged or stale index is *detected*, never
// trusted. Entries are append-only, written after the frame they describe, and
// every block carries a CRC — what loads is the longest validated prefix and
// the WAL tail beyond it is replayed. A missing, truncated, corrupt or
// regressed index therefore costs startup work, never correctness.
//
//	<baseSeq>.idx  header(32) + blocks: crc32c(4) + n * entry(20)
//	               entry = command hash(8) seqOff(4) dictID(4) version(4)
//	<baseSeq>.agx  header(32) + entries: crc32c(4) dictID(4) idLen(2) id
//	<baseSeq>.spx  header(32) + blocks: crc32c(4) + n * entry(16)
//	               entry = seq(8) file offset(8), the sparse seek index
//
// The aggregate id is not repeated per record: a segment-local dictionary maps
// a small integer to the id once (a slot's ids are long and repeated, so this
// is what keeps an entry 20 bytes instead of 50).

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

const (
	segIdxMagic = "ESIX"
	segAgxMagic = "ESAG"
	segSpxMagic = "ESSP"

	segIdxHeaderBytes = 32
	segIdxBlockMax    = 256 // entries per block
)

const (
	segIdxEntryBytes = 20 // hash(8) seqOff(4) dictID(4) version(4)
	segSpxEntryBytes = 16 // seq(8) pos(8)
)

var segIdxCRC = crc32.MakeTable(crc32.Castagnoli)

// Counters for the two paths a segment's index can take at startup: how often
// it was trusted, and how often it had to be distrusted (missing, stale past
// its coverage is fine; damaged is what these count).
var (
	segIndexUsed     atomic.Uint64 // segments restored from a valid index
	segIndexDamaged  atomic.Uint64 // index files that failed validation
	segIndexBuilt    atomic.Uint64 // segments whose index was written at startup
	segIndexReplayed atomic.Uint64 // records replayed past the index coverage
	segIndexUnusable atomic.Uint64 // segments that had to be walked (no index)
	segCmdBuilt      atomic.Uint64 // sealed command indexes written at startup
	segAggBuilt      atomic.Uint64 // sealed aggregate indexes written at startup
)

// segIndexEntry is one record's index entry.
type segIndexEntry struct {
	hash    uint64 // command hash (full; the in-memory table is keyed by it)
	seq     uint64
	aggID   string
	version uint32
}

// segIndexLoad is what a valid index file yielded.
type segIndexLoad struct {
	entries []segIndexEntry // records, in WAL order; coverage = len(entries)
	sparse  []indexEntry    // seq -> file offset, ascending
}

func segIdxPath(dir string, baseSeq uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%016d.idx", baseSeq))
}

func segAgxPath(dir string, baseSeq uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%016d.agx", baseSeq))
}

func segSpxPath(dir string, baseSeq uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%016d.spx", baseSeq))
}

func segIdxHeaderBuf(magic string, slotID int32, baseSeq uint64) []byte {
	h := make([]byte, segIdxHeaderBytes)
	copy(h, magic)
	h[4] = 1 // format version
	binary.BigEndian.PutUint32(h[8:12], uint32(slotID))
	binary.BigEndian.PutUint64(h[12:20], baseSeq)
	return h
}

// checkSegIdxHeader validates magic, version, slot and base seq.
func checkSegIdxHeader(h []byte, magic string, slotID int32, baseSeq uint64) bool {
	return checkSegIdxHeaderVer(h, 1, magic, slotID, baseSeq)
}

// checkSegIdxHeaderVer is checkSegIdxHeader with an explicit format version: the
// aggregate index bumped its version when the seq list became per-aggregate, so
// files written by the older, wrong layout are rejected and rewritten instead of
// trusted (a CRC cannot tell that a value is the wrong seq).
func checkSegIdxHeaderVer(h []byte, ver byte, magic string, slotID int32, baseSeq uint64) bool {
	if len(h) < segIdxHeaderBytes || string(h[:4]) != magic || h[4] != ver {
		return false
	}
	if int32(binary.BigEndian.Uint32(h[8:12])) != slotID {
		return false
	}
	return binary.BigEndian.Uint64(h[12:20]) == baseSeq
}

// segIndexWriter appends one segment's index entries. All three files are
// append-only; the record index is buffered a block at a time.
type segIndexWriter struct {
	dir     string
	slotID  int32
	baseSeq uint64

	idx *os.File
	agx *os.File
	spx *os.File

	idxOff int64 // write offset in the .idx file
	spxOff int64

	block  []byte // buffered record entries
	sparse []byte // buffered sparse entries

	dictIDs  map[string]uint32
	nextDict uint32

	err error // latched: stop indexing, the loader will fall back
}

// openSegIndexWriter opens (or continues) the index of one segment. Torn
// blocks from a crash are dropped by truncating to the validated prefix, so
// replayed records rewrite exactly the entries that were lost.
func openSegIndexWriter(dir string, slotID int32, baseSeq uint64) (*segIndexWriter, error) {
	w := &segIndexWriter{dir: dir, slotID: slotID, baseSeq: baseSeq, dictIDs: map[string]uint32{}}
	load, err := loadSegIndex(dir, slotID, baseSeq, -1, -1)
	if err != nil {
		return nil, err
	}
	if load != nil {
		w.idxOff = int64(blockBytes(len(load.entries)))
		w.spxOff = int64(blockBytes16(len(load.sparse)))
	}
	if w.agx, err = openAppendFile(segAgxPath(dir, baseSeq), segAgxMagic, slotID, baseSeq); err != nil {
		return nil, err
	}
	if w.idx, err = openAppendFile(segIdxPath(dir, baseSeq), segIdxMagic, slotID, baseSeq); err != nil {
		w.agx.Close()
		return nil, err
	}
	if w.spx, err = openAppendFile(segSpxPath(dir, baseSeq), segSpxMagic, slotID, baseSeq); err != nil {
		w.idx.Close()
		w.agx.Close()
		return nil, err
	}
	dict, agxEnd, err := loadSegDict(dir, slotID, baseSeq)
	if err != nil {
		w.close()
		return nil, err
	}
	for _, d := range dict {
		w.dictIDs[d.id] = d.dictID
		if d.dictID >= w.nextDict {
			w.nextDict = d.dictID + 1
		}
	}
	// Drop everything past the validated prefix: a torn block must not come
	// back to life when the replayed records are indexed again. idxOff/spxOff
	// count block bytes (header excluded); agxEnd is already absolute.
	for _, t := range []struct {
		f   *os.File
		end int64
	}{
		{w.idx, segIdxHeaderBytes + w.idxOff},
		{w.spx, segIdxHeaderBytes + w.spxOff},
		{w.agx, agxEnd},
	} {
		if err := t.f.Truncate(t.end); err != nil {
			w.close()
			return nil, err
		}
	}
	return w, nil
}

// openAppendFile opens an index file for reading and writing, creating it with
// a header when new.
func openAppendFile(path, magic string, slotID int32, baseSeq uint64) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() == 0 {
		if _, err := f.WriteAt(segIdxHeaderBuf(magic, slotID, baseSeq), 0); err != nil {
			f.Close()
			return nil, err
		}
		return f, nil
	}
	h := make([]byte, segIdxHeaderBytes)
	if _, err := f.ReadAt(h, 0); err != nil {
		f.Close()
		return nil, err
	}
	if !checkSegIdxHeader(h, magic, slotID, baseSeq) {
		f.Close()
		return nil, fmt.Errorf("%s: bad index header", filepath.Base(path))
	}
	return f, nil
}

// A block is [crc32c(4)][count(2)][count * entryBytes]: the entry count is on
// disk, so a reader never infers block boundaries from the remaining file size
// (blocks can be any size, and several small ones may sit side by side).
const segIdxBlockHead = 6

// blockBytes is the on-disk size of the first n record entries.
func blockBytes(n int) int {
	full := n / segIdxBlockMax
	rem := n % segIdxBlockMax
	total := full * (segIdxBlockHead + segIdxBlockMax*segIdxEntryBytes)
	if rem > 0 {
		total += segIdxBlockHead + rem*segIdxEntryBytes
	}
	return total
}

func blockBytes16(n int) int {
	full := n / segIdxBlockMax
	rem := n % segIdxBlockMax
	total := full * (segIdxBlockHead + segIdxBlockMax*segSpxEntryBytes)
	if rem > 0 {
		total += segIdxBlockHead + rem*segSpxEntryBytes
	}
	return total
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// add records one record's index entry, assigning its aggregate a dictionary
// id first so no entry ever references an id that is not on disk.
func (w *segIndexWriter) add(hash uint64, seq uint64, aggregateID string, version uint32) {
	if w.err != nil {
		return
	}
	if seq < w.baseSeq || seq-w.baseSeq > 1<<32-1 {
		w.err = fmt.Errorf("segment %d: seq %d outside index range", w.baseSeq, seq)
		return
	}
	dictID, ok := w.dictIDs[aggregateID]
	if !ok {
		dictID = w.nextDict
		w.nextDict++
		w.dictIDs[aggregateID] = dictID
		payload := make([]byte, 6+len(aggregateID))
		binary.BigEndian.PutUint32(payload[0:4], dictID)
		binary.BigEndian.PutUint16(payload[4:6], uint16(len(aggregateID)))
		copy(payload[6:], aggregateID)
		entry := make([]byte, 4+len(payload))
		binary.BigEndian.PutUint32(entry[0:4], crc32.Checksum(payload, segIdxCRC))
		copy(entry[4:], payload)
		if _, err := w.agx.WriteAt(entry, w.agxEnd()); err != nil {
			w.err = err
			return
		}
		// The dictionary must be durable before any entry can reference it.
		if err := w.agx.Sync(); err != nil {
			w.err = err
			return
		}
	}
	var buf [segIdxEntryBytes]byte
	binary.BigEndian.PutUint64(buf[0:8], hash)
	binary.BigEndian.PutUint32(buf[8:12], uint32(seq-w.baseSeq))
	binary.BigEndian.PutUint32(buf[12:16], dictID)
	binary.BigEndian.PutUint32(buf[16:20], version)
	w.block = append(w.block, buf[:]...)
	if len(w.block) >= segIdxBlockMax*segIdxEntryBytes {
		w.flushBlock()
	}
}

// addSparse records where in the file a seq lives (the segment's sparse seek
// index), so a loaded segment can seek without walking the WAL.
func (w *segIndexWriter) addSparse(seq uint64, pos int64) {
	if w.err != nil {
		return
	}
	var buf [segSpxEntryBytes]byte
	binary.BigEndian.PutUint64(buf[0:8], seq)
	binary.BigEndian.PutUint64(buf[8:16], uint64(pos))
	w.sparse = append(w.sparse, buf[:]...)
	if len(w.sparse) >= segIdxBlockMax*segSpxEntryBytes {
		w.flushSparse()
	}
}

func (w *segIndexWriter) agxEnd() int64 {
	st, err := w.agx.Stat()
	if err != nil {
		return segIdxHeaderBytes
	}
	return st.Size()
}

// flushBlock writes the buffered record entries as one CRC-framed block.
func (w *segIndexWriter) flushBlock() {
	if w.err != nil || len(w.block) == 0 {
		return
	}
	buf := make([]byte, segIdxBlockHead+len(w.block))
	binary.BigEndian.PutUint16(buf[4:6], uint16(len(w.block)/segIdxEntryBytes))
	copy(buf[6:], w.block)
	binary.BigEndian.PutUint32(buf[0:4], crc32.Checksum(buf[4:], segIdxCRC))
	if _, err := w.idx.WriteAt(buf, w.idxOff+segIdxHeaderBytes); err != nil {
		w.err = err
		return
	}
	w.idxOff += int64(len(buf))
	w.block = w.block[:0]
}

func (w *segIndexWriter) flushSparse() {
	if w.err != nil || len(w.sparse) == 0 {
		return
	}
	buf := make([]byte, segIdxBlockHead+len(w.sparse))
	binary.BigEndian.PutUint16(buf[4:6], uint16(len(w.sparse)/segSpxEntryBytes))
	copy(buf[6:], w.sparse)
	binary.BigEndian.PutUint32(buf[0:4], crc32.Checksum(buf[4:], segIdxCRC))
	if _, err := w.spx.WriteAt(buf, w.spxOff+segIdxHeaderBytes); err != nil {
		w.err = err
		return
	}
	w.spxOff += int64(len(buf))
	w.sparse = w.sparse[:0]
}

// flush pushes the buffers to the files and makes them durable.
func (w *segIndexWriter) flush() error {
	w.flushBlock()
	w.flushSparse()
	if w.err != nil {
		return w.err
	}
	for _, f := range []*os.File{w.idx, w.spx, w.agx} {
		if f == nil {
			continue
		}
		if err := f.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (w *segIndexWriter) close() error {
	if w == nil {
		return nil
	}
	err := w.flush()
	for _, f := range []*os.File{w.idx, w.agx, w.spx} {
		if f != nil {
			if cerr := f.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}
	}
	w.idx, w.agx, w.spx = nil, nil, nil
	return err
}

// truncateToSeq drops entries for seqs above lastSeq: a WAL that was truncated
// must not keep an index promising records that are gone. sparse is the
// segment's (rebuilt) sparse index, which is rewritten to match.
func (w *segIndexWriter) truncateToSeq(lastSeq uint64, sparse []indexEntry) error {
	if w == nil {
		return nil
	}
	keep := 0
	if lastSeq >= w.baseSeq {
		keep = int(lastSeq-w.baseSeq) + 1
	}
	if err := w.idx.Truncate(int64(segIdxHeaderBytes + blockBytes(keep))); err != nil {
		return err
	}
	w.idxOff = int64(blockBytes(keep))
	w.block = w.block[:0]

	var kept []byte
	for _, e := range sparse {
		if e.seq > lastSeq {
			break
		}
		var buf [segSpxEntryBytes]byte
		binary.BigEndian.PutUint64(buf[0:8], e.seq)
		binary.BigEndian.PutUint64(buf[8:16], uint64(e.pos))
		kept = append(kept, buf[:]...)
	}
	if err := w.spx.Truncate(segIdxHeaderBytes); err != nil {
		return err
	}
	w.sparse = w.sparse[:0]
	w.spxOff = 0
	if len(kept) > 0 {
		buf := make([]byte, segIdxBlockHead+len(kept))
		binary.BigEndian.PutUint16(buf[4:6], uint16(len(kept)/segSpxEntryBytes))
		copy(buf[6:], kept)
		binary.BigEndian.PutUint32(buf[0:4], crc32.Checksum(buf[4:], segIdxCRC))
		if _, err := w.spx.WriteAt(buf, segIdxHeaderBytes); err != nil {
			return err
		}
		w.spxOff = int64(len(buf))
	}
	return w.flush()
}

// broken reports that indexing stopped for this segment (its files will be
// rejected at load and the WAL walked instead).
func (w *segIndexWriter) broken() bool { return w != nil && w.err != nil }

// loadSegIndex reads a segment's index. It returns (nil, nil) when the index is
// absent, damaged or not trustworthy — the caller then walks the WAL. maxSeq
// and maxPos cap the result at what the WAL actually holds (a torn tail or a
// truncated segment must not leave the index pointing beyond the end).
func loadSegIndex(dir string, slotID int32, baseSeq uint64, maxEntries int, maxPos int64) (*segIndexLoad, error) {
	dict, _, err := loadSegDict(dir, slotID, baseSeq)
	if err != nil {
		return nil, err
	}
	if dict == nil {
		return nil, nil // no dictionary: nothing can be resolved
	}
	byID := make(map[uint32]string, len(dict))
	for _, d := range dict {
		byID[d.dictID] = d.id
	}

	entries, damaged, release, err := loadIdxEntries(segIdxPath(dir, baseSeq), segIdxMagic, slotID, baseSeq, segIdxEntryBytes, maxEntries)
	if err != nil {
		return nil, err
	}
	defer release()
	if damaged {
		segIndexDamaged.Add(1)
	}
	segIndexUsed.Add(1)
	// The entry count is known before the first append, so size the slice once
	// instead of letting it double: 123M entries at ~40 bytes per entry doubled
	// away 12.7 GB of the 21.5 GB this load allocated.
	load := &segIndexLoad{entries: make([]segIndexEntry, 0, entries.count)}
	for i := 0; i < entries.count; i++ {
		raw := entries.at(i)
		dictID := binary.BigEndian.Uint32(raw[12:16])
		id, ok := byID[dictID]
		if !ok {
			// An entry referencing an id that is not in the dictionary means
			// the pair is not consistent: distrust the whole thing.
			segIndexDamaged.Add(1)
			return nil, nil
		}
		load.entries = append(load.entries, segIndexEntry{
			hash:    binary.BigEndian.Uint64(raw[0:8]),
			seq:     baseSeq + uint64(binary.BigEndian.Uint32(raw[8:12])),
			aggID:   id,
			version: binary.BigEndian.Uint32(raw[16:20]),
		})
	}
	if len(load.entries) == 0 && maxEntries > 0 {
		return nil, nil // an empty index for a non-empty segment is no index
	}
	sparse, spxDamaged, spxRelease, err := loadIdxEntries(segSpxPath(dir, baseSeq), segSpxMagic, slotID, baseSeq, segSpxEntryBytes, -1)
	if err != nil {
		return nil, err
	}
	defer spxRelease()
	if spxDamaged {
		segIndexDamaged.Add(1)
	}
	load.sparse = make([]indexEntry, 0, sparse.count)
	for i := 0; i < sparse.count; i++ {
		raw := sparse.at(i)
		seq := binary.BigEndian.Uint64(raw[0:8])
		pos := int64(binary.BigEndian.Uint64(raw[8:16]))
		if maxPos >= 0 && pos >= maxPos {
			break
		}
		load.sparse = append(load.sparse, indexEntry{seq: seq, pos: pos})
	}
	return load, nil
}

// idxFlat is a whole index file's validated prefix in one buffer, entries dense
// at a fixed stride: entry i is buf[i*stride:(i+1)*stride]. The [][]byte this
// replaces allocated a slice header per entry — 262k of them for one segment —
// which on a 31 GiB load meant 8.8 GB of garbage to say the same thing.
type idxFlat struct {
	buf    []byte
	stride int
	count  int
}

func (f idxFlat) at(i int) []byte { return f.buf[i*f.stride : (i+1)*f.stride] }

// flatBufPool recycles those buffers: segments load one after another, so a
// single buffer per size serves the whole load instead of a fresh 5 MiB per
// segment (471 segments on the 31 GiB node).
var flatBufPool = sync.Pool{New: func() any { b := make([]byte, 0, 1<<20); return &b }}

// loadIdxEntries reads the longest CRC-validated prefix of a block file. A
// damaged (second return) file is not an error: the validated prefix is still
// usable and the caller replays the rest from the WAL. The third return lends
// the buffer back to the pool and must be called once the entries are consumed.
func loadIdxEntries(path, magic string, slotID int32, baseSeq uint64, entryBytes int, maxEntries int) (idxFlat, bool, func(), error) {
	noop := func() {}
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return idxFlat{}, false, noop, nil
		}
		return idxFlat{}, false, noop, err
	}
	defer f.Close()
	h := make([]byte, segIdxHeaderBytes)
	if err := readAtFull(f, h, 0); err != nil {
		return idxFlat{}, true, noop, nil
	}
	if !checkSegIdxHeader(h, magic, slotID, baseSeq) {
		return idxFlat{}, true, noop, nil
	}
	st, err := f.Stat()
	if err != nil {
		return idxFlat{}, false, noop, err
	}
	size := st.Size()
	// One recycled buffer holds the whole file. Each block is read at the tail
	// of it (CRC + entry count + entries) and, once validated, its payload is
	// moved down over that six byte header, so the entries end up dense and the
	// caller indexes them as buf[i*entryBytes:].
	bufp := flatBufPool.Get().(*[]byte)
	buf := (*bufp)[:0]
	// Size it once: the file length bounds the dense result, so the block reads
	// below never have to grow (growing per block copied the whole prefix every
	// time — O(n^2), and 460 GiB of garbage on the 31 GiB node).
	want := int(size) // an upper bound: the dense result drops the block headers
	if maxEntries > 0 && maxEntries*entryBytes < want {
		want = maxEntries*entryBytes + segIdxBlockHead*(maxEntries/segIdxBlockHead+1)
	}
	if cap(buf) < want {
		buf = make([]byte, 0, want)
	}
	release := func() {
		if cap(buf) > 32<<20 { // do not pin a huge buffer in the pool
			return
		}
		*bufp = buf[:0]
		flatBufPool.Put(bufp)
	}
	damaged := func() (idxFlat, bool, func(), error) {
		return idxFlat{buf: buf, stride: entryBytes, count: len(buf) / entryBytes}, true, release, nil
	}
	var head [segIdxBlockHead]byte
	for off := int64(segIdxHeaderBytes); off < size; {
		if size-off < int64(segIdxBlockHead) {
			return damaged()
		}
		if err := readAtFull(f, head[:], off); err != nil {
			return idxFlat{}, false, release, err
		}
		count := int(binary.BigEndian.Uint16(head[4:6]))
		payload := int64(count) * int64(entryBytes)
		if count <= 0 || off+int64(segIdxBlockHead)+payload > size {
			return damaged() // torn block: valid prefix only
		}
		if maxEntries > 0 && len(buf)/entryBytes >= maxEntries {
			break
		}
		start := len(buf)
		end := start + segIdxBlockHead + int(payload)
		if cap(buf) < end {
			grown := make([]byte, end)
			copy(grown, buf)
			buf = grown
		}
		buf = buf[:end]
		if err := readAtFull(f, buf[start:], off); err != nil {
			return idxFlat{}, false, release, err
		}
		if crc32.Checksum(buf[start+4:end], segIdxCRC) != binary.BigEndian.Uint32(buf[start:start+4]) {
			buf = buf[:start]
			return damaged()
		}
		n := int(binary.BigEndian.Uint16(buf[start+4 : start+6]))
		copy(buf[start:], buf[start+segIdxBlockHead:end])
		buf = buf[:start+n*entryBytes]
		off += int64(segIdxBlockHead) + payload
	}
	if maxEntries > 0 {
		if n := len(buf) / entryBytes; n > maxEntries {
			buf = buf[:maxEntries*entryBytes]
		}
	}
	return idxFlat{buf: buf, stride: entryBytes, count: len(buf) / entryBytes}, false, release, nil
}

// readAtFull reads exactly len(buf) bytes at off (a short read is an error).
func readAtFull(f *os.File, buf []byte, off int64) error {
	n, err := f.ReadAt(buf, off)
	if err == nil && n != len(buf) {
		return fmt.Errorf("short read: %d/%d", n, len(buf))
	}
	return err
}

// scanIdxEntries walks a block file's entries in order, calling fn for each
// until fn returns false. A damaged block ends the walk: only the validated
// prefix is ever handed out.
func scanIdxEntries(path, magic string, slotID int32, baseSeq uint64, entryBytes int, fn func(raw []byte) bool) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()
	h := make([]byte, segIdxHeaderBytes)
	if err := readAtFull(f, h, 0); err != nil {
		return nil
	}
	if !checkSegIdxHeader(h, magic, slotID, baseSeq) {
		return nil
	}
	st, err := f.Stat()
	if err != nil {
		return err
	}
	size := st.Size()
	for off := int64(segIdxHeaderBytes); off < size; {
		if size-off < int64(segIdxBlockHead) {
			return nil
		}
		head := make([]byte, segIdxBlockHead)
		if err := readAtFull(f, head, off); err != nil {
			return err
		}
		count := int(binary.BigEndian.Uint16(head[4:6]))
		payload := int64(count) * int64(entryBytes)
		if count <= 0 || off+int64(segIdxBlockHead)+payload > size {
			return nil
		}
		buf := make([]byte, int64(segIdxBlockHead)+payload)
		if err := readAtFull(f, buf, off); err != nil {
			return err
		}
		if crc32.Checksum(buf[4:], segIdxCRC) != binary.BigEndian.Uint32(buf[0:4]) {
			return nil
		}
		for i := int64(0); i < payload; i += int64(entryBytes) {
			if !fn(buf[segIdxBlockHead+i : segIdxBlockHead+i+int64(entryBytes)]) {
				return nil
			}
		}
		off += int64(segIdxBlockHead) + payload
	}
	return nil
}

// segDictEntry is one aggregate dictionary entry.
type segDictEntry struct {
	dictID uint32
	id     string
}

// loadSegDict reads the validated prefix of a segment's aggregate dictionary.
func loadSegDict(dir string, slotID int32, baseSeq uint64) ([]segDictEntry, int64, error) {
	f, err := os.Open(segAgxPath(dir, baseSeq))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	defer f.Close()
	h := make([]byte, segIdxHeaderBytes)
	if err := readAtFull(f, h, 0); err != nil {
		return nil, 0, nil
	}
	if !checkSegIdxHeader(h, segAgxMagic, slotID, baseSeq) {
		return nil, 0, nil
	}
	st, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	size := st.Size()
	var out []segDictEntry
	off := int64(segIdxHeaderBytes)
	for off < size {
		if size-off < 4+6 {
			break
		}
		head := make([]byte, 10)
		if err := readAtFull(f, head, off); err != nil {
			return out, off, err
		}
		idLen := int(binary.BigEndian.Uint16(head[8:10]))
		if idLen < 0 || size-off < int64(4+6+idLen) {
			break
		}
		payload := make([]byte, 6+idLen)
		if err := readAtFull(f, payload, off+4); err != nil {
			return out, off, err
		}
		if crc32.Checksum(payload, segIdxCRC) != binary.BigEndian.Uint32(head[0:4]) {
			break
		}
		out = append(out, segDictEntry{
			dictID: binary.BigEndian.Uint32(payload[0:4]),
			id:     string(payload[6:]),
		})
		off += int64(4 + 6 + idLen)
	}
	return out, off, nil
}
