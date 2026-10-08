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

// Sealed per-segment aggregate index: the on-disk half of "which seq holds
// version v of aggregate A".
//
// The in-memory arena holds every aggregate's seqs for the whole slot (8 bytes
// per record, 248 MiB of a 327 MiB heap at 31 GiB). A sealed segment's part of
// that never changes again, so it belongs on disk: at seal the slot writes each
// aggregate's seqs for that segment into <baseSeq>.aidx and drops them from
// memory. Only the writable segment keeps its seqs in memory, because reading
// the newest versions of an aggregate is the hot path.
//
//	<baseSeq>.aidx  header(32) + entries(aggCount * 24B) + lists(totalSeq * 4B) + crc32c(4)
//	                entry = id hash(8) firstVer(4) count(4) firstOrd(4) listOff(4)
//
// A list holds each record's byte offset inside the segment, so serving a
// version is one pread on the segment — not a walk from the nearest sparse hint
// (at 1 MiB granularity that walk is about a thousand frames, which is what made
// a sealed read cost a millisecond). firstOrd is the segment-local ordinal of the
// entry's first record, so the record's seq is baseSeq+firstOrd+index.
//
// Ids are not stored: an entry is found by hashing the requested aggregate and
// binary-searching that fixed-size array, then checking a candidate by reading
// the record its seq points at — the same verification every other index here
// does. firstVer is the version of the entry's first seq, so a version maps to
// index v - firstVer inside that segment's list. The file is written once, at
// seal, through a temp file and a rename: a partial index does not exist.

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
)

const (
	segAidxVersion    = 4 // 1 = shared arena, 2 = in-segment seqs, 3 = offsets with count/firstOrd swapped
	segAidxMagic      = "ESAI"
	segAidxHeaderByte = 32
	segAidxEntryBytes = 24
	segAidxSeqsMax    = 1 << 22 // cap one list read at 16 MiB
)

// errSegAggNoOffsets means the segment's command index (which carries the
// per-record offsets) is missing or unreadable, so the aggregate index cannot be
// written for it: the slot keeps those seqs in memory and the next start retries.
var errSegAggNoOffsets = errors.New("sealed segment has no command index to take offsets from")

// segAggList is one aggregate's contribution to a sealed segment.
type segAggList struct {
	hash     uint64   // hash of the aggregate id
	firstVer uint32   // version of the entry's first record
	firstOrd uint32   // segment-local ordinal of that record (seq - baseSeq)
	pairs    []uint32 // per record: (segment ordinal, byte offset), in version order
}

// segAggEntry is one directory entry as stored.
type segAggEntry struct {
	hash     uint64
	firstVer uint32
	count    uint32
	firstOrd uint32
	listOff  uint32
}

func segAidxPath(dir string, baseSeq uint64) string {
	return segIdxJoin(dir, baseSeq, ".aidx")
}

// writeSegAggIndex writes a sealed segment's aggregate index. Best effort by
// design: a segment without one is still correct (a versioned read of it falls
// back to reading by seq), and the next start writes it.
func writeSegAggIndex(seg *Segment, lists []segAggList) error {
	total := 0
	for _, l := range lists {
		total += len(l.pairs) / 2
	}
	if len(lists) == 0 || total == 0 {
		return nil
	}
	sorted := append([]segAggList(nil), lists...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].hash < sorted[j].hash })

	entries := make([]byte, len(sorted)*segAidxEntryBytes)
	seqs := make([]byte, total*8) // two uint32 per record: ordinal and offset
	off := 0
	for i, l := range sorted {
		p := i * segAidxEntryBytes
		binary.BigEndian.PutUint64(entries[p:p+8], l.hash)
		binary.BigEndian.PutUint32(entries[p+8:p+12], l.firstVer)
		binary.BigEndian.PutUint32(entries[p+12:p+16], uint32(len(l.pairs)/2))
		binary.BigEndian.PutUint32(entries[p+16:p+20], l.firstOrd)
		binary.BigEndian.PutUint32(entries[p+20:p+24], uint32(off/2))
		for _, v := range l.pairs {
			binary.BigEndian.PutUint32(seqs[off:off+4], v)
			off += 4
		}
	}
	crc := crc32.New(segIdxCRC)
	crc.Write(entries)
	crc.Write(seqs)

	dir := filepath.Dir(seg.Path)
	tmp := segAidxPath(dir, seg.BaseSeq) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	cleanup := func(e error) error {
		f.Close()
		os.Remove(tmp)
		return e
	}
	hdr := segIdxHeaderBuf(segAidxMagic, seg.SlotID, seg.BaseSeq)
	hdr[4] = segAidxVersion
	binary.BigEndian.PutUint32(hdr[20:24], uint32(len(sorted)))
	binary.BigEndian.PutUint64(hdr[24:32], uint64(total))
	for _, part := range [][]byte{hdr, entries, seqs} {
		if _, err := f.Write(part); err != nil {
			return cleanup(err)
		}
	}
	var tail [4]byte
	binary.BigEndian.PutUint32(tail[:], crc.Sum32())
	if _, err := f.Write(tail[:]); err != nil {
		return cleanup(err)
	}
	if err := f.Sync(); err != nil {
		return cleanup(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, segAidxPath(dir, seg.BaseSeq))
}

// segAggIndex is an opened, validated aggregate index.
type segAggIndex struct {
	baseSeq uint64
	aggN    int
	seqN    int
	f       *os.File
	entries []byte // cached directory (aggN * 20B)
}

// openSegAggIndex opens a sealed segment's aggregate index; nil when it is not
// there or does not validate.
func openSegAggIndex(dir string, slotID int32, baseSeq uint64) (*segAggIndex, error) {
	f, err := os.Open(segAidxPath(dir, baseSeq))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	h := make([]byte, segAidxHeaderByte)
	if err := readAtFull(f, h, 0); err != nil {
		f.Close()
		return nil, nil
	}
	if !checkSegIdxHeaderVer(h, segAidxVersion, segAidxMagic, slotID, baseSeq) {
		f.Close()
		return nil, nil
	}
	aggN := int(binary.BigEndian.Uint32(h[20:24]))
	seqN := int(binary.BigEndian.Uint64(h[24:32]))
	if aggN <= 0 || seqN <= 0 {
		f.Close()
		return nil, nil
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	want := int64(segAidxHeaderByte) + int64(aggN)*segAidxEntryBytes + int64(seqN)*8 + 4
	if st.Size() != want {
		f.Close()
		return nil, nil
	}
	ix := &segAggIndex{baseSeq: baseSeq, aggN: aggN, seqN: seqN, f: f}
	ix.entries = make([]byte, aggN*segAidxEntryBytes)
	if err := readAtFull(f, ix.entries, segAidxHeaderByte); err != nil {
		f.Close()
		return nil, err
	}
	return ix, nil
}

// Valid checks the file's trailing CRC over the directory and the lists.
func (ix *segAggIndex) Valid() bool {
	if ix == nil {
		return false
	}
	crc := crc32.New(segIdxCRC)
	crc.Write(ix.entries)
	seqs := make([]byte, ix.seqN*8)
	if err := readAtFull(ix.f, seqs, int64(segAidxHeaderByte+ix.aggN*segAidxEntryBytes)); err != nil {
		return false
	}
	crc.Write(seqs)
	var tail [4]byte
	if err := readAtFull(ix.f, tail[:], int64(segAidxHeaderByte+ix.aggN*segAidxEntryBytes+ix.seqN*8)); err != nil {
		return false
	}
	return crc.Sum32() == binary.BigEndian.Uint32(tail[:])
}

func (ix *segAggIndex) Close() error {
	if ix == nil || ix.f == nil {
		return nil
	}
	err := ix.f.Close()
	ix.f = nil
	return err
}

func (ix *segAggIndex) entryAt(i int) segAggEntry {
	p := i * segAidxEntryBytes
	return segAggEntry{
		hash:     binary.BigEndian.Uint64(ix.entries[p : p+8]),
		firstVer: binary.BigEndian.Uint32(ix.entries[p+8 : p+12]),
		count:    binary.BigEndian.Uint32(ix.entries[p+12 : p+16]),
		firstOrd: binary.BigEndian.Uint32(ix.entries[p+16 : p+20]),
		listOff:  binary.BigEndian.Uint32(ix.entries[p+20 : p+24]),
	}
}

// Lookup calls fn for every entry whose aggregate hash matches, in list order,
// until fn returns true. Candidates share a hash, so the caller confirms the
// real aggregate id by reading a record.
func (ix *segAggIndex) Lookup(hash uint64, fn func(segAggEntry) bool) bool {
	if ix == nil || ix.aggN == 0 {
		return false
	}
	lo := sort.Search(ix.aggN, func(i int) bool { return ix.entryAt(i).hash >= hash })
	for i := lo; i < ix.aggN; i++ {
		e := ix.entryAt(i)
		if e.hash != hash {
			break
		}
		if fn(e) {
			return true
		}
	}
	return false
}

// Covered returns the set of aggregate hashes the index holds an entry for.
// A segment's index is all-or-nothing per aggregate: an aggregate it lists has
// every one of its seqs in the segment recorded, one it does not list has none.
// The slot uses this to drop from memory exactly the seqs the file can serve.
func (ix *segAggIndex) Covered() map[uint64]bool {
	if ix == nil || ix.aggN == 0 {
		return nil
	}
	m := make(map[uint64]bool, ix.aggN)
	for i := 0; i < ix.aggN; i++ {
		m[ix.entryAt(i).hash] = true
	}
	return m
}

// Pair reads the entry's i-th record as (segment ordinal, byte offset). The
// ordinal is what the index cannot derive: an aggregate's records inside a
// segment are interleaved with other aggregates', so their ordinals are not
// consecutive and the seq is baseSeq+ordinal, not baseSeq+firstOrd+i.
func (ix *segAggIndex) Pair(e segAggEntry, i int) (uint32, uint32, bool) {
	if ix == nil || i < 0 || i >= int(e.count) {
		return 0, 0, false
	}
	var buf [8]byte
	off := int64(segAidxHeaderByte) + int64(ix.aggN)*segAidxEntryBytes + int64(e.listOff)*2 + int64(i)*8
	if err := readAtFull(ix.f, buf[:], off); err != nil {
		return 0, 0, false
	}
	return binary.BigEndian.Uint32(buf[0:4]), binary.BigEndian.Uint32(buf[4:8]), true
}
