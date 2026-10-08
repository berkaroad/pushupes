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

// Sealed per-segment command index: the on-disk replacement for the in-memory
// hash table.
//
// The slot's in-memory command index is the largest thing a node holds (32 B
// per record measured, 992 MiB of a 1353 MiB heap at 31 GiB). What a node
// actually needs in memory is a cheap "is this command id here at all" answer;
// the exact answer can come off the disk, because a duplicate command id means
// a client replay, not a hot path. So a sealed segment writes its commands
// sorted by hash into <baseSeq>.cidx, and a lookup binary-searches that file,
// reads the candidate record and compares the real command id — the same
// verification a hash-table hit already does.
//
//	<baseSeq>.cidx  header(32) + blocks of crc32c(4) + n * 12B
//	                entry = hash32(4) seqOff(4) off32(4), sorted by (hash32, seq)
//
// hash32 is the top of the command hash: several ids can share it, so a lookup
// walks the whole equal-hash run and verifies each candidate. The file is
// written once, when the segment seals, through a temp file and a rename: a
// partially written index is never trusted, it is simply absent for that
// segment and the next start rebuilds it.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"

	"github.com/berkaroad/pushupes/internal/data"
)

const (
	segCidxMagic      = "ESCI"
	segCidxHeaderByte = 32
	segCidxEntryBytes = 12
	segCidxBlockMax   = 256
)

// segCmdEntry is one record's entry in a sealed segment's command index.
type segCmdEntry struct {
	hash32 uint32
	seq    uint64
	off    int64 // file offset of the record's length prefix
}

type segCmdIndex struct {
	dir     string
	slotID  int32
	baseSeq uint64
	count   int
	f       *os.File

	block   int64  // block number currently cached
	cached  []byte // that block's payload
	invalid bool
	blm     *bloomSet // the segment's command bloom, when the file carries one
}

// segCidxBlocks is the number of entry blocks a file with count entries holds.
func segCidxBlocks(count int) int { return (count + segCidxBlockMax - 1) / segCidxBlockMax }

func segCidxPath(dir string, baseSeq uint64) string {
	return segIdxJoin(dir, baseSeq, ".cidx")
}

// writeSegCmdIndex walks a just-sealed segment and writes its commands sorted
// by hash to <baseSeq>.cidx (temp file + rename, so a partial index never
// exists). The walk is header-only and happens once per segment, at seal.
func writeSegCmdIndex(seg *Segment, filters []*bloomBits) error {
	dir := filepath.Dir(seg.Path)
	sorted, err := collectSegCmdEntries(seg)
	if err != nil {
		return err
	}
	if len(sorted) == 0 {
		return nil
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].hash32 != sorted[j].hash32 {
			return sorted[i].hash32 < sorted[j].hash32
		}
		return sorted[i].seq < sorted[j].seq
	})

	tmp := segCidxPath(dir, seg.BaseSeq) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	cleanup := func(e error) error {
		f.Close()
		os.Remove(tmp)
		return e
	}
	var bloom []byte
	if len(filters) > 0 {
		bloom = (&bloomSet{filters: filters}).encode()
	}
	hdr := segIdxHeaderBuf(segCidxMagic, seg.SlotID, seg.BaseSeq)
	binary.BigEndian.PutUint64(hdr[20:28], uint64(len(sorted)))
	binary.BigEndian.PutUint32(hdr[28:32], uint32(len(bloom)))
	if _, err := f.Write(hdr); err != nil {
		return cleanup(err)
	}
	for base := 0; base < len(sorted); base += segCidxBlockMax {
		end := base + segCidxBlockMax
		if end > len(sorted) {
			end = len(sorted)
		}
		payload := make([]byte, 0, (end-base)*segCidxEntryBytes)
		var buf [segCidxEntryBytes]byte
		for _, e := range sorted[base:end] {
			binary.BigEndian.PutUint32(buf[0:4], e.hash32)
			binary.BigEndian.PutUint32(buf[4:8], uint32(e.seq-seg.BaseSeq))
			binary.BigEndian.PutUint32(buf[8:12], uint32(e.off))
			payload = append(payload, buf[:]...)
		}
		block := make([]byte, 4+len(payload))
		binary.BigEndian.PutUint32(block[0:4], crc32.Checksum(payload, segIdxCRC))
		copy(block[4:], payload)
		if _, err := f.Write(block); err != nil {
			return cleanup(err)
		}
	}
	if len(bloom) > 0 {
		if _, err := f.Write(bloom); err != nil {
			return cleanup(err)
		}
	}
	if err := f.Sync(); err != nil {
		return cleanup(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, segCidxPath(dir, seg.BaseSeq))
}

// collectSegCmdEntries reads a segment's frame headers into sorted-ready
// entries. Offsets travel with the entry so a lookup needs no second seek.
func collectSegCmdEntries(seg *Segment) ([]segCmdEntry, error) {
	if seg.RecordCnt == 0 {
		return nil, nil
	}
	w := newFrameWalker(seg.File, int64(seg.dataStart), seg.sizeBytes)
	seq := seg.BaseSeq
	var out []segCmdEntry
	for {
		frame, off, err := w.next()
		if err == errShortTail || err == errBadRecordLen {
			break
		}
		if err != nil {
			return nil, err
		}
		meta, _, err := data.DecodeRecordMeta(frame)
		if err != nil {
			return nil, fmt.Errorf("segment %s seq %d: %w", seg.Path, seq, err)
		}
		if off > 1<<32-1 {
			// A segment whose offsets need more than 32 bits: no command index
			// (the lookup path falls back to reading by seq).
			return nil, nil
		}
		out = append(out, segCmdEntry{
			hash32: uint32(meta.CommandHash >> 32),
			seq:    seq,
			off:    off,
		})
		seq++
	}
	return out, nil
}

// openSegCmdIndex opens a sealed segment's command index; nil when it is not
// there or does not validate (the caller then falls back to walking).
func openSegCmdIndex(dir string, slotID int32, baseSeq uint64) (*segCmdIndex, error) {
	f, err := os.Open(segCidxPath(dir, baseSeq))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	h := make([]byte, segCidxHeaderByte)
	if err := readAtFull(f, h, 0); err != nil {
		f.Close()
		return nil, nil
	}
	if !checkSegIdxHeader(h, segCidxMagic, slotID, baseSeq) {
		f.Close()
		return nil, nil
	}
	count := int(binary.BigEndian.Uint64(h[20:28]))
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if count <= 0 || st.Size() < int64(segCidxHeaderByte)+int64((count+segCidxBlockMax-1)/segCidxBlockMax)*4 {
		f.Close()
		return nil, nil
	}
	ix := &segCmdIndex{dir: dir, slotID: slotID, baseSeq: baseSeq, count: count, f: f, block: -1}
	if bloomLen := int(binary.BigEndian.Uint32(h[28:32])); bloomLen > 0 {
		last := count % segCidxBlockMax
		entryBytes := int64(segCidxBlocks(count)) * int64(segCidxBlockMax*segCidxEntryBytes+4)
		if last != 0 {
			entryBytes -= int64(segCidxBlockMax*segCidxEntryBytes + 4 - (last*segCidxEntryBytes + 4))
		}
		buf := make([]byte, bloomLen)
		if err := readAtFull(f, buf, int64(segCidxHeaderByte)+entryBytes); err == nil {
			ix.blm = decodeBloomSet(buf)
		}
	}
	segIndexUsed.Add(1)
	return ix, nil
}

func (ix *segCmdIndex) Close() error {
	if ix == nil || ix.f == nil {
		return nil
	}
	err := ix.f.Close()
	ix.f = nil
	return err
}

// block returns the payload of block n (validated by its CRC), cached for the
// duration of one lookup.
func (ix *segCmdIndex) blockData(n int64) ([]byte, error) {
	if n < 0 || (n+1)*segCidxBlockMax > int64(ix.count)+segCidxBlockMax {
		return nil, nil
	}
	if ix.block == n {
		return ix.cached, nil
	}
	off := int64(segCidxHeaderByte) + n*(segCidxBlockMax*segCidxEntryBytes+4)
	payload := segCidxBlockMax * segCidxEntryBytes
	remaining := int64(ix.count)*segCidxEntryBytes - n*segCidxBlockMax*segCidxEntryBytes
	if remaining <= 0 {
		return nil, nil
	}
	if int64(payload) > remaining {
		payload = int(remaining)
	}
	buf := make([]byte, 4+payload)
	if err := readAtFull(ix.f, buf, off); err != nil {
		ix.invalid = true
		return nil, nil
	}
	if crc32.Checksum(buf[4:], segIdxCRC) != binary.BigEndian.Uint32(buf[0:4]) {
		ix.invalid = true
		return nil, nil
	}
	ix.block, ix.cached = n, buf[4:]
	return ix.cached, nil
}

func (ix *segCmdIndex) entryAt(i int64) (segCmdEntry, bool) {
	data, err := ix.blockData(i / segCidxBlockMax)
	if err != nil || data == nil {
		return segCmdEntry{}, false
	}
	p := (i % segCidxBlockMax) * segCidxEntryBytes
	if int(p)+segCidxEntryBytes > len(data) {
		return segCmdEntry{}, false
	}
	return segCmdEntry{
		hash32: binary.BigEndian.Uint32(data[p : p+4]),
		seq:    ix.baseSeq + uint64(binary.BigEndian.Uint32(data[p+4:p+8])),
		off:    int64(binary.BigEndian.Uint32(data[p+8 : p+12])),
	}, true
}

// Bloom returns the segment's command bloom filter (nil when the file has none
// or it did not validate).
func (ix *segCmdIndex) Bloom() *bloomSet {
	if ix == nil {
		return nil
	}
	return ix.blm
}

// Valid checks every block's CRC: a start uses it to tell "no index" from
// "an index that must be rewritten".
func (ix *segCmdIndex) Valid() bool {
	if ix == nil {
		return false
	}
	blocks := int64(segCidxBlocks(ix.count))
	for n := int64(0); n < blocks; n++ {
		ix.block = -1 // do not reuse a cache across blocks here
		if data, err := ix.blockData(n); err != nil || data == nil {
			ix.invalid = true
			return false
		}
	}
	return true
}

// LookupCmd walks every entry whose hash32 matches, calling verify for each
// candidate in seq order until it returns true. Missing entries and damaged
// blocks simply end the walk (a candidate we could not read is a candidate we
// do not report).
func (ix *segCmdIndex) LookupCmd(hash32 uint32, verify func(segCmdEntry) bool) (bool, error) {
	if ix == nil || ix.count == 0 {
		return false, nil
	}
	n := int64(ix.count)
	// First entry with hash32 >= target.
	lo := sort.Search(int(n), func(i int) bool {
		e, ok := ix.entryAt(int64(i))
		return !ok || e.hash32 >= hash32
	})
	if ix.invalid {
		return false, fmt.Errorf("segment %d: command index unreadable", ix.baseSeq)
	}
	for i := int64(lo); i < n; i++ {
		e, ok := ix.entryAt(i)
		if !ok {
			return false, nil // damage or end: nothing more we can trust
		}
		if e.hash32 != hash32 {
			break
		}
		if verify(e) {
			return true, nil
		}
	}
	return false, nil
}

// segIdxJoin is the shared path helper for the per-segment index files.
func segIdxJoin(dir string, baseSeq uint64, suffix string) string {
	return fmt.Sprintf("%s/%016d%s", dir, baseSeq, suffix)
}
