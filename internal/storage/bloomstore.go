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

// The slot's command-dedup bloom filter.
//
// Rule 1 asks one question of every append: "is this command id already
// stored?" An exact answer costs a record read (32 B per record in a hash
// table, measured at 992 MiB of a 1353 MiB heap at 31 GiB). A bloom filter
// answers the question in the direction that needs no read — a definite
// "absent" — at about 2.5 B per record, and its answers are safe in the only
// direction that matters: "absent" is reported only when every probed bit is
// zero, so a lookup that skips the disk can never miss a stored command;
// "maybe" costs a lookup that confirms against the record it points at.
//
// # Generations
//
// A slot has no record bound, so the filter grows by generations: each
// generation is one fixed bit array that takes bloomGenRecords hashes, and a
// full generation is kept while the next one opens. A lookup walks every
// generation, so the slot-wide false-positive rate is the SUM over
// generations (not the minimum): ten generations at 10⁹ records is 0.049%,
// which stays under the 0.1% target. Generation count is bounded by records
// alone — bloomGenRecords is the knob that trades memory per file against
// how many files a lookup walks.
//
// # Storage
//
// A generation's bits live in a sparse file under <slot>/bloom/, mapped
// MAP_SHARED. Pages are committed as they are written and are file-backed
// afterwards, so the filter of an idle slot is reclaimed by the page cache
// like any other file page instead of being pinned in the heap. A slot with
// 10⁹ records maps 2.5 GiB of address space and holds only the pages its
// probes touched.
//
// # Durability
//
// The persisted watermark is conservative: the bit array is synced first,
// then the header records the highest seq placed in it, so coveredSeq never
// claims more than the bits actually hold. Startup replays records after
// coveredSeq and re-places their hashes — the WAL is the truth, the filter
// is its projection. A header that does not validate makes that generation
// and every later one unusable: they are dropped and the replay rebuilds
// them, which costs the replay of those records and never a wrong "absent".
//
// A filter that could not be opened or grown (ENOSPC, a read-only directory)
// marks the store unsafe: it answers "maybe" for everything, so the slot
// serves from its exact indexes and never hides a stored command.

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// One generation: header pages and the bit array.
const (
	bloomHdrPage  = 4096 // two of them, written with the same contents
	bloomHdrMagic = "PSBLOOM1"
	bloomHdrVer   = 1
)

// bloomGen is one generation's bit array and its on-disk header.
type bloomGen struct {
	path       string
	f          *os.File
	bits       []byte // mmap of the bit array
	nbits      uint64
	records    uint64 // hashes placed so far
	coveredSeq uint64 // highest seq placed (persisted; only after a sync)
	gen        uint64 // header generation counter, bumped on every write
	dirty      bool   // bits changed since the last sync
}

// bloomGenBytes is the on-disk size of one generation.
func bloomGenBytes(nbits uint64) int64 { return bloomHdrPage*2 + int64(nbits/8) }

func bloomGenPath(dir string, idx int) string {
	return filepath.Join(dir, fmt.Sprintf("g%04d.blm", idx))
}

// set places a hash: bloomK double-hashed probes, masked over the bit array.
func (g *bloomGen) set(hash uint64) {
	h1, h2 := hash, (hash>>32)|1
	for i := 0; i < bloomK; i++ {
		bit := (h1 + uint64(i)*h2) & (g.nbits - 1)
		g.bits[bit/8] |= 1 << uint(bit%8)
	}
	g.dirty = true
}

// maybe reports whether the hash may be in this generation.
func (g *bloomGen) maybe(hash uint64) bool {
	h1, h2 := hash, (hash>>32)|1
	for i := 0; i < bloomK; i++ {
		bit := (h1 + uint64(i)*h2) & (g.nbits - 1)
		if g.bits[bit/8]&(1<<uint(bit%8)) == 0 {
			return false
		}
	}
	return true
}

func (g *bloomGen) headerBytes(slotID int32) []byte {
	hdr := make([]byte, bloomHdrPage)
	copy(hdr[0:8], bloomHdrMagic)
	binary.BigEndian.PutUint32(hdr[8:12], bloomHdrVer)
	binary.BigEndian.PutUint32(hdr[12:16], uint32(slotID))
	binary.BigEndian.PutUint32(hdr[16:20], uint32(bloomK))
	binary.BigEndian.PutUint64(hdr[20:28], g.nbits)
	binary.BigEndian.PutUint64(hdr[28:36], g.records)
	binary.BigEndian.PutUint64(hdr[36:44], g.coveredSeq)
	binary.BigEndian.PutUint64(hdr[44:52], g.gen)
	binary.BigEndian.PutUint32(hdr[52:56], crc32.Checksum(hdr[:52], segIdxCRC))
	return hdr
}

// writeHeader writes both header pages. Two copies with a CRC each: a torn
// write leaves one page rejected, and the reader takes the valid one.
func (g *bloomGen) writeHeader(slotID int32) error {
	hdr := g.headerBytes(slotID)
	if _, err := g.f.WriteAt(hdr, 0); err != nil {
		return err
	}
	if _, err := g.f.WriteAt(hdr, bloomHdrPage); err != nil {
		return err
	}
	return g.f.Sync()
}

// sync makes the bits durable and then records how far they reach.
func (g *bloomGen) sync(slotID int32) error {
	if !g.dirty {
		return nil
	}
	if err := unix.Msync(g.bits, unix.MS_SYNC); err != nil {
		return err
	}
	g.gen++
	if err := g.writeHeader(slotID); err != nil {
		return err
	}
	g.dirty = false
	return nil
}

func (g *bloomGen) close() {
	if g.bits != nil {
		_ = unix.Munmap(g.bits)
		g.bits = nil
	}
	if g.f != nil {
		_ = g.f.Close()
		g.f = nil
	}
}

// readBloomHeader parses one header page; ok is false when it does not
// validate against the shape this build writes.
func readBloomHeader(hdr []byte, slotID int32, nbits uint64) (records, coveredSeq, gen uint64, ok bool) {
	if len(hdr) < 56 || string(hdr[0:8]) != bloomHdrMagic {
		return 0, 0, 0, false
	}
	if binary.BigEndian.Uint32(hdr[8:12]) != bloomHdrVer ||
		int32(binary.BigEndian.Uint32(hdr[12:16])) != slotID ||
		binary.BigEndian.Uint32(hdr[16:20]) != uint32(bloomK) ||
		binary.BigEndian.Uint64(hdr[20:28]) != nbits {
		return 0, 0, 0, false
	}
	if crc32.Checksum(hdr[:52], segIdxCRC) != binary.BigEndian.Uint32(hdr[52:56]) {
		return 0, 0, 0, false
	}
	return binary.BigEndian.Uint64(hdr[28:36]),
		binary.BigEndian.Uint64(hdr[36:44]),
		binary.BigEndian.Uint64(hdr[44:52]), true
}

// openBloomGen attaches to an existing generation file. A file whose header
// does not validate, or whose size is short, is not trusted: ok is false and
// the caller drops it along with every later generation.
func openBloomGen(path string, slotID int32, nbits uint64) (*bloomGen, bool) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, false
	}
	st, err := f.Stat()
	if err != nil || st.Size() < bloomGenBytes(nbits) {
		f.Close()
		return nil, false
	}
	g := &bloomGen{path: path, f: f, nbits: nbits}
	var pages [2][]byte
	best := -1
	var bestGen uint64
	for i := 0; i < 2; i++ {
		pages[i] = make([]byte, bloomHdrPage)
		if _, err := f.ReadAt(pages[i], int64(i)*bloomHdrPage); err != nil {
			continue
		}
		records, covered, gen, ok := readBloomHeader(pages[i], slotID, nbits)
		if !ok {
			continue
		}
		if best < 0 || gen > bestGen {
			best, bestGen = i, gen
			g.records, g.coveredSeq = records, covered
		}
	}
	if best < 0 {
		f.Close()
		return nil, false
	}
	g.gen = bestGen
	bits, err := unix.Mmap(int(f.Fd()), int64(bloomHdrPage*2), int(nbits/8),
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, false
	}
	g.bits = bits
	return g, true
}

// createBloomGen makes a new, empty generation file (sparse: the bit array
// costs nothing until pages are written).
func createBloomGen(path string, slotID int32, nbits uint64) (*bloomGen, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(bloomGenBytes(nbits)); err != nil {
		f.Close()
		return nil, err
	}
	bits, err := unix.Mmap(int(f.Fd()), int64(bloomHdrPage*2), int(nbits/8),
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, err
	}
	g := &bloomGen{path: path, f: f, bits: bits, nbits: nbits}
	if err := g.writeHeader(slotID); err != nil {
		g.close()
		return nil, err
	}
	return g, nil
}

// bloomStore is a slot's filter: its generations, in order.
type bloomStore struct {
	dir    string
	slotID int32
	gens   []*bloomGen
	// genRecords is how many hashes one generation takes (bloomGenRecords in
	// production; a test sets it small so a handful of inserts roll over).
	genRecords uint64
	// genBits is one generation's bit count (bloomGenBits in production).
	genBits uint64
	// watermark is the highest seq the newest generation's header claims, and
	// placedSeq the highest seq placed in this process. Inserting at or below
	// placedSeq is a replay of an already-placed record; watermark is the same
	// answer as of the last commit, so a reopen starts from it.
	watermark uint64
	placedSeq uint64
	// unsafe marks a store that cannot cover every record (a generation that
	// failed to open or grow). Such a store says "maybe" for everything.
	unsafe bool
}

// bloomStoreDir maps a slot directory to its filter directory.
func bloomStoreDir(slotDir string) string { return filepath.Join(slotDir, "bloom") }

// openBloomStore attaches to a slot's filter with the production shape.
func openBloomStore(slotDir string, slotID int32) (*bloomStore, error) {
	return openBloomStoreShape(slotDir, slotID, bloomGenRecords, bloomGenBits)
}

// openBloomStoreShape attaches to a slot's filter. A generation whose header
// or size does not validate is dropped together with every later one, so the
// replay after the last trusted generation rebuilds them.
func openBloomStoreShape(slotDir string, slotID int32, genRecords, genBits uint64) (*bloomStore, error) {
	dir := bloomStoreDir(slotDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &bloomStore{dir: dir, slotID: slotID, genRecords: genRecords, genBits: genBits}
	for idx := 0; ; idx++ {
		path := bloomGenPath(dir, idx)
		if _, err := os.Stat(path); err != nil {
			break // generations are contiguous; the first gap ends the chain
		}
		g, ok := openBloomGen(path, slotID, genBits)
		if !ok {
			bloomGenDropped.Add(1)
			s.dropFrom(idx)
			break
		}
		s.gens = append(s.gens, g)
	}
	if n := len(s.gens); n > 0 {
		s.watermark = s.gens[n-1].coveredSeq
	}
	s.placedSeq = s.watermark
	return s, nil
}

// dropFrom removes generations idx and later: their headers do not validate,
// so the bits behind them cannot be trusted to answer "absent".
func (s *bloomStore) dropFrom(idx int) {
	for i := idx; i < len(s.gens); i++ {
		s.gens[i].close()
	}
	s.gens = s.gens[:idx]
	for i := idx; ; i++ {
		path := bloomGenPath(s.dir, i)
		if _, err := os.Stat(path); err != nil {
			break
		}
		_ = os.Remove(path)
	}
	if n := len(s.gens); n > 0 {
		s.watermark = s.gens[n-1].coveredSeq
	} else {
		s.watermark = 0
	}
	s.placedSeq = s.watermark
}

// maybe reports whether the hash may be stored in this slot. An empty or
// unsafe store says maybe: a filter that cannot answer must not hide records.
func (s *bloomStore) maybe(hash uint64) bool {
	if s.unsafe || len(s.gens) == 0 {
		return true
	}
	for _, g := range s.gens {
		if g.maybe(hash) {
			return true
		}
	}
	return false
}

// insert places a record's hash. A seq at or below placedSeq was placed by
// this process (or by the process whose header this store opened) and is
// skipped: re-placing spends a generation slot for a record already in it.
func (s *bloomStore) insert(seq, hash uint64) {
	if s.unsafe || seq <= s.placedSeq {
		return
	}
	n := len(s.gens)
	if n == 0 || s.gens[n-1].records >= s.genRecords {
		// A full generation is made durable before the next one takes over, so
		// its header claims exactly the seqs its bits hold and a restart
		// replays only what came after it.
		if n > 0 {
			if err := s.gens[n-1].sync(s.slotID); err != nil {
				bloomGenDropped.Add(1)
				s.unsafe = true
				return
			}
			s.watermark = s.gens[n-1].coveredSeq
		}
		g, err := createBloomGen(bloomGenPath(s.dir, n), s.slotID, s.genBits)
		if err != nil {
			// Without a generation to place into, the filter cannot claim any
			// record is absent — every lookup must confirm against the disk.
			bloomGenDropped.Add(1)
			s.unsafe = true
			return
		}
		s.gens = append(s.gens, g)
	}
	g := s.gens[len(s.gens)-1]
	g.set(hash)
	g.records++
	if seq > g.coveredSeq {
		g.coveredSeq = seq
	}
	if seq > s.placedSeq {
		s.placedSeq = seq
	}
}

// commit makes every dirty generation durable and advances the watermark.
func (s *bloomStore) commit() error {
	var firstErr error
	for _, g := range s.gens {
		if err := g.sync(s.slotID); err != nil {
			if firstErr == nil {
				firstErr = err
				s.unsafe = true
			}
		}
	}
	if firstErr == nil {
		if n := len(s.gens); n > 0 {
			s.watermark = s.gens[n-1].coveredSeq
		}
	}
	return firstErr
}

// records reports how many hashes the store holds (diagnostics and tests).
func (s *bloomStore) records() uint64 {
	var n uint64
	for _, g := range s.gens {
		n += g.records
	}
	return n
}

// gensCount reports the number of generations a lookup walks.
func (s *bloomStore) gensCount() int { return len(s.gens) }

func (s *bloomStore) close() error {
	first := s.commit()
	for _, g := range s.gens {
		g.close()
	}
	s.gens = nil
	if first != nil {
		return first
	}
	return nil
}
