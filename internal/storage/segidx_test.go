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

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"testing"
)

// The index files are derived data: what they must guarantee is that a valid
// prefix loads, that damage is detected (never trusted), and that reopening
// continues exactly where the validated prefix ended so replayed records
// rewrite precisely the entries that were lost.
func TestSegIndexRoundTripAndDamage(t *testing.T) {
	dir := t.TempDir()
	const slotID, base = int32(7), uint64(100)

	w, err := openSegIndexWriter(dir, slotID, base)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		w.add(0xAAAA0000_0000_0000+uint64(i), base+uint64(i), fmt.Sprintf("agg-%d", i%2), uint32(i%3+1))
		if i%64 == 0 {
			w.addSparse(base+uint64(i), int64(i)*40)
		}
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}

	load, err := openSegIndex(dir, slotID, base, -1, -1)
	if err != nil || load == nil {
		t.Fatalf("load: %v load=%v", err, load)
	}
	if load.Count() != 300 {
		t.Fatalf("loaded %d entries, want 300", load.Count())
	}
	if len(load.sparse) != 5 {
		t.Fatalf("loaded %d sparse entries, want 5", len(load.sparse))
	}
	for i := 0; i < load.Count(); i++ {
		e, _ := load.Entry(i)
		if e.seq != base+uint64(i) {
			t.Fatalf("entry %d: seq %d", i, e.seq)
		}
		if want := fmt.Sprintf("agg-%d", i%2); e.aggID != want {
			t.Fatalf("entry %d: agg %q want %q", i, e.aggID, want)
		}
		if want := uint32(i%3 + 1); e.version != want {
			t.Fatalf("entry %d: version %d want %d", i, e.version, want)
		}
		if want := uint64(0xAAAA0000_0000_0000 + uint64(i)); e.hash != want {
			t.Fatalf("entry %d: hash %#x want %#x", i, e.hash, want)
		}
	}
	if s := load.sparse[4]; s.seq != base+256 || s.pos != 256*40 {
		t.Fatalf("last sparse entry %+v", s)
	}

	// Damage inside the second block: the first block still validates, so the
	// prefix loads and the rest is up to the WAL replay.
	f, err := os.OpenFile(segIdxPath(dir, base), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, segIdxHeaderBytes+(segIdxBlockHead+segIdxBlockMax*segIdxEntryBytes)+7); err != nil {
		t.Fatal(err)
	}
	f.Close()
	load2, err := openSegIndex(dir, slotID, base, -1, -1)
	if err != nil || load2 == nil {
		t.Fatalf("damaged load: %v %v", err, load2)
	}
	if load2.Count() != 256 {
		t.Fatalf("damaged load kept %d entries, want the first block's 256", load2.Count())
	}
	if segIndexDamaged.Load() == 0 {
		t.Fatal("damage was not counted")
	}

	// A different slot must not be served this index.
	if l, err := openSegIndex(dir, slotID+1, base, -1, -1); err != nil || l != nil {
		t.Fatalf("slot mismatch: %v %v", err, l)
	}
	// Entries beyond what the WAL holds are dropped.
	capped, err := openSegIndex(dir, slotID, base, 10, -1)
	if err != nil || capped == nil || capped.Count() != 10 {
		t.Fatalf("cap: %v %v", err, capped)
	}

	// Reopening continues after the validated prefix: the 44 lost entries are
	// rewritten, and no stale body survives.
	w2, err := openSegIndexWriter(dir, slotID, base)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(segIdxBlockHead + 256*segIdxEntryBytes); w2.idxOff != want {
		t.Fatalf("reopen offset %d, want the validated prefix's extent %d", w2.idxOff, want)
	}
	for i := 256; i < 300; i++ {
		w2.add(0xAAAA0000_0000_0000+uint64(i), base+uint64(i), fmt.Sprintf("agg-%d", i%2), uint32(i%3+1))
	}
	if err := w2.close(); err != nil {
		t.Fatal(err)
	}
	load3, err := openSegIndex(dir, slotID, base, -1, -1)
	if err != nil || load3 == nil || load3.Count() != 300 {
		t.Fatalf("after repair: %v %v", err, load3)
	}
	for i := 0; i < load3.Count(); i++ {
		e, _ := load3.Entry(i)
		if e.seq != base+uint64(i) {
			t.Fatalf("repaired entry %d: seq %d", i, e.seq)
		}
	}
}

// TestSegIndexResumeAfterPartialBlocks pins the resume offset when the file is
// NOT densely packed — the shape every real index has.
//
// flush() writes whatever partial block is buffered (that is what makes the
// index durable), so a reopened writer starts a fresh block right after a
// short one and the file accumulates mid-stream partial blocks. Deriving the
// resume offset from the ENTRY COUNT (blockBytes, which computes a densely
// packed layout) then lands short of the true end: the truncate cuts into the
// last block and the next append overwrites the entries that were there,
// leaving a run of seqs missing from the middle of the file.
//
// That gap is not cosmetic. The loader feeds the index into a slot's aggregate
// directory, which adopts a version only when it follows the previous one, so
// one missing entry freezes the directory for every record above it: the copy
// then reports the leader's LEO with a short directory, reads past the gap fail
// on that node, and the rebalancer's equivalence gate refuses the slot forever.
func TestSegIndexResumeAfterPartialBlocks(t *testing.T) {
	dir := t.TempDir()
	const slotID, base = int32(9), uint64(1)
	add := func(w *segIndexWriter, from, to int) {
		for i := from; i < to; i++ {
			w.add(0xBBBB0000_0000_0000+uint64(i), base+uint64(i), "agg", uint32(i+1))
		}
	}

	w, err := openSegIndexWriter(dir, slotID, base)
	if err != nil {
		t.Fatal(err)
	}
	add(w, 0, 100)
	if err := w.flush(); err != nil { // partial block of 100 on disk
		t.Fatal(err)
	}
	add(w, 100, 200)
	if err := w.flush(); err != nil { // a second partial block
		t.Fatal(err)
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and continue. The offset must be the real end of the second
	// partial block, not a packed-layout guess.
	w2, err := openSegIndexWriter(dir, slotID, base)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(2 * (segIdxBlockHead + 100*segIdxEntryBytes)); w2.idxOff != want {
		t.Fatalf("resume offset %d, want the end of the two partial blocks %d", w2.idxOff, want)
	}
	add(w2, 200, 300)
	if err := w2.close(); err != nil {
		t.Fatal(err)
	}

	// Every entry must have survived, in seq order. A wrong resume offset shows
	// up here as missing seqs — the same shape that freezes a slot directory.
	load, err := openSegIndex(dir, slotID, base, -1, -1)
	if err != nil || load == nil {
		t.Fatalf("load: %v load=%v", err, load)
	}
	if load.Count() != 300 {
		t.Fatalf("loaded %d entries, want 300", load.Count())
	}
	for i := 0; i < load.Count(); i++ {
		e, ok := load.Entry(i)
		if !ok || e.seq != base+uint64(i) {
			t.Fatalf("entry %d reads back as seq %v, want %d — the resume offset dropped entries",
				i, e.seq, base+uint64(i))
		}
	}
}

// TestSegIndexRefusesASeqGap pins the detector for the one corruption the block
// checksums cannot see: a block whose entries skip ahead. A block CRC covers the
// bytes the block holds, not the seqs its entries name, so a file whose seqs
// jump passes every checksum — and then freezes the aggregate directory at the
// gap, because the directory adopts a version only when it follows the previous
// one. The loader must refuse the whole prefix instead: the WAL walk rebuilds
// the index, which costs startup time and never correctness.
func TestSegIndexRefusesASeqGap(t *testing.T) {
	dir := t.TempDir()
	const slotID, base = int32(11), uint64(1)
	w, err := openSegIndexWriter(dir, slotID, base)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		w.add(0xCCCC0000_0000_0000+uint64(i), base+uint64(i), "agg", uint32(i+1))
	}
	if err := w.close(); err != nil { // block 0 full (256), block 1 partial (44)
		t.Fatal(err)
	}

	// Shift every entry of the SECOND block 51 seqs ahead and keep that block's
	// CRC valid: exactly the shape a wrong resume offset leaves behind.
	p := segIdxPath(dir, base)
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	off := segIdxHeaderBytes
	off += segIdxBlockHead + int(binary.BigEndian.Uint16(raw[off+4:off+6]))*segIdxEntryBytes
	count := int(binary.BigEndian.Uint16(raw[off+4 : off+6]))
	if count == 0 {
		t.Fatal("test setup: expected a second block")
	}
	for i := 0; i < count; i++ {
		at := off + segIdxBlockHead + i*segIdxEntryBytes + 8
		binary.BigEndian.PutUint32(raw[at:at+4], binary.BigEndian.Uint32(raw[at:at+4])+51)
	}
	body := raw[off+4 : off+segIdxBlockHead+count*segIdxEntryBytes]
	binary.BigEndian.PutUint32(raw[off:off+4], crc32.Checksum(body, segIdxCRC))
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	if load, err := openSegIndex(dir, slotID, base, -1, -1); err != nil || load != nil {
		t.Fatalf("an index whose seqs skip ahead must be refused, got load=%v err=%v", load, err)
	}
	if segIndexDamaged.Load() == 0 {
		t.Fatal("refusing the gapped index was not counted as damage")
	}
}
