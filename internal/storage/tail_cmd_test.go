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
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"testing"

	"github.com/berkaroad/pushupes/internal/data"
)

// mustAppend appends one record and requires a fresh-write outcome.
func mustAppend(t *testing.T, sl *Slot, rec *data.EventRecord) {
	t.Helper()
	out, err := sl.Append(rec)
	if err != nil {
		t.Fatalf("append %s: %v", rec.CommandID, err)
	}
	if out.Status != data.StatusSuccess {
		t.Fatalf("append %s: status %s err %d, want success", rec.CommandID, out.Status, out.ErrID)
	}
}

// roll arms the next append to seal the current writable segment: the size
// threshold drops to the tail's current size, so tail() rolls before writing.
func roll(t *testing.T, sl *Slot) {
	t.Helper()
	sl.mu.Lock()
	defer sl.mu.Unlock()
	tail := sl.segments[len(sl.segments)-1]
	sl.segmentBytes = tail.SizeBytes()
}

// scanIdxEntries reads in windows yet hands out exactly the walk the
// per-block reads did: full order, early stop, validated prefix on a torn
// block, and the oversized-count block that does not fit a window.
func TestScanIdxEntriesWindowsAndTornBlocks(t *testing.T) {
	dir := t.TempDir()
	w, err := openSegIndexWriter(dir, 3, 1000)
	if err != nil {
		t.Fatal(err)
	}
	const total = 25600 // 100 full blocks: several scan windows deep
	for i := 0; i < total; i++ {
		w.add(uint64(i), uint64(1000+i), "agg", uint32(i%7+1), int64(i))
	}
	if w.err != nil {
		t.Fatal(w.err)
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	path := segIdxPath(dir, 1000)

	var seen []uint64
	read, err := scanIdxEntries(path, segIdxMagic, 3, 1000, segIdxEntryBytes, func(raw []byte) bool {
		seen = append(seen, binary.BigEndian.Uint64(raw[0:8]))
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != total {
		t.Fatalf("windowed walk yielded %d entries, want %d", len(seen), total)
	}
	for i, h := range seen {
		if h != uint64(i) {
			t.Fatalf("entry %d out of order: %d", i, h)
		}
	}
	if read <= 0 {
		t.Errorf("walked byte count %d, want > 0", read)
	}

	n := 0
	if _, err := scanIdxEntries(path, segIdxMagic, 3, 1000, segIdxEntryBytes, func([]byte) bool {
		n++
		return n < 5
	}); err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Errorf("early stop visited %d entries, want 5", n)
	}

	// Corrupt one payload byte of block 50: the walk must stop at the last
	// validated prefix (block 50's entries are never handed out).
	blockBytes := int64(segIdxBlockHead + segIdxBlockMax*segIdxEntryBytes)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, int64(segIdxHeaderBytes)+50*blockBytes+int64(segIdxBlockHead)+100); err != nil {
		t.Fatal(err)
	}
	f.Close()
	cut := 0
	if _, err := scanIdxEntries(path, segIdxMagic, 3, 1000, segIdxEntryBytes, func([]byte) bool {
		cut++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if cut != 50*segIdxBlockMax {
		t.Fatalf("torn block: walk yielded %d entries, want %d (validated prefix only)", cut, 50*segIdxBlockMax)
	}

	// A file with no index at all is not an error: it walks zero entries.
	if read, err := scanIdxEntries(segIdxPath(dir, 4242), segIdxMagic, 3, 4242, segIdxEntryBytes, func([]byte) bool {
		t.Error("callback on a missing index file")
		return true
	}); err != nil || read != 0 {
		t.Fatalf("missing file: read=%d err=%v, want 0, nil", read, err)
	}

	// An oversized block (count beyond what the writer produces) spans
	// several windows; the walk must still hand out its entries whole.
	const bigCount = 20000
	big := make([]byte, segIdxHeaderBytes+segIdxBlockHead+bigCount*segIdxEntryBytes)
	copy(big, segIdxHeaderBuf(segIdxMagic, 3, 7777))
	body := big[segIdxHeaderBytes:]
	binary.BigEndian.PutUint16(body[4:6], uint16(bigCount))
	for i := 0; i < bigCount; i++ {
		binary.BigEndian.PutUint64(body[segIdxBlockHead+i*segIdxEntryBytes:], uint64(1_000_000+i))
	}
	binary.BigEndian.PutUint32(body[0:4], crc32.Checksum(body[4:], segIdxCRC))
	bigPath := segIdxPath(dir, 7777)
	if err := os.WriteFile(bigPath, big, 0o644); err != nil {
		t.Fatal(err)
	}
	bn := 0
	if _, err := scanIdxEntries(bigPath, segIdxMagic, 3, 7777, segIdxEntryBytes, func(raw []byte) bool {
		if binary.BigEndian.Uint64(raw[0:8]) != uint64(1_000_000+bn) {
			t.Errorf("oversized block entry %d wrong", bn)
		}
		bn++
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if bn != bigCount {
		t.Fatalf("oversized block yielded %d entries, want %d", bn, bigCount)
	}
}

// The hash-specialised walk is the per-entry walk with the comparison
// pulled inline: same candidates in the same order, the same stop
// semantics, and a "saw a validated block" flag that distinguishes an
// empty or torn file from a file with no match.
func TestScanIdxEntriesHashWalkEquivalence(t *testing.T) {
	dir := t.TempDir()
	w, err := openSegIndexWriter(dir, 3, 1000)
	if err != nil {
		t.Fatal(err)
	}
	const total = 5120 // 20 blocks
	for i := 0; i < total; i++ {
		w.add(uint64(i%2), uint64(1000+i), "agg", uint32(i+1), int64(i)) // two hashes, interleaved across all 20 blocks
	}
	if w.err != nil {
		t.Fatal(w.err)
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	path := segIdxPath(dir, 1000)

	// Cross-check: the specialised walk over hash 0 must hand out exactly
	// the entries the general walk reports for the same hash, in order.
	var want []byte
	if _, err := scanIdxEntries(path, segIdxMagic, 3, 1000, segIdxEntryBytes, func(raw []byte) bool {
		if binary.BigEndian.Uint64(raw[0:8]) == 0 {
			want = append(want, raw...)
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	var got []byte
	matches := 0
	read, saw, err := scanIdxEntriesHash(path, segIdxMagic, 3, 1000, segIdxEntryBytes, 0, func(raw []byte) bool {
		matches++
		got = append(got, raw...)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if !saw {
		t.Fatalf("a walk over 20 blocks reported no validated block")
	}
	if read == 0 || !bytes.Equal(got, want) {
		t.Fatalf("hash walk yielded %d matches (%d bytes read), general walk agrees on %d bytes", matches, read, len(want))
	}

	// A hash with no entries: the walk runs, matches nothing, saw=true.
	if _, saw, err := scanIdxEntriesHash(path, segIdxMagic, 3, 1000, segIdxEntryBytes, 77, func([]byte) bool {
		t.Error("match callback on a hash the file never holds")
		return true
	}); err != nil || !saw {
		t.Fatalf("no-match walk: saw=%v err=%v", saw, err)
	}

	// Early stop after the first match.
	n := 0
	if _, _, err := scanIdxEntriesHash(path, segIdxMagic, 3, 1000, segIdxEntryBytes, 1, func([]byte) bool {
		n++
		return false
	}); err != nil || n != 1 {
		t.Fatalf("early stop ran %d callbacks, want 1 (err %v)", n, err)
	}

	// Missing file: zero bytes, no validated block, no error.
	if read, saw, err := scanIdxEntriesHash(segIdxPath(dir, 4242), segIdxMagic, 3, 4242, segIdxEntryBytes, 0, func([]byte) bool {
		t.Error("callback on a missing index file")
		return true
	}); err != nil || saw || read != 0 {
		t.Fatalf("missing file: read=%d saw=%v err=%v", read, saw, err)
	}
}

// A lookup opening the .cidx has no use for the bloom section (the gate
// answer came from memory before the open): the open skips reading and
// decoding it, while the binary search over the entries still answers.
func TestOpenSegCmdIndexWithoutBloomStillSearches(t *testing.T) {
	dir := t.TempDir()
	sl, err := OpenSlotRepairing(dir, 7, 1<<40, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 600; i++ {
		mustAppend(t, sl, mkRec(fmt.Sprintf("a%04d", i), 1, fmt.Sprintf("c%04d", i)))
	}
	roll(t, sl)
	mustAppend(t, sl, mkRec("a9999", 1, "c9999"))
	base := sl.segments[0].BaseSeq
	if err := sl.Close(); err != nil {
		t.Fatal(err)
	}

	ix, err := openSegCmdIndex(dir, 7, base)
	if err != nil {
		t.Fatal(err)
	}
	if ix == nil {
		t.Fatal("index not opened")
	}
	found, err := ix.LookupCmd(uint32(data.HashCommandID("c0007")>>32), func(e segCmdEntry) bool {
		return e.seq == 8
	})
	if err != nil || !found {
		t.Errorf("entry search without bloom: found=%v err=%v", found, err)
	}
	ix.Close()
}
