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

// A sealed segment's on-disk bloom must cover exactly the records of its own
// segment: the reopen path trusts it (blmsSkipInsert) and does not re-place
// those hashes. If inserts of the NEXT segment land in the still-unfilled
// last filter of the sealed segment — below blmsMark, so the next seal writes
// a slice that excludes them — those hashes live only in memory: after a
// reopen the disk bloom loads without them, replay stays suppressed, and a
// STORED command starts answering "definitely not stored". Idempotency for
// those commands is gone (the client retry writes a duplicate / answers
// not-found).
//
// The shape that exposes it: segment 0 overruns bloomFirstCapacity (1024) so
// a second filter starts partially filled; segment 1 keeps placing hashes
// into that shared filter until it doubles, then seals. Its first records —
// placed below the seal mark — are the victims.
func TestSealedBloomCoversItsOwnSegment(t *testing.T) {
	dir := t.TempDir()
	sl, err := OpenSlotRepairing(dir, 7, 1<<40, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	rec := func(i int) *data.EventRecord {
		return mkRec(fmt.Sprintf("a%06d", i), 1, fmt.Sprintf("c%06d", i))
	}
	cmd := func(i int) string { return fmt.Sprintf("c%06d", i) }

	// Segment 0: 1200 records, so filter 0 fills (capacity 1024) and filter 1
	// starts at 176/2048.
	const seg0Records = 1200
	for i := 0; i < seg0Records; i++ {
		mustAppend(t, sl, rec(i))
	}
	roll(t, sl) // the next append seals segment 0
	mustAppend(t, sl, rec(seg0Records))
	if got := sl.segments[0].RecordCnt; got != seg0Records {
		t.Fatalf("segment 0 sealed with %d records, want %d", got, seg0Records)
	}

	// Segment 1: fill the shared filter 1 past capacity (2048 - 176 entries
	// to go) and place 8 more in the doubled filter, then seal.
	const leaked = 2048 - (seg0Records - 1024) // 1872: the victim run
	for i := seg0Records + 1; i <= seg0Records+leaked+8; i++ {
		mustAppend(t, sl, rec(i))
	}
	roll(t, sl)
	mustAppend(t, sl, rec(seg0Records+leaked+9))

	// Reopen: the chain must answer "maybe" for every stored command. A lost
	// hash is a stored command that idempotency will report as absent.
	sl2, err := OpenSlotRepairing(dir, 7, 1<<40, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	total := seg0Records + leaked + 10
	for i := 0; i < total; i++ {
		if !sl2.blms.maybe(data.HashCommandID(cmd(i))) {
			t.Fatalf("stored command %d (seq %d) reports definitely absent after reopen"+
				" — its hash was placed below the seal mark and never reached disk", i, i+1)
		}
	}

	// Behavioural face of the same hole: a retry of the FIRST segment-1 record
	// (placed in the shared filter, below segment 1's seal mark) must answer
	// exists, not "new" or a version conflict.
	out, err := sl2.Append(rec(seg0Records))
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != data.StatusExists {
		t.Fatalf("replayed command status %s (err %d), want exists", out.Status, out.ErrID)
	}
}

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

// The writable segment's own filter range (blmsMark onward, pinned at
// placement) gates the tail .idx scan: a command stored in a SEALED segment
// must not pay for walking the tail, and the sealed segment's own filter
// decides whether its .cidx gets opened at all.
func TestTailFilterGateSkipsAbsentScans(t *testing.T) {
	dir := t.TempDir()
	sl, err := OpenSlotRepairing(dir, 7, 1<<40, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	rec := func(i int) *data.EventRecord {
		return mkRec(fmt.Sprintf("a%04d", i), 1, fmt.Sprintf("c%04d", i))
	}
	for i := 0; i < 60; i++ { // segment 0
		mustAppend(t, sl, rec(i))
	}
	roll(t, sl)
	mustAppend(t, sl, rec(60)) // seals segment 0, opens segment 1
	for i := 61; i < 90; i++ { // writable tail
		mustAppend(t, sl, rec(i))
	}

	scan0, skip0, open0 := idemTailScans.Load(), idemTailSkips.Load(), idemSealedOpens.Load()
	got, seq, err := sl.RecordByCommand(fmt.Sprintf("c%04d", 4)) // stored in sealed segment 0
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || seq != 5 {
		t.Fatalf("sealed command c0004 resolved to found=%v seq=%d", got != nil, seq)
	}
	if s := idemTailScans.Load(); s != scan0 {
		t.Errorf("tail .idx scanned %d time(s) for a sealed command; the tail filter gate should have skipped it", s-scan0)
	}
	if idemTailSkips.Load() == skip0 {
		t.Errorf("tail filter gate recorded no skip")
	}
	if idemSealedOpens.Load() == open0 {
		t.Errorf("sealed .cidx open was gated even though the storing segment's filter holds the hash")
	}

	// The reverse shape: a command the tail holds is found by the (gated)
	// tail path; no sealed index opens for it.
	open1 := idemSealedOpens.Load()
	scan1 := idemTailScans.Load()
	got, seq, err = sl.RecordByCommand(fmt.Sprintf("c%04d", 80))
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || seq != 81 {
		t.Fatalf("tail command c0080 resolved to found=%v seq=%d", got != nil, seq)
	}
	if idemTailScans.Load() == scan1 {
		t.Errorf("tail filter gate skipped a scan for a hash the tail itself placed")
	}
	if s := idemSealedOpens.Load(); s != open1 {
		t.Errorf("sealed .cidx opened %d time(s) for a command the tail already answered", s-open1)
	}
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

// Startup self-heal for the pre-pinning leak era on disk: a sealed .cidx
// whose bloom validates but is MISSING hashes must be gap-filled when the
// slot opens — in memory at every open, and persisted (bloom section only,
// entry bytes untouched) at a startup repair. A further start then repairs
// nothing. The doctored file below is exactly what the old placement bug
// left behind: a valid bloom that answers "absent" for stored commands.
func TestSealedBloomGapSelfHealsAtStartup(t *testing.T) {
	dir := t.TempDir()
	const n = 300
	rec := func(i int) *data.EventRecord {
		return mkRec(fmt.Sprintf("a%06d", i), 1, fmt.Sprintf("c%06d", i))
	}
	cmd := func(i int) string { return fmt.Sprintf("c%06d", i) }

	sl, err := OpenSlotRepairing(dir, 7, 1<<40, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		mustAppend(t, sl, rec(i))
	}
	roll(t, sl)
	mustAppend(t, sl, rec(n)) // seals segment 0; one record lands in the tail
	base := sl.segments[0].BaseSeq
	if err := sl.Close(); err != nil {
		t.Fatal(err)
	}

	path := segCidxPath(dir, base)
	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	blLen := int(binary.BigEndian.Uint32(full[28:32]))
	entries := full[segCidxHeaderByte : len(full)-blLen]

	// Doctor: a bloom that only knows the FIRST command — 299 stored records
	// would answer "definitely absent" against it.
	df := newBloomBits(1024)
	df.insert(data.HashCommandID(cmd(0)))
	if err := writeSegCidxBloom(dir, 7, base, []*bloomBits{df}); err != nil {
		t.Fatal(err)
	}
	doctored, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	lookAll := func(sl2 *Slot) {
		t.Helper()
		for i := 0; i < n; i++ {
			got, seq, err := sl2.RecordByCommand(cmd(i))
			if err != nil {
				t.Fatal(err)
			}
			if got == nil || seq != uint64(i+1) {
				t.Fatalf("stored command %d: found=%v seq=%d want seq %d", i, got != nil, seq, i+1)
			}
		}
	}

	// A running open (load mode) heals the answer — and leaves the disk alone.
	gap0, rep0 := segBloomGaps.Load(), segBloomRepaired.Load()
	slL, err := OpenSlot(dir, 7, 1<<40, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if d := segBloomGaps.Load() - gap0; d != uint64(n-1) {
		t.Fatalf("load open gap-filled %d hashes, want %d", d, n-1)
	}
	if d := segBloomRepaired.Load() - rep0; d != 1 {
		t.Fatalf("load open counted %d repaired segments, want 1", d)
	}
	lookAll(slL)
	if err := slL.Close(); err != nil {
		t.Fatal(err)
	}
	if cur, err := os.ReadFile(path); err != nil || !bytes.Equal(cur, doctored) {
		t.Fatalf("load mode rewrote the .cidx — persistence is startup work")
	}

	// A startup repair persists it: entry bytes unchanged, bloom section fixed.
	gap1, rep1 := segBloomGaps.Load(), segBloomRepaired.Load()
	sl2, err := OpenSlotRepairing(dir, 7, 1<<40, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if d := segBloomGaps.Load() - gap1; d != uint64(n-1) {
		t.Fatalf("repair open gap-filled %d hashes, want %d", d, n-1)
	}
	if d := segBloomRepaired.Load() - rep1; d != 1 {
		t.Fatalf("repair open counted %d repaired segments, want 1", d)
	}
	lookAll(sl2)
	if err := sl2.Close(); err != nil {
		t.Fatal(err)
	}
	fixed, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fbl := int(binary.BigEndian.Uint32(fixed[28:32]))
	if !bytes.Equal(fixed[segCidxHeaderByte:len(fixed)-fbl], entries) {
		t.Fatalf("repair changed the entry section — only the bloom may move")
	}
	if bytes.Equal(fixed[segCidxHeaderByte+len(entries):], doctored[segCidxHeaderByte+len(entries):]) {
		t.Fatalf("repair did not rewrite the bloom section")
	}

	// The next start finds nothing to repair.
	gap2, rep2 := segBloomGaps.Load(), segBloomRepaired.Load()
	sl3, err := OpenSlotRepairing(dir, 7, 1<<40, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if d := segBloomGaps.Load() - gap2; d != 0 {
		t.Fatalf("a healed segment repaired again: %d gaps", d)
	}
	if d := segBloomRepaired.Load() - rep2; d != 0 {
		t.Fatalf("a healed segment counted as repaired again")
	}
	lookAll(sl3)
	if err := sl3.Close(); err != nil {
		t.Fatal(err)
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

	ix, err := openSegCmdIndex(dir, 7, base, false)
	if err != nil {
		t.Fatal(err)
	}
	if ix == nil {
		t.Fatal("index not opened")
	}
	if ix.Bloom() != nil {
		t.Errorf("wantBloom=false still decoded a bloom set")
	}
	found, err := ix.LookupCmd(uint32(data.HashCommandID("c0007")>>32), func(e segCmdEntry) bool {
		return e.seq == 8
	})
	if err != nil || !found {
		t.Errorf("entry search without bloom: found=%v err=%v", found, err)
	}
	ix.Close()

	// And the bloom does come back when the caller asks for it.
	ix2, err := openSegCmdIndex(dir, 7, base, true)
	if err != nil {
		t.Fatal(err)
	}
	if ix2.Bloom() == nil {
		t.Errorf("wantBloom=true decoded no bloom")
	}
	if !ix2.Bloom().maybe(data.HashCommandID("c0007")) {
		t.Errorf("decoded bloom lost a placed hash")
	}
	ix2.Close()
}
