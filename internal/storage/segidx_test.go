package storage

import (
	"fmt"
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

	load, err := loadSegIndex(dir, slotID, base, -1, -1)
	if err != nil || load == nil {
		t.Fatalf("load: %v load=%v", err, load)
	}
	if len(load.entries) != 300 {
		t.Fatalf("loaded %d entries, want 300", len(load.entries))
	}
	if len(load.sparse) != 5 {
		t.Fatalf("loaded %d sparse entries, want 5", len(load.sparse))
	}
	for i, e := range load.entries {
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
	load2, err := loadSegIndex(dir, slotID, base, -1, -1)
	if err != nil || load2 == nil {
		t.Fatalf("damaged load: %v %v", err, load2)
	}
	if len(load2.entries) != 256 {
		t.Fatalf("damaged load kept %d entries, want the first block's 256", len(load2.entries))
	}
	if segIndexDamaged.Load() == 0 {
		t.Fatal("damage was not counted")
	}

	// A different slot must not be served this index.
	if l, err := loadSegIndex(dir, slotID+1, base, -1, -1); err != nil || l != nil {
		t.Fatalf("slot mismatch: %v %v", err, l)
	}
	// Entries beyond what the WAL holds are dropped.
	capped, err := loadSegIndex(dir, slotID, base, 10, -1)
	if err != nil || capped == nil || len(capped.entries) != 10 {
		t.Fatalf("cap: %v %v", err, capped)
	}

	// Reopening continues after the validated prefix: the 44 lost entries are
	// rewritten, and no stale body survives.
	w2, err := openSegIndexWriter(dir, slotID, base)
	if err != nil {
		t.Fatal(err)
	}
	if w2.idxOff != int64(blockBytes(256)) {
		t.Fatalf("reopen offset %d, want the validated prefix %d", w2.idxOff, blockBytes(256))
	}
	for i := 256; i < 300; i++ {
		w2.add(0xAAAA0000_0000_0000+uint64(i), base+uint64(i), fmt.Sprintf("agg-%d", i%2), uint32(i%3+1))
	}
	if err := w2.close(); err != nil {
		t.Fatal(err)
	}
	load3, err := loadSegIndex(dir, slotID, base, -1, -1)
	if err != nil || load3 == nil || len(load3.entries) != 300 {
		t.Fatalf("after repair: %v %v", err, load3)
	}
	for i, e := range load3.entries {
		if e.seq != base+uint64(i) {
			t.Fatalf("repaired entry %d: seq %d", i, e.seq)
		}
	}
}
