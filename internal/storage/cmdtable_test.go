package storage

import "testing"

// The command index keeps hashes only, so a collision must extend the probe
// chain instead of hiding or overwriting an entry, and growth must keep every
// entry reachable. Hash 0 must not be mistaken for the empty marker.
func TestCmdTableProbeChainsAndGrowth(t *testing.T) {
	var tab cmdTable
	type entry struct {
		hash, seq uint64
	}
	var entries []entry
	for i := uint64(1); i <= 300; i++ {
		entries = append(entries, entry{hash: i*8 + 1, seq: i})
	}
	const colliding = 0xABCD
	for i := uint64(0); i < 3; i++ {
		entries = append(entries, entry{hash: colliding, seq: 1000 + i})
	}
	entries = append(entries, entry{hash: 0, seq: 7777}) // a real hash of 0

	for _, e := range entries {
		tab.put(e.hash, e.seq)
	}
	if tab.len() != len(entries) {
		t.Fatalf("table holds %d entries, want %d", tab.len(), len(entries))
	}

	for _, e := range entries {
		want := e.seq
		seq, ok, err := tab.lookup(e.hash, func(s uint64) (bool, error) { return s == want, nil })
		if err != nil {
			t.Fatal(err)
		}
		if !ok || seq != want {
			t.Fatalf("lookup(hash %#x, seq %d) = %d,%v want %d,true", e.hash, want, seq, ok, want)
		}
	}

	// All three colliding entries stay reachable: a verifier that rejects the
	// earlier ones must still find the last.
	seen := map[uint64]bool{}
	for i := uint64(0); i < 3; i++ {
		want := 1000 + i
		seq, ok, err := tab.lookup(colliding, func(s uint64) (bool, error) { return s == want, nil })
		if err != nil || !ok || seq != want {
			t.Fatalf("colliding lookup %d = %d,%v,%v want %d", i, seq, ok, err, want)
		}
		seen[seq] = true
	}
	if len(seen) != 3 {
		t.Fatalf("colliding chain resolved to %d entries, want 3", len(seen))
	}

	// The same (hash, seq) pair is idempotent, and an absent hash ends the chain.
	before := tab.len()
	tab.put(colliding, 1001)
	if tab.len() != before {
		t.Fatalf("re-putting the same pair changed the count %d -> %d", before, tab.len())
	}
	if _, ok, err := tab.lookup(0xDEADBEEF, func(uint64) (bool, error) { return true, nil }); err != nil || ok {
		t.Fatalf("absent hash reported ok=%v err=%v", ok, err)
	}
}
