package storage

// cmdTable maps a command id's 64-bit hash to the seq of the record holding it
// (the slot's idempotency index, rule 1).
//
// A 30 GiB node keeps one entry per record — tens of millions — so this index
// is the largest thing the process holds in memory. map[string]uint64 stores
// the id itself on top of bucket, string-header and GC overhead (100+ bytes an
// entry); this table is two flat uint64 arrays, 16 bytes an entry, no ids.
//
// The ids are not needed: a hit is confirmed by reading the record at that seq
// and comparing its command id, and every caller of a hit does exactly that
// read anyway (an EXISTS answer returns the stored record; a by-command read
// returns it). Two commands whose hashes collide simply make the probe chain
// longer — never a wrong answer.
type cmdTable struct {
	hashes []uint64 // 0 marks an empty slot (a computed 0 is stored as 1)
	seqs   []uint64
	count  int
}

// Fill factor that triggers a grow.
const (
	cmdTableMaxLoadNum = 7
	cmdTableMaxLoadDen = 10
	cmdTableMinSize    = 64
)

func (t *cmdTable) len() int { return t.count }

// put records seq for hash h. The same hash with the same seq is ignored (an
// idempotent replay of the same record); the same hash with a different seq is
// a second entry in the same probe chain.
func (t *cmdTable) put(h, seq uint64) {
	if h == 0 {
		h = 1
	}
	if len(t.hashes) == 0 {
		t.grow(cmdTableMinSize)
	} else if t.count*cmdTableMaxLoadDen >= len(t.hashes)*cmdTableMaxLoadNum {
		t.grow(len(t.hashes) * 2)
	}
	mask := uint64(len(t.hashes) - 1)
	for i := h & mask; ; i = (i + 1) & mask {
		switch t.hashes[i] {
		case 0:
			t.hashes[i], t.seqs[i] = h, seq
			t.count++
			return
		case h:
			if t.seqs[i] == seq {
				return
			}
		}
	}
}

// lookup walks the chain for h until verify accepts a candidate seq, and
// reports ok=false when the chain ends at an empty slot.
func (t *cmdTable) lookup(h uint64, verify func(seq uint64) (bool, error)) (uint64, bool, error) {
	if len(t.hashes) == 0 {
		return 0, false, nil
	}
	if h == 0 {
		h = 1
	}
	mask := uint64(len(t.hashes) - 1)
	for i := h & mask; ; i = (i + 1) & mask {
		if t.hashes[i] == 0 {
			return 0, false, nil
		}
		if t.hashes[i] == h {
			ok, err := verify(t.seqs[i])
			if err != nil {
				return 0, false, err
			}
			if ok {
				return t.seqs[i], true, nil
			}
		}
	}
}

// grow rehashes into a table of at least the given size (rounded up to a power
// of two). Callers only grow while at most 70% full, so the rehash itself never
// triggers another grow.
func (t *cmdTable) grow(size int) {
	for size < cmdTableMinSize {
		size = cmdTableMinSize
	}
	for size&(size-1) != 0 {
		size = (size | (size - 1)) + 1
	}
	oldHashes, oldSeqs := t.hashes, t.seqs
	t.hashes, t.seqs, t.count = make([]uint64, size), make([]uint64, size), 0
	for i, h := range oldHashes {
		if h != 0 {
			t.put(h, oldSeqs[i])
		}
	}
}
