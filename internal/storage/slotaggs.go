package storage

// The per-slot aggregate index: which aggregates the slot holds, their latest
// version, and where their seqs live. It used to be two maps — one
// aggregate→version, one aggregate→[]uint64 — which paid a slice header plus up
// to 2x capacity slack per aggregate and kept the id in two maps.
//
// Now there is one directory entry per aggregate (version + a range into a
// shared arena) and the seqs live in fixed-size chunks of the arena. Fixed
// chunks (rather than one slice per aggregate, or growing ones) give an exact
// fit: the waste is at most one chunk per slot, no matter how many aggregates
// the slot holds or how the records are distributed among them, and indexing a
// seq is a shift and a mask instead of a walk.
type aggEntry struct {
	version uint32 // latest committed version (rule 2)
	off     int    // index of the aggregate's first seq in the arena (n > 0)
	n       int    // number of seqs held (== version while appends stay contiguous)
}

// Arena chunking: 1024 seqs (8 KiB) per chunk. Small enough that a slot with a
// handful of records wastes a few KiB, large enough that a slot with hundreds
// of thousands of seqs needs hundreds — not millions — of chunks.
const (
	seqChunkShift = 10
	seqChunkSize  = 1 << seqChunkShift
	seqChunkMask  = seqChunkSize - 1
)

// aggLocked returns the aggregate's directory entry (zero value when unknown).
// Callers hold s.mu.
func (s *Slot) aggLocked(aggregateID string) aggEntry {
	return s.aggs[aggregateID]
}

// appendSeqLocked appends one seq to the arena. Callers hold s.mu.
func (s *Slot) appendSeqLocked(v uint64) {
	if s.seqLen&seqChunkMask == 0 {
		s.seqChunks = append(s.seqChunks, make([]uint64, seqChunkSize))
	}
	s.seqChunks[s.seqLen>>seqChunkShift][s.seqLen&seqChunkMask] = v
	s.seqLen++
}

// seqAtLocked reads the arena entry at a logical index (0 when out of range).
// Callers hold s.mu.
func (s *Slot) seqAtLocked(i int) uint64 {
	if i < 0 || i >= s.seqLen {
		return 0
	}
	return s.seqChunks[i>>seqChunkShift][i&seqChunkMask]
}

// arenaBytes is the memory the seq arena holds (diagnostics/tests).
func (s *Slot) arenaBytes() int { return len(s.seqChunks)*seqChunkSize*8 + len(s.seqChunks)*16 }
