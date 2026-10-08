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

// The per-slot aggregate index: which aggregates the slot holds, their latest
// version, and where their seqs live.
//
// Each aggregate owns a chunked list of its own seqs, in the order the
// aggregate's records were committed. That list — not a shared arena — is what
// "the version v of this aggregate is at index v-1" rests on: a shared arena
// interleaves aggregates (arena = [seqA1 seqB1 seqA2 ...]), so an aggregate's
// seqs are not a contiguous range of it and indexing by version resolves to
// another aggregate's record as soon as two aggregates interleave.
//
// Chunk sizes grow 16, 64, 256 and then 1024: a slot with hundreds of thousands
// of one-version aggregates pays 128 bytes each instead of 8 KiB, while a large
// aggregate still reads a seq with a shift and a mask. Sealed seqs are dropped
// from the front (sealedN counts them) and the head is compacted when the dead
// prefix grows past the live part.
type aggEntry struct {
	version uint32 // latest committed version (rule 2)
	// The aggregate's seqs are split: sealedN of them live on disk in the
	// sealed segments' aggregate indexes (see segaidx.go), n of them are still
	// in this list because they belong to the writable segment. The n live seqs
	// are versions sealedN+1..sealedN+n, at list indexes 0..n-1.
	sealedN int
	head    int        // seqs already dropped from the front of chunks
	n       int        // live seqs held (list indexes 0..n-1)
	chunks  [][]uint64 // ascending chunks; the first head+n entries are the aggregate's seqs
	// stamps holds each live record's unix_time with the same geometry as
	// chunks, so a point-in-time answer for a live version is an in-memory
	// read instead of a WAL frame read (see VersionAtOrBeforeTime). Sealed
	// versions take their stamps from the sealed segment's aggregate index.
	stamps [][]uint64
	// lastUnix is the unix_time of the latest version (the directory's own
	// view of "when was this stream last written") — what the console's event
	// stream listing shows. It moves only when version does.
	lastUnix int64
}

const seqChunkMax = 1024 // 8 KiB per chunk once an aggregate is past 336 seqs

// seqChunkPos maps a list index to its chunk and offset inside it.
func seqChunkPos(i int) (ci, off int) {
	switch {
	case i < 16:
		return 0, i
	case i < 80:
		return 1, i - 16
	case i < 336:
		return 2, i - 80
	}
	j := i - 336
	return 3 + j/seqChunkMax, j % seqChunkMax
}

func seqChunkLen(ci int) int {
	switch ci {
	case 0:
		return 16
	case 1:
		return 64
	case 2:
		return 256
	}
	return seqChunkMax
}

// seqAt reads the aggregate's i-th live seq (0 when out of range).
func (e *aggEntry) seqAt(i int) uint64 {
	if i < 0 || i >= e.n {
		return 0
	}
	ci, off := seqChunkPos(i + e.head)
	if ci >= len(e.chunks) || off >= len(e.chunks[ci]) {
		return 0
	}
	return e.chunks[ci][off]
}

// stampAt reads the unix_time of the aggregate's i-th live record (0 when out
// of range). It is aligned with seqAt: both index the same live list.
func (e *aggEntry) stampAt(i int) int64 {
	if i < 0 || i >= e.n {
		return 0
	}
	ci, off := seqChunkPos(i + e.head)
	if ci >= len(e.stamps) || off >= len(e.stamps[ci]) {
		return 0
	}
	return int64(e.stamps[ci][off])
}

// appendSeq adds one seq and its record's unix_time (and counts it).
func (e *aggEntry) appendSeq(v uint64, unix int64) {
	ci, off := seqChunkPos(e.head + e.n)
	for len(e.chunks) <= ci {
		e.chunks = append(e.chunks, make([]uint64, seqChunkLen(len(e.chunks))))
	}
	e.chunks[ci][off] = v
	for len(e.stamps) <= ci {
		e.stamps = append(e.stamps, make([]uint64, seqChunkLen(len(e.stamps))))
	}
	e.stamps[ci][off] = uint64(unix)
	e.n++
}

// dropFirst forgets the first k live seqs (they are on disk now) and compacts
// the dead prefix once it outgrows what is left.
func (e *aggEntry) dropFirst(k int) {
	if k > e.n {
		k = e.n
	}
	e.head += k
	e.n -= k
	if e.n == 0 {
		e.chunks = nil
		e.stamps = nil
		e.head = 0
		return
	}
	if e.head >= 64 && e.head >= e.n {
		live := make([]uint64, e.n)
		times := make([]uint64, e.n)
		for i := 0; i < e.n; i++ {
			live[i] = e.seqAt(i)
			times[i] = uint64(e.stampAt(i))
		}
		e.chunks, e.stamps, e.head = nil, nil, 0
		for i, v := range live {
			e.appendSeq(v, int64(times[i]))
		}
	}
}

// bytes is the memory this aggregate's list holds (diagnostics/tests).
func (e *aggEntry) bytes() int {
	n := 0
	for _, c := range e.chunks {
		n += len(c)*8 + 24
	}
	for _, c := range e.stamps {
		n += len(c)*8 + 24
	}
	return n
}

// aggLocked returns the aggregate's directory entry (zero value when unknown).
// Callers hold s.mu.
func (s *Slot) aggLocked(aggregateID string) aggEntry {
	return s.aggs[aggregateID]
}

// arenaBytes is the memory the per-aggregate seq lists hold (diagnostics/tests).
// Callers hold s.mu.
func (s *Slot) arenaBytes() int {
	n := 0
	for _, e := range s.aggs {
		n += e.bytes()
	}
	return n
}
