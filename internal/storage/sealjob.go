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

// Seal index materialization, away from the slot write lock.
//
// Rolling a segment used to run the whole index pass under Slot.mu: close the
// .idx writer, walk the 64 MiB segment for the command index, walk it again for
// the aggregate index, sort both, and fsync three files. Every append and every
// replica apply on that slot queued behind it — measured as 10-27 ms stalls at
// the moment a slot rolled.
//
// The roll now only flips the segment to sealed and queues a job; the flusher's
// seal worker takes two short windows on the slot lock (take the .idx writer
// and snapshot the aggregates' sealed prefixes; settle the covered seqs) and
// does the walk, the sorts, the writes and the fsyncs with no lock held. The
// fsyncs go through flushMu, so the process-wide one-fsync-at-a-time invariant
// (I2) still holds.
//
// Failure stays what it was: these indexes are best effort. A segment whose
// materialization fails keeps its seqs in memory and the next start rebuilds
// the index from the segment (openSlot's repair mode). The one semantic change
// is that a failing .idx flush no longer fails the append that rolled the
// segment: it is booked on segIndexUnusable like the other two indexes.

import (
	"sort"
)

// sealJob is one sealed segment's index materialization in flight.
type sealJob struct {
	sl  *Slot
	seg *Segment
}

// sealAggSnap is one aggregate's slice of a sealed segment, copied under the
// lock so the walk and the writes need none. The content is final the moment
// the segment seals: records only land in later segments.
type sealAggSnap struct {
	id       string
	hash     uint64
	firstVer uint32
	seqs     []uint64
	stamps   []int64
}

// enqueueSealJob queues one segment for the seal worker. A frozen slot (Close or
// Drop in progress) accepts nothing: the segment's indexes rebuild at the next
// start.
func (f *flusher) enqueueSealJob(sl *Slot, seg *Segment) {
	f.mu.Lock()
	if f.frozen[sl.ID] {
		f.mu.Unlock()
		return
	}
	f.sealQ = append(f.sealQ, &sealJob{sl: sl, seg: seg})
	f.mu.Unlock()
	select {
	case f.sealWake <- struct{}{}:
	default:
	}
}

// sealWorker materializes sealed segments one at a time until the flusher
// retires. One worker bounds the transient memory of the snapshot copies; the
// walk itself is per-segment work measured in milliseconds.
func (f *flusher) sealWorker() {
	defer close(f.sealDone)
	for {
		select {
		case <-f.sealStop:
			return
		case <-f.sealWake:
		}
		for {
			f.mu.Lock()
			if len(f.sealQ) == 0 {
				f.mu.Unlock()
				break
			}
			j := f.sealQ[0]
			f.sealQ = f.sealQ[1:]
			done := make(chan struct{})
			f.sealBusy[j.sl.ID] = done
			f.mu.Unlock()

			select {
			case <-f.sealStop:

			default:
				f.materializeSeal(j)
			}

			f.mu.Lock()
			delete(f.sealBusy, j.sl.ID)
			f.mu.Unlock()
			close(done)
		}
	}
}

// sealIdle reports whether no seal job is queued or running. Tests use it to
// wait for the materialized state; the drop path waits on sealBusy instead.
func (f *flusher) sealIdle() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sealQ) == 0 && len(f.sealBusy) == 0
}

// sealHook, when set, runs between the two lock windows of a seal job — after
// the segment and its aggregate prefixes are taken, before anything is written.
// Tests use it to prove the roll returns without the materialization and that
// a drop waits for an in-flight job. Production leaves it nil.
var sealHook func(seg *Segment)

// materializeSeal writes one sealed segment's indexes with no slot lock held,
// between two short lock windows.
func (f *flusher) materializeSeal(j *sealJob) {
	sl, seg := j.sl, j.seg

	// Window 1: take the .idx writer (the job owns it from here) and snapshot
	// the aggregate prefixes this segment covers. A dropped or reloaded slot
	// no longer owns the segment: abandon.
	sl.mu.Lock()
	if !sl.ownsLocked(seg) {
		sl.mu.Unlock()
		return
	}
	idx := seg.idx
	seg.idx = nil
	if idx != nil {
		idx.syncFn = f.syncThroughMu
	}
	filters := seg.cmdFilters
	snaps := sl.snapshotSealedAggsLocked(seg)
	sl.mu.Unlock()

	// No slot lock held from here on.
	if sealHook != nil {
		sealHook(seg)
	}
	if idx != nil {
		if err := idx.close(); err != nil {
			segIndexUnusable.Add(1)
		}
	}
	if seg.RecordCnt == 0 {
		return
	}
	entries, err := collectSegCmdEntries(seg)
	if err != nil || len(entries) == 0 {
		// No trustworthy offsets: no index this round. The seqs stay in
		// memory and the next start writes the indexes (repair mode).
		segIndexUnusable.Add(1)
		return
	}
	if err := writeSegCmdIndexFrom(seg, filters, entries, f.syncThroughMu); err != nil {
		segIndexUnusable.Add(1)
	}
	lists := buildSealedAggLists(snaps, seg, entries)
	if len(lists) > 0 {
		if err := writeSegAggIndexFrom(seg, lists, f.syncThroughMu); err != nil {
			segIndexUnusable.Add(1)
			lists = nil // nothing was persisted: the seqs must stay in memory
		}
	}
	var covered map[uint64]bool
	if len(lists) > 0 {
		covered = make(map[uint64]bool, len(lists))
		for _, l := range lists {
			covered[l.hash] = true
		}
	}

	// Window 2: settle. Only what the index really covers leaves memory.
	sl.mu.Lock()
	if sl.ownsLocked(seg) {
		sl.dropSealedSeqsLocked(seg, covered)
	}
	sl.mu.Unlock()
}

// ownsLocked reports whether seg is still one of the slot's segments. A Drop
// clears the list (and Close follows it), so a queued job can tell it has been
// overtaken. Caller holds s.mu.
func (s *Slot) ownsLocked(seg *Segment) bool {
	for _, x := range s.segments {
		if x == seg {
			return true
		}
	}
	return false
}

// snapshotSealedAggsLocked copies every aggregate's prefix that belongs to the
// sealed segment: the entries its .aidx will cover, as (seq, unix_time) pairs.
// An aggregate whose earliest live seq lies in an earlier segment is skipped —
// coverage has to stay a contiguous prefix. Caller holds s.mu.
func (s *Slot) snapshotSealedAggsLocked(seg *Segment) []sealAggSnap {
	if seg.RecordCnt == 0 {
		return nil
	}
	var out []sealAggSnap
	for id, e := range s.aggs {
		if e.n == 0 {
			continue
		}
		k := sort.Search(e.n, func(i int) bool { return e.seqAt(i) > seg.LastSeq })
		if k == 0 {
			continue
		}
		if e.seqAt(0) < seg.BaseSeq {
			continue
		}
		sn := sealAggSnap{
			id:       id,
			hash:     aggHash(id),
			firstVer: uint32(e.sealedN + 1),
			seqs:     make([]uint64, k),
			stamps:   make([]int64, k),
		}
		for i := 0; i < k; i++ {
			sn.seqs[i] = e.seqAt(i)
			sn.stamps[i] = e.stampAt(i)
		}
		out = append(out, sn)
	}
	return out
}

// buildSealedAggLists turns the snapshot into the aggregate index's lists,
// using the frame walk's offsets. An aggregate with a record the walk did not
// place is left out: the index must never cover a hole.
func buildSealedAggLists(snaps []sealAggSnap, seg *Segment, entries []segCmdEntry) []segAggList {
	if len(snaps) == 0 || seg.RecordCnt == 0 {
		return nil
	}
	offs := make([]uint32, seg.RecordCnt)
	for _, e := range entries {
		if ord := e.seq - seg.BaseSeq; int64(ord) < seg.RecordCnt {
			offs[ord] = uint32(e.off)
		}
	}
	lists := make([]segAggList, 0, len(snaps))
	for _, sn := range snaps {
		pairs := make([]segAggPair, 0, len(sn.seqs))
		ok := true
		for i, seq := range sn.seqs {
			ord := seq - seg.BaseSeq
			if int64(ord) >= seg.RecordCnt || offs[ord] == 0 {
				ok = false
				break
			}
			pairs = append(pairs, segAggPair{ord: uint32(ord), off: offs[ord], unix: sn.stamps[i]})
		}
		if !ok || len(pairs) == 0 {
			continue
		}
		lists = append(lists, segAggList{
			hash:     sn.hash,
			firstVer: sn.firstVer,
			firstOrd: uint32(sn.seqs[0] - seg.BaseSeq),
			pairs:    pairs,
		})
	}
	return lists
}

// enqueueSealedIndexLocked hands the sealed segment's index materialization to
// the store's seal worker. A slot opened without a store (tests drive slots
// directly) runs the same work inline, under the lock, as it always did.
// Caller holds s.mu.
func (s *Slot) enqueueSealedIndexLocked(seg *Segment) error {
	if f := s.flusher(); f != nil {
		f.enqueueSealJob(s, seg)
		return nil
	}
	return s.materializeSealedLocked(seg)
}

// materializeSealedLocked is the synchronous form used when no flusher exists.
// Its error contract is the old one: a failing .idx flush fails the append that
// rolled the segment, while the two best-effort indexes only count.
func (s *Slot) materializeSealedLocked(seg *Segment) error {
	if err := seg.indexFlushAndClose(); err != nil {
		return err
	}
	if err := writeSegCmdIndex(seg, seg.cmdFilters); err != nil {
		segIndexUnusable.Add(1)
	}
	if err := s.sealAggSeqsLocked(seg); err != nil {
		segIndexUnusable.Add(1)
	}
	return nil
}
