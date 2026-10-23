package storage

// Stranded-frame repair (indexMetaLocked / closeStrandedLocked): a WAL whose
// frames ARRIVE out of version order (a rebuild pushed a scrumbled suffix, a
// fence replayed a twin chain) must never leave the directory stalled while
// every record is on disk. A withheld claim is remembered, and the chain
// closes THE MOMENT its predecessor lands — live or during open replay — so
// there is no restart-wait window in which reads answer short of what was
// acknowledged. A TRUE gap (the predecessor never existed anywhere) still
// stops the chain: repair never fabricates resolvability, and the invariant
// claimed == resolvable survives every shape below.

import (
	"testing"

	"github.com/berkaroad/pushupes/internal/data"
)

func strandedRec(id string, version uint32) *data.EventRecord {
	return &data.EventRecord{
		AggregateID: id, Version: version,
		CommandID: id + "-s" + itoaSmall(int(version)),
		Events:    []data.Event{{Type: "E", Body: []byte("x")}},
	}
}

func TestStrandedChainClosesWhenPredecessorLands(t *testing.T) {
	const agg = "str-agg"
	dir := t.TempDir()

	s, err := OpenSlotRepairing(dir, 3, 64*1024*1024, DefaultFlushPolicy())
	if err != nil {
		t.Fatal(err)
	}
	// v2 lands at seq 1 (withheld), v4 at seq 2 (withheld), v1 at seq 3 —
	// which claims 1 AND immediately adopts stranded 2, then v3 at seq 4 —
	// claims 3 AND adopts stranded 4. No restart needed: the chain closes
	// when the predecessor lands.
	frames := []struct {
		seq uint64
		ver uint32
	}{{1, 2}, {2, 4}, {3, 1}, {4, 3}}
	for _, fr := range frames {
		if err := s.AppendFrameAtSeq(fr.seq, strandedRec(agg, fr.ver).EncodeBinary(nil)); err != nil {
			t.Fatalf("frame v%d at seq %d: %v", fr.ver, fr.seq, err)
		}
	}
	if a, v, r := s.SlotDigest(); v != 4 || r != 4 || a != 1 {
		t.Fatalf("live close must complete the chain, got agg=%d ver=%d res=%d", a, v, r)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen agrees (the walk closes the same chains from the WAL).
	s2, err := OpenSlot(dir, 3, 64*1024*1024, DefaultFlushPolicy())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if a, v, r := s2.SlotDigest(); v != 4 || r != 4 || a != 1 {
		t.Fatalf("reopen must agree with the live close, got agg=%d ver=%d res=%d", a, v, r)
	}
	recs, seqs, err := s2.AggregateVersion(agg, 1, 0, 0)
	if err != nil {
		t.Fatalf("repaired chain must be readable: %v", err)
	}
	if len(recs) != 4 || len(seqs) != 4 {
		t.Fatalf("want 4 readable versions, got %d", len(recs))
	}
	for i, rec := range recs {
		if rec.Version != uint32(i+1) {
			t.Fatalf("version out of order at %d: %d", i, rec.Version)
		}
	}
	// The version->seq map is the ARRIVAL map, not seq order: v1@3, v2@1,
	// v3@4, v4@2. Reads resolve each version to its own frame regardless.
	if seqs[0] != 3 || seqs[1] != 1 || seqs[2] != 4 || seqs[3] != 2 {
		t.Fatalf("version->seq mapping wrong: %v", seqs)
	}
}

func TestStrandedChainStopsAtMissingVersion(t *testing.T) {
	const agg = "gap-agg"
	dir := t.TempDir()
	s, err := OpenSlotRepairing(dir, 3, 64*1024*1024, DefaultFlushPolicy())
	if err != nil {
		t.Fatal(err)
	}
	// v3, v4 on disk, v1/v2 never written: nothing to adopt (v3's
	// predecessors do not exist), and the directory must stay self-consistent
	// (claimed == resolvable), never claim a version it cannot serve.
	if err := s.AppendFrameAtSeq(1, strandedRec(agg, 3).EncodeBinary(nil)); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendFrameAtSeq(2, strandedRec(agg, 4).EncodeBinary(nil)); err != nil {
		t.Fatal(err)
	}
	if _, v, r := s.SlotDigest(); v != 0 || r != 0 {
		t.Fatalf("a true gap must stop the chain live: ver=%d res=%d", v, r)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenSlot(dir, 3, 64*1024*1024, DefaultFlushPolicy())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if _, v, r := s2.SlotDigest(); v != 0 || r != 0 {
		t.Fatalf("a true gap must stop the repair across restart too: ver=%d res=%d", v, r)
	}
}

func TestStrandedRepairIgnoresCoveredFrames(t *testing.T) {
	const agg = "dup-agg"
	dir := t.TempDir()
	s, err := OpenSlotRepairing(dir, 3, 64*1024*1024, DefaultFlushPolicy())
	if err != nil {
		t.Fatal(err)
	}
	// First chain clean (v1, v2 at seqs 1, 2), then a duplicate SECOND chain
	// (v1, v2 again at seqs 3, 4) followed by v3 — the idempotent twins must
	// not be counted twice; v3 still resolves (to its seq 5 frame).
	for _, fr := range []struct {
		seq uint64
		ver uint32
	}{{1, 1}, {2, 2}, {3, 1}, {4, 2}, {5, 3}} {
		if err := s.AppendFrameAtSeq(fr.seq, strandedRec(agg, fr.ver).EncodeBinary(nil)); err != nil {
			t.Fatalf("seq %d: %v", fr.seq, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenSlot(dir, 3, 64*1024*1024, DefaultFlushPolicy())
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if a, v, r := s2.SlotDigest(); v != 3 || r != 3 || a != 1 {
		t.Fatalf("dupes must not inflate the chain: agg=%d ver=%d res=%d", a, v, r)
	}
}

// The dg8 slot-23 shape: a foreign-version frame lands as a NEW frame (the
// replay of a copy whose source chain skipped one version during a leader
// handover), withholds every later claim, and the WAL queue moves past the
// missing predecessor for good. The directory stays self-consistent (claimed
// == resolvable), so digests cannot see the loss — StalledAggs is what turns
// it into an actionable divergence signal for the replication layer.
func TestStalledAggsDetectsPermanentChainBreak(t *testing.T) {
	d := t.TempDir()
	s, err := OpenSlotRepairing(d, 3, 64*1024*1024, DefaultFlushPolicy())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// v1,v2 land in order; v4 then v5 park as stranded (v3 never arrives —
	// it is the version the demotion-lag frame swallowed); finally a frame
	// of ANOTHER aggregate pushes the queue tail past the strand.
	for _, fr := range []struct {
		seq uint64
		agg string
		ver uint32
	}{{1, "a", 1}, {2, "a", 2}, {3, "a", 4}, {4, "a", 5}, {5, "b", 1}} {
		if err := s.AppendFrameAtSeq(fr.seq, strandedRec(fr.agg, fr.ver).EncodeBinary(nil)); err != nil {
			t.Fatalf("seq %d: %v", fr.seq, err)
		}
	}
	stalled := s.StalledAggs()
	if len(stalled) != 1 || stalled[0] != "a" {
		t.Fatalf("chain break behind the queue tail must be reported: %v", stalled)
	}
	// The invariant repair never fabricates resolvability: a's chain stops
	// at the gap (v4/v5 on disk, unresolvable), b is whole — the slot
	// digest sums versions across aggregates: a contributes 2, b 1.
	if a, v, r := s.SlotDigest(); a != 2 || v != 3 || r != 3 {
		t.Fatalf("digest must stop at the break: agg=%d ver=%d res=%d", a, v, r)
	}
	s.mu.Lock()
	ea := s.aggs["a"]
	s.mu.Unlock()
	if ea.version != 2 {
		t.Fatalf("agg a must stay at 2 with the gap versions stranded: %+v", ea)
	}
}

// Mid-stream strands are ordinary: a batch boundary can park a version whose
// predecessor is the NEXT frame of the same source WAL (a re-sequenced source
// — the dg4 class — always heals when the replay reaches it). The detector
// must stay silent while the strand is at the queue tail, and clear entirely
// once the predecessor lands.
func TestStalledAggsSilentAtBatchBoundaryAndHeals(t *testing.T) {
	d := t.TempDir()
	s, err := OpenSlotRepairing(d, 3, 64*1024*1024, DefaultFlushPolicy())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, fr := range []struct {
		seq uint64
		ver uint32
	}{{1, 2}, {2, 3}} {
		if err := s.AppendFrameAtSeq(fr.seq, strandedRec("a", fr.ver).EncodeBinary(nil)); err != nil {
			t.Fatalf("seq %d: %v", fr.seq, err)
		}
	}
	// Strand sits at the very tail (seq2 == LEO): end-of-batch view, the
	// predecessor may still be the next frame to arrive — stay silent.
	if stalled := s.StalledAggs(); len(stalled) != 0 {
		t.Fatalf("a tail strand must not fire mid-stream: %v", stalled)
	}
	// The predecessor lands and the whole chain heals.
	if err := s.AppendFrameAtSeq(3, strandedRec("a", 1).EncodeBinary(nil)); err != nil {
		t.Fatal(err)
	}
	if stalled := s.StalledAggs(); len(stalled) != 0 {
		t.Fatalf("healed chain must report nothing: %v", stalled)
	}
	if a, v, r := s.SlotDigest(); v != 3 || r != 3 || a != 1 {
		t.Fatalf("digest after heal: agg=%d ver=%d res=%d", a, v, r)
	}
}
