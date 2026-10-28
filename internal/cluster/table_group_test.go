package cluster

import (
	"fmt"
	"testing"

	"github.com/berkaroad/pushupes/internal/data"
)

// TestTableBinaryCarriesGeneration pins the v9 record: the served slot
// generation travels with the snapshot, so a node restored through one routes
// into the same segment the cluster agreed on.
func TestTableBinaryCarriesGeneration(t *testing.T) {
	tbl := NewTable(2048, 2)
	tbl.GroupGen = 10
	tbl.GroupBase = 1024
	tbl.GroupSize = 1024
	tbl.LayoutEpoch = 7
	tbl.Slots[1024] = &Placement{Leader: "node-1", Replicas: []string{"node-1", "node-2"}, Epoch: 1, State: SlotStable}

	raw := tbl.EncodeTableBinary()
	back, err := DecodeTableBinary(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if back.GroupGen != 10 || back.GroupBase != 1024 || back.GroupSize != 1024 || back.LayoutEpoch != 7 {
		t.Fatalf("generation lost in the snapshot: gen=%d base=%d size=%d epoch=%d",
			back.GroupGen, back.GroupBase, back.GroupSize, back.LayoutEpoch)
	}
	if p := back.Slots[1024]; p == nil || p.Leader != "node-1" {
		t.Fatalf("slot lost in the snapshot: %+v", p)
	}
}

// TestTableCloneCarriesGeneration: a clone that dropped the generation would
// route the next append into the wrong segment.
func TestTableCloneCarriesGeneration(t *testing.T) {
	tbl := NewTable(2048, 2)
	tbl.GroupGen, tbl.GroupBase, tbl.GroupSize, tbl.LayoutEpoch = 8, 256, 256, 3
	c := tbl.Clone()
	if c.GroupGen != 8 || c.GroupBase != 256 || c.GroupSize != 256 || c.LayoutEpoch != 3 {
		t.Fatalf("clone lost the generation: gen=%d base=%d size=%d epoch=%d",
			c.GroupGen, c.GroupBase, c.GroupSize, c.LayoutEpoch)
	}
}

// TestDecodeRejectsBadGeneration: base is derived (it must equal the size) and
// the segment has to fit the fixed table — a snapshot carrying anything else is
// refused rather than routing into the wrong range.
func TestDecodeRejectsBadGeneration(t *testing.T) {
	cases := []struct {
		name string
		gen  int
		base int32
		size int32
	}{
		{"base != size", 10, 512, 1024},
		{"size not a power of two", 10, 1000, 1000},
		{"gen does not match size", 9, 1024, 1024},
		{"size exceeds half the table", 11, 2048, 2048},
		{"fields set with size 0", 3, 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tbl := NewTable(2048, 2)
			tbl.GroupGen, tbl.GroupBase, tbl.GroupSize = c.gen, c.base, c.size
			raw := tbl.EncodeTableBinary()
			if _, err := DecodeTableBinary(raw); err == nil {
				t.Fatalf("decode accepted an invalid generation (gen=%d base=%d size=%d)", c.gen, c.base, c.size)
			}
		})
	}
}

// TestSlotForAgg pins the generation routing the write path uses: a generation
// of size s occupies [s, 2s-1] (the base IS the size), a zero-based segment
// covers [0, size), and a table with no segment at all routes nowhere — there is
// no flat rule left to fall back to.
func TestSlotForAgg(t *testing.T) {
	count := int32(2048)
	if got := SlotForAgg("agg-1", SlotGroup{}, count); got != -1 {
		t.Fatalf("no segment must route nowhere, got %d", got)
	}
	zeroBased := SlotGroup{Gen: -1, Base: 0, Size: count, Epoch: 1}
	if got, want := SlotForAgg("agg-1", zeroBased, count), data.LogicalSlotOf("agg-1", int(count)); got != want {
		t.Fatalf("zero-based segment: got %d want %d", got, want)
	}
	for _, size := range []int32{1, 2, 4, 8, 16, 256, 1024} {
		g := SlotGroup{Base: size, Size: size, Epoch: 1}
		for i := 0; i < 500; i++ {
			id := fmt.Sprintf("agg-%d", i)
			got := SlotForAgg(id, g, count)
			if want := g.Base + data.LogicalSlotOf(id, int(size)); got != want {
				t.Fatalf("SlotForAgg(%q, size=%d) = %d, want %d", id, size, got, want)
			}
			if got < size || got > 2*size-1 {
				t.Fatalf("SlotForAgg(%q, size=%d) = %d outside [%d, %d]", id, size, got, size, 2*size-1)
			}
		}
	}
}

// TestParseSlotGroup pins the -slot-group names: the number is the generation
// index, and the segment has to fit the table twice.
func TestParseSlotGroup(t *testing.T) {
	ok := []struct {
		name string
		gen  int
		size int32
	}{
		{"g0", 0, 1}, {"g1", 1, 2}, {"g3", 3, 8}, {"g10", 10, 1024}, {"G10", 10, 1024},
		{"10", 10, 1024}, {" g2 ", 2, 4},
	}
	for _, c := range ok {
		g, err := ParseSlotGroup(c.name, 2048)
		if err != nil {
			t.Fatalf("ParseSlotGroup(%q): %v", c.name, err)
		}
		if g.Gen != c.gen || g.Base != c.size || g.Size != c.size || g.Epoch != 1 {
			t.Fatalf("ParseSlotGroup(%q) = %+v, want gen=%d base=size=%d epoch=1", c.name, g, c.gen, c.size)
		}
	}
	for _, name := range []string{"g11", "-1", "x", "", "g", "g1x"} {
		if _, err := ParseSlotGroup(name, 2048); err == nil {
			t.Fatalf("ParseSlotGroup(%q) accepted an invalid name", name)
		}
	}
	// g10 does not fit a 1024-slot table (the segment must stay inside it).
	if _, err := ParseSlotGroup("g10", 1024); err == nil {
		t.Fatal("ParseSlotGroup accepted a segment larger than half the table")
	}
}

// TestPlanSlotsFollowsGeneration: with a generation on, the plan covers ONLY
// the segment [size, 2size) and its leaders agree with ringLeader (the leader
// rebalancer's frame), so plan and rebalance cannot fight each other.
func TestPlanSlotsFollowsGeneration(t *testing.T) {
	tbl := NewTable(2048, 2)
	nodes := []string{"n1", "n2", "n3"}

	all := tbl.planSlots(nodes)
	if len(all) != 2048 {
		t.Fatalf("no generation must plan the whole table: got %d slots", len(all))
	}

	tbl.GroupGen, tbl.GroupBase, tbl.GroupSize, tbl.LayoutEpoch = 3, 8, 8, 1
	seg := tbl.planSlots(nodes)
	if len(seg) != 8 {
		t.Fatalf("g3 must plan 8 slots, got %d", len(seg))
	}
	for s := int32(8); s < 16; s++ {
		if seg[s] == nil {
			t.Fatalf("g3 must plan slot %d", s)
		}
		if got, want := seg[s].Leader, tbl.ringLeader(nodes, s); got != want {
			t.Fatalf("slot %d leader %q != ringLeader %q", s, got, want)
		}
	}
	if seg[0] != nil || seg[7] != nil || seg[16] != nil || seg[2047] != nil {
		t.Fatal("g3 planned a slot outside [8, 16)")
	}
	counts := map[string]int{}
	for _, p := range seg {
		counts[p.Leader]++
		if len(p.Replicas) != 2 || p.Replicas[0] == p.Replicas[1] {
			t.Fatalf("slot replica set not a factor-2 pair: %v", p.Replicas)
		}
	}
	lo, hi := 100, 0
	for _, n := range nodes {
		if counts[n] < lo {
			lo = counts[n]
		}
		if counts[n] > hi {
			hi = counts[n]
		}
	}
	if hi-lo > 1 {
		t.Fatalf("g3 not balanced over 3 nodes: %v", counts)
	}
}

// TestApplySetGroupSwitchesSegment pins the generation switch: one command
// re-plans the new segment, bumps layout_epoch and leaves no slot of the old
// segment in the table. Replaying the same command is a no-op (an operator retry
// must not move the layout again).
func TestApplySetGroupSwitchesSegment(t *testing.T) {
	tbl := NewTable(2048, 2)
	tbl.Peers["n1"] = Peer{ID: "n1", Role: "learner", PeerAddr: "127.0.0.1:1", ClientAddr: "127.0.0.1:11"}
	tbl.Peers["n2"] = Peer{ID: "n2", Role: "learner", PeerAddr: "127.0.0.1:2", ClientAddr: "127.0.0.1:12"}
	tbl.GroupGen, tbl.GroupBase, tbl.GroupSize, tbl.LayoutEpoch = 3, 8, 8, 1
	tbl.Slots = tbl.planSlots([]string{"n1", "n2"})
	if len(tbl.Slots) != 8 {
		t.Fatalf("bootstrap g3 planned %d slots, want 8", len(tbl.Slots))
	}

	if err := tbl.Apply(&Command{Op: OpSetGroup, GroupGen: 4, GroupBase: 16, GroupSize: 16}); err != nil {
		t.Fatalf("set_group g4: %v", err)
	}
	if tbl.GroupGen != 4 || tbl.GroupBase != 16 || tbl.GroupSize != 16 || tbl.LayoutEpoch != 2 {
		t.Fatalf("after switch: gen=%d base=%d size=%d epoch=%d", tbl.GroupGen, tbl.GroupBase, tbl.GroupSize, tbl.LayoutEpoch)
	}
	if len(tbl.Slots) != 16 {
		t.Fatalf("g4 planned %d slots, want 16", len(tbl.Slots))
	}
	for s := int32(16); s < 32; s++ {
		if tbl.Slots[s] == nil {
			t.Fatalf("g4 must plan slot %d", s)
		}
	}
	for _, s := range []int32{0, 8, 15, 32, 2047} {
		if tbl.Slots[s] != nil {
			t.Fatalf("g4 planned slot %d outside [16, 32)", s)
		}
	}

	// Idempotent replay: same command again changes nothing.
	if err := tbl.Apply(&Command{Op: OpSetGroup, GroupGen: 4, GroupBase: 16, GroupSize: 16}); err != nil {
		t.Fatalf("idempotent set_group: %v", err)
	}
	if tbl.LayoutEpoch != 2 || len(tbl.Slots) != 16 {
		t.Fatalf("replay moved the layout: epoch=%d slots=%d", tbl.LayoutEpoch, len(tbl.Slots))
	}

	// A segment that does not fit the table is refused and nothing changes.
	if err := tbl.Apply(&Command{Op: OpSetGroup, GroupGen: 11, GroupBase: 2048, GroupSize: 2048}); err == nil {
		t.Fatal("set_group accepted a segment larger than half the table")
	}
	if tbl.GroupSize != 16 || tbl.LayoutEpoch != 2 {
		t.Fatalf("a refused switch changed the table: size=%d epoch=%d", tbl.GroupSize, tbl.LayoutEpoch)
	}
}

// TestSealingOutSurvivesTheSnapshot pins the new slot state in the binary table:
// a state the codec does not know would decode as some other value and turn a
// sealing slot (which must not take new client writes) back into a stable one on
// every node that restores from a snapshot.
func TestSealingOutSurvivesTheSnapshot(t *testing.T) {
	tbl := seatTable([]string{"node-1", "node-2", "node-3"}, 16, 2)
	tbl.GroupGen, tbl.GroupBase, tbl.GroupSize = 3, 8, 8
	tbl.Slots[8].State = SlotSealingOut
	raw := tbl.EncodeTableBinary()
	back, err := DecodeTableBinary(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if p := back.Slots[8]; p == nil || p.State != SlotSealingOut {
		t.Fatalf("sealing_out did not survive the snapshot: %+v", p)
	}
	if p := back.Slots[9]; p == nil || p.State != SlotStable {
		t.Fatalf("a stable slot turned into %+v", back.Slots[9])
	}
}
