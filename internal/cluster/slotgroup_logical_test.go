package cluster

import "testing"

// TestSlotGroupPhysicalLogical pins the only conversion the client plane relies
// on: a logical slot id (0 .. size-1) plus the generation base IS the table slot,
// and the reverse. g2 is the smallest production generation — its physical ids
// are 4..7, its logical ids 0..3.
func TestSlotGroupPhysicalLogical(t *testing.T) {
	g := SlotGroup{Gen: 2, Base: 4, Size: 4, Epoch: 1}
	for logical := int32(0); logical < g.Size; logical++ {
		if got, want := g.Physical(logical), 4+logical; got != want {
			t.Fatalf("Physical(%d) = %d, want %d", logical, got, want)
		}
		if got := g.Logical(g.Physical(logical)); got != logical {
			t.Fatalf("round trip %d -> %d", logical, got)
		}
	}
	// A slot outside the segment keeps its raw id: the conversion must not point
	// at a wrong row when something upstream is broken.
	if got := g.Logical(0); got != 0 {
		t.Fatalf("out-of-segment slot 0 became %d", got)
	}
	if got := g.Logical(8); got != 8 {
		t.Fatalf("out-of-segment slot 8 became %d", got)
	}
	// A zero-based segment is the identity mapping, which is what keeps the
	// development shape and the tests free of a second rule.
	z := SlotGroup{Gen: -1, Base: 0, Size: 2048, Epoch: 1}
	for _, s := range []int32{0, 1, 1023, 2047} {
		if got := z.Physical(s); got != s {
			t.Fatalf("zero-based Physical(%d) = %d", s, got)
		}
		if got := z.Logical(s); got != s {
			t.Fatalf("zero-based Logical(%d) = %d", s, got)
		}
	}
}
