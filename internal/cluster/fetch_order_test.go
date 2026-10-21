package cluster

import "testing"

// TestFetchVisitOrderIsPermutation pins the invariant the MFetch visit loop
// indexes on: every entry of order is a position in [0, n) and order is a
// permutation of it — the loop feeds each entry straight into req.Slots[i].
// The two bulk classes (ring seats, backlog) share one scratch buffer; when
// their arrivals interleave, a growing ring offset makes the ring phase
// overwrite an already-placed bulk entry and leave a HOLE behind, which
// copies stale scratch contents (a previous round's larger index) into the
// visit order — the index-out-of-range panic observed on a live leader when
// two follower sessions with different slot-set sizes recycled one pooled
// scan (panic: index out of range [406] with length 341).
func TestFetchVisitOrderIsPermutation(t *testing.T) {
	// The shape that reproduces the hole: bulk, ring, bulk, ack. The ring
	// entry writes rest[0] over the first bulk entry, the second bulk entry
	// lands at rest[2], and rest[1] is never written — the poison below
	// stands in for the previous round's residue in the pooled buffer.
	for _, rot := range []int{0, 1, 3, 4, 7} {
		n := 4
		ack := map[int]bool{3: true}
		ringMoved := map[int]bool{1: true}
		order := make([]int, n)
		rest := make([]int, n)
		for i := range rest {
			rest[i] = 406 // stale scratch from a bigger round
		}
		buildFetchOrder(n, rot, order, rest, make([]int, n),
			func(i int) bool { return ack[i] },
			func(i int) bool { return ringMoved[i] })
		seen := make([]bool, n)
		for _, i := range order {
			if i < 0 || i >= n {
				t.Fatalf("rot %d: visit order %v carries out-of-range index %d", rot, order, i)
			}
			if seen[i] {
				t.Fatalf("rot %d: visit order %v visits position %d twice", rot, order, i)
			}
			seen[i] = true
		}
	}
	// A long mixed interleave keeps the permutation over a larger round.
	n := 64
	ack, ring := map[int]bool{}, map[int]bool{}
	for i := 0; i < n; i++ {
		switch i % 5 {
		case 0:
			ack[i] = true
		case 2:
			ring[i] = true
		}
	}
	order := make([]int, n)
	rest := make([]int, n)
	for i := range rest {
		rest[i] = 999
	}
	buildFetchOrder(n, 17, order, rest, make([]int, n),
		func(i int) bool { return ack[i] },
		func(i int) bool { return ring[i] })
	seen := make([]bool, n)
	for _, i := range order {
		if i < 0 || i >= n || seen[i] {
			t.Fatalf("mixed interleave: order not a permutation (entry %d seen twice or out of range)", i)
		}
		seen[i] = true
	}
}
