package grpcapi

import (
	"context"
	"testing"
	"time"

	"pushupes/internal/cluster"
	"pushupes/internal/data"
	pushupesv1 "pushupes/internal/grpcapi/pushupes/v1"
	"pushupes/internal/storage"
)

// ReadTails replaced a per-aggregate probe chain (exponential + binary
// ReadStream calls) in the bench's resume scan. The scan feeds
// `version = tail + 1` into every append, so a tail that is even one low turns
// into a stream of 1001 version conflicts, and one that is too high silently
// drops the intervening versions. The bulk form must therefore return exactly
// what the per-aggregate probe returned, per aggregate, for every shape of
// stream: absent, single-record, and a long one whose tail sits well past any
// doubling boundary.
//
// The reference here is the storage accessor the probe resolved to — the tail
// of the visible log — plus the version a bounded ReadStream reports as its
// last_version. Asserting against both is what pins "same answer as before"
// (tail value) and "same answer as ReadStream" (boundary semantics).
func TestReadTailsMatchesPerAggregateProbe(t *testing.T) {
	store, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	eng := cluster.NewEngine(nil, store, "node-1", nil)

	for _, id := range []string{"node-1", "node-2", "node-3"} {
		c := &cluster.Command{Op: cluster.OpJoinNode, Peer: &cluster.Peer{
			ID: id, PeerAddr: "127.0.0.1:1", AdminAddr: "127.0.0.1:1", ClientAddr: "127.0.0.1:1",
		}}
		if _, err := eng.ApplyCommand(c.Encode()); err != nil {
			t.Fatal(err)
		}
	}
	// Read-path fixture: the copy count is pinned (production derives it from
	// the member count), so the reads under test see the layout they expect.
	pinTwoReplicas(t, eng)
	if _, err := eng.ApplyCommand((&cluster.Command{Op: cluster.OpPlanSlots}).Encode()); err != nil {
		t.Fatal(err)
	}

	// Records land on whatever slot the aggregate hashes to; node-1 leads every
	// slot in a one-voter table, so local reads answer all of them.
	shapes := []struct {
		agg     string
		records int
	}{
		{"tails-absent", 0},
		{"tails-single", 1},
		{"tails-long", 37}, // crosses the 1->2->4->8->16->32 doubling steps
		{"tails-exact-power", 32},
	}
	for _, sh := range shapes {
		for v := 1; v <= sh.records; v++ {
			if _, err := store.Append(&data.EventRecord{
				AggregateID: sh.agg, Version: uint32(v),
				CommandID: sh.agg + "-cmd-" + itoa(v),
				Events:    []data.Event{{Type: "E", Body: []byte("x")}},
			}); err != nil {
				t.Fatal(err)
			}
		}
	}

	cli := serveEngine(t, eng, store)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// One call carrying every shape at once: order must survive, and the
	// absent aggregate must answer 0 rather than shifting its neighbours.
	ids := make([]string, len(shapes))
	for i, sh := range shapes {
		ids[i] = sh.agg
	}
	resp, err := cli.ReadTails(ctx, &pushupesv1.ReadTailsRequest{AggregateIds: ids})
	if err != nil {
		t.Fatalf("ReadTails: %v", err)
	}
	if len(resp.Versions) != len(ids) {
		t.Fatalf("ReadTails returned %d versions for %d aggregates", len(resp.Versions), len(ids))
	}

	// The reference: what the removed exponential+binary probe resolved to,
	// i.e. the tail of the visible log, and independently the last_version a
	// bounded ReadStream reports.
	for i, sh := range shapes {
		wantTail, err := store.TailVersionOf(sh.agg, 0)
		if err != nil {
			t.Fatalf("reference tail %s: %v", sh.agg, err)
		}
		if wantTail != uint32(sh.records) {
			t.Fatalf("test setup: reference tail for %s is %d, want %d", sh.agg, wantTail, sh.records)
		}
		if got := resp.Versions[i]; got != wantTail {
			t.Errorf("ReadTails[%s] = %d, want %d (the per-aggregate probe's answer)",
				sh.agg, got, wantTail)
		}

		// ReadStream must agree on the boundary for the same stream.
		rs, err := cli.ReadStream(ctx, &pushupesv1.ReadStreamRequest{
			AggregateId: sh.agg, FromVersion: 1, Limit: 0,
		})
		if err != nil {
			t.Fatalf("ReadStream %s: %v", sh.agg, err)
		}
		wantLast := uint32(0)
		if len(rs.Records) > 0 {
			wantLast = rs.Records[len(rs.Records)-1].Version
		}
		if got := resp.Versions[i]; got != wantLast {
			t.Errorf("ReadTails[%s] = %d but ReadStream ends at version %d",
				sh.agg, got, wantLast)
		}
	}

	// A duplicate aggregate must get the same answer in both positions (the
	// resume scan can list one id twice through overlapping worker partitions).
	dup, err := cli.ReadTails(ctx, &pushupesv1.ReadTailsRequest{
		AggregateIds: []string{"tails-long", "tails-absent", "tails-long"},
	})
	if err != nil {
		t.Fatalf("ReadTails dup: %v", err)
	}
	if len(dup.Versions) != 3 || dup.Versions[0] != 37 || dup.Versions[1] != 0 || dup.Versions[2] != 37 {
		t.Errorf("ReadTails with duplicates/absent = %v, want [37 0 37]", dup.Versions)
	}

	// An empty request is legal and answers nothing.
	empty, err := cli.ReadTails(ctx, &pushupesv1.ReadTailsRequest{})
	if err != nil {
		t.Fatalf("ReadTails empty: %v", err)
	}
	if len(empty.Versions) != 0 {
		t.Errorf("ReadTails empty returned %d versions, want 0", len(empty.Versions))
	}
}

// A tail must never report versions the node cannot actually serve. A read is
// bounded by the answering node's own durable log, so an aggregate whose newest
// records are not covered must report the last version that IS covered — the
// same clipping a bounded ReadStream applies — rather than the directory's
// unclipped count. Getting this wrong makes resume skip versions and the
// following appends fail 1001 forever.
func TestReadTailsClipsAtBound(t *testing.T) {
	store, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	const agg = "tails-clip"
	for v := 1; v <= 5; v++ {
		if _, err := store.Append(&data.EventRecord{
			AggregateID: agg, Version: uint32(v),
			CommandID: "clip-cmd-" + itoa(v),
			Events:    []data.Event{{Type: "E", Body: []byte("x")}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	slot := store.SlotOf(agg)
	leo := store.LastSeqOf(slot)
	if leo == 0 {
		t.Fatal("test setup: expected a non-empty slot")
	}

	// Unbounded: the whole tail (each append got its own seq, so versions and
	// seqs advance together here).
	full, err := store.TailVersionOf(agg, 0)
	if err != nil {
		t.Fatal(err)
	}
	if full != 5 {
		t.Fatalf("unbounded tail = %d, want 5", full)
	}

	// Clip below the tail: the answer must be the last covered version, and it
	// must match the last version a ReadStream bounded by the same seq returns.
	for _, cut := range []uint64{1, 2, 3, 4, 5} {
		got, err := store.TailVersionOf(agg, cut)
		if err != nil {
			t.Fatalf("clipped tail at %d: %v", cut, err)
		}
		want := uint32(0)
		if cut >= 1 && cut <= 5 {
			want = uint32(cut)
		}
		if got != want {
			t.Errorf("tail clipped at seq %d = %d, want %d", cut, got, want)
		}
	}
}
