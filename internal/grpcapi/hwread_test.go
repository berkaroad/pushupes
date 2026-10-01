package grpcapi

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"pushupes/internal/cluster"
	"pushupes/internal/data"
	pushupesv1 "pushupes/internal/grpcapi/pushupes/v1"
	"pushupes/internal/storage"
)

// A leader must be able to read its own durable log. Capping a LEADER's read at
// the replication high watermark turns a complete stream into a SILENT short
// read whenever a replica lags behind — which is exactly how "acked writes come
// back missing from the new leader" was observed. The watermark bound belongs
// to a follower's view, not to the node that owns the log.
func TestLeaderReadIsNotCappedByHighWatermark(t *testing.T) {
	store, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	eng := cluster.NewEngine(nil, store, "node-1", "all", nil)

	for _, id := range []string{"node-1", "node-2", "node-3"} {
		c := &cluster.Command{Op: cluster.OpJoinNode, Peer: &cluster.Peer{
			ID: id, PeerAddr: "127.0.0.1:1", AdminAddr: "127.0.0.1:1", ClientAddr: "127.0.0.1:1",
		}}
		if _, err := eng.ApplyCommand(c.Encode()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := eng.ApplyCommand((&cluster.Command{Op: cluster.OpPlanSlots}).Encode()); err != nil {
		t.Fatal(err)
	}

	// Find an aggregate whose slot node-1 leads and node-2 replicates (so the
	// watermark can be pinned by a lagging replica).
	tbl := eng.TableSnapshot()
	agg, slot := "", int32(-1)
	follower := ""
	for i := 0; i < 4096 && agg == ""; i++ {
		id := "hw-agg-" + itoa(i)
		s := data.SlotOf(id, data.DefaultSlotCount)
		p, ok := tbl.Slots[s]
		if !ok || p.Leader != "node-1" {
			continue
		}
		// the watermark is the min over the table's replicas including the
		// leader; any other replica will do
		for _, r := range p.Replicas {
			if r != "node-1" {
				follower = r
				break
			}
		}
		if follower == "" {
			continue
		}
		agg, slot = id, s
	}
	if agg == "" {
		t.Skip("no slot led by node-1 with a replica in this table")
	}
	if !eng.Leads(slot) {
		t.Fatalf("test setup: node-1 should lead slot %d", slot)
	}

	// Three durable records, then pin the watermark at 1 as a lagging replica
	// would. Before the fix a leader read stopped at seq 1.
	for v := 1; v <= 3; v++ {
		if _, err := store.Append(&data.EventRecord{
			AggregateID: agg, Version: uint32(v),
			CommandID: "hw-cmd-" + itoa(v),
			Events:    []data.Event{{Type: "E", Body: []byte("x")}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	eng.NoteReplicaProgress(slot, follower, 1)
	if hw := eng.HW(slot); hw != 1 {
		t.Fatalf("test setup: want watermark 1, got %d", hw)
	}

	cli := serveEngine(t, eng, store)
	cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.ReadStream(cctx, &pushupesv1.ReadStreamRequest{AggregateId: agg, FromVersion: 1, Limit: 100})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(resp.Records) != 3 {
		t.Fatalf("leader read returned %d records, want 3 (the durable log must not be capped by HW=%d)",
			len(resp.Records), eng.HW(slot))
	}
}

// A node that once led a slot keeps the leader-side replication bookkeeping
// (repl[slot]) it accumulated while leading: nothing clears it when the table
// moves the slot's leadership elsewhere. The high watermark inside it is then
// FROZEN at the step-down point, and a replica read that trusted it silently
// truncates there — zero records for an aggregate written after the step-down,
// a few short for one that straddles it, and no error either way.
//
// A replica's local read must be bounded by its OWN durable LEO instead: it
// returns every record its log actually holds, and a leftover watermark that
// only ever described what a former leader had confirmed against its
// followers must not narrow that.
func TestSteppedDownReplicaReadsItsOwnLog(t *testing.T) {
	store, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	eng := cluster.NewEngine(nil, store, "node-1", "all", nil)

	for _, id := range []string{"node-1", "node-2", "node-3"} {
		c := &cluster.Command{Op: cluster.OpJoinNode, Peer: &cluster.Peer{
			ID: id, PeerAddr: "127.0.0.1:1", AdminAddr: "127.0.0.1:1", ClientAddr: "127.0.0.1:1",
		}}
		if _, err := eng.ApplyCommand(c.Encode()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := eng.ApplyCommand((&cluster.Command{Op: cluster.OpPlanSlots}).Encode()); err != nil {
		t.Fatal(err)
	}

	// An aggregate whose slot node-1 leads and another node replicates, so a
	// watermark can be pinned by a lagging replica in the first place.
	tbl := eng.TableSnapshot()
	agg, slot, follower := "", int32(-1), ""
	for i := 0; i < 4096 && agg == ""; i++ {
		id := "stepdown-agg-" + itoa(i)
		s := eng.SlotOf(id)
		p, ok := tbl.Slots[s]
		if !ok || p.Leader != "node-1" {
			continue
		}
		for _, r := range p.Replicas {
			if r != "node-1" {
				follower = r
				break
			}
		}
		if follower == "" {
			continue
		}
		agg, slot = id, s
	}
	if agg == "" {
		t.Skip("no slot led by node-1 with a replica in this table")
	}

	appendV := func(v int) {
		t.Helper()
		if _, err := store.Append(&data.EventRecord{
			AggregateID: agg, Version: uint32(v),
			CommandID: "sd-cmd-" + itoa(v),
			Events:    []data.Event{{Type: "E", Body: []byte("x")}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Three versions while node-1 leads, with the watermark pinned low (seq 1)
	// by a lagging replica: this is the value that freezes at step-down.
	for v := 1; v <= 3; v++ {
		appendV(v)
	}
	eng.NoteReplicaProgress(slot, follower, 1)
	if hw := eng.HW(slot); hw != 1 {
		t.Fatalf("test setup: want live watermark 1 while leading, got %d", hw)
	}

	// node-1 steps down: the table hands the slot to the follower and node-1
	// is left as an ordinary replica (still in the replica set).
	if _, err := eng.ApplyCommand((&cluster.Command{
		Op: cluster.OpLeaderMove, Slots: []int32{slot}, NewLeader: follower,
	}).Encode()); err != nil {
		t.Fatal(err)
	}
	if eng.Leads(slot) {
		t.Fatalf("test setup: node-1 should no longer lead slot %d", slot)
	}

	// One more version lands AFTER the step-down - exactly what the new leader
	// accepts and this replica fetches. The aggregate now straddles the
	// step-down point.
	appendV(4)
	if leo := store.LastSeqOf(slot); leo != 4 {
		t.Fatalf("test setup: local LEO %d want 4", leo)
	}

	cli := serveEngine(t, eng, store)
	cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.ReadStream(cctx, &pushupesv1.ReadStreamRequest{AggregateId: agg, FromVersion: 1, Limit: 100})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(resp.Records) != 4 {
		t.Fatalf("stepped-down replica read returned %d records (up to v%d); its local log "+
			"holds all 4 (v1..v4) - a frozen watermark must not silently short-read a replica",
			len(resp.Records), resp.LastVersion)
	}
	for i, r := range resp.Records {
		if r.Version != uint32(i+1) {
			t.Fatalf("record %d has version %d, want %d: silent gap in the stream", i, r.Version, i+1)
		}
	}
	if resp.NextVersion != 5 || resp.LastVersion != 4 {
		t.Fatalf("next/last version %d/%d want 5/4", resp.NextVersion, resp.LastVersion)
	}
	// Root cause: the stepped-down node still reports the leader-side
	// watermark frozen at its step-down point. It must report 0 instead —
	// that is what makes the read fall back to the node's own durable LEO.
	if hw := eng.HW(slot); hw != 0 {
		t.Fatalf("stepped-down replica reports frozen watermark %d, want 0: the "+
			"watermark is leader-side bookkeeping and is not valid on a non-leader", hw)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [12]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}

// serveEngine wires a gRPC server onto an engine over an in-memory listener.
func serveEngine(t *testing.T, eng *cluster.Engine, store *storage.Store) pushupesv1.EventServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	NewServer(eng, store, nil).Register(gs)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pushupesv1.NewEventServiceClient(conn)
}
