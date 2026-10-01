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
