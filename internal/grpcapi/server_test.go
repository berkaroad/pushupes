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
	pushupesv1 "pushupes/internal/grpcapi/pushupes/v1"
	"pushupes/internal/storage"
)

// newTestClient spins up the gRPC server over an in-memory listener and
// returns a connected client plus the backing store (for HW/tail checks).
func newTestClient(t *testing.T) (pushupesv1.EventServiceClient, *storage.Store) {
	t.Helper()
	st, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	eng := cluster.NewEngine(nil, st, "node-1", "leader", nil)

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	NewServer(eng, st, nil).Register(gs)
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
	return pushupesv1.NewEventServiceClient(conn), st
}

func appendReq(agg string, ver uint32, cmd, body string) *pushupesv1.AppendRequest {
	return &pushupesv1.AppendRequest{
		AggregateId: agg,
		Version:     ver,
		CommandId:   cmd,
		Events:      []*pushupesv1.Event{{Type: "T", Body: []byte(body)}},
	}
}

func TestGRPCAppendReadCycle(t *testing.T) {
	cli, _ := newTestClient(t)
	ctx := context.Background()

	resp, err := cli.Append(ctx, appendReq("agg-1", 1, "c-1", `{"a":1}`))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if resp.Status != pushupesv1.AppendResponse_STATUS_SUCCESS || resp.Seq != 1 {
		t.Fatalf("append1: %+v", resp)
	}
	// Success answers status/seq only: echoing the stored record back would
	// double the bytes on the wire for every large-bodied append.
	if resp.Record != nil {
		t.Fatalf("success must not echo the stored record: %+v", resp.Record)
	}

	// idempotent replay -> EXISTS, which does carry the stored record (the one
	// case the caller cannot reconstruct itself).
	resp2, err := cli.Append(ctx, appendReq("agg-1", 1, "c-1", `{"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp2.Status != pushupesv1.AppendResponse_STATUS_EXISTS || resp2.Seq != 1 {
		t.Fatalf("append dup: %+v", resp2)
	}
	if resp2.Record == nil || resp2.Record.Seq != 1 || string(resp2.Record.Events[0].Body) != `{"a":1}` {
		t.Fatalf("exists must echo the stored record: %+v", resp2.Record)
	}

	// version skip -> FAIL 1001 + current_version for self-heal
	resp3, err := cli.Append(ctx, appendReq("agg-1", 5, "c-2", `{}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp3.Status != pushupesv1.AppendResponse_STATUS_FAIL || resp3.ErrId != 1001 || resp3.CurrentVersion != 1 {
		t.Fatalf("append conflict: %+v", resp3)
	}

	// a second version lands
	if r, err := cli.Append(ctx, appendReq("agg-1", 2, "c-3", `{"b":2}`)); err != nil || r.Status != pushupesv1.AppendResponse_STATUS_SUCCESS {
		t.Fatalf("append2: %+v %v", r, err)
	}

	// binary (non-JSON) body must survive store->json->gRPC verbatim
	binReq := &pushupesv1.AppendRequest{
		AggregateId: "agg-bin", Version: 1, CommandId: "cb-1",
		Events: []*pushupesv1.Event{{Type: "Raw", Body: []byte{0x00, 0x01, 0xff, 'A'}}},
	}
	if r, err := cli.Append(ctx, binReq); err != nil || r.Status != pushupesv1.AppendResponse_STATUS_SUCCESS {
		t.Fatalf("append bin: %+v %v", r, err)
	}
	brr, err := cli.ReadStream(ctx, &pushupesv1.ReadStreamRequest{AggregateId: "agg-bin"})
	if err != nil || len(brr.Records) != 1 {
		t.Fatalf("read bin: %+v %v", brr, err)
	}
	if got := brr.Records[0].Events[0].Body; len(got) != 4 || got[0] != 0 || got[2] != 0xff || got[3] != 'A' {
		t.Fatalf("binary body mangled through gRPC plane: %v", got)
	}

	// read range
	rr, err := cli.ReadStream(ctx, &pushupesv1.ReadStreamRequest{AggregateId: "agg-1", FromVersion: 1})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(rr.Records) != 2 || rr.LastVersion != 2 || rr.NextVersion != 3 {
		t.Fatalf("read: %+v", rr)
	}
	if rr.Records[0].Seq != 1 || rr.Records[1].Seq != 2 || rr.Records[1].CommandId != "c-3" {
		t.Fatalf("read records: %+v", rr.Records)
	}
	// body bytes survive verbatim (no base64 layering)
	if string(rr.Records[0].Events[0].Body) != `{"a":1}` {
		t.Fatalf("body mangled: %q", rr.Records[0].Events[0].Body)
	}

	// from_version beyond tail -> empty
	er, err := cli.ReadStream(ctx, &pushupesv1.ReadStreamRequest{AggregateId: "agg-1", FromVersion: 9})
	if err != nil || len(er.Records) != 0 || er.NextVersion != 9 || er.LastVersion != 8 {
		t.Fatalf("empty read: %+v %v", er, err)
	}

	// by-command probe
	bc, err := cli.ReadByCommand(ctx, &pushupesv1.ReadByCommandRequest{AggregateId: "agg-1", CommandId: "c-3"})
	if err != nil || !bc.Found || bc.Record.Version != 2 {
		t.Fatalf("by-command: %+v %v", bc, err)
	}
	nf, err := cli.ReadByCommand(ctx, &pushupesv1.ReadByCommandRequest{AggregateId: "agg-1", CommandId: "missing"})
	if err != nil || nf.Found || nf.Record != nil {
		t.Fatalf("by-command miss: %+v %v", nf, err)
	}
}

func TestGRPCValidation(t *testing.T) {
	cli, _ := newTestClient(t)
	ctx := context.Background()
	if r, err := cli.Append(ctx, &pushupesv1.AppendRequest{}); err != nil || r.Status != pushupesv1.AppendResponse_STATUS_FAIL || r.ErrId != 1002 {
		t.Fatalf("empty append: %+v %v", r, err)
	}
	if _, err := cli.ReadStream(ctx, &pushupesv1.ReadStreamRequest{}); err == nil {
		t.Fatal("missing aggregate_id must be InvalidArgument")
	}
}
