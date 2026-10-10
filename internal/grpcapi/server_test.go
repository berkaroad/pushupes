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

package grpcapi

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/berkaroad/pushupes/internal/cluster"
	"github.com/berkaroad/pushupes/internal/data"
	"github.com/berkaroad/pushupes/internal/lease"
	"github.com/berkaroad/pushupes/internal/storage"
	pushupesv1 "github.com/berkaroad/pushupes/pkg/grpcapi/pushupes/v1"
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
	eng := cluster.NewEngine(nil, st, "node-1", nil)

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	NewServer(eng, st, nil, 0, "bufnet-self").Register(gs)
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

// appendOne writes a single record through BatchAppend — the client plane's
// only write entry (a single write is a batch of one record) — and hands back
// that record's response.
func appendOne(ctx context.Context, cli pushupesv1.EventServiceClient, req *pushupesv1.AppendRequest) (*pushupesv1.AppendResponse, error) {
	resp, err := cli.BatchAppend(ctx, &pushupesv1.BatchAppendRequest{
		Records: []*pushupesv1.AppendRequest{req},
	})
	if err != nil {
		return nil, err
	}
	return resp.Results[0].Response, nil
}

func TestGRPCAppendReadCycle(t *testing.T) {
	// Run the cycle through the production codec, which aliases the request's
	// event bodies into the receive buffer and holds a lease on it for the
	// duration of the handler.
	leasesBefore := lease.Outstanding()

	cli, _ := newTestClient(t)
	ctx := context.Background()

	resp, err := appendOne(ctx, cli, appendReq("agg-1", 1, "c-1", `{"a":1}`))
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
	resp2, err := appendOne(ctx, cli, appendReq("agg-1", 1, "c-1", `{"a":1}`))
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
	resp3, err := appendOne(ctx, cli, appendReq("agg-1", 5, "c-2", `{}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp3.Status != pushupesv1.AppendResponse_STATUS_FAIL || resp3.ErrId != 1001 || resp3.CurrentVersion != 1 {
		t.Fatalf("append conflict: %+v", resp3)
	}

	// a second version lands
	if r, err := appendOne(ctx, cli, appendReq("agg-1", 2, "c-3", `{"b":2}`)); err != nil || r.Status != pushupesv1.AppendResponse_STATUS_SUCCESS {
		t.Fatalf("append2: %+v %v", r, err)
	}

	// binary (non-JSON) body must survive store->json->gRPC verbatim
	binReq := &pushupesv1.AppendRequest{
		AggregateId: "agg-bin", Version: 1, CommandId: "cb-1",
		Events: []*pushupesv1.Event{{Type: "Raw", Body: []byte{0x00, 0x01, 0xff, 'A'}}},
	}
	if r, err := appendOne(ctx, cli, binReq); err != nil || r.Status != pushupesv1.AppendResponse_STATUS_SUCCESS {
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

	if got := lease.Outstanding(); got != leasesBefore {
		t.Fatalf("%d receive-buffer leases left outstanding after the append cycle", got-leasesBefore)
	}
}

func TestGRPCValidation(t *testing.T) {
	cli, _ := newTestClient(t)
	ctx := context.Background()
	if r, err := appendOne(ctx, cli, &pushupesv1.AppendRequest{}); err != nil || r.Status != pushupesv1.AppendResponse_STATUS_FAIL || r.ErrId != 1002 {
		t.Fatalf("empty append: %+v %v", r, err)
	}
	if _, err := cli.ReadStream(ctx, &pushupesv1.ReadStreamRequest{}); err == nil {
		t.Fatal("missing aggregate_id must be InvalidArgument")
	}
}

// ---- BatchAppend -------------------------------------------------------------

// batchPair returns two aggregate ids that route to the SAME slot, plus one
// id that routes to a DIFFERENT slot, probed against the store's own routing.
func batchPair(t *testing.T, st *storage.Store) (sameA, sameB, otherA string) {
	t.Helper()
	seen := map[int32]string{}
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("bagg-%d", i)
		slot := st.SlotOf(id)
		if prev, ok := seen[slot]; ok {
			return prev, id, "bagg-other"
		}
		seen[slot] = id
	}
	t.Fatal("no two ids share a slot in 1000 probes (routing broken?)")
	return
}

func TestBatchAppendResults(t *testing.T) {
	cli, st := newTestClient(t)
	ctx := context.Background()
	sameA, sameB, otherA := batchPair(t, st)

	// Seed one record so the batch can produce an EXISTS against real state.
	if r, err := appendOne(ctx, cli, appendReq(sameA, 1, "seed-1", "s")); err != nil ||
		r.Status != pushupesv1.AppendResponse_STATUS_SUCCESS {
		t.Fatalf("seed: %+v %v", r, err)
	}

	// (a) A batch with a DUPLICATED aggregate_id: exactly the duplicate
	// records fail 1002 and neither executes; the distinct aggregates land.
	resp, err := cli.BatchAppend(ctx, &pushupesv1.BatchAppendRequest{Records: []*pushupesv1.AppendRequest{
		appendReq(otherA, 1, "b-1", "o"), // success
		appendReq(sameB, 1, "b-2", "x"),  // success (same slot as sameA)
		appendReq(sameA, 2, "b-3", "y"),  // FAIL 1002: sameA duplicated in batch
		appendReq(sameA, 3, "b-4", "z"),  // FAIL 1002: sameA duplicated in batch
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Results) != 4 {
		t.Fatalf("results len: %d", len(resp.Results))
	}
	want := []struct {
		agg    string
		status pushupesv1.AppendResponse_Status
		errID  uint32
	}{
		{otherA, pushupesv1.AppendResponse_STATUS_SUCCESS, 0},
		{sameB, pushupesv1.AppendResponse_STATUS_SUCCESS, 0},
		{sameA, pushupesv1.AppendResponse_STATUS_FAIL, data.ErrIDBadRequest},
		{sameA, pushupesv1.AppendResponse_STATUS_FAIL, data.ErrIDBadRequest},
	}
	for i, w := range want {
		g := resp.Results[i]
		if g.AggregateId != w.agg || g.Response.Status != w.status || g.Response.ErrId != w.errID {
			t.Fatalf("result %d: want %+v got agg=%q resp=%+v", i, w, g.AggregateId, g.Response)
		}
	}
	// The rejected sameA v2 must NOT have been written: its tail is still 1.
	if tv, _ := st.TailVersionOf(sameA, 0); tv != 1 {
		t.Fatalf("duplicate-agg record must not execute: tail %d want 1", tv)
	}

	// (b) EXISTS echo + version-conflict self-heal fields, distinct aggregates.
	resp2, err := cli.BatchAppend(ctx, &pushupesv1.BatchAppendRequest{Records: []*pushupesv1.AppendRequest{
		appendReq(sameA, 1, "seed-1", "s"), // EXISTS: idempotent replay, echoes record
		appendReq(otherA, 9, "b-5", "y"),   // 1001 conflict, current_version=1
	}})
	if err != nil {
		t.Fatal(err)
	}
	e := resp2.Results[0]
	if e.AggregateId != sameA || e.Response.Status != pushupesv1.AppendResponse_STATUS_EXISTS ||
		e.Response.Record == nil || e.Response.Record.CommandId != "seed-1" {
		t.Fatalf("exists result: %+v", e)
	}
	c := resp2.Results[1]
	if c.AggregateId != otherA || c.Response.Status != pushupesv1.AppendResponse_STATUS_FAIL ||
		c.Response.ErrId != data.ErrIDVersionConflict || c.Response.CurrentVersion != 1 {
		t.Fatalf("conflict result: %+v", c)
	}
}

// batchSameSlot returns n distinct aggregate ids that all route to the SAME
// slot of st (probed, not guessed), to exercise the serial path inside one
// slot's worker.
func batchSameSlot(t *testing.T, st *storage.Store, n int) []string {
	t.Helper()
	bySlot := map[int32][]string{}
	for i := 0; len(bySlot[int32(i%8)]) < n; i++ {
		id := fmt.Sprintf("ss-agg-%d", i)
		slot := st.SlotOf(id)
		bySlot[slot] = append(bySlot[slot], id)
		if len(bySlot[slot]) >= n {
			return bySlot[slot]
		}
	}
	t.Fatal("unreachable")
	return nil
}

func TestBatchAppendEmptyAndSerial(t *testing.T) {
	cli, st := newTestClient(t)
	ctx := context.Background()

	// Empty batch: empty results, no error.
	resp, err := cli.BatchAppend(ctx, &pushupesv1.BatchAppendRequest{})
	if err != nil || len(resp.Results) != 0 {
		t.Fatalf("empty batch: %+v %v", resp, err)
	}

	// Serial execution INSIDE one slot: n distinct aggregates that all hash
	// to the same slot, written in request order. Every result succeeds and
	// seqs strictly increase in request order — the per-slot worker keeps
	// WAL append order = request order even across aggregates.
	aggs := batchSameSlot(t, st, 20)
	recs := make([]*pushupesv1.AppendRequest, 0, len(aggs))
	for i, agg := range aggs {
		recs = append(recs, appendReq(agg, 1, fmt.Sprintf("ser-%d", i), "b"))
	}
	resp, err = cli.BatchAppend(ctx, &pushupesv1.BatchAppendRequest{Records: recs})
	if err != nil {
		t.Fatal(err)
	}
	var lastSeq uint64
	for i, r := range resp.Results {
		if r.AggregateId != aggs[i] {
			t.Fatalf("result %d misaligned: agg %q want %q", i, r.AggregateId, aggs[i])
		}
		if r.Response.Status != pushupesv1.AppendResponse_STATUS_SUCCESS {
			t.Fatalf("serial result %d: %+v", i, r.Response)
		}
		if r.Response.Seq <= lastSeq {
			t.Fatalf("seq not increasing at %d: %d after %d", i, r.Response.Seq, lastSeq)
		}
		lastSeq = r.Response.Seq
	}
	// Spot-check one stream landed exactly as sent.
	rr, err := cli.ReadStream(ctx, &pushupesv1.ReadStreamRequest{AggregateId: aggs[0], FromVersion: 1})
	if err != nil || len(rr.Records) != 1 || rr.Records[0].CommandId != "ser-0" {
		t.Fatalf("stream after serial batch: %+v %v", rr, err)
	}
}

func TestBatchAppendLeaseRelease(t *testing.T) {
	// receive buffer; the handler releases the whole-request lease once.
	leasesBefore := lease.Outstanding()
	cli, _ := newTestClient(t)
	ctx := context.Background()

	big := make([]byte, 64<<10) // past the alias threshold: bodies alias the buffer
	for i := range big {
		big[i] = byte(i)
	}
	recs := make([]*pushupesv1.AppendRequest, 0, 8)
	for i := 0; i < 8; i++ {
		recs = append(recs, &pushupesv1.AppendRequest{
			AggregateId: fmt.Sprintf("lease-agg-%d", i), Version: 1,
			CommandId: fmt.Sprintf("lease-cmd-%d", i),
			Events:    []*pushupesv1.Event{{Type: "T", Body: big}},
		})
	}
	resp, err := cli.BatchAppend(ctx, &pushupesv1.BatchAppendRequest{Records: recs})
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range resp.Results {
		if r.Response.Status != pushupesv1.AppendResponse_STATUS_SUCCESS {
			t.Fatalf("result %d: %+v", i, r.Response)
		}
	}
	if got := lease.Outstanding(); got != leasesBefore {
		t.Fatalf("%d leases left outstanding after batch append", got-leasesBefore)
	}
}
