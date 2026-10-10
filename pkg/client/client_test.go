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

package client

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	pushupesv1 "github.com/berkaroad/pushupes/pkg/grpcapi/pushupes/v1"
)

// ---- fake cluster ----------------------------------------------------------
//
// The contract package's test server implements EventService over real TCP
// loopback listeners (the Client dials host:port targets by design), so
// routing, grouping, and the redirect self-heal run over genuine gRPC
// connections — no internal/ imports: a client-contract test stays inside
// the contract.

type fakeNode struct {
	pushupesv1.UnimplementedEventServiceServer

	mu        sync.Mutex
	idx       int
	addrs     []string      // every node's addr (shared view)
	leaders   map[int32]int // slot -> index into addrs
	appendReq []*pushupesv1.BatchAppendRequest
	// onAppend scripts the answer for the n-th recorded call; nil = all-success.
	onAppend func(n int, req *pushupesv1.BatchAppendRequest) *pushupesv1.BatchAppendResponse
}

func (f *fakeNode) PrefetchRoutes(_ context.Context, _ *pushupesv1.PrefetchRoutesRequest) (*pushupesv1.PrefetchRoutesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := &pushupesv1.PrefetchRoutesResponse{NodeClientAddrs: append([]string{}, f.addrs...)}
	for slot := int32(0); slot < 8; slot++ {
		out.SlotNodeIndex = append(out.SlotNodeIndex, int32(f.leaders[slot]))
	}
	return out, nil
}

func (f *fakeNode) BatchAppend(_ context.Context, req *pushupesv1.BatchAppendRequest) (*pushupesv1.BatchAppendResponse, error) {
	f.mu.Lock()
	n := len(f.appendReq)
	f.appendReq = append(f.appendReq, req)
	script := f.onAppend
	f.mu.Unlock()
	if script != nil {
		if resp := script(n, req); resp != nil {
			return resp, nil
		}
	}
	out := &pushupesv1.BatchAppendResponse{}
	for _, r := range req.Records {
		out.Results = append(out.Results, &pushupesv1.BatchAppendResult{
			AggregateId: r.AggregateId,
			Response:    &pushupesv1.AppendResponse{Status: pushupesv1.AppendResponse_STATUS_SUCCESS, Seq: 1},
		})
	}
	return out, nil
}

func (f *fakeNode) ReadTails(_ context.Context, req *pushupesv1.ReadTailsRequest) (*pushupesv1.ReadTailsResponse, error) {
	out := &pushupesv1.ReadTailsResponse{}
	for _, id := range req.AggregateIds {
		out.Versions = append(out.Versions, uint32(len(id))) // deterministic: version = id length
	}
	return out, nil
}

func (f *fakeNode) ReadByCommand(_ context.Context, req *pushupesv1.ReadByCommandRequest) (*pushupesv1.ReadByCommandResponse, error) {
	return &pushupesv1.ReadByCommandResponse{Found: true, Record: &pushupesv1.Record{
		AggregateId: req.AggregateId, CommandId: req.CommandId, Version: 7}}, nil
}

func (f *fakeNode) calls() []*pushupesv1.BatchAppendRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*pushupesv1.BatchAppendRequest{}, f.appendReq...)
}

// fakeCluster wires n nodes over real listeners: a fixed 8-slot table, slot
// s led by s%n. Ports are reserved first (every fake's addrs view must be
// complete at serve time), and each reserved listener is the one served.
func fakeCluster(t *testing.T, n int) ([]*fakeNode, []string) {
	t.Helper()
	lis := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := range lis {
		var err error
		lis[i], err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs[i] = lis[i].Addr().String()
	}
	nodes := make([]*fakeNode, n)
	for i := range nodes {
		f := &fakeNode{idx: i, addrs: append([]string{}, addrs...), leaders: map[int32]int{}}
		for slot := int32(0); slot < 8; slot++ {
			f.leaders[slot] = int(slot) % n
		}
		g := grpc.NewServer()
		pushupesv1.RegisterEventServiceServer(g, f)
		go g.Serve(lis[i])
		t.Cleanup(g.Stop)
		nodes[i] = f
	}
	return nodes, addrs
}

func rec(id string) *pushupesv1.AppendRequest {
	return &pushupesv1.AppendRequest{AggregateId: id, Version: 1, CommandId: "cmd-" + id}
}

func TestClientSeedsRoutingAndWritesLeadersDirectly(t *testing.T) {
	nodes, addrs := fakeCluster(t, 2)
	c, err := New([]string{"http://" + addrs[0]}, nil) // scheme stripped on the way in
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := c.SlotCount(); got != 8 {
		t.Fatalf("slot count: %d, want 8 (the answer's length is the count)", got)
	}
	// "a" -> slot SlotOf("a",8)=0 -> node 0; "agg-1" -> slot 7 -> node 1.
	recs := []*pushupesv1.AppendRequest{rec("a"), rec("agg-1")}
	resps, err := c.BatchAppend(context.Background(), recs)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range resps {
		if r == nil || r.Status != pushupesv1.AppendResponse_STATUS_SUCCESS {
			t.Fatalf("resp[%d] = %+v, want success", i, r)
		}
	}
	// one RPC per leader, each carrying exactly its routed record
	if got := nodes[0].calls(); len(got) != 1 || len(got[0].Records) != 1 || got[0].Records[0].AggregateId != "a" {
		t.Fatalf("node0 calls: %+v", got)
	}
	if got := nodes[1].calls(); len(got) != 1 || len(got[0].Records) != 1 || got[0].Records[0].AggregateId != "agg-1" {
		t.Fatalf("node1 calls: %+v", got)
	}
	if c.Redirects() != 0 {
		t.Fatalf("redirects: %d, want 0", c.Redirects())
	}
}

func TestClientFollowsMovedPatchesRow(t *testing.T) {
	nodes, addrs := fakeCluster(t, 2)
	// node 0 answers slot 0's write with MOVED -> node 1, exactly once.
	nodes[0].mu.Lock()
	nodes[0].onAppend = func(n int, req *pushupesv1.BatchAppendRequest) *pushupesv1.BatchAppendResponse {
		if n == 0 {
			out := &pushupesv1.BatchAppendResponse{}
			for _, r := range req.Records {
				out.Results = append(out.Results, &pushupesv1.BatchAppendResult{
					AggregateId: r.AggregateId,
					Response: &pushupesv1.AppendResponse{
						Status: pushupesv1.AppendResponse_STATUS_FAIL,
						ErrId:  uint32(ErrIDSlotNotLocal), Node: addrs[1], Slot: 0,
					}})
			}
			return out
		}
		return nil // later attempts answer success
	}
	nodes[0].mu.Unlock()

	c, err := New([]string{addrs[0]}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	resps, err := c.BatchAppend(context.Background(), []*pushupesv1.AppendRequest{rec("a")})
	if err != nil {
		t.Fatal(err)
	}
	if resps[0] == nil || resps[0].Status != pushupesv1.AppendResponse_STATUS_SUCCESS {
		t.Fatalf("after MOVED: %+v, want success from the redirect target", resps[0])
	}
	if c.Redirects() != 1 {
		t.Fatalf("redirects: %d, want 1", c.Redirects())
	}
	// the patched row held: a second write lands on node 1 without node 0
	// seeing it at all
	if _, err := c.BatchAppend(context.Background(), []*pushupesv1.AppendRequest{rec("a")}); err != nil {
		t.Fatal(err)
	}
	if len(nodes[0].calls()) != 1 {
		t.Fatalf("node0 got %d calls, want only the first (row patched)", len(nodes[0].calls()))
	}
	if len(nodes[1].calls()) != 2 {
		t.Fatalf("node1 got %d calls, want redirect resend + patched write", len(nodes[1].calls()))
	}
}

func TestClientRedirectExhaustedAnswersNil(t *testing.T) {
	// every attempt redirects to a DEAD addr (ASK target unreachable ->
	// transport failure -> rotation; single-entry rotation lands back on
	// the redirecting node, which answers ASK again): attempts run out and
	// the record answers nil.
	nodes, addrs := fakeCluster(t, 1)
	d, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := d.Addr().String()
	d.Close()
	nodes[0].mu.Lock()
	nodes[0].onAppend = func(_ int, req *pushupesv1.BatchAppendRequest) *pushupesv1.BatchAppendResponse {
		out := &pushupesv1.BatchAppendResponse{}
		for _, r := range req.Records {
			out.Results = append(out.Results, &pushupesv1.BatchAppendResult{
				AggregateId: r.AggregateId,
				Response: &pushupesv1.AppendResponse{
					Status: pushupesv1.AppendResponse_STATUS_FAIL,
					ErrId:  uint32(ErrIDMigrating), Node: dead, Slot: 0,
				}})
		}
		return out
	}
	nodes[0].mu.Unlock()

	c, err := New([]string{addrs[0]}, &Config{Attempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	resps, _ := c.BatchAppend(context.Background(), []*pushupesv1.AppendRequest{rec("a")})
	if resps[0] != nil {
		t.Fatalf("exhausted retries must answer nil, got %+v", resps[0])
	}
}

// The client API surface is the routed methods only — no raw-stub escape
// hatch: protocol-level checks against a specific node belong to grpccheck,
// which is allowed to use the generated stubs directly. What the library
// owes its callers instead is the empty-batch contract: no RPC, aligned
// empty answer.
func TestClientEmptyBatchLocalShortCircuit(t *testing.T) {
	_, addrs := fakeCluster(t, 1)
	c, err := New([]string{addrs[0]}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	resps, err := c.BatchAppend(context.Background(), nil)
	if err != nil || len(resps) != 0 {
		t.Fatalf("empty batch: %v %+v", err, resps)
	}
	c.connsMu.Lock()
	got := len(c.conns)
	c.connsMu.Unlock()
	if got != 1 {
		t.Fatalf("conns after empty batch: %d, want 1 (New's seeding fetch only — no extra dial)", got)
	}
}

func TestClientReadPaths(t *testing.T) {
	nodes, addrs := fakeCluster(t, 2)
	c, err := New([]string{addrs[0]}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	tails, err := c.ReadTails(context.Background(), []string{"a", "agg-1", "x"})
	if err != nil {
		t.Fatal(err)
	}
	if len(tails.Versions) != 3 {
		t.Fatalf("tails order mismatch: %+v", tails.Versions)
	}
	// "a" routes to node 0 (slot 0), so a leader-targeted call exists;
	// ReadTails may land on either node (entry round-robin) — the fake
	// answers with id lengths, so verify the answer content instead.
	if tails.Versions[0] != 1 || tails.Versions[1] != 5 || tails.Versions[2] != 1 {
		t.Fatalf("tails versions: %+v", tails.Versions)
	}
	_ = nodes

	bc, err := c.ReadByCommand(context.Background(), "agg-1", "cmd-x")
	if err != nil {
		t.Fatal(err)
	}
	if !bc.Found || bc.Record.CommandId != "cmd-x" {
		t.Fatalf("ReadByCommand: %+v", bc)
	}
}

// TestClientConnsAreShared pins the reuse contract: BatchAppend, a read,
// and the route refresh all dial the same node over ONE cached conn.
func TestClientConnsAreShared(t *testing.T) {
	_, addrs := fakeCluster(t, 1)
	c, err := New([]string{addrs[0]}, &Config{RouteRefresh: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	before := len(c.conns)
	if _, err := c.BatchAppend(context.Background(), []*pushupesv1.AppendRequest{rec("a")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReadTails(context.Background(), []string{"a"}); err != nil {
		t.Fatal(err)
	}
	if err := c.routes.fetch(); err != nil { // the refresh path reuses it too
		t.Fatal(err)
	}
	c.connsMu.Lock()
	got := len(c.conns)
	c.connsMu.Unlock()
	if got != before || before != 1 {
		t.Fatalf("conns: %d after three call paths, want 1 (one long-lived conn per addr)", got)
	}
	// and the map keys are scheme-free dial targets
	if _, ok := c.conns[addrs[0]]; !ok {
		t.Fatalf("conn keyed %v, want %s", keysOf(c.conns), addrs[0])
	}
}

func keysOf(m map[string]*grpc.ClientConn) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}
