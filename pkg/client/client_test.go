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
	"strings"
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

// ---- flow-control backpressure ----

// throttleResp builds a per-record all-1006 batch answer: a node whose
// token bucket is empty refuses every record of the batch it received.
func throttleResp(req *pushupesv1.BatchAppendRequest) *pushupesv1.BatchAppendResponse {
	out := &pushupesv1.BatchAppendResponse{}
	for _, r := range req.Records {
		out.Results = append(out.Results, &pushupesv1.BatchAppendResult{
			AggregateId: r.AggregateId,
			Response: &pushupesv1.AppendResponse{
				Status: pushupesv1.AppendResponse_STATUS_FAIL, ErrId: ErrIDFlowControl,
			},
		})
	}
	return out
}

func TestClientFlowControlRetriesUntilClean(t *testing.T) {
	nodes, addrs := fakeCluster(t, 1)
	node := nodes[0]
	node.onAppend = func(n int, req *pushupesv1.BatchAppendRequest) *pushupesv1.BatchAppendResponse {
		if n < 2 {
			return throttleResp(req) // refused twice, clean on the third answer
		}
		return nil
	}
	c, err := New([]string{addrs[0]}, &Config{RouteRefresh: time.Hour, FlowBackoff: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resps, err := c.BatchAppend(ctx, []*pushupesv1.AppendRequest{rec("fc-a"), rec("fc-b")})
	if err != nil {
		t.Fatalf("lastErr = %v", err)
	}
	for i, r := range resps {
		if r == nil {
			t.Fatalf("record %d unanswered", i)
		}
		if r.GetErrId() == ErrIDFlowControl {
			t.Fatalf("record %d answered the raw 1006: the library must absorb it", i)
		}
		if r.GetStatus() != pushupesv1.AppendResponse_STATUS_SUCCESS {
			t.Fatalf("record %d status = %v", i, r.GetStatus())
		}
	}
	if got := len(node.calls()); got != 3 {
		t.Fatalf("node saw %d batches, want 3 (two refused, one clean)", got)
	}
	if c.FlowPauses() < 2 {
		t.Fatalf("FlowPauses = %d, want >= 2 (each refused round counts)", c.FlowPauses())
	}
}

// TestClientFlowControlGateIsPerNode proves the pause is scoped to the
// throttled node: while node 0 (slot 0 leader, "a-*" aggregates) is in
// backpressure, writes routed to node 1 (slot 1 leader, "b-*" aggregates)
// must flow through untouched — a gate is one node's ledger, and parking
// a healthy node's writers would turn one throttle into a client-wide
// outage.
func TestClientFlowControlGateIsPerNode(t *testing.T) {
	nodes, addrs := fakeCluster(t, 2)
	// pick two aggregates whose slots land on DIFFERENT nodes, then the
	// throttled node is A's leader, wherever the table puts it.
	if SlotOf("a-x", 8)%2 == SlotOf("b-x", 8)%2 {
		t.Fatalf("fixture assumption broken: a-x slot %d, b-x slot %d share a node", SlotOf("a-x", 8), SlotOf("b-x", 8))
	}
	a := nodes[SlotOf("a-x", 8)%2] // the throttled node (A's leader)
	b := nodes[SlotOf("b-x", 8)%2] // the healthy node (B's leader)
	a.onAppend = func(n int, req *pushupesv1.BatchAppendRequest) *pushupesv1.BatchAppendResponse {
		return throttleResp(req) // A's node never recovers during the window
	}
	c, err := New([]string{addrs[0]}, &Config{RouteRefresh: time.Hour, FlowBackoff: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A's call enters backpressure and parks its node's gate (the gate is
	// created closed by the first refused answer; every later refused
	// round keeps holding it).
	errA := make(chan error, 1)
	go func() {
		_, err := c.BatchAppend(ctx, []*pushupesv1.AppendRequest{rec("a-x")})
		errA <- err
	}()
	// Give A its first refused round (one RPC + one 50ms sleep): the gate
	// is definitely closed when B starts.
	time.Sleep(120 * time.Millisecond)

	t0 := time.Now()
	resps, err := c.BatchAppend(ctx, []*pushupesv1.AppendRequest{rec("b-x")})
	waited := time.Since(t0)
	if err != nil {
		t.Fatal(err)
	}
	if resps[0] == nil || resps[0].GetStatus() != pushupesv1.AppendResponse_STATUS_SUCCESS {
		t.Fatalf("B answered %v", resps[0])
	}
	// B rides one uncontended RPC: if the pause were client-wide (or the
	// gate keyed by something other than the node), B would have waited
	// behind A's never-ending backpressure until the ctx deadline.
	if waited > 2*time.Second {
		t.Fatalf("B waited %v while A's node was throttled: the pause leaked across nodes", waited)
	}
	if len(b.calls()) == 0 {
		t.Fatal("the healthy node never saw B's batch")
	}
	// And B's batch must not have been swallowed by the throttled node's
	// ledger: only A's aggregate went there.
	for _, req := range a.calls() {
		for _, r := range req.Records {
			if strings.HasPrefix(r.GetAggregateId(), "b-") {
				t.Fatalf("B's record %q hit the throttled node", r.GetAggregateId())
			}
		}
	}

	// Now clear the throttle: A's next re-send must land and its
	// call must finish — the per-node gate did not corrupt A's own
	// recovery path either.
	a.mu.Lock()
	a.onAppend = nil
	a.mu.Unlock()
	if err := <-errA; err != nil {
		t.Fatalf("A returned %v after its node recovered", err)
	}
}

func TestClientFlowControlGateParksConcurrentWriters(t *testing.T) {
	nodes, addrs := fakeCluster(t, 1)
	node := nodes[0]
	node.onAppend = func(n int, req *pushupesv1.BatchAppendRequest) *pushupesv1.BatchAppendResponse {
		if n < 3 {
			return throttleResp(req) // A's stretch: three refused rounds, then clean
		}
		return nil
	}
	c, err := New([]string{addrs[0]}, &Config{RouteRefresh: time.Hour, FlowBackoff: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	errA := make(chan error, 1)
	go func() {
		_, err := c.BatchAppend(ctx, []*pushupesv1.AppendRequest{rec("gate-a")})
		errA <- err
	}()
	// A is inside its backpressure window after ~5ms of RPC time: its
	// refused answer came back and the gate closed before B starts.
	time.Sleep(30 * time.Millisecond)
	t0 := time.Now()
	resps, err := c.BatchAppend(ctx, []*pushupesv1.AppendRequest{rec("gate-b")})
	waited := time.Since(t0)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-errA; err != nil {
		t.Fatal(err)
	}
	if resps[0] == nil || resps[0].GetErrId() == ErrIDFlowControl {
		t.Fatalf("B answered %v", resps[0])
	}
	// B parked for at least A's remaining refused rounds (3 sleeps of
	// 100ms, minus the 30ms head start) — a client that did NOT pause
	// would have been answered in ~one RPC.
	if waited < 250*time.Millisecond {
		t.Fatalf("B returned after %v: the gate did not park it", waited)
	}
	calls := node.calls()
	if len(calls) < 4 {
		t.Fatalf("node saw %d batches, want >= 4", len(calls))
	}
	for i, req := range calls[:len(calls)-1] {
		if strings.HasPrefix(req.Records[0].GetAggregateId(), "gate-b") {
			t.Fatalf("batch #%d (%s) landed before A's backpressure ended: concurrent writers bypassed the gate", i, req.Records[0].GetAggregateId())
		}
	}
	if calls[len(calls)-1].Records[0].GetAggregateId() != "gate-b" {
		t.Fatalf("last batch is not B's: %v", calls[len(calls)-1].Records[0].GetAggregateId())
	}
}

func TestClientFlowControlCancelledHolderReleasesGate(t *testing.T) {
	nodes, addrs := fakeCluster(t, 1)
	node := nodes[0]
	node.onAppend = func(n int, req *pushupesv1.BatchAppendRequest) *pushupesv1.BatchAppendResponse {
		return throttleResp(req) // the throttle never clears
	}
	c, err := New([]string{addrs[0]}, &Config{RouteRefresh: time.Hour, FlowBackoff: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// A holder whose ctx dies must release the gate: a cancelled holder
	// that left it closed would strand every later writer on the node.
	ctxA, cancelA := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancelA()
	if _, err := c.BatchAppend(ctxA, []*pushupesv1.AppendRequest{rec("dead-a")}); err == nil {
		t.Fatal("cancelled holder returned no error")
	}

	// The next call rides a fresh script (throttle the first batch, then
	// clean) — a stuck gate would hang it past the deadline instead of
	// answering. seen flips only inside onAppend, and the two rounds are
	// ordered by the RPC responses on this one goroutine.
	node.mu.Lock()
	seen := false
	node.onAppend = func(n int, req *pushupesv1.BatchAppendRequest) *pushupesv1.BatchAppendResponse {
		if !seen {
			seen = true
			return throttleResp(req)
		}
		return nil
	}
	node.mu.Unlock()
	cleanCtx, cancelClean := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelClean()
	resps, err := c.BatchAppend(cleanCtx, []*pushupesv1.AppendRequest{rec("dead-b")})
	if err != nil {
		t.Fatalf("post-cancel call failed: %v", err)
	}
	if resps[0] == nil || resps[0].GetErrId() == ErrIDFlowControl {
		t.Fatalf("post-cancel call answered %v: gate stuck closed", resps[0])
	}
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
