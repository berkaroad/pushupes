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
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	pushupesv1 "github.com/berkaroad/pushupes/pkg/grpcapi/pushupes/v1"
)

// Config tunes a Client. Zero values select the defaults.
type Config struct {
	RouteRefresh time.Duration // slot->leader table re-fetch interval; 0 = fetch once at startup (default 30s)
	Attempts     int           // max send rounds per BatchAppend call, retries of redirected records included (default 3)
	CallTimeout  time.Duration // per-attempt RPC timeout inside BatchAppend (default 30s)
	FlowBackoff  time.Duration // pause after a node answers 1005 (flow control); the refused records re-send and the node's gate stays closed until one answer comes back un-throttled (default 100ms)
	// Writer-side gzip (see compress.go). CompressMinTotalBody is the floor:
	// a record whose combined event bodies are smaller stores raw. 0 (the
	// default) disables compression entirely — the library keeps the
	// store-what-you-send behaviour until a caller opts in. CompressLevel is
	// the gzip level; 0 = gzip.BestSpeed (level 1: the writer-side codec
	// trades ratio for the per-append CPU that the high-pressure write path
	// does not have).
	CompressMinTotalBody int
	CompressLevel        int
	// AutoDecompress inflates gzip-stamped records before they reach the
	// caller (read responses and exists records). nil = on; only meaningful
	// together with records written under the codec.
	AutoDecompress *bool
}

func (a Config) decompressOn() bool { return a.AutoDecompress == nil || *a.AutoDecompress }

// Client talks to a pushupes cluster's data plane: routing resolved and
// maintained by PrefetchRoutes (seeded at New, refreshed on a timer,
// patched per MOVED/ASK in between), one long-lived gRPC connection per
// node address shared by every call, per-record redirect self-heal on
// the write and read paths, and flow-control backpressure: a node that answers 1005
// gets its refused records re-sent every FlowBackoff (default 100ms) while
// a per-node gate stays closed, pausing every later write to that node
// until one re-send comes back un-throttled. Business rules (idempotency,
// version checks) pass through untouched: a Client answers what the
// cluster answered — except the 1005 itself, which the library absorbs as
// waiting rather than relaying.
type Client struct {
	cfg     Config
	connsMu sync.Mutex
	conns   map[string]*grpc.ClientConn
	gatesMu sync.Mutex
	gates   map[string]*gateT // per-node flow-control gate; created open on the first 1005, kept until the Client is dropped
	routes  *routeTable
	rr      atomic.Uint64 // round-robin cursor for entry picks
	// Dead-address penalty box: an entry address whose transport just
	// failed is skipped by the guess-rotation for entryPenalty, so a node
	// that left the cluster (a scale-down removes it from the table, not
	// from the caller's static entry list) stops burning Attempts on
	// dial timeouts. Entries are immutable after New, so a plain map under
	// its own mutex is enough.
	penaltyMu   sync.Mutex
	penalty     map[string]time.Time
	redirects   atomic.Uint64
	flowPauses  atomic.Uint64
	refreshStop chan struct{}
	refreshOnce sync.Once
}

// New dials the given client-plane (gRPC) addresses and seeds the routing
// table with one PrefetchRoutes call — the entry the table was fetched
// from doubles as the round-robin fallback set. Addresses may carry a URL
// scheme ("http://"), stripped before dialing like every other gRPC target
// in the fleet.
func New(nodes []string, cfg *Config) (*Client, error) {
	var entries []string
	for _, n := range nodes {
		if n = strings.TrimSpace(n); n != "" {
			entries = append(entries, hostPort(n))
		}
	}
	sort.Strings(entries)
	if len(entries) == 0 {
		return nil, fmt.Errorf("client: no node addresses")
	}
	c := &Client{conns: map[string]*grpc.ClientConn{}, refreshStop: make(chan struct{})}
	if cfg != nil {
		c.cfg = *cfg
	}
	if c.cfg.RouteRefresh == 0 {
		c.cfg.RouteRefresh = 30 * time.Second
	}
	if c.cfg.Attempts <= 0 {
		c.cfg.Attempts = 3
	}
	if c.cfg.CallTimeout <= 0 {
		// Deliberately longer than the cluster's write-ack watermark deadline
		// (10s by default): when a leader cannot confirm a record it answers
		// fail/1007 (ack timeout), and that answer must reach the caller. If the two
		// timeouts coincide the RPC dies first, the library requeues the
		// records it never saw an answer for, and the retry hits the
		// idempotency fence and answers exists — a stalled append reported as
		// a duplicate write.
		c.cfg.CallTimeout = 30 * time.Second
	}
	if c.cfg.FlowBackoff <= 0 {
		c.cfg.FlowBackoff = 100 * time.Millisecond
	}
	c.routes = &routeTable{entries: entries, client: c, wake: make(chan struct{}, 1)}
	c.penalty = map[string]time.Time{}
	if err := c.routes.fetch(); err != nil {
		return nil, err
	}
	go c.routes.refreshEvery(c.cfg.RouteRefresh, c.refreshStop)
	return c, nil
}

// SlotCount reports the cluster's slot count as learned from the routing
// table's answer length — the count is cluster state, never a client-side
// constant.
func (c *Client) SlotCount() int { return c.routes.slotCountNow() }

// Redirects counts the MOVED/ASK answers the write path has patched into
// the local table since New — a nonzero count on a healthy cluster means
// the refresh period is too long for the churn, not an error.
func (c *Client) Redirects() uint64 { return c.redirects.Load() }

// FlowPauses counts the 1005-answered re-send rounds the write path has
// run since New: one per refused answer (the first batch and every
// FlowBackoff retry). Growth means some node's token bucket is actively
// refusing this client's writes — the library is absorbing it as waiting,
// not surfacing it as an error.
func (c *Client) FlowPauses() uint64 { return c.flowPauses.Load() }

// Close stops the route refresh loop and releases every cached connection.
func (c *Client) Close() error {
	c.refreshOnce.Do(func() { close(c.refreshStop) })
	c.connsMu.Lock()
	defer c.connsMu.Unlock()
	var first error
	for addr, cc := range c.conns {
		if err := cc.Close(); err != nil && first == nil {
			first = fmt.Errorf("close %s: %w", addr, err)
		}
	}
	c.conns = map[string]*grpc.ClientConn{}
	return first
}

// event returns the shared client stub for an address, dialing at most one
// long-lived connection per node (gRPC keeps it warm with HTTP/2
// multiplexing; Close tears them all down).
func (c *Client) event(addr string) pushupesv1.EventServiceClient {
	c.connsMu.Lock()
	defer c.connsMu.Unlock()
	cc, ok := c.conns[addr]
	if !ok {
		var err error
		cc, err = grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			// Liveness pings on the client surface: a node whose network
			// DIED HARD (no TCP RST — a pulled cable, a hung host) would
			// otherwise park every RPC on its connections until the call
			// timeout, because a silent peer is indistinguishable from an
			// idle one. An unanswered ping closes the transport within
			// Time+Timeout (7s), so the failure lands in the ordinary
			// retry path (penalty box + rotation + command_id idempotency)
			// instead of a 30s stall. PermitWithoutStream keeps pings
			// alive between calls too — an idle connection that blacked
			// out must fail BEFORE the next request rides on it.
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                clientKeepaliveTime,
				Timeout:             clientKeepaliveTimeout,
				PermitWithoutStream: true,
			}))
		if err != nil {
			panic(err) // malformed target: a programming error, not runtime state
		}
		c.conns[addr] = cc
	}
	return pushupesv1.NewEventServiceClient(cc)
}

// ---- flow-control gate --------------------------------------------------

// gateT is one node's gate. While the node refuses appends (1005 — the
// operator throttle at its node-wide token bucket), the goroutine that
// entered backpressure HOLDS the gate: it sleeps FlowBackoff, re-sends the
// refused records, and keeps holding until an answer comes back without a
// single 1005, then releases. Every other write destined for that node
// parks meanwhile — the pause is what keeps a throttled node from being
// hammered by a whole fleet of concurrent callers. The open flag is one
// atomic so the fast path (gate open, the overwhelming majority of calls)
// costs a single load and no lock; mu guards the flag flip and the wake
// channel, which release CLOSES and remakes — a close broadcasts, so
// every parked waiter re-checks the flag at once (a 1-slot signal would
// wake only one and strand the rest behind a stale channel).
type gateT struct {
	open atomic.Bool
	wake chan struct{} // closed on release, then remade; readers take the ref under mu
	mu   sync.Mutex
}

// gateFor returns the addr's gate, creating it OPEN on first sight (an
// addr with no gate has never refused this client a write). Entries are
// only created, never removed, so a pointer returned here stays valid for
// the Client's lifetime.
func (c *Client) gateFor(addr string) *gateT {
	c.gatesMu.Lock()
	defer c.gatesMu.Unlock()
	if c.gates == nil {
		c.gates = map[string]*gateT{}
	}
	g := c.gates[addr]
	if g == nil {
		g = &gateT{wake: make(chan struct{})}
		g.open.Store(true)
		c.gates[addr] = g
	}
	return g
}

// peekGate looks a gate up WITHOUT creating one: the send loop asks before
// every batch, and a node that has never throttled this client stays a
// single map miss away from the fast path.
func (c *Client) peekGate(addr string) *gateT {
	c.gatesMu.Lock()
	defer c.gatesMu.Unlock()
	return c.gates[addr]
}

func (g *gateT) isOpen() bool { return g.open.Load() }

// waitOpen parks until the gate reopens or ctx dies. The channel ref is
// read under mu: a release that lands between the open-check and the
// select must still wake this waiter (closing the channel it holds —
// read before the swap, so it IS the old one — delivers even a parked
// receive instantly).
func (g *gateT) waitOpen(ctx context.Context) error {
	for {
		g.mu.Lock()
		if g.open.Load() {
			g.mu.Unlock()
			return nil
		}
		ch := g.wake
		g.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// hold parks the caller until no other goroutine holds the gate, then
// CLOSES it and returns true — the caller becomes the holder and owns that
// node's backpressure loop; false means ctx died while parking. The flip
// is under mu, so concurrent contenders for the same throttle serialize
// into exactly one holder and never herd retries onto a still-throttled
// node.
func (g *gateT) hold(ctx context.Context) bool {
	for {
		g.mu.Lock()
		if g.open.Load() {
			g.open.Store(false)
			g.mu.Unlock()
			return true
		}
		ch := g.wake
		g.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return false
		}
	}
}

// release reopens the gate after a clean (1005-free) answer and wakes ALL
// parked waiters by closing (then remaking) the channel. Holder-only.
// Woken writers that are BatchAppend callers re-check via the send-loop
// fast path and ride the next batch open; the next throttle is entered by
// whichever answer carries a 1005 from then on.
func (g *gateT) release() {
	g.mu.Lock()
	g.open.Store(true)
	close(g.wake)
	g.wake = make(chan struct{})
	g.mu.Unlock()
}

// sleepCtx sleeps d unless ctx dies first (false = ctx died: the caller
// gives up its records and releases the gate — a cancelled holder must not
// leave the node parked forever).
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// BatchAppend writes a batch of records through the data plane. Records
// are grouped by the cached slot leader, one BatchAppend per destination,
// AND ALL DESTINATIONS ARE DRIVEN CONCURRENTLY — a node under flow
// control parks only its own records (and only writes destined for it),
// never the rest of the batch. MOVED/ASK answers patch that slot's row and
// re-send just those records (up to Config.Attempts rounds; a transport
// failure rotates the row onto the next entry node instead). The returned
// slice is in the
// order of recs; a nil entry means the record was still redirected or
// unanswered when the attempts ran out — the error (last transport
// failure, informational) says which. Business outcomes (success/exists/
// fail with their error ids) are the cluster's, untouched — EXCEPT flow
// control: a 1005 never reaches the caller. Refused records are re-sent
// every Config.FlowBackoff while the node's gate is held closed, parking
// every later write to that node, until one answer comes back
// un-throttled (see gateT).
func (c *Client) BatchAppend(ctx context.Context,
	recs []*pushupesv1.AppendRequest) ([]*pushupesv1.AppendResponse, error) {

	// The writer-side codec runs before anything goes on the wire: every
	// retry round re-sends these (possibly compressed) records verbatim,
	// and the server persists whichever bytes it receives.
	recs = c.applyCompression(recs)

	out := make([]*pushupesv1.AppendResponse, len(recs))
	if len(recs) == 0 {
		return out, nil
	}
	var lastErr error
	// pending pairs original positions with their records; it shrinks to
	// the redirected/unanswered subset after each attempt.
	pending := make([]int, len(recs))
	for i := range pending {
		pending[i] = i
	}
	for attempt := 0; attempt < c.cfg.Attempts && len(pending) > 0; attempt++ {
		byAddr := map[string][]int{}
		var order []string
		for _, i := range pending {
			addr := c.routeFor(c.slotOf(recs[i].AggregateId), int(c.rr.Load()))
			if _, ok := byAddr[addr]; !ok {
				order = append(order, addr)
			}
			byAddr[addr] = append(byAddr[addr], i)
		}
		// Destinations are driven CONCURRENTLY: one goroutine per node, so
		// a throttle on one never parks the others inside the same call —
		// serial sends would make every record of the batch wait behind
		// the throttled node's backpressure loop. Records belong to exactly
		// one group per round, so writes to out are disjoint; next and
		// lastErr are joined under mu. The round ends when all destinations
		// answer (or the ctx dies), which is what a flow-control pause
		// costs a multi-node batch: one node's window, not the sum.
		var wg sync.WaitGroup
		var mu sync.Mutex
		var next []int
		for _, addr := range order {
			wg.Add(1)
			go func(addr string, idx []int) {
				defer wg.Done()
				requeue, err := c.sendGroup(ctx, attempt, addr, idx, recs, out)
				mu.Lock()
				next = append(next, requeue...)
				if err != nil {
					lastErr = err
				}
				mu.Unlock()
			}(addr, byAddr[addr])
		}
		wg.Wait()
		pending = next
		c.rr.Add(1)
	}
	if c.cfg.decompressOn() {
		if err := inflateBatchResponses(out); err != nil {
			return out, err
		}
	}
	return out, lastErr
}

// sendGroup drives one destination's subset of one attempt round: it parks
// behind that node's flow-control gate while another caller holds it,
// sends the batch, consumes the answer (redirect rows patched, business
// answers written to out), and runs the backpressure loop for refused
// records. It returns the indices the round could not answer (redirected,
// throttled-until-cancelled, or unanswered) for re-queueing, and the last
// error seen (informational, like BatchAppend's).
func (c *Client) sendGroup(ctx context.Context, attempt int, addr string,
	idx []int, recs []*pushupesv1.AppendRequest, out []*pushupesv1.AppendResponse,
) ([]int, error) {

	var next []int
	var lastErr error
	batch := make([]*pushupesv1.AppendRequest, len(idx))
	for k, i := range idx {
		batch[k] = recs[i]
	}
	// Flow-control backpressure: park while another caller holds this
	// node's gate (the node is refusing writes). Waiting happens OUTSIDE
	// the attempt clock — time behind a gate must not burn the caller's
	// Attempts — and only costs a map miss plus one atomic load while no
	// throttle is active.
	if gate := c.peekGate(addr); gate != nil && !gate.isOpen() {
		if werr := gate.waitOpen(ctx); werr != nil {
			return append(next, idx...), werr
		}
	}
	cctx, cancel := context.WithTimeout(ctx, c.cfg.CallTimeout)
	resp, err := c.event(addr).BatchAppend(cctx, &pushupesv1.BatchAppendRequest{Records: batch})
	cancel()
	if err != nil {
		lastErr = err
	}
	if err != nil || len(resp.GetResults()) != len(batch) {
		// transport failure or misaligned answer: rotate the route and
		// requeue these records for the next attempt. A rotation is a
		// guess, not a redirect the cluster told us — the Redirects()
		// counter stays out of it. The destination itself just failed at
		// the transport, so it goes in the penalty box: a node removed by
		// a scale-down never leaves the caller's static entry list, and
		// without the box every guess rotation re-fires a dial timeout
		// into it until the attempt budget burns out (no-response).
		if err != nil {
			c.markUnhealthy(addr)
		}
		for _, i := range idx {
			c.routes.patch(c.slotOf(recs[i].AggregateId),
				c.healthyEntry(int(c.rr.Load())+attempt+1))
		}
		return append(next, idx...), lastErr
	}
	// the batch protocol echoes aggregate_id per result; an echo mismatch
	// means the answer does not line up with the request — check before
	// consuming, and requeue like a misaligned answer.
	aligned := true
	for k, r := range resp.Results {
		if r.GetAggregateId() != recs[idx[k]].GetAggregateId() {
			aligned = false
			lastErr = fmt.Errorf("batch result echo mismatch at %d: %q, requeued", k, r.GetAggregateId())
			break
		}
	}
	if !aligned {
		for _, i := range idx {
			c.routes.patch(c.slotOf(recs[i].AggregateId), c.healthyEntry(int(c.rr.Load())+attempt+1))
		}
		return append(next, idx...), lastErr
	}
	var throttled []int
	for k, r := range resp.Results {
		i := idx[k]
		switch r.Response.GetErrId() {
		case ErrIDSlotNotLocal, ErrIDMigrating:
			if r.Response.GetNode() != "" {
				c.routes.patch(c.slotOf(recs[i].AggregateId), hostPort(r.Response.GetNode()))
			}
			c.redirects.Add(1)
			next = append(next, i)
		case ErrIDFlowControl:
			throttled = append(throttled, i)
		default:
			out[i] = r.Response
		}
	}
	// The node's token bucket refused part of the batch: become the gate
	// holder (concurrent contenders for the same throttle park in hold),
	// sleep FlowBackoff, and re-send ONLY the refused records until an
	// answer carries no 1005, then release the gate. Re-sending is safe:
	// append idempotency keys on command_id, so a record the node wrote
	// before throttling answers exists on the next round, never twice. The
	// loop runs past Config.Attempts on purpose — Attempts counts REDIRECT
	// rounds; throttling is waiting, not retrying, and ends when the node
	// recovers or ctx dies (a dead holder releases the gate and requeues
	// its records like a transport failure). Other destinations, driven in
	// their own goroutines, are unaffected by this node's window.
	if len(throttled) > 0 {
		gate := c.gateFor(addr)
		if !gate.hold(ctx) {
			return append(next, throttled...), ctx.Err()
		}
		c.flowPauses.Add(1)
		th := throttled
		for len(th) > 0 {
			if !sleepCtx(ctx, c.cfg.FlowBackoff) {
				lastErr = ctx.Err()
				next = append(next, th...)
				break
			}
			thBatch := make([]*pushupesv1.AppendRequest, len(th))
			for k, i := range th {
				thBatch[k] = recs[i]
			}
			cctx, cancel := context.WithTimeout(ctx, c.cfg.CallTimeout)
			thResp, thErr := c.event(addr).BatchAppend(cctx, &pushupesv1.BatchAppendRequest{Records: thBatch})
			cancel()
			if thErr != nil {
				lastErr = thErr
			}
			if thErr != nil || len(thResp.GetResults()) != len(thBatch) {
				next = append(next, th...)
				break
			}
			var again []int
			for k, r := range thResp.Results {
				i := th[k]
				switch r.Response.GetErrId() {
				case ErrIDSlotNotLocal, ErrIDMigrating:
					if r.Response.GetNode() != "" {
						c.routes.patch(c.slotOf(recs[i].AggregateId), hostPort(r.Response.GetNode()))
					}
					c.redirects.Add(1)
					next = append(next, i)
				case ErrIDFlowControl:
					again = append(again, i)
				default:
					out[i] = r.Response
				}
			}
			if len(again) == 0 {
				break
			}
			th = again
			c.flowPauses.Add(1)
		}
		gate.release()
	}
	return next, lastErr
}

// ---- single-key reads: redirect-following ------------------------------
//
// Reads are served by the node that holds the slot's leadership (or a
// replica whose sync with the current epoch is proven); every other node
// answers MOVED in-band. The library follows that redirect with the same
// self-heal the write path uses — patch the slot's routing row from the
// answer, retry, count the redirect. A stale routing row therefore costs
// one extra round trip, never a wrong answer and never a server-to-server
// hop.

// readWithRedirect drives one single-key read call through the cached
// table: send, follow MOVED/ASK (patch + retry), rotate the row onto the
// next entry node on transport failure — Config.Attempts rounds, then the
// last error surfaces.
func readWithRedirect[T any](c *Client, ctx context.Context, aggID string,
	call func(addr string) (*T, uint32, string, error)) (*T, error) {

	slot := c.slotOf(aggID)
	var lastErr error
	for attempt := 0; attempt < c.cfg.Attempts; attempt++ {
		addr := c.routeFor(slot, int(c.rr.Add(1)-1))
		resp, errID, node, err := call(addr)
		if err != nil {
			// transport failure: a rotation is a guess, not a redirect
			// the cluster told us — the Redirects() counter stays out of it.
			lastErr = err
			c.routes.patch(slot, c.healthyEntry(int(c.rr.Load())+attempt+1))
			continue
		}
		switch errID {
		case ErrIDSlotNotLocal, ErrIDMigrating:
			if node != "" {
				c.routes.patch(slot, hostPort(node))
			}
			c.redirects.Add(1)
			lastErr = nil
		default:
			return resp, nil
		}
	}
	if lastErr != nil {
		return nil, fmt.Errorf("client: read %q not served after %d attempts: %w", aggID, c.cfg.Attempts, lastErr)
	}
	return nil, fmt.Errorf("client: read %q still redirected after %d attempts", aggID, c.cfg.Attempts)
}

// ReadStream reads a version range of one aggregate stream (from_version
// 0 = from the head, limit 0 = all). The call goes to the slot leader as
// the cached table sees it; a MOVED answer patches the row and retries
// (see readWithRedirect).
func (c *Client) ReadStream(ctx context.Context, aggID string,
	fromVersion uint32, limit uint64) (*pushupesv1.ReadStreamResponse, error) {

	return readWithRedirect(c, ctx, aggID, func(addr string) (*pushupesv1.ReadStreamResponse, uint32, string, error) {
		cctx, cancel := c.callCtx(ctx)
		defer cancel()
		resp, err := c.event(addr).ReadStream(cctx, &pushupesv1.ReadStreamRequest{
			AggregateId: aggID, FromVersion: fromVersion, Limit: limit,
		})
		if err != nil {
			return nil, 0, "", err
		}
		if c.cfg.decompressOn() {
			for _, rec := range resp.Records {
				if derr := inflateRecord(rec); derr != nil {
					return nil, 0, "", derr
				}
			}
		}
		return resp, resp.GetErrId(), resp.GetNode(), nil
	})
}

// ReadTailsFanout caps how many destinations one ReadTails round drives
// concurrently — the client-side twin of the server's BatchAppend
// slot cap: a resume scan over a large
// id list fans out to one sub-call per holding node, and the cap keeps a
// wide batch from opening them all at once.
const ReadTailsFanout = 16

// ReadTails asks the latest version of many aggregates, in request order
// (version 0 with err_id 0 = the aggregate has no records visible within
// the answering node's bound). The list is grouped by the cached routing
// table and each destination gets ONE sub-call — the fan-out that used to
// live on the server now lives here, with the same per-entry redirect
// self-heal as BatchAppend: a MOVED result patches that slot's row and
// re-sends just that entry (up to Config.Attempts rounds; a transport
// failure rotates the rows onto the next entry nodes instead). The answer
// is one result per requested aggregate in request order, echoing each
// aggregate_id; an entry that is still redirected or unanswered when the
// rounds run out fails the call whole — a tail scan that silently reports
// 0 for a slice of its ids is the wrong answer, not a partial one.
func (c *Client) ReadTails(ctx context.Context, aggIDs []string) (*pushupesv1.ReadTailsResponse, error) {
	out := &pushupesv1.ReadTailsResponse{Results: make([]*pushupesv1.ReadTailsResult, len(aggIDs))}
	if len(aggIDs) == 0 {
		return out, nil
	}
	pending := make([]int, len(aggIDs))
	for i := range pending {
		pending[i] = i
	}
	var lastErr error
	for attempt := 0; attempt < c.cfg.Attempts && len(pending) > 0; attempt++ {
		next, err := c.readTailsRound(ctx, attempt, pending, aggIDs, out)
		lastErr = err
		pending = next
	}
	if len(pending) > 0 {
		return nil, fmt.Errorf("client: read tails: %d of %d aggregates still redirected or unanswered after %d rounds: %w",
			len(pending), len(aggIDs), c.cfg.Attempts, lastErr)
	}
	return out, nil
}

// readTailsRound drives one fan-out round: group the pending entries by
// their cached leader, one sub-call per destination (bounded
// concurrency), and merge the answers. It returns the entry indices that
// still need a round — redirected entries (their rows patched from the
// answer first) and entries behind a failed or misaligned destination
// (their rows rotated, the BatchAppend convention) — plus the last
// transport error, informational.
func (c *Client) readTailsRound(ctx context.Context, attempt int, idxs []int,
	aggIDs []string, out *pushupesv1.ReadTailsResponse) ([]int, error) {

	type group struct {
		addr string
		idx  []int
		ids  []string
	}
	byAddr := map[string]*group{}
	var order []*group
	for _, i := range idxs {
		slot := c.slotOf(aggIDs[i])
		addr := c.routeFor(slot, int(c.rr.Add(1)-1))
		g := byAddr[addr]
		if g == nil {
			g = &group{addr: addr}
			byAddr[addr] = g
			order = append(order, g)
		}
		g.idx = append(g.idx, i)
		g.ids = append(g.ids, aggIDs[i])
	}

	// Groups run concurrently, each writing only its own entries' slots in
	// out and its own requeue list; the merge is lock-free by construction.
	type gres struct {
		g       *group
		results []*pushupesv1.ReadTailsResult
		err     error
	}
	sem := make(chan struct{}, ReadTailsFanout)
	done := make(chan gres, len(order))
	for _, g := range order {
		sem <- struct{}{}
		go func(g *group) {
			defer func() { <-sem }()
			cctx, cancel := c.callCtx(ctx)
			defer cancel()
			resp, err := c.event(g.addr).ReadTails(cctx, &pushupesv1.ReadTailsRequest{AggregateIds: g.ids})
			done <- gres{g: g, results: resp.GetResults(), err: err}
		}(g)
	}

	var next []int
	var lastErr error
	for range order {
		r := <-done
		if r.err != nil {
			lastErr = r.err
			c.markUnhealthy(r.g.addr)
			for _, i := range r.g.idx {
				c.routes.patch(c.slotOf(aggIDs[i]), c.healthyEntry(int(c.rr.Load())+attempt+1))
				next = append(next, i)
			}
			continue
		}
		if len(r.results) != len(r.g.ids) {
			lastErr = fmt.Errorf("client: read tails: %s answered %d results for %d ids", r.g.addr, len(r.results), len(r.g.ids))
			for _, i := range r.g.idx {
				c.routes.patch(c.slotOf(aggIDs[i]), c.healthyEntry(int(c.rr.Load())+attempt+1))
				next = append(next, i)
			}
			continue
		}
		for k, res := range r.results {
			i := r.g.idx[k]
			// the protocol echoes aggregate_id per result; an echo
			// mismatch means the answer does not line up with the request.
			if res.GetAggregateId() != aggIDs[i] {
				lastErr = fmt.Errorf("client: read tails: result echo mismatch from %s at %d: %q", r.g.addr, k, res.GetAggregateId())
				for _, j := range r.g.idx {
					c.routes.patch(c.slotOf(aggIDs[j]), c.healthyEntry(int(c.rr.Load())+attempt+1))
					next = append(next, j)
				}
				break
			}
			switch res.GetErrId() {
			case ErrIDSlotNotLocal, ErrIDMigrating:
				if res.GetNode() != "" {
					c.routes.patch(c.slotOf(aggIDs[i]), hostPort(res.GetNode()))
				}
				c.redirects.Add(1)
				next = append(next, i)
			default:
				out.Results[i] = res
			}
		}
	}
	return next, lastErr
}

// ReadByCommand looks one record up by command_id (idempotency probe).
// Redirect-following as in ReadStream.
func (c *Client) ReadByCommand(ctx context.Context, aggID, commandID string) (*pushupesv1.ReadByCommandResponse, error) {
	return readWithRedirect(c, ctx, aggID, func(addr string) (*pushupesv1.ReadByCommandResponse, uint32, string, error) {
		cctx, cancel := c.callCtx(ctx)
		defer cancel()
		resp, err := c.event(addr).ReadByCommand(cctx, &pushupesv1.ReadByCommandRequest{
			AggregateId: aggID, CommandId: commandID,
		})
		if err != nil {
			return nil, 0, "", err
		}
		if c.cfg.decompressOn() && resp.GetFound() {
			if derr := inflateRecord(resp.GetRecord()); derr != nil {
				return nil, 0, "", derr
			}
		}
		return resp, resp.GetErrId(), resp.GetNode(), nil
	})
}

// ReadVersionByTime reports the highest version whose record has unix_time
// at or before the given time (0 = none visible) — the anchor for a
// point-in-time replay that continues with ReadStream. Redirect-following
// as in ReadStream.
func (c *Client) ReadVersionByTime(ctx context.Context, aggID string,
	unixTime int64) (*pushupesv1.ReadVersionByTimeResponse, error) {

	return readWithRedirect(c, ctx, aggID, func(addr string) (*pushupesv1.ReadVersionByTimeResponse, uint32, string, error) {
		cctx, cancel := c.callCtx(ctx)
		defer cancel()
		resp, err := c.event(addr).ReadVersionByTime(cctx, &pushupesv1.ReadVersionByTimeRequest{
			AggregateId: aggID, UnixTime: unixTime,
		})
		if err != nil {
			return nil, 0, "", err
		}
		return resp, resp.GetErrId(), resp.GetNode(), nil
	})
}

// callCtx wraps ctx with the per-call timeout; callers own ctx and stay
// free of leaks (the timeout context is cancelled before returning).
func (c *Client) callCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.cfg.CallTimeout)
}

// ---- routing table -----------------------------------------------------

// routeTable caches the cluster's slot->leader routing as the client sees
// it. PrefetchRoutes seeds it and re-fetches it wholesale on a timer (the
// answering node's Raft-replicated table is the authoritative view); a
// write's MOVED/ASK answer patches individual rows in between, which is why
// the table is only ever a hint — append correctness is defined by the
// redirect self-heal, not by the table being current.
type routeTable struct {
	mu        sync.RWMutex
	slotCount int      // len(bySlot): the cluster's slot count, from the table itself
	bySlot    []string // slot -> leader client addr ("host:port")
	entries   []string // every known node addr, the round-robin fallback set
	cursor    int      // last entry addr that answered a fetch
	client    *Client
	// wake asks the refresh loop to fetch NOW, not on the next ticker
	// beat: when an address is declared unhealthy the cached table is
	// known-stale (a scale-down just moved its slots), and the blind
	// entry rotation over empty rows is a slower convergence than one
	// table fetch from a live node. Buffered size 1; extra wakes coalesce.
	wake chan struct{}
}

// fetch pulls the whole routing table, trying entry nodes round-robin.
func (rt *routeTable) fetch() error {
	for k := 0; k < len(rt.entries); k++ {
		i := (rt.cursor + k) % len(rt.entries)
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := rt.client.event(rt.entries[i]).PrefetchRoutes(cctx, &pushupesv1.PrefetchRoutesRequest{})
		cancel()
		if err != nil {
			continue
		}
		bySlot := make([]string, len(resp.SlotNodeIndex))
		for slot, idx := range resp.SlotNodeIndex {
			bySlot[slot] = hostPort(resp.NodeClientAddrs[idx])
		}
		rt.mu.Lock()
		rt.slotCount = len(bySlot)
		rt.bySlot = bySlot
		rt.cursor = i
		rt.mu.Unlock()
		return nil
	}
	return fmt.Errorf("prefetch routes: no entry node answered")
}

// refreshEvery re-fetches the whole table until stop closes. A failed
// refresh keeps the old rows — the table is a hint, stale beats absent.
func (rt *routeTable) refreshEvery(d time.Duration, stop <-chan struct{}) {
	tk := time.NewTicker(d)
	defer tk.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tk.C:
			rt.fetch()
		case <-rt.wake:
			rt.fetch()
		}
	}
}

func (rt *routeTable) slotCountNow() int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.slotCount
}

// slotOf routes an aggregate id with the shared algorithm over the CURRENT
// table's slot count.
func (rt *routeTable) slotOf(agg string) int32 {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return SlotOf(agg, rt.slotCount)
}

// addrFor answers the cached leader of a slot; fallback picks the
// round-robin entry when the row is missing (a slot beyond a stale,
// shorter table mid-reslot).
func (rt *routeTable) addrFor(slot int32, fallback int) string {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	if slot >= 0 && int(slot) < len(rt.bySlot) && rt.bySlot[slot] != "" {
		return rt.bySlot[slot]
	}
	return rt.entries[fallback%len(rt.entries)]
}

// invalidateAddr forgets every cached row pointing at addr — the transport
// failure answer to "this node is not answering": a scale-down leader holds
// a CONTIGUOUS REGION of slots, so patching only the rows of the batch that
// just failed would leave the next batch (other aggregates, same dead
// leader) to burn its own attempt budget on the same address. Dropping the
// whole region sends every later guess through the healthy rotation
// instead; the next authoritative refresh re-arms the rows as needed.
func (rt *routeTable) invalidateAddr(addr string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for i, a := range rt.bySlot {
		if a == addr {
			rt.bySlot[i] = ""
		}
	}
}

// patch points one slot's row at addr (the MOVED/ASK answer, or a rotated
// guess after a transport failure). The next refresh overwrites it with
// the authoritative table.
func (rt *routeTable) patch(slot int32, addr string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if slot >= 0 && int(slot) < len(rt.bySlot) {
		rt.bySlot[slot] = addr
	}
}

func (c *Client) slotOf(agg string) int32 { return c.routes.slotOf(agg) }

// Liveness-ping cadence for the client surface, matching what the server's
// keepalive enforcement policy allows (cmd/pushupes: MinTime covers the
// interval, PermitWithoutStream covers the idle gaps).
const (
	clientKeepaliveTime    = 5 * time.Second
	clientKeepaliveTimeout = 2 * time.Second
)

func (c *Client) routeFor(slot int32, fallback int) string {
	return c.routes.addrFor(slot, fallback)
}

// entryPenalty is how long a transport-failed entry address stays out of the
// guess-rotation. It is deliberately shorter than any operator would wait:
// a node that is really gone is off the routing rows anyway (redirects never
// name it again), and a node that was only sick gets a second chance quickly.
const entryPenalty = 3 * time.Second

// markUnhealthy puts an address in the penalty box (transport failure or a
// misaligned answer — the shapes where the destination itself is suspect,
// NOT a business-level answer riding back on a healthy connection) and
// drops every cached routing row that still names it: the destination is
// not answering, so no slot should keep aiming at it. A scale-down leader
// holds a contiguous region of slots; patching only the batch that just
// failed would make every later batch re-burn its attempts on the same
// dead address (the no-response signature this closes).
func (c *Client) markUnhealthy(addr string) {
	c.penaltyMu.Lock()
	c.penalty[addr] = time.Now().Add(entryPenalty)
	c.penaltyMu.Unlock()
	c.routes.invalidateAddr(addr)
	// The dead destination's slots moved — fetch the real table instead
	// of letting the next batches rotate blind through entries guessing
	// (that guessing is what turned a scale-down stop into no-response
	// failures when the ticker had not refreshed yet).
	select {
	case c.routes.wake <- struct{}{}:
	default:
	}
}

// healthyEntry walks the entry list from base and returns the first address
// outside the penalty window. When every entry is penalised the rotation
// proceeds anyway — a guess that might be the only live node beats refusing
// to send.
func (c *Client) healthyEntry(base int) string {
	ents := c.routes.entries
	now := time.Now()
	c.penaltyMu.Lock()
	defer c.penaltyMu.Unlock()
	for k := range ents {
		addr := ents[(base+k)%len(ents)]
		if until, bad := c.penalty[addr]; !bad || now.After(until) {
			return addr
		}
	}
	return ents[base%len(ents)]
}

// hostPort strips a URL scheme for gRPC dialing (routing-table addresses
// carry "http://" by convention; a bare host:port is passed through).
func hostPort(addr string) string {
	if i := strings.Index(addr, "://"); i >= 0 {
		return addr[i+3:]
	}
	return addr
}
