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

	pushupesv1 "github.com/berkaroad/pushupes/pkg/grpcapi/pushupes/v1"
)

// Config tunes a Client. Zero values select the defaults.
type Config struct {
	RouteRefresh time.Duration // slot->leader table re-fetch interval; 0 = fetch once at startup (default 30s)
	Attempts     int           // max send rounds per BatchAppend call, retries of redirected records included (default 3)
	CallTimeout  time.Duration // per-attempt RPC timeout inside BatchAppend (default 10s)
	FlowBackoff  time.Duration // pause after a node answers 1006 (flow control); the refused records re-send and the node's gate stays closed until one answer comes back un-throttled (default 100ms)
}

// Client talks to a pushupes cluster's data plane: routing resolved and
// maintained by PrefetchRoutes (seeded at New, refreshed on a timer,
// patched per MOVED/ASK in between), one long-lived gRPC connection per
// node address shared by every call, per-record redirect self-heal on
// the write path, and flow-control backpressure: a node that answers 1006
// gets its refused records re-sent every FlowBackoff (default 100ms) while
// a per-node gate stays closed, pausing every later write to that node
// until one re-send comes back un-throttled. Business rules (idempotency,
// version checks) pass through untouched: a Client answers what the
// cluster answered — except the 1006 itself, which the library absorbs as
// waiting rather than relaying.
type Client struct {
	cfg         Config
	connsMu     sync.Mutex
	conns       map[string]*grpc.ClientConn
	gatesMu     sync.Mutex
	gates       map[string]*gateT // per-node flow-control gate; created open on the first 1006, kept until the Client is dropped
	routes      *routeTable
	rr          atomic.Uint64 // round-robin cursor for entry picks
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
		c.cfg.CallTimeout = 10 * time.Second
	}
	if c.cfg.FlowBackoff <= 0 {
		c.cfg.FlowBackoff = 100 * time.Millisecond
	}
	c.routes = &routeTable{entries: entries, client: c}
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

// FlowPauses counts the 1006-answered re-send rounds the write path has
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
		cc, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			panic(err) // malformed target: a programming error, not runtime state
		}
		c.conns[addr] = cc
	}
	return pushupesv1.NewEventServiceClient(cc)
}

// ---- flow-control gate --------------------------------------------------

// gateT is one node's gate. While the node refuses appends (1006 — the
// operator throttle at its node-wide token bucket), the goroutine that
// entered backpressure HOLDS the gate: it sleeps FlowBackoff, re-sends the
// refused records, and keeps holding until an answer comes back without a
// single 1006, then releases. Every other write destined for that node
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

// release reopens the gate after a clean (1006-free) answer and wakes ALL
// parked waiters by closing (then remaking) the channel. Holder-only.
// Woken writers that are BatchAppend callers re-check via the send-loop
// fast path and ride the next batch open; the next throttle is entered by
// whichever answer carries a 1006 from then on.
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
// and MOVED/ASK answers patch that slot's row and re-send just those
// records (up to Config.Attempts rounds; a transport failure rotates the
// row onto the next entry node instead). The returned slice is in the
// order of recs; a nil entry means the record was still redirected or
// unanswered when the attempts ran out — the error (last transport
// failure, informational) says which. Business outcomes (success/exists/
// fail with their error ids) are the cluster's, untouched — EXCEPT flow
// control: a 1006 never reaches the caller. Refused records are re-sent
// every Config.FlowBackoff while the node's gate is held closed, parking
// every later write to that node, until one answer comes back
// un-throttled (see gateT).
func (c *Client) BatchAppend(ctx context.Context,
	recs []*pushupesv1.AppendRequest) ([]*pushupesv1.AppendResponse, error) {

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
		var next []int
		for _, addr := range order {
			idx := byAddr[addr]
			batch := make([]*pushupesv1.AppendRequest, len(idx))
			for k, i := range idx {
				batch[k] = recs[i]
			}
			// Flow-control backpressure: park while another caller holds
			// this node's gate (the node is refusing writes). Waiting
			// happens OUTSIDE the attempt clock — time behind a gate must
			// not burn the caller's Attempts — and only costs a map miss
			// plus one atomic load while no throttle is active.
			if gate := c.peekGate(addr); gate != nil && !gate.isOpen() {
				if werr := gate.waitOpen(ctx); werr != nil {
					lastErr = werr
					next = append(next, idx...)
					continue
				}
			}
			cctx, cancel := context.WithTimeout(ctx, c.cfg.CallTimeout)
			resp, err := c.event(addr).BatchAppend(cctx, &pushupesv1.BatchAppendRequest{Records: batch})
			cancel()
			if err != nil {
				lastErr = err
			}
			if err != nil || len(resp.GetResults()) != len(batch) {
				// transport failure or misaligned answer: rotate the route
				// and requeue these records for the next attempt. A rotation
				// is a guess, not a redirect the cluster told us — the
				// Redirects() counter stays out of it.
				for _, i := range idx {
					c.routes.patch(c.slotOf(recs[i].AggregateId),
						c.routes.entries[(int(c.rr.Load())+attempt+1)%len(c.routes.entries)])
				}
				next = append(next, idx...)
				continue
			}
			// the batch protocol echoes aggregate_id per result; an echo
			// mismatch means the answer does not line up with the request —
			// check before consuming, and requeue like a misaligned answer.
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
					c.routes.patch(c.slotOf(recs[i].AggregateId), c.routes.entries[(int(c.rr.Load())+attempt+1)%len(c.routes.entries)])
				}
				next = append(next, idx...)
				continue
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
			// The node's token bucket refused part of the batch: become the
			// gate holder (concurrent contenders for the same throttle park
			// in hold), sleep FlowBackoff, and re-send ONLY the refused
			// records until an answer carries no 1006, then release the
			// gate. Re-sending is safe: append idempotency keys on
			// command_id, so a record the node wrote before throttling
			// answers exists on the next round, never twice. The loop runs
			// past Config.Attempts on purpose — Attempts counts REDIRECT
			// rounds; throttling is waiting, not retrying, and ends when
			// the node recovers or ctx dies (a dead holder releases the
			// gate and requeues its records like a transport failure).
			if len(throttled) > 0 {
				gate := c.gateFor(addr)
				if !gate.hold(ctx) {
					lastErr = ctx.Err()
					next = append(next, throttled...)
				} else {
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
			}
		}
		pending = next
		c.rr.Add(1)
	}
	return out, lastErr
}

// ReadStream reads a version range of one aggregate stream (from_version
// 0 = from the head, limit 0 = all). The call goes to the slot leader as
// the cached table sees it; a stale row costs one server-side proxy hop,
// not a wrong answer — reads on a node that holds neither slot nor
// replica are forwarded to the leader by the cluster itself.
func (c *Client) ReadStream(ctx context.Context, aggID string,
	fromVersion uint32, limit uint64) (*pushupesv1.ReadStreamResponse, error) {

	addr := c.routeFor(c.slotOf(aggID), int(c.rr.Add(1)-1))
	cctx, cancel := c.callCtx(ctx)
	defer cancel()
	return c.event(addr).ReadStream(cctx, &pushupesv1.ReadStreamRequest{
		AggregateId: aggID, FromVersion: fromVersion, Limit: limit,
	})
}

// ReadTails asks the latest version of many aggregates in one call, in
// request order (0 = the aggregate has no visible records). The whole
// list goes to one entry node, which groups by slot internally and
// forwards one grouped RPC per destination it does not hold — the bulk
// form of "does version k exist" for a resume scan.
func (c *Client) ReadTails(ctx context.Context, aggIDs []string) (*pushupesv1.ReadTailsResponse, error) {
	addr := c.routes.entries[int(c.rr.Add(1)-1)%len(c.routes.entries)]
	cctx, cancel := c.callCtx(ctx)
	defer cancel()
	return c.event(addr).ReadTails(cctx, &pushupesv1.ReadTailsRequest{AggregateIds: aggIDs})
}

// ReadByCommand looks one record up by command_id (idempotency probe).
func (c *Client) ReadByCommand(ctx context.Context, aggID, commandID string) (*pushupesv1.ReadByCommandResponse, error) {
	addr := c.routeFor(c.slotOf(aggID), int(c.rr.Add(1)-1))
	cctx, cancel := c.callCtx(ctx)
	defer cancel()
	return c.event(addr).ReadByCommand(cctx, &pushupesv1.ReadByCommandRequest{
		AggregateId: aggID, CommandId: commandID,
	})
}

// ReadVersionByTime reports the highest version whose record has unix_time
// at or before the given time (0 = none visible) — the anchor for a
// point-in-time replay that continues with ReadStream.
func (c *Client) ReadVersionByTime(ctx context.Context, aggID string,
	unixTime int64) (*pushupesv1.ReadVersionByTimeResponse, error) {

	addr := c.routeFor(c.slotOf(aggID), int(c.rr.Add(1)-1))
	cctx, cancel := c.callCtx(ctx)
	defer cancel()
	return c.event(addr).ReadVersionByTime(cctx, &pushupesv1.ReadVersionByTimeRequest{
		AggregateId: aggID, UnixTime: unixTime,
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

func (c *Client) routeFor(slot int32, fallback int) string {
	return c.routes.addrFor(slot, fallback)
}

// hostPort strips a URL scheme for gRPC dialing (routing-table addresses
// carry "http://" by convention; a bare host:port is passed through).
func hostPort(addr string) string {
	if i := strings.Index(addr, "://"); i >= 0 {
		return addr[i+3:]
	}
	return addr
}
