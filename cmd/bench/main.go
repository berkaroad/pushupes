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

// Command bench drives sustained writes against a pushupes cluster to
// measure append throughput. Event traffic goes over the gRPC data plane
// only: -nodes lists the client-plane (gRPC) addresses, from which the
// slot -> leader routing table is resolved via one PrefetchRoutes call and
// refreshed periodically. It spreads events across -aggs aggregate IDs
// (default 100, which the routing hash scatters over many slots — the run
// fails its own check if fewer than 2 slots end up touched), keeps appending
// consecutive versions per aggregate, and keeps every single event body
// within 1 KiB. It follows MOVED/ASK redirects per slot (they carry the
// leader's gRPC address) and reports msg/sec.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/berkaroad/pushupes/pkg/client"
	pushupesv1 "github.com/berkaroad/pushupes/pkg/grpcapi/pushupes/v1"
)

// clients caches one gRPC connection per address (HTTP/2 keep-alive).
var (
	connMu sync.Mutex
	conns  = map[string]*grpc.ClientConn{}
)

func eventClient(addr string) pushupesv1.EventServiceClient {
	connMu.Lock()
	defer connMu.Unlock()
	c, ok := conns[addr]
	if !ok {
		var err error
		c, err = grpc.NewClient(hostPort(addr), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			panic(err)
		}
		conns[addr] = c
	}
	return pushupesv1.NewEventServiceClient(c)
}

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
}

// fetch pulls the whole routing table, trying entry nodes round-robin.
func (rt *routeTable) fetch() error {
	for k := 0; k < len(rt.entries); k++ {
		i := (rt.cursor + k) % len(rt.entries)
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := eventClient(rt.entries[i]).PrefetchRoutes(cctx, &pushupesv1.PrefetchRoutesRequest{})
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
			if err := rt.fetch(); err != nil {
				fmt.Printf("warn: route refresh: %v\n", err)
			}
		}
	}
}

// slotOf routes an aggregate id with the shared algorithm over the CURRENT
// table's slot count.
func (rt *routeTable) slotOf(agg string) int32 {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return client.SlotOf(agg, rt.slotCount)
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
// guess after a transport failure). The next refresh overwrites it with the
// authoritative table.
func (rt *routeTable) patch(slot int32, addr string) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if slot >= 0 && int(slot) < len(rt.bySlot) {
		rt.bySlot[slot] = addr
	}
}

// nodeCount counts distinct addrs the current table routes to.
func (rt *routeTable) nodeCount() int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	seen := map[string]bool{}
	for _, a := range rt.bySlot {
		seen[a] = true
	}
	return len(seen)
}

// anyGrpc is the round-robin entry-point list (all peer gRPC addrs).
var anyGrpc []string

// first batch transport errors, verbatim (capped): a no-response record needs
// its cause on the report, not just a count.
var (
	errSampleMu sync.Mutex
	errSamples  []string
)

func main() {
	nodes := flag.String("nodes", "127.0.0.1:8591", "comma-separated node client-plane (gRPC) addrs; the routing table is resolved from one of them via PrefetchRoutes")
	aggs := flag.Int("aggs", 100, "number of aggregate ids")
	dur := flag.Duration("duration", 10*time.Second, "sustained write duration")
	connsFlag := flag.Int("conns", 16, "workers; each worker owns a partition of the aggregates and appends one at a time (awaits each reply before sending the next), so throughput is bounded by conns / p50 — conns=1 measures a single serialized round trip, not cluster capacity")
	size := flag.Int("size", 1024, "event body bytes (must be <= 1024 * 1024)")
	batch := flag.Int("batch", 0, "records per BatchAppend call (the write path is always BatchAppend: 0 = one BatchAppend carrying a single record per round, >0 = a chunk of distinct aggregates grouped per slot leader). Latency lines report per-batch round trips")
	reportEvery := flag.Duration("report", time.Second, "live report interval")
	routeRefresh := flag.Duration("route-refresh", 30*time.Second, "how often the slot->leader table is re-fetched from PrefetchRoutes (0 = fetch once at startup; MOVED/ASK answers patch the table in between either way)")
	flag.Parse()

	if *size > 1024*1024 {
		fmt.Println("size must stay within 1 MiB per event body")
		os.Exit(2)
	}
	entryList := splitAddrs(*nodes) // client-plane (gRPC) addrs
	if len(entryList) == 0 {
		fmt.Println("no nodes given")
		os.Exit(2)
	}
	body := makeBody(*size)
	// Routing over the data plane: one PrefetchRoutes against any entry
	// node seeds the slot->leader table, and the dense answer is where the
	// cluster's slot count comes from — the routing ALGORITHM is shared
	// (pkg/client), the COUNT is cluster state, read from here rather than
	// hard-coded. The entry list doubles as the round-robin set for
	// transport-failure fallbacks.
	anyGrpc = append(anyGrpc, entryList...)
	sort.Strings(anyGrpc)

	routes := &routeTable{entries: anyGrpc}
	if err := routes.fetch(); err != nil {
		fmt.Printf("FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("routing: %d slots over %d cluster nodes (PrefetchRoutes)\n",
		routes.slotCount, routes.nodeCount())
	if *routeRefresh > 0 {
		stop := make(chan struct{})
		defer close(stop)
		go routes.refreshEvery(*routeRefresh, stop)
	}

	// 1) pick aggregate ids, verify slot spread
	aggIDs := make([]string, 0, *aggs)
	slotSeen := map[int32]int{}
	for i := 0; len(aggIDs) < *aggs; i++ {
		id := fmt.Sprintf("bench-%d", i)
		aggIDs = append(aggIDs, id)
		slotSeen[routes.slotOf(id)]++
	}
	if len(slotSeen) < 2 {
		fmt.Printf("FAIL: %d aggregates landed on %d slot(s), need >= 2\n", len(aggIDs), len(slotSeen))
		os.Exit(1)
	}
	fmt.Printf("aggregates=%d across %d slots (body=%dB conns=%d duration=%s)\n",
		len(aggIDs), len(slotSeen), *size, *connsFlag, *dur)

	// resume per-aggregate versions against the slot LEADER (replicas only
	// show <=HW data; under-estimating the tail burns retries on 1001).
	//
	// Done as one ReadTails call per node instead of one ReadStream probe
	// chain per aggregate: the per-aggregate form cost O(aggregates x
	// log(tail)) RPCs and grew with the data already on disk (measured
	// 5.5s at 58k existing records across 1000 aggregates).
	//
	// This phase is NOT part of the write window, and it is timed separately
	// so the reported throughput stays a write-phase number.
	resumeStart := time.Now()
	lastVers := make([]uint32, len(aggIDs))
	{
		// Group aggregates by the node we will ask: ReadTails groups by slot
		// internally, so asking the slot leader directly keeps it to one RPC
		// per node with no cross-node forwarding.
		byAddr := map[string][]int{}
		var order []string
		for i, id := range aggIDs {
			addr := routes.addrFor(routes.slotOf(id), 0)
			if _, ok := byAddr[addr]; !ok {
				order = append(order, addr)
			}
			byAddr[addr] = append(byAddr[addr], i)
		}
		type tailsReq struct {
			idx []int
			ids []string
		}
		reqs := make([]tailsReq, len(order))
		var wg sync.WaitGroup
		for n, addr := range order {
			idx := byAddr[addr]
			ids := make([]string, len(idx))
			for k, i := range idx {
				ids[k] = aggIDs[i]
			}
			reqs[n] = tailsReq{idx: idx, ids: ids}
			wg.Add(1)
			go func(addr string, r tailsReq) {
				defer wg.Done()
				cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				resp, err := eventClient(addr).ReadTails(cctx, &pushupesv1.ReadTailsRequest{AggregateIds: r.ids})
				if err != nil {
					fmt.Printf("warn: read tails %s: %v (tails left at 0)\n", addr, err)
					return
				}
				if len(resp.Versions) != len(r.ids) {
					fmt.Printf("warn: read tails %s: got %d versions for %d aggregates\n",
						addr, len(resp.Versions), len(r.ids))
					return
				}
				for k, i := range r.idx {
					lastVers[i] = resp.Versions[k]
				}
			}(addr, reqs[n])
		}
		wg.Wait()
	}
	var resumeSum uint64
	for _, v := range lastVers {
		resumeSum += uint64(v)
	}
	fmt.Printf("resuming: %d existing records across aggregates (probed in %s)\n",
		resumeSum, time.Since(resumeStart).Round(time.Millisecond))

	// 3) run
	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\ninterrupted, draining...")
		cancel()
	}()

	var total, okCnt, existsCnt, failCnt, redirectCnt, noRespCnt atomic.Int64
	var errIDMu sync.Mutex
	errIDCnt := map[uint32]*atomic.Int64{} // per wire error id of batch failures
	var latMu sync.Mutex
	var latencies []float64 // ms

	nw := *connsFlag
	if nw > len(aggIDs) {
		nw = len(aggIDs)
	}
	// Each worker walks its partition strictly in sequence: it sends one
	// append and awaits the reply before building the next request. With a
	// single worker there is nothing to overlap, so the run measures one
	// serialized round trip (throughput ~= 1/p50) rather than cluster
	// capacity — say so loudly, because that number is easy to misread.
	if nw == 1 {
		fmt.Printf("notice: conns=1 -> appends are strictly serialized (one in flight);\n" +
			"        throughput is a single round-trip measurement (~1/p50), not cluster capacity.\n" +
			"        Raise -conns to overlap requests (conns partitions -aggs across workers).\n")
	}
	var wg sync.WaitGroup
	// per-run salt: a fixed RNG seed would reproduce last run's command_ids
	// and every append would idempotently return "exists" instead of writing.
	runSalt := time.Now().UnixNano()
	start := time.Now()
	for w := 0; w < nw; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(runSalt + int64(w)*7919 + 13))
			targets := make([]string, 0, len(aggIDs))
			for i := w; i < len(aggIDs); i += nw {
				targets = append(targets, aggIDs[i])
			}
			ver := make(map[string]uint32, len(targets))
			for _, t := range targets {
				if idx := aggIndex(aggIDs, t); idx >= 0 {
					ver[t] = lastVers[idx]
				}
			}
			seq := 0
			// bump builds one fresh append request for agg (next version,
			// fresh command id).
			bump := func(agg string) *pushupesv1.AppendRequest {
				seq++
				ver[agg]++
				return &pushupesv1.AppendRequest{
					AggregateId: agg, Version: ver[agg],
					CommandId: fmt.Sprintf("b%d-%d-%d", w, seq, rng.Int63n(1<<40)),
					Events:    []*pushupesv1.Event{{Type: "BenchAppend", Body: body}},
				}
			}
			if *batch > 0 {
				// Batched mode: one BatchAppend per chunk of DISTINCT
				// aggregates (the batch rule rejects repeats). The chunk is
				// sent to the leader of the first aggregate's slot;
				// redirected records are resent (as one batch) to their own
				// leader by batchWithRetry. Throughput counts records,
				// latency counts batch round trips (retries included).
				for ctx.Err() == nil {
					for c := 0; c < len(targets); c += *batch {
						if ctx.Err() != nil {
							return
						}
						end := c + *batch
						if end > len(targets) {
							end = len(targets)
						}
						chunk := targets[c:end]
						recs := make([]*pushupesv1.AppendRequest, len(chunk))
						for k, agg := range chunk {
							recs[k] = bump(agg)
						}
						t0 := time.Now()
						resps := batchWithRetry(w, recs, routes)
						lat := float64(time.Since(t0).Microseconds()) / 1000.0
						total.Add(int64(len(recs)))
						latMu.Lock()
						latencies = append(latencies, lat)
						latMu.Unlock()
						for k, resp := range resps {
							if resp == nil {
								failCnt.Add(1)
								noRespCnt.Add(1)
								continue
							}
							switch resp.Status {
							case pushupesv1.AppendResponse_STATUS_SUCCESS:
								okCnt.Add(1)
							case pushupesv1.AppendResponse_STATUS_EXISTS:
								existsCnt.Add(1)
							default:
								failCnt.Add(1)
								errIDMu.Lock()
								c := errIDCnt[resp.ErrId]
								if c == nil {
									c = &atomic.Int64{}
									errIDCnt[resp.ErrId] = c
								}
								errIDMu.Unlock()
								c.Add(1)
								// self-heal the local version map on 1001 so
								// the next round sends tail+1 (same rule as
								// the single-append path)
								if resp.ErrId == client.ErrIDVersionConflict {
									ver[chunk[k]] = resp.CurrentVersion
								}
							}
							if resp.ErrId == client.ErrIDSlotNotLocal || resp.ErrId == client.ErrIDMigrating {
								redirectCnt.Add(1)
							}
						}
					}
				}
				return
			}
			for ctx.Err() == nil {
				for _, agg := range targets {
					if ctx.Err() != nil {
						return
					}
					seq++
					ver[agg]++
					req := &pushupesv1.AppendRequest{
						AggregateId: agg, Version: ver[agg],
						CommandId: fmt.Sprintf("b%d-%d-%d", w, seq, rng.Int63n(1<<40)),
						Events:    []*pushupesv1.Event{{Type: "BenchAppend", Body: body}},
					}
					t0 := time.Now()
					resp := batchWithRetry(w, []*pushupesv1.AppendRequest{req}, routes)[0]
					// version self-heal: if the leader reports a newer tail
					// (resume estimate lagged), adopt it and retry once.
					if resp != nil &&
						resp.Status == pushupesv1.AppendResponse_STATUS_FAIL &&
						resp.ErrId == client.ErrIDVersionConflict {
						ver[agg] = resp.CurrentVersion
						req.Version = resp.CurrentVersion + 1
						t0 = time.Now()
						resp = batchWithRetry(w, []*pushupesv1.AppendRequest{req}, routes)[0]
					}
					lat := float64(time.Since(t0).Microseconds()) / 1000.0
					total.Add(1)
					if resp == nil {
						failCnt.Add(1)
						continue
					}
					latMu.Lock()
					latencies = append(latencies, lat)
					latMu.Unlock()
					switch resp.Status {
					case pushupesv1.AppendResponse_STATUS_SUCCESS:
						okCnt.Add(1)
					case pushupesv1.AppendResponse_STATUS_EXISTS:
						existsCnt.Add(1)
					default:
						failCnt.Add(1) // version conflicts etc. should not happen
					}
					if resp.ErrId == client.ErrIDSlotNotLocal || resp.ErrId == client.ErrIDMigrating {
						redirectCnt.Add(1)
					}
				}
			}
		}(w)
	}

	// live report
	done := make(chan struct{})
	go func() {
		tk := time.NewTicker(*reportEvery)
		defer tk.Stop()
		var last int64
		for {
			select {
			case <-done:
				return
			case t := <-tk.C:
				cur := total.Load()
				fmt.Printf("t+%4.1fs  total=%8d  rate=%8.0f msg/s  ok=%d exists=%d fail=%d\n",
					t.Sub(start).Seconds(), cur, float64(cur-last)/reportEvery.Seconds(),
					okCnt.Load(), existsCnt.Load(), failCnt.Load())
				last = cur
			}
		}
	}()
	wg.Wait()
	close(done)
	elapsed := time.Since(start)

	// 4) final report
	sort.Float64s(latencies)
	pct := func(q float64) string {
		if len(latencies) == 0 {
			return "-"
		}
		return fmt.Sprintf("%.2fms", latencies[int(q*float64(len(latencies)-1))])
	}
	resumeElapsed := start.Sub(resumeStart)
	fmt.Printf("\n== bench done in %s ==\n", elapsed.Round(time.Millisecond))
	fmt.Printf("attempts=%d ok=%d exists=%d fail=%d redirects=%d\n",
		total.Load(), okCnt.Load(), existsCnt.Load(), failCnt.Load(), redirectCnt.Load())
	if *batch > 0 {
		// batch failures need their cause spelled out: unreachable-vs-answer
		// and which wire error dominated (1001 self-heal races vs 1005 HW)
		errIDMu.Lock()
		fmt.Printf("batch fail detail: no-response=%d", noRespCnt.Load())
		for id, c := range errIDCnt {
			fmt.Printf(" err_%d=%d", id, c.Load())
		}
		errIDMu.Unlock()
		fmt.Println()
		errSampleMu.Lock()
		for _, es := range errSamples {
			fmt.Printf("  batch transport error: %s\n", es)
		}
		errSampleMu.Unlock()
	}
	fmt.Printf("throughput=%.0f msg/s  latency p50=%s p90=%s p99=%s\n",
		float64(total.Load())/elapsed.Seconds(), pct(0.5), pct(0.9), pct(0.99))
	// Two different denominators are easy to confuse: the line above divides
	// by the write window only, while the per-slot delta further down divides
	// by everything the process spent after the chain was ready (resume
	// probing included). Print both explicitly instead of letting one of them
	// masquerade as "the" throughput.
	fmt.Printf("phases: resume=%s (probe only, excluded from throughput=), window=%s (append phase)\n",
		resumeElapsed.Round(time.Millisecond), elapsed.Round(time.Millisecond))

	if failCnt.Load() > 0 {
		fmt.Println("NOTE: some appends failed; check cluster health")
		os.Exit(1)
	}
}

// batchWithRetry posts one batch over gRPC, following MOVED/ASK at most
// twice per record: a redirected record is resent to its redirect target on
// the next attempt (grouped by destination, so each node still gets one RPC
// per attempt). Returns the final response per record position (nil =
// unreachable or still redirected).
func batchWithRetry(w int, recs []*pushupesv1.AppendRequest,
	routes *routeTable) []*pushupesv1.AppendResponse {

	slotOf := func(agg string) int32 { return routes.slotOf(agg) }
	routeFor := func(slot int32) string { return routes.addrFor(slot, w) }

	out := make([]*pushupesv1.AppendResponse, len(recs))
	// pending pairs original positions with their records; it shrinks to the
	// redirected/unanswered subset after each attempt.
	pending := make([]int, len(recs))
	for i := range pending {
		pending[i] = i
	}
	for attempt := 0; attempt < 3 && len(pending) > 0; attempt++ {
		// One BatchAppend per destination node: group by the cached route.
		byAddr := map[string][]int{}
		var order []string
		for _, i := range pending {
			addr := routeFor(slotOf(recs[i].AggregateId))
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
			cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			resp, err := eventClient(addr).BatchAppend(cctx, &pushupesv1.BatchAppendRequest{Records: batch})
			cancel()
			if err != nil {
				errSampleMu.Lock()
				if len(errSamples) < 8 {
					errSamples = append(errSamples, err.Error())
				}
				errSampleMu.Unlock()
			}
			if err != nil || len(resp.Results) != len(batch) {
				// transport failure or misaligned answer: rotate the route
				// and requeue these records for the next attempt
				for _, i := range idx {
					routes.patch(slotOf(recs[i].AggregateId), anyGrpc[(w+attempt+1)%len(anyGrpc)])
				}
				next = append(next, idx...)
				continue
			}
			for k, r := range resp.Results {
				i := idx[k]
				switch r.Response.ErrId {
				case client.ErrIDSlotNotLocal, client.ErrIDMigrating:
					if r.Response.Node != "" {
						routes.patch(slotOf(recs[i].AggregateId), hostPort(r.Response.Node))
					}
					next = append(next, i)
				default:
					out[i] = r.Response
				}
			}
		}
		pending = next
	}
	return out
}

// aggIndex finds an aggregate id's position; -1 when absent.
func aggIndex(ids []string, want string) int {
	for i, id := range ids {
		if id == want {
			return i
		}
	}
	return -1
}

func makeBody(size int) []byte {
	pad := make([]byte, size)
	for i := range pad {
		pad[i] = 'a' + byte(i%26)
	}
	b, _ := json.Marshal(map[string]string{"pad": string(pad)})
	return b
}

// hostPort strips a URL scheme for gRPC dialing (routing-table addresses
// carry "http://" by convention; a bare host:port is passed through).
func hostPort(addr string) string {
	if i := strings.Index(addr, "://"); i >= 0 {
		return addr[i+3:]
	}
	return addr
}

func splitAddrs(s string) []string {
	var out []string
	cur := ""
	for _, c := range s {
		if c == ',' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		if c != ' ' {
			cur += string(c)
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
