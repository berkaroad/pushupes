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

	"github.com/berkaroad/pushupes/pkg/client"
	pushupesv1 "github.com/berkaroad/pushupes/pkg/grpcapi/pushupes/v1"
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
	// The reusable client (pkg/client) owns everything routing-shaped:
	// PrefetchRoutes seeds the slot->leader table (its answer length is the
	// cluster's slot count — the ALGORITHM is the shared contract, the COUNT
	// is cluster state), one goroutine refreshes it on -route-refresh, MOVED/
	// ASK answers patch rows in between, and one long-lived gRPC connection
	// per node backs every call.
	cl, err := client.New(entryList, &client.Config{RouteRefresh: *routeRefresh})
	if err != nil {
		fmt.Printf("FAIL: %v\n", err)
		os.Exit(1)
	}
	defer cl.Close()
	fmt.Printf("routing: %d slots (PrefetchRoutes via %d entry addrs)\n",
		cl.SlotCount(), len(entryList))

	// 1) pick aggregate ids, verify slot spread
	aggIDs := make([]string, 0, *aggs)
	slotSeen := map[int32]int{}
	for i := 0; len(aggIDs) < *aggs; i++ {
		id := fmt.Sprintf("bench-%d", i)
		aggIDs = append(aggIDs, id)
		slotSeen[client.SlotOf(id, cl.SlotCount())]++
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
	// One ReadTails for the whole list instead of a per-aggregate ReadStream
	// probe chain: the per-aggregate form cost O(aggregates x log(tail)) RPCs
	// and grew with the data already on disk (measured 5.5s at 58k existing
	// records across 1000 aggregates). The server groups the request by slot
	// and forwards one grouped RPC per node it does not hold, so the client
	// side stays a single call.
	//
	// This phase is NOT part of the write window, and it is timed separately
	// so the reported throughput stays a write-phase number.
	resumeStart := time.Now()
	lastVers := make([]uint32, len(aggIDs))
	{
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		resp, err := cl.ReadTails(cctx, aggIDs)
		cancel()
		if err != nil {
			fmt.Printf("warn: read tails: %v (tails left at 0)\n", err)
		} else if len(resp.Versions) != len(aggIDs) {
			fmt.Printf("warn: read tails: got %d versions for %d aggregates\n",
				len(resp.Versions), len(aggIDs))
		} else {
			copy(lastVers, resp.Versions)
		}
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

	var total, okCnt, existsCnt, failCnt, noRespCnt atomic.Int64
	var errIDMu sync.Mutex
	errIDCnt := map[uint32]*atomic.Int64{} // per wire error id of batch failures
	var latMu sync.Mutex
	var latencies []float64 // ms
	// first batch transport errors, verbatim (capped): a no-response record
	// needs its cause on the report, not just a count.
	var errSampleMu sync.Mutex
	var errSamples []string

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
				// aggregates (the batch rule rejects repeats). cl.BatchAppend
				// groups the chunk per slot leader itself; redirected records
				// are resent (as one batch) to their own leader. Throughput
				// counts records, latency counts batch round trips (retries
				// included).
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
						// the write-window ctx is deliberately NOT passed: a batch
						// in flight when the window closes gets its answer
						// (the client bounds each attempt by CallTimeout
						// internally), same as before the client refactor.
						resps, berr := cl.BatchAppend(context.Background(), recs)
						if berr != nil {
							errSampleMu.Lock()
							if len(errSamples) < 8 {
								errSamples = append(errSamples, berr.Error())
							}
							errSampleMu.Unlock()
						}
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
					resp := singleAppend(cl, req)
					// version self-heal: if the leader reports a newer tail
					// (resume estimate lagged), adopt it and retry once.
					if resp != nil &&
						resp.Status == pushupesv1.AppendResponse_STATUS_FAIL &&
						resp.ErrId == client.ErrIDVersionConflict {
						ver[agg] = resp.CurrentVersion
						req.Version = resp.CurrentVersion + 1
						t0 = time.Now()
						resp = singleAppend(cl, req)
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
		total.Load(), okCnt.Load(), existsCnt.Load(), failCnt.Load(), cl.Redirects())
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

// singleAppend writes one record through the client (a batch of exactly one
// AppendRequest) and answers its position-0 response.
func singleAppend(cl *client.Client, req *pushupesv1.AppendRequest) *pushupesv1.AppendResponse {
	// Detached from the write window: an append in flight when the window
	// closes gets its answer (the client bounds each attempt by CallTimeout
	// internally), same as before the client refactor.
	resps, _ := cl.BatchAppend(context.Background(), []*pushupesv1.AppendRequest{req})
	return resps[0]
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

func splitAddrs(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
