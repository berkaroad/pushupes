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
	// records across 1000 aggregates). The library groups the list by the
	// cached routing table and drives one sub-call per holding node,
	// following per-entry MOVED itself, so the call site stays a single
	// call.
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
		} else if len(resp.Results) != len(aggIDs) {
			fmt.Printf("warn: read tails: got %d results for %d aggregates\n",
				len(resp.Results), len(aggIDs))
		} else {
			for i, r := range resp.Results {
				lastVers[i] = r.Version
			}
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

	var total, okCnt, existsCnt, failCnt, noRespCnt, windowDropped atomic.Int64
	// Settled outcomes for requests the wire did not answer cleanly. They are
	// NOT failures: `settledLanded` ones were written and only lost their
	// answer (counted in ok), `settledRetried` ones were proven unwritten and
	// re-sent (the retry decides), and `unresolved` ones never got a verdict
	// before the window closed. Reported next to `redirects` so the operator
	// sees the ambiguity without it polluting the success count.
	var settledLanded, settledRetried, unresolvedCnt, ambigCnt atomic.Int64
	var errIDMu sync.Mutex
	errIDCnt := map[uint32]*atomic.Int64{} // per wire error id of batch failures
	var latMu sync.Mutex
	var latencies []float64 // ms
	// first batch transport errors, verbatim (capped): a no-response record
	// needs its cause on the report, not just a count.
	var errSampleMu sync.Mutex
	var errSamples []string
	// first 1001s verbatim: the exact (agg, sent version, server tail)
	// triple distinguishes an ack-loss residue from a routing race —
	// counts alone cannot.
	var bizSampleMu sync.Mutex
	var bizSamples []string

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
			// dirty holds aggregates whose last send left the server-side
			// version UNKNOWN (a batch that ran out of answers on a dead
			// transport — the record may or may not have landed). Sending
			// the next round at ver+1 against a record that DID land is
			// how a 1001 secondary wound forms; probing the tail before
			// the next bump makes the uncertainty cost one ReadTails, not
			// a spurious write failure.
			dirty := map[string]bool{}
			// unresolved holds the version of a record whose fate the wire did
			// not answer: a request that never came back (no-response) or an
			// ambiguous 1005. Those are NOT failures — the request may have
			// landed and lost its answer in transport. The tail probe below
			// settles each one: tail >= that version ⇒ it WAS written ⇒ count
			// it as a success; otherwise the ledger rolls back and the retry
			// decides. Only records with no verdict when the window closes are
			// counted as failures, so the summary reports what was actually
			// written, not how many round trips it took.
			unresolved := map[string]uint32{}
			// unwritten holds the latest attempt per aggregate that was ANSWERED
			// but did not store the record (a version conflict or a business
			// refusal). It is a candidate, not a failure: the next round
			// resends the same version, and a SUCCESS/EXISTS clears it. Only
			// the entries still standing when the window closes are counted as
			// failures — so `fail` means "this record was never written",
			// never "this attempt was refused".
			unwritten := map[string]uint32{}
			// Only what is STILL unwritten when this worker stops is a failure:
			// every other outcome either stored the record (ok/exists), was
			// settled by the probe (landed ⇒ ok), or is being retried.
			defer func() {
				// One last authoritative read for the records the wire never
				// answered: if the cluster holds that version, the write DID
				// land and the lost answer was transport, not data.
				if len(unresolved) > 0 {
					aggs := make([]string, 0, len(unresolved))
					for agg := range unresolved {
						aggs = append(aggs, agg)
					}
					cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					presp, perr := cl.ReadTails(cctx, aggs)
					cancel()
					if perr == nil && len(presp.Results) == len(aggs) {
						for i, r := range presp.Results {
							agg := aggs[i]
							uv, ok := unresolved[agg]
							if !ok {
								continue
							}
							if r.Version >= uv {
								// the write DID land; only its answer was lost
								okCnt.Add(1)
								settledLanded.Add(1)
								delete(unwritten, agg)
							} else {
								// provably not written: it stays a candidate and
								// the retry decides (a resend that succeeds
								// clears it; one that never happens is a fail)
								unwritten[agg] = uv
								settledRetried.Add(1)
							}
							delete(unresolved, agg)
						}
					}
				}
				// Truly unresolved (the probe itself never answered) is NOT a
				// failure — nobody proved the record missing. It is reported
				// on its own line; only records with a positive "not written"
				// verdict count as failures.
				if n := len(unresolved); n > 0 {
					unresolvedCnt.Add(int64(n))
				}
				if n := len(unwritten); n > 0 {
					failCnt.Add(int64(n))
				}
			}()
			// lastSeen records, per aggregate, what the cluster
			// AUTHENTICATED on its last answered round: the ver it
			// sent, the server tail that answer carried, and whether
			// the bump was then rolled back (ver--). A 1001 sample
			// prints this history so the gap's origin is visible —
			// guesswork over "+2" ends where the trace begins.
			type lastSeenT struct {
				sent, tail uint32
				rolled     bool
			}
			lastSeen := map[string]lastSeenT{}
			bumpRound := map[string]int{}
			bumpPrev := map[string]uint32{}
			// trace: per-aggregate event ledger (round, kind, ver,
			// tail). Every event for EVERY agg goes in — the ledger
			// is small (one line per answered round) and the full
			// sequence around a 1001 is what turns the +2 debate
			// into a printout. Dumped at exit for sampled aggs.
			type traceEv struct {
				round int
				kind  string
				ver   uint32
				tail  uint32
			}
			trace := map[string][]traceEv{}
			traced := map[string]bool{} // aggs worth dumping (1001 hits)
			round := 0
			// traceAdd appends with a sliding window so the LEDGER
			// around the 1001 (which can land at round 30k) survives;
			// a first-400 cap would only ever show round-1 history.
			defer func() {
				for agg := range traced {
					bizSampleMu.Lock()
					fmt.Printf("  trace %s:\n", agg)
					for _, ev := range trace[agg] {
						fmt.Printf("    r%d %s v=%d tail=%d\n", ev.round, ev.kind, ev.ver, ev.tail)
					}
					bizSampleMu.Unlock()
				}
			}()
			traceAdd := func(agg string, kind string, ver, tail uint32) {
				if traced[agg] {
					return // freeze the ledger at the conflict
				}
				evs := append(trace[agg], traceEv{round, kind, ver, tail})
				if len(evs) > 150 {
					evs = append(evs[:0:0], evs[len(evs)-120:]...)
				}
				trace[agg] = evs
			}
			seq := 0
			// bump builds one fresh append request for agg (next version,
			// fresh command id).
			bump := func(agg string) *pushupesv1.AppendRequest {
				seq++
				prev := ver[agg]
				ver[agg]++
				bumpRound[agg] = round
				bumpPrev[agg] = prev
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
					round++
					if len(dirty) > 0 {
						probes := make([]string, 0, len(dirty))
						for agg := range dirty {
							probes = append(probes, agg)
						}
						cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						presp, perr := cl.ReadTails(cctx, probes)
						cancel()
						if perr == nil && len(presp.Results) == len(probes) {
							for i, r := range presp.Results {
								// A tail of 0 is not an answer: a fenced
								// or wrong node reads empty. Keep the
								// probe armed; the window dies with the
								// fence, not with a guess.
								//
								// A REGRESSED tail needs one more proof
								// before the ledger trusts it. The
								// single-bump window (1005 refused and
								// provably unwrote) rolls the ledger
								// back exactly one; accepting that tail
								// verbatim is right. But the handover
								// window answers the probe from a node
								// whose table lags the move (the demoted
								// old leader still reads like a leader,
								// unbounded): it answers N-1 while the
								// REAL leader's directory already holds
								// N — the record that landed and lost
								// its ack in transport. Trusting the
								// stale N-1 disarms the probe, and the
								// next bump walks into 1001 at the real
								// leader (the dg7 tail=2307-then-CAS-
								// 2308 residue). A one-shot append at
								// N settles it authoritatively: landed
								// ⇒ EXISTS (the old leader dropped N,
								// disarm and continue at N+1); refused
								// ⇒ a real CAS conflict with a higher
								// tail (adopt current and disarm);
								// redirect ⇒ a routing race, ride the
								// next probe round.
								if ver[probes[i]] > 0 && r.Version+1 < ver[probes[i]] {
									continue
								}
								if rolled := lastSeen[probes[i]].rolled; rolled && r.Version+1 == ver[probes[i]] {
									octx, ocancel := context.WithTimeout(context.Background(), 10*time.Second)
									oresps, oerr := cl.BatchAppend(octx, []*pushupesv1.AppendRequest{{
										AggregateId: probes[i],
										Version:     ver[probes[i]],
										CommandId:   fmt.Sprintf("probe-own-%s-%d", probes[i], ver[probes[i]]),
										Events:      []*pushupesv1.Event{{Type: "BenchProbe", Body: []byte("x")}},
									}})
									ocancel()
									// Three authoritative answers:
									//   SUCCESS/EXISTS at ver — the real
									//     leader holds ver (or took the
									//     probe's write): the ledger stays
									//     at ver and the next bump sends
									//     ver+1. Do NOT regress the ledger
									//     to the stale probe tail — that
									//     re-bumps ver into an EXISTS/1001.
									//   FAIL with a higher current —
									//     adopt it (the CAS answered the
									//     real tail, same rule as the
									//     1001 self-heal below).
									//   anything else (1005/1007/transport)
									//     — the probe was aimed at the
									//     wrong node or the window is
									//     still open: keep armed.
									if oerr != nil || len(oresps) != 1 || oresps[0] == nil {
										continue
									}
									oresp := oresps[0]
									if oresp.Status == pushupesv1.AppendResponse_STATUS_SUCCESS ||
										oresp.Status == pushupesv1.AppendResponse_STATUS_EXISTS {
										traceAdd(probes[i], "probe-own", ver[probes[i]], oresp.CurrentVersion)
										if _, ok := unresolved[probes[i]]; ok {
											// SUCCESS/EXISTS at this version means the
											// cluster holds it: the lost answer was
											// transport, the write landed
											okCnt.Add(1)
											settledLanded.Add(1)
											delete(unwritten, probes[i])
											delete(unresolved, probes[i])
										}
										delete(dirty, probes[i])
										continue
									}
									if oresp.ErrId == client.ErrIDVersionConflict && oresp.CurrentVersion >= ver[probes[i]] {
										ver[probes[i]] = oresp.CurrentVersion
										traceAdd(probes[i], "probe-own-adopt", oresp.CurrentVersion, oresp.CurrentVersion)
										delete(dirty, probes[i])
										continue
									}
									// 1005/1007/other failure: the probe
									// aimed at the wrong node or the
									// window is still open — keep armed.
									continue
								}
								ver[probes[i]] = r.Version
								delete(dirty, probes[i])
								if uv, ok := unresolved[probes[i]]; ok {
									if r.Version >= uv {
										// the record WAS written; the answer
										// was lost in transport, not the write
										okCnt.Add(1)
										settledLanded.Add(1)
										delete(unwritten, probes[i])
									} else {
										// provably not written: the retry below
										// decides, and an exit with it still
										// standing is a real failure
										unwritten[probes[i]] = uv
										settledRetried.Add(1)
									}
									delete(unresolved, probes[i])
								}
								traceAdd(probes[i], "probe", ver[probes[i]], r.Version)
								bizSampleMu.Lock()
								if len(bizSamples) < 24 {
									bizSamples = append(bizSamples,
										fmt.Sprintf("heal agg=%s tail=%d", probes[i], r.Version))
								}
								bizSampleMu.Unlock()
							}
							// per-record clears only: an untrusted tail
							// (fenced read answered empty) stays armed
							// and rides the next round's probe
						}
						// a probe that failed rides the NEXT round's
						// probe (dropping dirty here would resurrect the
						// sent_v=tail+2 conflict the probe exists to kill)
					}
					// A dirty aggregate must NOT bump this round: the
					// tail probe is the precondition of its next send.
					// Letting it through while the probe rides failed
					// rounds is how ver outran the server's tail by +2
					// and beyond (each extra failed-probe round adds one
					// more speculative version to the same aggregate —
					// the +2 signature every 1001 sample here carried).
					rt := targets
					if len(dirty) > 0 {
						rt = rt[:0:0]
						for _, agg := range targets {
							if !dirty[agg] {
								rt = append(rt, agg)
							} else {
								traceAdd(agg, "skip", ver[agg], 0)
							}
						}
					}
					for c := 0; c < len(rt); c += *batch {
						if ctx.Err() != nil {
							return
						}
						end := c + *batch
						if end > len(rt) {
							end = len(rt)
						}
						chunk := rt[c:end]
						recs := make([]*pushupesv1.AppendRequest, len(chunk))
						for k, agg := range chunk {
							recs[k] = bump(agg)
						}
						t0 := time.Now()
						// The write-window ctx IS passed: a batch in flight at
						// the deadline must not drain a throttled node's
						// backlog past -duration — the library releases its
						// gate on ctx death and answers what it can; records
						// left unanswered because the WINDOW closed are counted
						// as window-dropped, never as failures.
						resps, berr := cl.BatchAppend(ctx, recs)
						if berr != nil && ctx.Err() == nil && len(resps) == 0 {
							errSampleMu.Lock()
							if len(errSamples) < 8 {
								errSamples = append(errSamples, berr.Error())
							}
							errSampleMu.Unlock()
							// The whole call was refused with nothing
							// answered: every record's fate is unknown
							// (it may have landed on the WAL and lost
							// its ack on the dead connection). Undo each
							// speculative bump and arm the tail probe —
							// the same rule as an unanswered record —
							// or the NEXT round sends tail+2 into a 1001
							// that is bench's bookkeeping leak, not the
							// cluster's.
							for _, agg := range chunk {
								noRespCnt.Add(1)
								ls := lastSeen[agg]
								lastSeen[agg] = lastSeenT{sent: ls.sent, tail: ls.tail, rolled: true}
								unresolved[agg] = ver[agg]
								ver[agg]--
								dirty[agg] = true
							}
							continue
						}
						if berr != nil && ctx.Err() == nil {
							errSampleMu.Lock()
							if len(errSamples) < 8 {
								errSamples = append(errSamples, berr.Error())
							}
							errSampleMu.Unlock()
						}
						// A transport error that answered SOME records
						// truncates the slice: the tail records never
						// even got a nil slot to roll back through —
						// their speculative bumps must be handed back
						// with the same rule as an unanswered record
						// (fate unknown: a ReadTails probe, never a
						// +2 into a self-made 1001).
						if berr != nil && ctx.Err() == nil && len(resps) < len(chunk) {
							for _, agg := range chunk[len(resps):] {
								noRespCnt.Add(1)
								ls := lastSeen[agg]
								lastSeen[agg] = lastSeenT{sent: ls.sent, tail: ls.tail, rolled: true}
								unresolved[agg] = ver[agg]
								ver[agg]--
								traceAdd(agg, "nil", ver[agg], 0)
								dirty[agg] = true
							}
						}
						answered := 0
						for k, resp := range resps {
							if resp == nil {
								// The only way a record goes unanswered is
								// the window closing mid-backpressure (the
								// attempt loop itself answers or requeues
								// every record); count it as dropped, not
								// failed.
								if ctx.Err() != nil {
									windowDropped.Add(1)
								} else {
									// Fate unknown, not a failure: the tail
									// probe settles it (see `unresolved`).
									noRespCnt.Add(1)
									// the bump's +1 was never confirmed:
									// hand it back, and keep the tail
									// probe armed in case the record
									// DID land (the unknown costs a
									// ReadTails, never a 1001)
									ls := lastSeen[chunk[k]]
									lastSeen[chunk[k]] = lastSeenT{sent: ls.sent, tail: ls.tail, rolled: true}
									unresolved[chunk[k]] = ver[chunk[k]]
									ver[chunk[k]]--
									traceAdd(chunk[k], "nil", ver[chunk[k]], 0)
									dirty[chunk[k]] = true
								}
								continue
							}
							answered++
							switch resp.Status {
							case pushupesv1.AppendResponse_STATUS_SUCCESS:
								delete(unwritten, chunk[k])
								okCnt.Add(1)
								if len(trace[chunk[k]]) < 400 {
									traceAdd(chunk[k], "ok", ver[chunk[k]], resp.CurrentVersion)
								}
							case pushupesv1.AppendResponse_STATUS_EXISTS:
								delete(unwritten, chunk[k])
								existsCnt.Add(1)
								if len(trace[chunk[k]]) < 400 {
									traceAdd(chunk[k], "exists", ver[chunk[k]], resp.CurrentVersion)
								}
							default:
								unwritten[chunk[k]] = ver[chunk[k]]
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
								if resp.ErrId == client.ErrIDNotLeader {
									// 1005 is DOUBLE-MEANING on the wire:
									// a wrong-node refusal (nothing wrote)
									// AND an ack-watermark timeout on the
									// true leader (the record IS in the
									// WAL). No client-side guess distinguishes
									// them — and guessing wrong either
									// re-sends an already-landed version
									// (1001) or skips a slot (lost write).
									// The tail probe answers authoritatively:
									// keep the bump, arm the probe, and let
									// the probe credit the record if it turns
									// out to have landed.
									ambigCnt.Add(1)
									unresolved[chunk[k]] = ver[chunk[k]]
									dirty[chunk[k]] = true
								} else if resp.ErrId != client.ErrIDVersionConflict &&
									resp.CurrentVersion == 0 {
									// a business fail that provably wrote
									// nothing and reports no tail (1002,
									// 1007): the bump was speculative,
									// hand it back so the retry sends the
									// SAME version the server still expects.
									// The arm is not optional: a version
									// that was refused once may have
									// landed on an earlier holder whose
									// tail the next node has not seen —
									// the next bump would then collide
									// at tail+2 while ver already knew
									// better. One ReadTails settles it.
									ver[chunk[k]]--
									dirty[chunk[k]] = true
								}
								if resp.ErrId == client.ErrIDVersionConflict {
									bizSampleMu.Lock()
									if len(bizSamples) < 12 {
										ls := lastSeen[chunk[k]]
										bizSamples = append(bizSamples,
											fmt.Sprintf("1001 agg=%s sent_v=%d server_tail=%d last{sent=%d tail=%d rolled=%v} bumpRound=%d",
												chunk[k], ver[chunk[k]], resp.CurrentVersion,
												ls.sent, ls.tail, ls.rolled, bumpRound[chunk[k]]))
									}
									bizSampleMu.Unlock()
								}
								lastSeen[chunk[k]] = lastSeenT{sent: ver[chunk[k]], tail: resp.CurrentVersion}
								traceAdd(chunk[k], fmt.Sprintf("biz_%d", resp.ErrId), ver[chunk[k]], resp.CurrentVersion)
								if resp.ErrId == client.ErrIDVersionConflict {
									traced[chunk[k]] = true
								}
								if resp.ErrId == client.ErrIDVersionConflict ||
									resp.CurrentVersion > 0 {
									// 1001 answers "the tail is higher than
									// you thought"; 1005 (watermark timeout)
									// answers "the record IS durable above" —
									// either way, adopt the reported version
									// now or the NEXT bump walks into a 1001.
									ver[chunk[k]] = resp.CurrentVersion
								}
							}
						}
						// Throughput counts ANSWERED records only (the
						// window-cut drops are reported separately) and the
						// round-trip sample is recorded only for calls that
						// completed inside the window — a call cut at the
						// deadline has no real latency to report.
						total.Add(int64(answered))
						if ctx.Err() == nil {
							lat := float64(time.Since(t0).Microseconds()) / 1000.0
							latMu.Lock()
							latencies = append(latencies, lat)
							latMu.Unlock()
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
					resp := singleAppend(cl, ctx, req)
					// version self-heal: if the leader reports a newer tail
					// (resume estimate lagged), adopt it and retry once.
					if resp != nil &&
						resp.Status == pushupesv1.AppendResponse_STATUS_FAIL &&
						resp.ErrId == client.ErrIDVersionConflict {
						ver[agg] = resp.CurrentVersion
						req.Version = resp.CurrentVersion + 1
						t0 = time.Now()
						resp = singleAppend(cl, ctx, req)
					}
					if resp == nil {
						// unanswered only ever means the window closed
						// mid-backpressure — dropped, not failed
						if ctx.Err() != nil {
							windowDropped.Add(1)
						} else {
							// Fate unknown: the exit probe below settles it,
							// and the same version is retried next round.
							unresolved[agg] = ver[agg]
							ver[agg]--
						}
						continue
					}
					lat := float64(time.Since(t0).Microseconds()) / 1000.0
					total.Add(1)
					latMu.Lock()
					latencies = append(latencies, lat)
					latMu.Unlock()
					switch resp.Status {
					case pushupesv1.AppendResponse_STATUS_SUCCESS:
						delete(unwritten, agg)
						okCnt.Add(1)
					case pushupesv1.AppendResponse_STATUS_EXISTS:
						delete(unwritten, agg)
						existsCnt.Add(1)
					default:
						unwritten[agg] = ver[agg] // retried; counted only if it never lands
					}
				}
			}
			// (the dump rides a defer — the batch loop exits through a
			// bare `return` when the window closes, and a trailing
			// block after the if/else would never run)
			// dump trace for the aggs that hit 1001: the full event
			// ledger around the +2 gap — round, kind, what ver said,
			// what the server said. Guesswork ends at a printout.
			for agg := range traced {
				bizSampleMu.Lock()
				fmt.Printf("  trace %s:\n", agg)
				for _, ev := range trace[agg] {
					fmt.Printf("    r%d %s v=%d tail=%d\n", ev.round, ev.kind, ev.ver, ev.tail)
				}
				bizSampleMu.Unlock()
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
	if n := noRespCnt.Load() + ambigCnt.Load(); n > 0 {
		// Settled attempts, not record failures: a record whose answer was
		// lost (or whose refusal was ambiguous) is settled by a tail probe —
		// `landed` ones are already inside ok, `retried` ones were proven
		// unwritten and re-sent, `unresolved` ones never got a verdict before
		// the window closed (nobody proved them missing).
		fmt.Printf("settled: no-response=%d ambiguous=%d -> landed=%d retried=%d unresolved=%d\n",
			noRespCnt.Load(), ambigCnt.Load(), settledLanded.Load(), settledRetried.Load(), unresolvedCnt.Load())
	}
	if n := windowDropped.Load(); n > 0 {
		// Records in flight when -duration expired and the cluster was
		// still refusing them: counted neither in attempts/ok/fail nor in
		// the throughput numerator — the window is the window.
		fmt.Printf("window-dropped=%d (records caught by a throttled node's backpressure at the deadline, excluded from all counts)\n", n)
	}
	if *batch > 0 {
		// batch failures need their cause spelled out: unreachable-vs-answer
		// and which wire error dominated (1001 self-heal races vs 1005 HW)
		errIDMu.Lock()
		// These are ATTEMPT-level wire outcomes, not record outcomes: `fail`
		// above counts records that were never written, while a no-response or
		// a mid-flight refusal is settled afterwards (tail probe: landed ⇒ ok,
		// otherwise the retry decides).
		fmt.Printf("batch wire detail (attempts, not records): no-response=%d", noRespCnt.Load())
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
		bizSampleMu.Lock()
		for _, bs := range bizSamples {
			fmt.Printf("  version conflict detail: %s\n", bs)
		}
		bizSampleMu.Unlock()

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
func singleAppend(cl *client.Client, ctx context.Context, req *pushupesv1.AppendRequest) *pushupesv1.AppendResponse {
	// The write-window ctx is passed: an append caught in a throttled
	// node's backpressure at the deadline returns unanswered (nil) instead
	// of draining the node's backlog past -duration. The caller counts nil
	// as a window-drop, never a failure.
	resps, _ := cl.BatchAppend(ctx, []*pushupesv1.AppendRequest{req})
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
