// Command bench drives sustained writes against a pushupes cluster to
// measure append throughput. Event traffic goes over the gRPC data plane
// only (the HTTP surface is replication + admin): -nodes lists the HTTP
// admin addresses, from which every peer's gRPC address is resolved via
// the slot table. It spreads events across -aggs aggregate IDs (default
// 100, which CRC16 routing scatters over many slots — the run fails its
// own check if fewer than 2 slots end up touched), keeps appending
// consecutive versions per aggregate, and keeps every single event body
// within 1 KiB. It follows MOVED/ASK redirects per slot (they carry the
// leader's gRPC address) and reports msg/sec plus the per-slot durable
// write delta observed on the cluster.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"pushupes/internal/data"
	pushupesv1 "pushupes/internal/grpcapi/pushupes/v1"
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

// anyGrpc is the round-robin entry-point list (all peer gRPC addrs).
var anyGrpc []string

func main() {
	nodes := flag.String("nodes", "127.0.0.1:8091", "comma-separated node admin addrs (client-plane addrs resolved from the slot table)")
	aggs := flag.Int("aggs", 100, "number of aggregate ids")
	dur := flag.Duration("duration", 10*time.Second, "sustained write duration")
	connsFlag := flag.Int("conns", 16, "workers (aggregates are partitioned across them)")
	size := flag.Int("size", 1024, "event body bytes (must be <= 1024)")
	acks := flag.String("acks", "leader", "acks: leader|all|none")
	reportEvery := flag.Duration("report", time.Second, "live report interval")
	flag.Parse()

	if *size > 1024 {
		fmt.Println("size must stay within 1 KiB per event body")
		os.Exit(2)
	}
	adminList := splitAddrs(*nodes)
	if len(adminList) == 0 {
		fmt.Println("no nodes given")
		os.Exit(2)
	}

	// 1) pick aggregate ids, verify slot spread
	aggIDs := make([]string, 0, *aggs)
	slotSeen := map[int32]int{}
	for i := 0; len(aggIDs) < *aggs; i++ {
		id := fmt.Sprintf("bench-%d", i)
		aggIDs = append(aggIDs, id)
		slotSeen[data.SlotOf(id, data.DefaultSlotCount)]++
	}
	if len(slotSeen) < 2 {
		fmt.Printf("FAIL: %d aggregates landed on %d slot(s), need >= 2\n", len(aggIDs), len(slotSeen))
		os.Exit(1)
	}
	fmt.Printf("aggregates=%d across %d slots (body=%dB acks=%s duration=%s)\n",
		len(aggIDs), len(slotSeen), *size, *acks, *dur)

	body := makeBody(*size)
	hc := &http.Client{Timeout: 5 * time.Second}

	// Resolve the data plane from one node's admin status: every peer's
	// gRPC address plus the slot -> leader mapping.
	grpcByID := map[string]string{} // node id -> grpc addr
	leaderOf := map[int32]string{}  // slot -> leader grpc addr
	if st, err := fetchStatus(hc, adminList[0]); err == nil {
		for _, p := range st.Peers {
			if p.ClientAddr != "" {
				grpcByID[p.ID] = p.ClientAddr
			}
		}
		for slotStr, p := range st.Slots {
			if a, ok := grpcByID[p.Leader]; ok {
				s, err := strconv.Atoi(slotStr)
				if err == nil {
					leaderOf[int32(s)] = a
				}
			}
		}
	}
	if len(grpcByID) == 0 {
		fmt.Println("FAIL: no client_addr in cluster status (nodes started without -client?)")
		os.Exit(1)
	}
	for _, a := range grpcByID {
		anyGrpc = append(anyGrpc, a)
	}
	sort.Strings(anyGrpc)

	// resume per-aggregate versions against the slot LEADER (replicas only
	// show <=HW data; under-estimating the tail burns retries on 1001).
	lastVers := make([]uint32, len(aggIDs))
	{
		var bm sync.WaitGroup
		sem := make(chan struct{}, 16)
		for i := range aggIDs {
			bm.Add(1)
			go func(i int) {
				defer bm.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				slot := data.SlotOf(aggIDs[i], data.DefaultSlotCount)
				addr, ok := leaderOf[slot]
				if !ok {
					addr = anyGrpc[0] // single-node bootstrap etc.
				}
				lastVers[i] = tailVersion(addr, aggIDs[i])
			}(i)
		}
		bm.Wait()
	}
	var resumeSum uint64
	for _, v := range lastVers {
		resumeSum += uint64(v)
	}
	fmt.Printf("resuming: %d existing records across aggregates\n", resumeSum)

	// 2) per-node write counters before the run (admin plane, HTTP)
	before := map[string][]uint64{}
	for _, n := range adminList {
		before[n] = fetchWrites(hc, n)
	}

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

	var total, okCnt, existsCnt, failCnt, redirectCnt atomic.Int64
	var latMu sync.Mutex
	var latencies []float64 // ms

	// routing: slot -> leader gRPC addr (seeded from the table, refined by
	// MOVED/ASK responses)
	var routeMu sync.RWMutex
	routes := map[int32]string{}
	for s, a := range leaderOf {
		routes[s] = a
	}

	nw := *connsFlag
	if nw > len(aggIDs) {
		nw = len(aggIDs)
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
			for ctx.Err() == nil {
				for _, agg := range targets {
					if ctx.Err() != nil {
						return
					}
					seq++
					ver[agg]++
					cmd := fmt.Sprintf("b%d-%d-%d", w, seq, rng.Int63n(1<<40))
					req := &pushupesv1.AppendRequest{
						AggregateId: agg, Version: ver[agg], CommandId: cmd, Acks: *acks,
						Events: []*pushupesv1.Event{{Type: "BenchAppend", Body: body}},
					}
					t0 := time.Now()
					resp, err := appendWithRetry(w, agg, req, routes, &routeMu)
					// version self-heal: if the leader reports a newer tail
					// (resume estimate lagged), adopt it and retry once.
					if err == nil && resp != nil &&
						resp.Status == pushupesv1.AppendResponse_STATUS_FAIL &&
						resp.ErrId == data.ErrIDVersionConflict {
						ver[agg] = resp.CurrentVersion
						req.Version = resp.CurrentVersion + 1
						t0 = time.Now()
						resp, err = appendWithRetry(w, agg, req, routes, &routeMu)
					}
					lat := float64(time.Since(t0).Microseconds()) / 1000.0
					total.Add(1)
					if err != nil {
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
					if resp.ErrId == data.ErrIDSlotNotLocal || resp.ErrId == data.ErrIDMigrating {
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
	fmt.Printf("\n== bench done in %s ==\n", elapsed.Round(time.Millisecond))
	fmt.Printf("attempts=%d ok=%d exists=%d fail=%d redirects=%d\n",
		total.Load(), okCnt.Load(), existsCnt.Load(), failCnt.Load(), redirectCnt.Load())
	fmt.Printf("throughput=%.0f msg/s  latency p50=%s p90=%s p99=%s\n",
		float64(total.Load())/elapsed.Seconds(), pct(0.5), pct(0.9), pct(0.99))

	// 5) per-slot durable delta per node (admin plane, HTTP)
	afterMap := map[string][]uint64{}
	for _, n := range adminList {
		afterMap[n] = fetchWrites(hc, n)
	}
	fmt.Println("\nper-slot durable writes (delta over run):")
	hdr := "slot"
	for _, n := range adminList {
		hdr += fmt.Sprintf("  %12s", n)
	}
	fmt.Println(hdr)
	grand := map[int32]uint64{}
	for _, n := range adminList {
		beforeN, afterN := before[n], afterMap[n]
		for s := range afterN {
			if s < len(beforeN) {
				if d := afterN[s] - beforeN[s]; d > 0 {
					grand[int32(s)] += d
				}
			}
		}
	}
	slots := make([]int, 0, len(grand))
	for s := range grand {
		slots = append(slots, int(s))
	}
	sort.Ints(slots)
	for _, s := range slots {
		line := fmt.Sprintf("%4d", s)
		for _, n := range adminList {
			var d uint64
			if s < len(before[n]) && s < len(afterMap[n]) {
				d = afterMap[n][s] - before[n][s]
			}
			line += fmt.Sprintf("  %12d", d)
		}
		line += fmt.Sprintf("   rate=%.0f msg/s", float64(grand[int32(s)])/elapsed.Seconds())
		fmt.Println(line)
	}
	var sum uint64
	for _, v := range grand {
		sum += v
	}
	fmt.Printf("touched slots=%d total durable=%d (%.0f msg/s)\n",
		len(grand), sum, float64(sum)/elapsed.Seconds())
	if len(grand) < 2 {
		fmt.Println("FAIL: writes landed on fewer than 2 slots")
		os.Exit(1)
	}
	if failCnt.Load() > 0 {
		fmt.Println("NOTE: some appends failed; check cluster health")
		os.Exit(1)
	}
}

// appendWithRetry posts one append over gRPC, following MOVED/ASK at most
// twice; redirect responses carry the leader's gRPC address and get cached
// per slot.
func appendWithRetry(w int, agg string, req *pushupesv1.AppendRequest,
	routes map[int32]string, routeMu *sync.RWMutex) (*pushupesv1.AppendResponse, error) {
	slot := data.SlotOf(agg, data.DefaultSlotCount)
	routeMu.RLock()
	addr, ok := routes[slot]
	routeMu.RUnlock()
	if !ok {
		addr = anyGrpc[w%len(anyGrpc)]
	}

	for attempt := 0; attempt < 3; attempt++ {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := eventClient(addr).Append(cctx, req)
		cancel()
		if err != nil {
			// node may be gone: fall back to round-robin next attempt
			addr = anyGrpc[(w+attempt)%len(anyGrpc)]
			continue
		}
		switch resp.ErrId {
		case data.ErrIDSlotNotLocal, data.ErrIDMigrating:
			if resp.Node != "" {
				routeMu.Lock()
				routes[slot] = resp.Node
				routeMu.Unlock()
				addr = resp.Node
				continue
			}
		}
		return resp, nil
	}
	return nil, fmt.Errorf("append unreachable")
}

// probeVersion reports whether version k exists for one aggregate (versions
// are contiguous from 1, so existence of k means tail >= k).
func probeVersion(addr, agg string, k uint32) bool {
	cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := eventClient(addr).ReadStream(cctx, &pushupesv1.ReadStreamRequest{
		AggregateId: agg, FromVersion: k, Limit: 1,
	})
	if err != nil {
		return false
	}
	return len(out.Records) > 0
}

// tailVersion finds the highest existing version of one aggregate from the
// slot leader with exponential probe + binary search — O(log tail) one-row
// reads, no full-stream fetch.
func tailVersion(addr, agg string) uint32 {
	if !probeVersion(addr, agg, 1) {
		return 0
	}
	hi := uint32(1)
	for probeVersion(addr, agg, hi) {
		hi *= 2
	}
	lo := hi / 2
	for lo+1 < hi {
		mid := (lo + hi) / 2
		if probeVersion(addr, agg, mid) {
			lo = mid
		} else {
			hi = mid
		}
	}
	return lo
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

// fetchWrites pulls the per-slot durable write counters of one node
// (lightweight admin endpoint).
func fetchWrites(hc *http.Client, addr string) []uint64 {
	httpResp, err := hc.Get(urlOr(addr, "/admin/writes"))
	if err != nil {
		fmt.Printf("warn: writes fetch %s: %v\n", addr, err)
		return nil
	}
	defer httpResp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(httpResp.Body, 8<<20))
	var st struct {
		Writes []uint64 `json:"writes"`
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		fmt.Printf("warn: writes decode %s: %v\n", addr, err)
		return nil
	}
	return st.Writes
}

// fetchStatus pulls one node's admin status (peers + slot table).
func fetchStatus(hc *http.Client, addr string) (*statusLite, error) {
	httpResp, err := hc.Get(urlOr(addr, "/admin/cluster/status"))
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(httpResp.Body, 8<<20))
	var st statusLite
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

type statusLite struct {
	Node  string `json:"node"`
	Peers map[string]struct {
		ID         string `json:"id"`
		AdminAddr  string `json:"admin_addr"`
		ClientAddr string `json:"client_addr"`
	} `json:"peers"`
	Slots map[string]struct {
		Leader string `json:"leader"`
	} `json:"slots"`
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

// urlOr builds an HTTP URL from a possibly-schemeless admin address.
func urlOr(addr, path string) string {
	if strings.Contains(addr, "://") {
		return addr + path
	}
	return "http://" + addr + path
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
