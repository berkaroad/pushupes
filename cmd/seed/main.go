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

// Command seed writes approximately -mib of event records into one slot so
// migration benchmarks have a realistic sealed-segment layout. It is a pure
// client of the data plane: PrefetchRoutes reveals the slot count and the
// target slot's leader, the shared routing algorithm (pkg/client) picks
// aggregates that hash into that slot, and appends follow MOVED/ASK the way
// any client's would.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/berkaroad/pushupes/pkg/client"
	pushupesv1 "github.com/berkaroad/pushupes/pkg/grpcapi/pushupes/v1"
)

// clients keyed by node client addr, shared across the worker goroutines.
var (
	connMu sync.Mutex
	pools  = map[string]pushupesv1.EventServiceClient{}
)

func clientAt(addr string) pushupesv1.EventServiceClient {
	connMu.Lock()
	defer connMu.Unlock()
	if c, ok := pools[addr]; ok {
		return c
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	c := pushupesv1.NewEventServiceClient(conn)
	pools[addr] = c
	return c
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8591", "any node's client-plane (gRPC) addr")
	slot := flag.Int("slot", 0, "target slot id")
	mib := flag.Int("mib", 100, "approx bytes to write, in MiB")
	conns := flag.Int("conns", 8, "parallel aggregates")
	flag.Parse()

	body := make([]byte, 1024)
	for i := range body {
		body[i] = byte('a' + i%26)
	}

	// Routing comes from the cluster, not from client-side constants: the
	// slot count is the length of the dense table, and slot -slot's leader
	// is the address its row points at.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	rt, err := clientAt(hostPort(*addr)).PrefetchRoutes(ctx, &pushupesv1.PrefetchRoutesRequest{})
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "prefetch routes: %v\n", err)
		os.Exit(1)
	}
	slotCount := len(rt.SlotNodeIndex)
	if *slot < 0 || *slot >= slotCount {
		fmt.Fprintf(os.Stderr, "slot %d outside 0..%d (cluster slot count)\n", *slot, slotCount-1)
		os.Exit(1)
	}
	target := hostPort(rt.NodeClientAddrs[rt.SlotNodeIndex[*slot]])
	fmt.Printf("seeding slot %d via %s (cluster: %d slots, %d nodes)\n", *slot, target, slotCount, len(rt.NodeClientAddrs))

	// distinct aggregates that all hash into the slot, by the shared algorithm
	var aggs []string
	for i := 0; len(aggs) < *conns; i++ {
		a := fmt.Sprintf("seed-%d-%d", *slot, i)
		if int(client.SlotOf(a, slotCount)) == *slot {
			aggs = append(aggs, a)
		}
	}

	perAgg64 := int64(*mib) * 1024 * 1024 / int64(len(aggs)) / int64(len(body)+64)
	perAgg := int(perAgg64)
	var seq atomic.Int64
	var fails atomic.Int64
	// the write target can move under a migration; a MOVED/ASK answer is the
	// live routing signal, shared by all workers (the prefetched table stays
	// the startup source, the redirect the in-flight correction).
	var routeMu sync.Mutex
	route := target
	var wg sync.WaitGroup
	t0 := time.Now()
	for _, agg := range aggs {
		wg.Add(1)
		go func(agg string) {
			defer wg.Done()
			for v := uint32(1); v <= uint32(perAgg); v++ {
				n := seq.Add(1)
				req := &pushupesv1.BatchAppendRequest{Records: []*pushupesv1.AppendRequest{{
					AggregateId: agg, Version: v, CommandId: fmt.Sprintf("s-%d", n),
					Events: []*pushupesv1.Event{{Type: "Seed", Body: body}},
				}}}
				lastHop := ""
				for pass := 0; pass < 3; pass++ {
					routeMu.Lock()
					dst := route
					routeMu.Unlock()
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					resp, err := clientAt(dst).BatchAppend(ctx, req)
					cancel()
					if err != nil {
						fails.Add(1)
						if fails.Load() < 5 {
							fmt.Fprintf(os.Stderr, "append fail: %v\n", err)
						}
						break
					}
					r := resp.Results[0].Response
					if r.Status == pushupesv1.AppendResponse_STATUS_FAIL &&
						(r.ErrId == client.ErrIDSlotNotLocal || r.ErrId == client.ErrIDMigrating) &&
						r.Node != "" && r.Node != lastHop {
						lastHop = r.Node
						routeMu.Lock()
						route = hostPort(r.Node)
						routeMu.Unlock()
						continue
					}
					if r.Status == pushupesv1.AppendResponse_STATUS_FAIL {
						fails.Add(1)
						if fails.Load() < 5 {
							fmt.Fprintf(os.Stderr, "append rejected: %+v\n", r)
						}
					}
					break
				}
			}
		}(agg)
	}
	wg.Wait()
	fmt.Printf("seeded %d records in %v (fails=%d, ~%d MiB)\n", seq.Load(), time.Since(t0).Round(time.Millisecond), fails.Load(), seq.Load()*int64(len(body)+64)/(1<<20))
	if fails.Load() > 0 {
		os.Exit(1)
	}
}

func hostPort(addr string) string {
	if i := strings.Index(addr, "://"); i >= 0 {
		addr = addr[i+3:]
	}
	return addr
}
