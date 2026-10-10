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
// client of the data plane, on the reusable pkg/client library: New seeds
// the routing table (its answer length is the cluster's slot count) and
// keeps it fresh, the shared SlotOf picks aggregates that hash into the
// target slot, and BatchAppend keeps writes at that slot's leader across
// refreshes and MOVED/ASK self-heal — long runs cross migrations, and the
// client tracks the leader either way.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/berkaroad/pushupes/pkg/client"
	pushupesv1 "github.com/berkaroad/pushupes/pkg/grpcapi/pushupes/v1"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8591", "any node's client-plane (gRPC) addr (entry point)")
	slot := flag.Int("slot", 0, "target slot id")
	mib := flag.Int("mib", 100, "approx bytes to write, in MiB")
	conns := flag.Int("conns", 8, "parallel aggregates")
	flag.Parse()

	body := make([]byte, 1024)
	for i := range body {
		body[i] = byte('a' + i%26)
	}

	cl, err := client.New([]string{*addr}, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "seed: %v\n", err)
		os.Exit(1)
	}
	defer cl.Close()
	slotCount := cl.SlotCount()
	if *slot < 0 || *slot >= slotCount {
		fmt.Fprintf(os.Stderr, "slot %d outside 0..%d (cluster slot count)\n", *slot, slotCount-1)
		os.Exit(1)
	}
	fmt.Printf("seeding slot %d (cluster: %d slots, entry %s)\n", *slot, slotCount, *addr)

	// distinct aggregates that all hash into the slot, by the shared algorithm
	var aggs []string
	for i := 0; len(aggs) < *conns; i++ {
		a := fmt.Sprintf("seed-%d-%d", *slot, i)
		if int(client.SlotOf(a, slotCount)) == *slot {
			aggs = append(aggs, a)
		}
	}

	perAgg := int(int64(*mib) * 1024 * 1024 / int64(len(aggs)) / int64(len(body)+64))
	var seq atomic.Int64
	var fails atomic.Int64
	var wg sync.WaitGroup
	t0 := time.Now()
	for _, agg := range aggs {
		wg.Add(1)
		go func(agg string) {
			defer wg.Done()
			for v := uint32(1); v <= uint32(perAgg); v++ {
				n := seq.Add(1)
				rec := &pushupesv1.AppendRequest{
					AggregateId: agg, Version: v, CommandId: fmt.Sprintf("s-%d", n),
					Events: []*pushupesv1.Event{{Type: "Seed", Body: body}},
				}
				resps, err := cl.BatchAppend(context.Background(), []*pushupesv1.AppendRequest{rec})
				r := resps[0]
				if r != nil && r.Status != pushupesv1.AppendResponse_STATUS_FAIL {
					continue // success (or exists: a re-seeded command id)
				}
				fails.Add(1)
				if fails.Load() < 5 {
					if r == nil {
						fmt.Fprintf(os.Stderr, "append unanswered (last error: %v)\n", err)
					} else {
						fmt.Fprintf(os.Stderr, "append rejected: %+v\n", r)
					}
				}
				// Versions must stay consecutive: stopping this stream at
				// the first hole beats skipping ahead (the old loop kept
				// writing v+1 after a failure and left the gap behind).
				return
			}
		}(agg)
	}
	wg.Wait()
	fmt.Printf("seeded %d records in %v (fails=%d, redirects=%d, ~%d MiB)\n",
		seq.Load(), time.Since(t0).Round(time.Millisecond), fails.Load(),
		cl.Redirects(), seq.Load()*int64(len(body)+64)/(1<<20))
	if fails.Load() > 0 {
		os.Exit(1)
	}
}
