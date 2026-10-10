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

package main

// Smoke: BatchAppend against the live cluster through the reusable client
// (pkg/client) — routing, per-leader grouping and redirect self-heal are
// the client's job here; the smoke asserts what the client hands back:
// success, exists (replayed command), version conflict, duplicate-
// aggregate rejection, result alignment, and the stored-record readback.
// Protocol-level checks on a specific node (the raw empty-batch answer)
// are grpccheck's job — the library exposes no raw-stub escape hatch.
import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/berkaroad/pushupes/pkg/client"
	pushupesv1 "github.com/berkaroad/pushupes/pkg/grpcapi/pushupes/v1"
)

func req(agg string, ver uint32, cmd string) *pushupesv1.AppendRequest {
	return &pushupesv1.AppendRequest{AggregateId: agg, Version: ver, CommandId: cmd,
		Events: []*pushupesv1.Event{{Type: "Smoke", Body: []byte(`{"k":1}`)}}}
}

func main() {
	nodes := flag.String("nodes", "127.0.0.1:8591", "comma-separated node client-plane (gRPC) addrs")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	fail := func(format string, a ...any) {
		fmt.Printf("FAIL: "+format+"\n", a...)
		os.Exit(1)
	}

	var entries []string
	for _, n := range strings.Split(*nodes, ",") {
		if n = strings.TrimSpace(n); n != "" {
			entries = append(entries, n)
		}
	}
	cl, err := client.New(entries, &client.Config{RouteRefresh: time.Hour})
	if err != nil {
		fail("client: %v", err)
	}
	defer cl.Close()
	fmt.Printf("routing: %d slots (PrefetchRoutes via %d entry addrs)\n", cl.SlotCount(), len(entries))

	// (a) 50 fresh aggregates in one client call: the client groups the
	// batch per cached slot leader and drains redirects itself; every
	// record must answer success (fresh command ids — exists would be a
	// rule bug, nil an unanswered record).
	batch := make([]*pushupesv1.AppendRequest, 0, 50)
	for i := 0; i < 50; i++ {
		batch = append(batch, req(fmt.Sprintf("smoke-agg-%d", i), 1, fmt.Sprintf("smoke-cmd-%d", i)))
	}
	respa, err := cl.BatchAppend(ctx, batch)
	if err != nil {
		fail("pass A: %v", err)
	}
	succ := 0
	for i, r := range respa {
		if r == nil {
			fail("pass A: record %d unanswered: %+v", i, batch[i])
		}
		if r.Status == pushupesv1.AppendResponse_STATUS_EXISTS {
			fail("pass A: fresh command answered exists: %+v", r)
		}
		if r.Status != pushupesv1.AppendResponse_STATUS_SUCCESS {
			fail("pass A: unexpected fail on fresh record: %+v", r)
		}
		succ++
	}
	fmt.Printf("pass A: %d records landed (redirects so far=%d)\n", succ, cl.Redirects())

	// (b) exists + version conflict + duplicate pair + plain success, one
	// client call. Business answers pass through untouched, aligned with
	// the request positions.
	b := []*pushupesv1.AppendRequest{
		req("smoke-agg-0", 1, "smoke-cmd-0"),  // replay -> exists
		req("smoke-agg-1", 9, "smoke-cmd-x"),  // version conflict 1001
		req("smoke-agg-2", 2, "smoke-cmd-d1"), // duplicate aggregate...
		req("smoke-agg-2", 3, "smoke-cmd-d2"), // ...rejected pair
		req("smoke-agg-3", 2, "smoke-cmd-ok"), // plain success v2
	}
	resp, err := cl.BatchAppend(ctx, b)
	if err != nil {
		fail("pass B: %v", err)
	}
	byAgg := map[string]*pushupesv1.AppendResponse{}
	for i, r := range resp {
		if r == nil {
			fail("pass B: record %d unanswered", i)
		}
		byAgg[b[i].AggregateId] = r
	}
	check := func(agg string, want pushupesv1.AppendResponse_Status, errID uint32) *pushupesv1.AppendResponse {
		r := byAgg[agg]
		if r == nil {
			fail("missing result for %s", agg)
		}
		if r.Status != want || r.ErrId != errID {
			fail("%s: got %+v want status=%v err=%d", agg, r, want, errID)
		}
		return r
	}
	e := check("smoke-agg-0", pushupesv1.AppendResponse_STATUS_EXISTS, 0)
	if e.Record == nil || e.Record.CommandId != "smoke-cmd-0" {
		fail("exists must echo stored record: %+v", e)
	}
	c1 := check("smoke-agg-1", pushupesv1.AppendResponse_STATUS_FAIL, client.ErrIDVersionConflict)
	if c1.CurrentVersion != 1 {
		fail("conflict current_version: %d", c1.CurrentVersion)
	}
	// The duplicate pair: BOTH rejected (the map keeps the last answer for
	// agg-2; both carry the same 1002 — pass D proves neither executed).
	check("smoke-agg-2", pushupesv1.AppendResponse_STATUS_FAIL, client.ErrIDBadRequest)
	check("smoke-agg-3", pushupesv1.AppendResponse_STATUS_SUCCESS, 0)
	fmt.Println("pass B: exists/1001/dup-reject/success in one batch, all aligned")

	// (c) rejected dup never executed: agg-2 holds only its v1. One
	// ReadStream through the client — wherever agg-2's leader sits, the
	// read arrives (the cluster proxies reads a node does not hold).
	rr, err := cl.ReadStream(ctx, "smoke-agg-2", 1, 0)
	if err != nil {
		fail("read agg-2: %v", err)
	}
	if len(rr.Records) != 1 {
		fail("rejected dup must not write: got %+v", rr)
	}
	fmt.Println("pass C: rejected duplicate records never executed")
	fmt.Println("SMOKE OK")
}
