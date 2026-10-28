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

// client-plane (gRPC) smoke client: append + read against a running cluster.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/berkaroad/pushupes/pkg/client"
	pushupesv1 "github.com/berkaroad/pushupes/pkg/grpcapi/pushupes/v1"
)

var clientAddrs = flag.String("addrs", "127.0.0.1:9991,127.0.0.1:9992,127.0.0.1:9993", "comma list of node client-plane (gRPC) addrs")

func cli(addr string) pushupesv1.EventServiceClient {
	if i := strings.Index(addr, "://"); i >= 0 {
		addr = addr[i+3:] // scheme is notation; gRPC dials host:port
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	return pushupesv1.NewEventServiceClient(conn)
}

// appendOne writes one record over BatchAppend — the client plane's single
// write entry (a single write is a batch of one record).
func appendOne(ctx context.Context, addr string, req *pushupesv1.AppendRequest) (*pushupesv1.AppendResponse, error) {
	resp, err := cli(addr).BatchAppend(ctx, &pushupesv1.BatchAppendRequest{Records: []*pushupesv1.AppendRequest{req}})
	if err != nil {
		return nil, err
	}
	return resp.Results[0].Response, nil
}

// readRedirected is the in-band redirect surface every read response carries.
type readRedirected interface {
	GetErrId() uint32
	GetNode() string
}

// followRead issues one raw read from `from` and follows the in-band MOVED
// chain (what pkg/client automates per call): a node that serves the slot
// answers with err_id 0, every other node names the leader and the probe
// retries there. grpccheck keeps this loop in raw stubs on purpose — it is
// asserting the WIRE shape, not the library.
func followRead[T readRedirected](ctx context.Context, from string, call func(addr string) (T, error)) (T, int) {
	var zero T
	addr := from
	for hop := 0; hop <= 4; hop++ {
		resp, err := call(addr)
		check(err)
		if resp.GetErrId() == 0 {
			return resp, hop
		}
		if resp.GetErrId() != 1003 || resp.GetNode() == "" {
			fatal("read@%s: err_id=%d node=%q, want a 1003 carrying the leader node",
				addr, resp.GetErrId(), resp.GetNode())
		}
		addr = resp.GetNode()
	}
	fatal("read from %s: redirect loop never answered", from)
	return zero, 0
}

func main() {
	flag.Parse()
	addrs := splitList(*clientAddrs)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	agg := fmt.Sprintf("grpc-smoke-%d", time.Now().UnixNano())

	// 1) write v1 + v2 following MOVED to the leader
	target := addrs[0]
	var v2OK bool
	for pass := 0; pass < 3 && !v2OK; pass++ {
		r, err := appendOne(ctx, target, &pushupesv1.AppendRequest{
			AggregateId: agg, Version: 1, CommandId: "s-1",
			Events: []*pushupesv1.Event{{Type: "Hello", Body: []byte(`{"g":"世界"}`)}},
		})
		check(err)
		switch r.Status {
		case pushupesv1.AppendResponse_STATUS_SUCCESS, pushupesv1.AppendResponse_STATUS_EXISTS:
		case pushupesv1.AppendResponse_STATUS_FAIL:
			if r.ErrId == 1003 {
				target = r.Node // redirects carry the leader's gRPC address
				continue
			}
			fatal("append v1: %v", r)
		}
		r2, err := appendOne(ctx, target, &pushupesv1.AppendRequest{
			AggregateId: agg, Version: 2, CommandId: "s-2",
			Events: []*pushupesv1.Event{{Type: "Bin", Body: []byte{0, 1, 0xff, 'A'}}},
		})
		check(err)
		switch r2.Status {
		case pushupesv1.AppendResponse_STATUS_SUCCESS, pushupesv1.AppendResponse_STATUS_EXISTS:
			v2OK = true
		case pushupesv1.AppendResponse_STATUS_FAIL:
			if r2.ErrId == 1003 {
				target = r2.Node
				continue
			}
			fatal("append v2: %v", r2)
		}
		break
	}
	if !v2OK {
		fatal("could not land v2 on leader")
	}
	fmt.Printf("wrote v1(seq via leader %s) + v2 (ISR-replicated), agg=%s\n", target, agg)

	// 2) 1001 self-heal
	r3, _ := appendOne(ctx, target, &pushupesv1.AppendRequest{
		AggregateId: agg, Version: 9, CommandId: "s-3",
		Events: []*pushupesv1.Event{{Type: "X", Body: []byte("{}")}},
	})
	if r3.Status != pushupesv1.AppendResponse_STATUS_FAIL || r3.ErrId != 1001 || r3.CurrentVersion != 2 {
		fatal("version-skip resp wrong: %+v", r3)
	}
	fmt.Printf("version skip -> FAIL 1001 current_version=2 ok\n")

	// 3) wait for replication, then read on every node: the leader and
	// proven-sync replicas answer locally, every other node MOVEDs — the
	// probe follows the chain and asserts the eventual answer plus that
	// the redirect itself is the wire shape (err_id 1003 + leader node).
	time.Sleep(3 * time.Second)
	for _, a := range addrs {
		rr, hops := followRead(ctx, a, func(addr string) (*pushupesv1.ReadStreamResponse, error) {
			return cli(addr).ReadStream(ctx, &pushupesv1.ReadStreamRequest{AggregateId: agg})
		})
		if len(rr.Records) != 2 || rr.LastVersion != 2 {
			fatal("read@%s: %d records last=%d", a, len(rr.Records), rr.LastVersion)
		}
		if rr.Records[1].Events[0].Body[2] != 0xff || string(rr.Records[0].Events[0].Body) != `{"g":"世界"}` {
			fatal("read@%s: body bytes mangled", a)
		}
		fmt.Printf("read@%s: 2 records (answered after %d redirect(s)), binary body verbatim ok\n", a, hops)
	}

	// 4) idempotent replay -> EXISTS
	r4, _ := appendOne(ctx, target, &pushupesv1.AppendRequest{
		AggregateId: agg, Version: 2, CommandId: "s-2",
		Events: []*pushupesv1.Event{{Type: "Bin", Body: []byte{0, 1, 0xff, 'A'}}},
	})
	if r4.Status != pushupesv1.AppendResponse_STATUS_EXISTS || r4.Record == nil {
		fatal("dup append: %+v", r4)
	}
	fmt.Printf("idempotent replay -> EXISTS with stored record ok\n")

	// 5) by-command (followed from the first entry, like every read)
	bc, _ := followRead(ctx, addrs[0], func(addr string) (*pushupesv1.ReadByCommandResponse, error) {
		return cli(addr).ReadByCommand(ctx, &pushupesv1.ReadByCommandRequest{AggregateId: agg, CommandId: "s-1"})
	})
	if !bc.Found || bc.Record.Version != 1 || bc.Record.Seq == 0 {
		fatal("by-command: %+v", bc)
	}
	fmt.Printf("by-command found v1 (seq=%d) ok\n", bc.Record.Seq)

	// 6) point-in-time version anchor (ReadVersionByTime, on every node so
	// the redirect chain is covered too): v1/v2 carry explicit stamps, so
	// the anchor for "before v2's time" is exactly 1, and past-the-clock is
	// the full tail 2. The time stream hashes to its own slot, so its
	// appends follow MOVED to that slot's leader like step 1 does.
	const t1, t2 = int64(1600000001), int64(1600000002)
	tagg := agg + "-t"
	tTarget := target
	for pass := 0; pass < 3; pass++ {
		okBoth := true
		r, err := appendOne(ctx, tTarget, &pushupesv1.AppendRequest{
			AggregateId: tagg, Version: 1, UnixTime: t1, CommandId: "t-1",
			Events: []*pushupesv1.Event{{Type: "T", Body: []byte("{}")}},
		})
		check(err)
		if r.Status == pushupesv1.AppendResponse_STATUS_FAIL && r.ErrId == 1003 {
			tTarget, okBoth = r.Node, false
		} else if r.Status != pushupesv1.AppendResponse_STATUS_SUCCESS && r.Status != pushupesv1.AppendResponse_STATUS_EXISTS {
			fatal("append time v1: %+v", r)
		}
		if okBoth {
			r2, err := appendOne(ctx, tTarget, &pushupesv1.AppendRequest{
				AggregateId: tagg, Version: 2, UnixTime: t2, CommandId: "t-2",
				Events: []*pushupesv1.Event{{Type: "T", Body: []byte("{}")}},
			})
			check(err)
			if r2.Status == pushupesv1.AppendResponse_STATUS_FAIL && r2.ErrId == 1003 {
				tTarget, okBoth = r2.Node, false
			} else if r2.Status != pushupesv1.AppendResponse_STATUS_SUCCESS && r2.Status != pushupesv1.AppendResponse_STATUS_EXISTS {
				fatal("append time v2: %+v", r2)
			}
		}
		if okBoth {
			break
		}
	}
	time.Sleep(2 * time.Second) // let the replica see the time stream
	for _, a := range addrs {
		vb, _ := followRead(ctx, a, func(addr string) (*pushupesv1.ReadVersionByTimeResponse, error) {
			return cli(addr).ReadVersionByTime(ctx, &pushupesv1.ReadVersionByTimeRequest{
				AggregateId: tagg, UnixTime: t1, // at-or-before v1's stamp -> exactly 1
			})
		})
		if vb.Version != 1 {
			fatal("version-by-time@%s at=%d: want 1 got %d", a, t1, vb.Version)
		}
		vb2, _ := followRead(ctx, a, func(addr string) (*pushupesv1.ReadVersionByTimeResponse, error) {
			return cli(addr).ReadVersionByTime(ctx, &pushupesv1.ReadVersionByTimeRequest{
				AggregateId: tagg, UnixTime: t2,
			})
		})
		if vb2.Version != 2 {
			fatal("version-by-time@%s at=%d: want 2 got %d", a, t2, vb2.Version)
		}
		vb0, _ := followRead(ctx, a, func(addr string) (*pushupesv1.ReadVersionByTimeResponse, error) {
			return cli(addr).ReadVersionByTime(ctx, &pushupesv1.ReadVersionByTimeRequest{
				AggregateId: tagg, UnixTime: t1 - 1, // before the first stamp -> 0
			})
		})
		if vb0.Version != 0 {
			fatal("version-by-time@%s at=%d: want 0 got %d", a, t1-1, vb0.Version)
		}
		fmt.Printf("version-by-time@%s: at=%d -> 1, at=%d -> 2, before=%d -> 0 ok\n", a, t1, t2, t1-1)
	}
	// 7) PrefetchRoutes: the routing table is cluster state every node can
	// answer (Raft-replicated), dense over slots (its length IS the slot
	// count a client applies to client.SlotOf), every row pointing at a
	// known node addr — and all nodes answer the same table. Then the
	// smoke's own streams must be routable straight from it: a fresh
	// command lands through the table's row for its slot, so a healthy
	// cluster never needs a redirect hop to find the leader.
	rt, err := cli(addrs[0]).PrefetchRoutes(ctx, &pushupesv1.PrefetchRoutesRequest{})
	check(err)
	if len(rt.NodeClientAddrs) == 0 || len(rt.SlotNodeIndex) == 0 {
		fatal("prefetch: empty table (addrs=%d slots=%d)", len(rt.NodeClientAddrs), len(rt.SlotNodeIndex))
	}
	for _, idx := range rt.SlotNodeIndex {
		if idx < 0 || int(idx) >= len(rt.NodeClientAddrs) {
			fatal("prefetch: row index %d outside %d addrs", idx, len(rt.NodeClientAddrs))
		}
	}
	fmt.Printf("prefetch: slots=%d group_gen=%d base=%d size=%d epoch=%d\n",
		len(rt.SlotNodeIndex), rt.GetGroupGen(), rt.GetGroupBase(), rt.GetGroupSize(), rt.GetLayoutEpoch())
	for _, a := range addrs {
		r2, err := cli(a).PrefetchRoutes(ctx, &pushupesv1.PrefetchRoutesRequest{})
		check(err)
		if len(r2.SlotNodeIndex) != len(rt.SlotNodeIndex) {
			fatal("prefetch@%s: slot count %d != %d", a, len(r2.SlotNodeIndex), len(rt.SlotNodeIndex))
		}
		// slot_node_index is dense over LOGICAL slots, so every row belongs to the
		// served generation: compare all of them.
		for slot := 0; slot < len(rt.SlotNodeIndex); slot++ {
			idx := rt.SlotNodeIndex[slot]
			if r2.NodeClientAddrs[r2.SlotNodeIndex[slot]] != rt.NodeClientAddrs[idx] {
				fatal("prefetch@%s: slot %d routes to %s, first answer said %s",
					a, slot, r2.NodeClientAddrs[r2.SlotNodeIndex[slot]], rt.NodeClientAddrs[idx])
			}
		}
	}
	slotCount := len(rt.SlotNodeIndex)
	ragg := fmt.Sprintf("%s-r", agg)
	// Route the way the cluster does: onto a LOGICAL slot of the served
	// generation, which is the same id space the table is indexed by.
	routed := client.LogicalSlotOf(ragg, slotCount)
	row := rt.SlotNodeIndex[routed]
	rr, err := cli(rt.NodeClientAddrs[row]).BatchAppend(ctx, &pushupesv1.BatchAppendRequest{
		Records: []*pushupesv1.AppendRequest{{
			AggregateId: ragg, Version: 1, CommandId: "rt-1",
			Events: []*pushupesv1.Event{{Type: "RT", Body: []byte("{}")}},
		}},
	})
	check(err)
	rres := rr.Results[0].Response
	if rres.Status != pushupesv1.AppendResponse_STATUS_SUCCESS &&
		!(rres.Status == pushupesv1.AppendResponse_STATUS_FAIL &&
			(rres.ErrId == client.ErrIDSlotNotLocal || rres.ErrId == client.ErrIDMigrating)) {
		fatal("routed append: %+v", rres)
	}
	if rres.Status == pushupesv1.AppendResponse_STATUS_SUCCESS {
		fmt.Printf("prefetch-routes: %d slots / %d addrs, table consistent across %d nodes, routed write hit leader directly\n",
			slotCount, len(rt.NodeClientAddrs), len(addrs))
	} else {
		// the row moved (migration / unannounced leader): follow once, the
		// hint-plus-self-heal contract in action.
		fr, err := cli(rres.Node).BatchAppend(ctx, &pushupesv1.BatchAppendRequest{
			Records: []*pushupesv1.AppendRequest{{
				AggregateId: ragg, Version: 1, CommandId: "rt-1",
				Events: []*pushupesv1.Event{{Type: "RT", Body: []byte("{}")}},
			}},
		})
		check(err)
		if fr.Results[0].Response.Status != pushupesv1.AppendResponse_STATUS_SUCCESS {
			fatal("routed append after redirect: %+v", fr.Results[0].Response)
		}
		fmt.Printf("prefetch-routes: %d slots / %d addrs, table consistent across %d nodes, routed write followed 1 redirect\n",
			slotCount, len(rt.NodeClientAddrs), len(addrs))
	}
	// 8) empty batch: the protocol answer, RAW (grpccheck dials stubs
	// directly by design — the routed client short-circuits an empty batch
	// locally, so only a raw call can prove what a NODE answers).
	eb, err := cli(addrs[0]).BatchAppend(ctx, &pushupesv1.BatchAppendRequest{})
	check(err)
	if len(eb.Results) != 0 {
		fatal("empty batch on %s: %d results, want 0", addrs[0], len(eb.Results))
	}
	fmt.Println("empty batch answers empty results (raw protocol) ok")

	fmt.Println("GRPC SMOKE PASS")
}

func splitList(s string) []string {
	out := []string{}
	cur := ""
	for _, c := range s {
		if c == ',' {
			out = append(out, cur)
			cur = ""
		} else {
			cur += string(c)
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func check(err error) {
	if err != nil {
		fatal("%v", err)
	}
}

func fatal(f string, a ...any) {
	fmt.Printf("FATAL "+f+"\n", a...)
	os.Exit(1)
}
