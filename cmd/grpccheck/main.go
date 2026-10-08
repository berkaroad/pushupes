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

	pushupesv1 "github.com/berkaroad/pushupes/internal/grpcapi/pushupes/v1"
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
		r, err := cli(target).Append(ctx, &pushupesv1.AppendRequest{
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
		r2, err := cli(target).Append(ctx, &pushupesv1.AppendRequest{
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
	r3, _ := cli(target).Append(ctx, &pushupesv1.AppendRequest{
		AggregateId: agg, Version: 9, CommandId: "s-3",
		Events: []*pushupesv1.Event{{Type: "X", Body: []byte("{}")}},
	})
	if r3.Status != pushupesv1.AppendResponse_STATUS_FAIL || r3.ErrId != 1001 || r3.CurrentVersion != 2 {
		fatal("version-skip resp wrong: %+v", r3)
	}
	fmt.Printf("version skip -> FAIL 1001 current_version=2 ok\n")

	// 3) wait for replication, then read on every node (leader, replica, proxy)
	time.Sleep(3 * time.Second)
	for _, a := range addrs {
		rr, err := cli(a).ReadStream(ctx, &pushupesv1.ReadStreamRequest{AggregateId: agg})
		check(err)
		if len(rr.Records) != 2 || rr.LastVersion != 2 {
			fatal("read@%s: %d records last=%d", a, len(rr.Records), rr.LastVersion)
		}
		if rr.Records[1].Events[0].Body[2] != 0xff || string(rr.Records[0].Events[0].Body) != `{"g":"世界"}` {
			fatal("read@%s: body bytes mangled", a)
		}
		fmt.Printf("read@%s: 2 records, binary body verbatim ok\n", a)
	}

	// 4) idempotent replay -> EXISTS
	r4, _ := cli(target).Append(ctx, &pushupesv1.AppendRequest{
		AggregateId: agg, Version: 2, CommandId: "s-2",
		Events: []*pushupesv1.Event{{Type: "Bin", Body: []byte{0, 1, 0xff, 'A'}}},
	})
	if r4.Status != pushupesv1.AppendResponse_STATUS_EXISTS || r4.Record == nil {
		fatal("dup append: %+v", r4)
	}
	fmt.Printf("idempotent replay -> EXISTS with stored record ok\n")

	// 5) by-command
	bc, err := cli(addrs[0]).ReadByCommand(ctx, &pushupesv1.ReadByCommandRequest{AggregateId: agg, CommandId: "s-1"})
	check(err)
	if !bc.Found || bc.Record.Version != 1 || bc.Record.Seq == 0 {
		fatal("by-command: %+v", bc)
	}
	fmt.Printf("by-command found v1 (seq=%d) ok\n", bc.Record.Seq)

	// 6) point-in-time version anchor (ReadVersionByTime, on every node so
	// the proxy path is covered too): v1/v2 carry explicit stamps, so the
	// anchor for "before v2's time" is exactly 1, and past-the-clock is the
	// full tail 2. The time stream hashes to its own slot, so its appends
	// follow MOVED to that slot's leader like step 1 does.
	const t1, t2 = int64(1600000001), int64(1600000002)
	tagg := agg + "-t"
	tTarget := target
	for pass := 0; pass < 3; pass++ {
		okBoth := true
		r, err := cli(tTarget).Append(ctx, &pushupesv1.AppendRequest{
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
			r2, err := cli(tTarget).Append(ctx, &pushupesv1.AppendRequest{
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
		vb, err := cli(a).ReadVersionByTime(ctx, &pushupesv1.ReadVersionByTimeRequest{
			AggregateId: tagg, UnixTime: t1, // at-or-before v1's stamp -> exactly 1
		})
		check(err)
		if vb.Version != 1 {
			fatal("version-by-time@%s at=%d: want 1 got %d", a, t1, vb.Version)
		}
		vb2, err := cli(a).ReadVersionByTime(ctx, &pushupesv1.ReadVersionByTimeRequest{
			AggregateId: tagg, UnixTime: t2,
		})
		check(err)
		if vb2.Version != 2 {
			fatal("version-by-time@%s at=%d: want 2 got %d", a, t2, vb2.Version)
		}
		vb0, err := cli(a).ReadVersionByTime(ctx, &pushupesv1.ReadVersionByTimeRequest{
			AggregateId: tagg, UnixTime: t1 - 1, // before the first stamp -> 0
		})
		check(err)
		if vb0.Version != 0 {
			fatal("version-by-time@%s at=%d: want 0 got %d", a, t1-1, vb0.Version)
		}
		fmt.Printf("version-by-time@%s: at=%d -> 1, at=%d -> 2, before=%d -> 0 ok\n", a, t1, t2, t1-1)
	}
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
