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

	pushupesv1 "pushupes/internal/grpcapi/pushupes/v1"
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
