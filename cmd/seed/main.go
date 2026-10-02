// Command seed writes approximately -mib of event records into one slot so
// migration benchmarks have a realistic sealed-segment layout. It finds an
// aggregate whose hash lands on -slot, then appends to the
// slot leader (resolved from an admin status snapshot).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"pushupes/internal/data"
	pushupesv1 "pushupes/internal/grpcapi/pushupes/v1"
)

func main() {
	admin := flag.String("admin", "127.0.0.1:8091", "one admin addr of the cluster")
	slot := flag.Int("slot", 0, "target slot id")
	mib := flag.Int("mib", 100, "approx bytes to write, in MiB")
	conns := flag.Int("conns", 8, "parallel aggregates")
	flag.Parse()

	body := make([]byte, 1024)
	for i := range body {
		body[i] = byte('a' + i%26)
	}

	st := fetchStatus(*admin)
	ldr, ok := st.Slots[fmt.Sprint(*slot)]
	if !ok {
		fmt.Fprintf(os.Stderr, "slot %d not in table (run plan first?)\n", *slot)
		os.Exit(1)
	}
	addr := st.Peers[ldr.Leader].ClientAddr
	if addr == "" {
		fmt.Fprintln(os.Stderr, "leader has no client_addr")
		os.Exit(1)
	}
	fmt.Printf("seeding slot %d via leader %s (%s)\n", *slot, ldr.Leader, addr)

	conn, err := grpc.NewClient(hostPort(addr), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	cli := pushupesv1.NewEventServiceClient(conn)

	// distinct aggregates that all hash into the slot
	var aggs []string
	for i := 0; len(aggs) < *conns; i++ {
		a := fmt.Sprintf("seed-%d-%d", *slot, i)
		if int(data.SlotOf(a, data.DefaultSlotCount)) == *slot {
			aggs = append(aggs, a)
		}
	}

	perAgg64 := int64(*mib) * 1024 * 1024 / int64(len(aggs)) / int64(len(body)+64)
	perAgg := int(perAgg64)
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
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				_, err := cli.Append(ctx, &pushupesv1.AppendRequest{
					AggregateId: agg, Version: v, CommandId: fmt.Sprintf("s-%d", n),
					Events: []*pushupesv1.Event{{Type: "Seed", Body: body}},
				})
				cancel()
				if err != nil {
					fails.Add(1)
					if fails.Load() < 5 {
						fmt.Fprintf(os.Stderr, "append fail: %v\n", err)
					}
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

type status struct {
	Slots map[string]struct {
		Leader string `json:"leader"`
	} `json:"slots"`
	Peers map[string]struct {
		ClientAddr string `json:"client_addr"`
	} `json:"peers"`
}

func fetchStatus(admin string) status {
	hc := &http.Client{Timeout: 10 * time.Second}
	resp, err := hc.Get("http://" + hostPort(admin) + "/admin/cluster/status")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var s status
	if err := json.Unmarshal(b, &s); err != nil {
		panic(err)
	}
	return s
}

func hostPort(addr string) string {
	if i := indexOf(addr, "://"); i >= 0 {
		addr = addr[i+3:]
	}
	return addr
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
