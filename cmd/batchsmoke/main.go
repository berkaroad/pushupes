package main

// Smoke: BatchAppend against the live cluster covering success, exists
// (replayed command), version conflict, duplicate-aggregate rejection,
// redirect following, and an empty batch; asserts result alignment end to end.
import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"pushupes/internal/data"
	pushupesv1 "pushupes/internal/grpcapi/pushupes/v1"
)

func req(agg string, ver uint32, cmd string) *pushupesv1.AppendRequest {
	return &pushupesv1.AppendRequest{AggregateId: agg, Version: ver, CommandId: cmd,
		Events: []*pushupesv1.Event{{Type: "Smoke", Body: []byte(`{"k":1}`)}}}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	fail := func(format string, a ...any) {
		fmt.Printf("FAIL: "+format+"\n", a...)
		os.Exit(1)
	}
	call := func(addr string, batch []*pushupesv1.AppendRequest) []*pushupesv1.BatchAppendResult {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fail("dial %s: %v", addr, err)
		}
		defer conn.Close()
		resp, err := pushupesv1.NewEventServiceClient(conn).BatchAppend(ctx,
			&pushupesv1.BatchAppendRequest{Records: batch})
		if err != nil {
			fail("batchappend %s: %v", addr, err)
		}
		if len(resp.Results) != len(batch) {
			fail("results misaligned: got %d want %d", len(resp.Results), len(batch))
		}
		for i, r := range resp.Results {
			if r.AggregateId != batch[i].AggregateId {
				fail("aggregate echo at %d: got %q want %q", i, r.AggregateId, batch[i].AggregateId)
			}
		}
		return resp.Results
	}

	// (a) 50 fresh aggregates. One BatchAppend per destination node: the
	// client groups by the slot leader it knows (from cluster status). For
	// the smoke, send everything to node-1 and follow redirects per record:
	// a redirected record is re-sent verbatim (same command_id, so an
	// accepted duplicate converges on exists) to the node the answer names.
	type pending struct {
		req  *pushupesv1.AppendRequest
		addr string
	}
	batch := make([]*pushupesv1.AppendRequest, 0, 50)
	for i := 0; i < 50; i++ {
		batch = append(batch, req(fmt.Sprintf("smoke-agg-%d", i), 1, fmt.Sprintf("smoke-cmd-%d", i)))
	}
	left := make([]pending, len(batch))
	for i, q := range batch {
		left[i] = pending{q, "127.0.0.1:8591"}
	}
	succ, redir := 0, 0
	for round := 0; round < 8 && len(left) > 0; round++ {
		// group by destination: one RPC per node per round
		byAddr := map[string][]int{}
		var order []string
		for i, p := range left {
			if _, ok := byAddr[p.addr]; !ok {
				order = append(order, p.addr)
			}
			byAddr[p.addr] = append(byAddr[p.addr], i)
		}
		var next []pending
		for _, addr := range order {
			idx := byAddr[addr]
			sub := make([]*pushupesv1.AppendRequest, len(idx))
			for k, i := range idx {
				sub[k] = left[i].req
			}
			for k, r := range call(addr, sub) {
				switch r.Response.Status {
				case pushupesv1.AppendResponse_STATUS_SUCCESS:
					succ++
				case pushupesv1.AppendResponse_STATUS_EXISTS:
					fail("fresh command answered exists: %+v", r)
				default:
					if r.Response.ErrId == data.ErrIDSlotNotLocal || r.Response.ErrId == data.ErrIDMigrating {
						if r.Response.Node == "" {
							fail("redirect without node: %+v", r.Response)
						}
						redir++
						next = append(next, pending{left[idx[k]].req, r.Response.Node})
						continue
					}
					fail("unexpected fail on fresh record: %+v", r.Response)
				}
			}
		}
		left = next
	}
	if len(left) != 0 {
		fail("redirects did not drain: %d left", len(left))
	}
	fmt.Printf("pass A: 50 records landed (success=%d redirects-followed=%d)\n", succ, redir)

	cli := func() pushupesv1.EventServiceClient {
		conn, err := grpc.NewClient("127.0.0.1:8591", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fail("dial: %v", err)
		}
		return pushupesv1.NewEventServiceClient(conn)
	}()

	// (b) exists + version conflict + duplicate pair + plain success. Note
	// smoke-agg-N may live on another node; (b) only makes sense where the
	// data is: resolve from the status table the same way A did — but A
	// already landed agg-0..49 somewhere; re-run via the redirect-following
	// call() helper against node-1 and treat redirects as "go there".
	// To keep (b) simple, drive agg-1 through the SAME follow loop.
	b := []*pushupesv1.AppendRequest{
		req("smoke-agg-0", 1, "smoke-cmd-0"),  // replay -> exists (wherever agg-0 lives)
		req("smoke-agg-1", 9, "smoke-cmd-x"),  // version conflict 1001
		req("smoke-agg-2", 2, "smoke-cmd-d1"), // duplicate aggregate...
		req("smoke-agg-2", 3, "smoke-cmd-d2"), // ...rejected pair
		req("smoke-agg-3", 2, "smoke-cmd-ok"), // plain success v2
	}
	l2 := make([]pending, len(b))
	for i, q := range b {
		l2[i] = pending{q, "127.0.0.1:8591"}
	}
	// follow redirects until every record gets a non-redirect answer
	var results []*pushupesv1.BatchAppendResult
	for round := 0; round < 8 && len(l2) > 0; round++ {
		for len(l2) > 0 {
			addr := l2[0].addr
			var idx []int
			var sub []*pushupesv1.AppendRequest
			var rest []pending
			for i, p := range l2 {
				if p.addr == addr {
					idx = append(idx, i)
					sub = append(sub, p.req)
				} else {
					rest = append(rest, p)
				}
			}
			rs := call(addr, sub)
			var next []pending
			for k, r := range rs {
				if (r.Response.Status == pushupesv1.AppendResponse_STATUS_FAIL) &&
					(r.Response.ErrId == data.ErrIDSlotNotLocal || r.Response.ErrId == data.ErrIDMigrating) {
					next = append(next, pending{l2[idx[k]].req, r.Response.Node})
					continue
				}
				results = append(results, r)
			}
			l2 = append(rest, next...)
		}
		_ = round
	}
	byAggCmd := map[string]*pushupesv1.AppendResponse{}
	for _, r := range results {
		byAggCmd[r.AggregateId+"/"+r.Response.Record.GetCommandId()] = r.Response
		byAggCmd[r.AggregateId] = r.Response
	}
	check := func(agg string, want pushupesv1.AppendResponse_Status, errID uint32) *pushupesv1.AppendResponse {
		r := byAggCmd[agg]
		if r == nil {
			fail("missing result for %s", agg)
		}
		if r.Status != want || r.ErrId != errID {
			fail("%s: got %+v want status=%v err=%d", agg, r, want, errID)
		}
		return r
	}
	// agg-0: the replay answer (exists with stored record) may have gone
	// through a redirect round first; the final answer for that record is
	// either exists (already durable) — success would be a rule bug.
	e := check("smoke-agg-0", pushupesv1.AppendResponse_STATUS_EXISTS, 0)
	if e.Record == nil || e.Record.CommandId != "smoke-cmd-0" {
		fail("exists must echo stored record: %+v", e)
	}
	c1 := check("smoke-agg-1", pushupesv1.AppendResponse_STATUS_FAIL, data.ErrIDVersionConflict)
	if c1.CurrentVersion != 1 {
		fail("conflict current_version: %d", c1.CurrentVersion)
	}
	// The duplicate pair: BOTH rejected, and agg-2 must still have exactly 1
	// record (the v1 from phase A).
	check("smoke-agg-2", pushupesv1.AppendResponse_STATUS_FAIL, data.ErrIDBadRequest)
	check("smoke-agg-3", pushupesv1.AppendResponse_STATUS_SUCCESS, 0)
	fmt.Println("pass B: exists/1001/dup-reject/success in one batch, all aligned")

	// (c) empty batch
	er, err := cli.BatchAppend(ctx, &pushupesv1.BatchAppendRequest{})
	if err != nil || len(er.Results) != 0 {
		fail("empty batch: %+v %v", er, err)
	}
	fmt.Println("pass C: empty batch returns empty results")

	// (d) rejected dup never executed: agg-2 has only its v1.
	// agg-2 may sit on another node; follow the redirect of a plain
	// Append-style probe via cli on the right node: use ReadStream against
	// each node until one answers non-empty with the stored data.
	var rr *pushupesv1.ReadStreamResponse
	for _, addr := range []string{"127.0.0.1:8591", "127.0.0.1:8592", "127.0.0.1:8593"} {
		conn, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		c := pushupesv1.NewEventServiceClient(conn)
		resp, err := c.ReadStream(ctx, &pushupesv1.ReadStreamRequest{AggregateId: "smoke-agg-2", FromVersion: 1})
		conn.Close()
		if err == nil && len(resp.Records) > 0 {
			rr = resp
			break
		}
	}
	if rr == nil || len(rr.Records) != 1 {
		fail("rejected dup must not write: got %+v", rr)
	}
	fmt.Println("pass D: rejected duplicate records never executed")
	fmt.Println("SMOKE OK")
}
