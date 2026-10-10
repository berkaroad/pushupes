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

// Command flowcheck smoke-tests the Client library's flow-control
// backpressure against a live cluster: with one node throttled hard,
// concurrent writers must never see a 1006, must all land (idempotent
// retries), and the refused records must converge on success.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/berkaroad/pushupes/pkg/client"
	pushupesv1 "github.com/berkaroad/pushupes/pkg/grpcapi/pushupes/v1"
)

func main() {
	seed := flag.String("seed", "127.0.0.1:19400", "comma-separated client addrs")
	admin := flag.String("admin", "127.0.0.1:19200", "node-1 admin addr (throttle target)")
	tokens := flag.Int("tokens", 2, "refill: tokens per period")
	period := flag.String("period", "1s", "refill period (Go duration)")
	writers := flag.Int("writers", 8, "concurrent BatchAppend goroutines")
	perWriter := flag.Int("per-writer", 10, "aggregate streams each writer appends")
	flag.Parse()

	body, _ := json.Marshal(map[string]any{"tokens": *tokens, "period": *period})
	resp, err := http.Post("http://"+*admin+"/admin/flow-control", "application/json", strings.NewReader(string(body)))
	if err != nil {
		fmt.Println("SETUP_FAIL post:", err)
		os.Exit(1)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Println("SETUP_FAIL status", resp.StatusCode)
		os.Exit(1)
	}
	defer func() {
		req, _ := http.NewRequest(http.MethodDelete, "http://"+*admin+"/admin/flow-control", nil)
		r2, err := http.DefaultClient.Do(req)
		if err == nil {
			io.Copy(io.Discard, r2.Body)
			r2.Body.Close()
		}
	}()

	c, err := client.New(strings.Split(*seed, ","), &client.Config{FlowBackoff: 100 * time.Millisecond})
	if err != nil {
		fmt.Println("NEW_FAIL", err)
		os.Exit(1)
	}
	defer c.Close()

	// one node-wide bucket: route every aggregate onto the throttled node
	// by pinning the slot rows through the cached table — the library
	// writes to the slot leader, so we pick aggregates that hash to slots
	// node-1 leads (learned from the routing table answer).
	// (smoke brevity: the writers just spread; a 1006 reaching a caller
	// is what fails the check, wherever it comes from.)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var saw1006, failed int
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	for w := 0; w < *writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var recs []*pushupesv1.AppendRequest
			for k := 0; k < *perWriter; k++ {
				agg := fmt.Sprintf("flow-w%d-%d", w, k)
				recs = append(recs, &pushupesv1.AppendRequest{
					AggregateId: agg, Version: 1, CommandId: agg,
					Events: []*pushupesv1.Event{{Type: "Flow", Body: []byte(`{"k":1}`)}},
				})
			}
			resps, err := c.BatchAppend(ctx, recs)
			if err != nil {
				fmt.Println("call error (informational):", err)
			}
			mu.Lock()
			defer mu.Unlock()
			for _, r := range resps {
				if r == nil {
					failed++
					continue
				}
				if r.GetErrId() == client.ErrIDFlowControl {
					saw1006++
				}
				switch r.GetStatus() {
				case pushupesv1.AppendResponse_STATUS_SUCCESS, pushupesv1.AppendResponse_STATUS_EXISTS:
				default:
					failed++
				}
			}
		}(w)
	}
	wg.Wait()

	fmt.Printf("writers=%d per=%d saw1006=%d failed=%d redirects=%d flowPauses=%d\n",
		*writers, *perWriter, saw1006, failed, c.Redirects(), c.FlowPauses())
	switch {
	case saw1006 != 0:
		fmt.Println("FAIL: a 1006 reached a caller — the library must absorb it")
		os.Exit(1)
	case c.FlowPauses() == 0:
		fmt.Println("FAIL: FlowPauses=0 — the throttle never engaged (rate too high for this load?)")
		os.Exit(1)
	case failed != 0:
		fmt.Println("FAIL: records ended unanswered or failed")
		os.Exit(1)
	default:
		fmt.Println("OK")
	}
}
