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

// slotcheck compares every slot's copies across the cluster: the leader's
// digest is the reference, and each other holder of the slot must agree with
// it.
//
// It exists because a copy can be structurally short while looking perfectly
// healthy from the outside: a slot's aggregate directory adopts a version only
// when it follows the previous one, so one record the directory never adopted
// freezes it for every record above it. The node then reports the LEADER's LEO
// (its WAL holds the bytes) with a shorter directory (it cannot resolve them),
// which means reads past the gap fail there while the console shows an
// in-sync, in-ISR copy.
//
// Nothing else surfaces that:
//   - the leader rebalancer only examines slots that DEVIATE from the ring, so
//     a diverged copy of a slot the ring is already happy with is invisible to
//     it (it is also why "rebalance will not balance" and "replica is short"
//     were the same bug seen from two ends);
//   - /admin/slots/{slot}/describe is per-node, so seeing the disagreement
//     means asking both sides and comparing.
//
// So this walks every slot of every node and reports the disagreements. Run it
// as a routine health check, or after anything that rebuilt a copy.
//
//	behind     the target's LEO is below the leader's: its fetch is still
//	           catching up. Normal, transient.
//	DIVERGED   same (or higher) LEO, different directory: the copy is short and
//	           no amount of fetching repairs it. The rebalancer rebuilds such a
//	           target when it hands leadership over; a diverged copy of a slot
//	           that is already on the ring needs an explicit migration.
//
// Exit code is 1 when any slot diverged (the actionable condition), 0
// otherwise — unreachable nodes make the scan incomplete and are reported
// separately.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/berkaroad/pushupes/internal/health"
)

var (
	admins = flag.String("admins", "127.0.0.1:8091,127.0.0.1:8092,127.0.0.1:8093",
		"comma list of node admin (HTTP) addrs; any one of them is enough to discover the rest")
	jobs   = flag.Int("jobs", 16, "slots compared in parallel (each one issues a few describe requests)")
	limit  = flag.Int("limit", 0, "compare only the first N slots (0 = all)")
	list   = flag.Int("list", 20, "how many diverged slots to print")
	quiet  = flag.Bool("quiet", false, "print only the summary")
	asJSON = flag.Bool("json", false, "print the diverged slots as JSON")
)

type peer struct {
	ID        string `json:"id"`
	AdminAddr string `json:"admin_addr"`
	Down      bool   `json:"down"`
}

type placement struct {
	Leader   string   `json:"leader"`
	Replicas []string `json:"replicas"`
	State    string   `json:"state"`
}

type status struct {
	Node  string               `json:"node"`
	Peers map[string]peer      `json:"peers"`
	Slots map[string]placement `json:"slots"`
}

// The comparison itself lives in internal/health — one standard shared with
// the console's 槽位健康 card, so the CLI and the page cannot drift apart.

func main() {
	flag.Parse()
	hc := &http.Client{Timeout: 15 * time.Second}

	st, err := fetchStatus(hc, splitList(*admins))
	if err != nil {
		fatal("no node answered: %v", err)
	}
	adminOf := map[string]string{}
	for id, p := range st.Peers {
		adminOf[id] = p.AdminAddr
	}

	slots := make([]int64, 0, len(st.Slots))
	for k := range st.Slots {
		s, err := strconv.Atoi(k)
		if err != nil {
			continue
		}
		slots = append(slots, int64(s))
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
	if *limit > 0 && len(slots) > *limit {
		slots = slots[:*limit]
	}
	if !*quiet && !*asJSON {
		fmt.Printf("answered by %s — comparing %d slots across %d nodes\n", st.Node, len(slots), len(st.Peers))
	}

	placements := make(map[int64]health.SlotPlacement, len(slots))
	for _, s := range slots {
		p := st.Slots[strconv.FormatInt(s, 10)]
		placements[s] = health.SlotPlacement{Leader: p.Leader, Replicas: p.Replicas}
	}
	bad := health.Scan(nil, adminOf, placements, *jobs)
	if bad == nil {
		bad = health.Result{} // empty, not nil: -json must print [] rather than null
	}
	seen := bad.Summary(len(slots)).Counts

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(bad)
	} else {
		if !*quiet {
			for _, d := range bad {
				if d.Kind == "diverged" {
					fmt.Printf("DIVERGED slot %d on %s: last_seq=%d agg=%d ver=%d res=%d  |  leader last_seq=%d agg=%d ver=%d res=%d\n",
						d.Slot, d.Node, d.LastSeq, d.Aggregates, d.Versions, d.Resolvable,
						d.LeaderSeq, d.LeaderAgg, d.LeaderVer, d.LeaderRes)
				}
			}
		}
		fmt.Printf("slots compared: %d   diverged: %d   behind: %d   not-loaded: %d   unreachable: %d\n",
			len(slots), seen["diverged"], seen["behind"], seen["not-loaded"], seen["unreachable"])
		if n := seen["diverged"]; n > *list && *list > 0 {
			fmt.Printf("(%d diverged slots not printed — raise -list)\n", n-*list)
		}
	}

	if seen["diverged"] > 0 {
		os.Exit(1)
	}
}

func fetchStatus(hc *http.Client, admins []string) (*status, error) {
	var last error
	for _, a := range admins {
		resp, err := hc.Get(urlOr(a, "/admin/cluster/status"))
		if err != nil {
			last = err
			continue
		}
		var st status
		err = json.NewDecoder(io.LimitReader(resp.Body, 32<<20)).Decode(&st)
		resp.Body.Close()
		if err != nil {
			last = err
			continue
		}
		return &st, nil
	}
	if last == nil {
		last = fmt.Errorf("no admin address given")
	}
	return nil, last
}

func urlOr(addr, path string) string {
	if strings.Contains(addr, "://") {
		return addr + path
	}
	return "http://" + addr + path
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func fatal(f string, a ...any) {
	fmt.Printf("FATAL "+f+"\n", a...)
	os.Exit(1)
}
