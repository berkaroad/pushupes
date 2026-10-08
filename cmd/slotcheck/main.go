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
	"sync"
	"time"
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

type describe struct {
	Slot       int64  `json:"slot"`
	Node       string `json:"node"`
	Role       string `json:"role"`
	Loaded     bool   `json:"loaded"`
	LastSeq    int64  `json:"last_seq"`
	Aggregates int64  `json:"aggregates"`
	Versions   int64  `json:"versions"`
	Resolvable int64  `json:"resolvable"`
}

// divergence is one slot holding a copy that disagrees with its leader's.
type divergence struct {
	Slot       int64  `json:"slot"`
	Node       string `json:"node"`
	Kind       string `json:"kind"` // diverged | behind | not-loaded | unreachable
	LastSeq    int64  `json:"last_seq,omitempty"`
	Aggregates int64  `json:"aggregates,omitempty"`
	Versions   int64  `json:"versions,omitempty"`
	Resolvable int64  `json:"resolvable,omitempty"`
	LeaderSeq  int64  `json:"leader_last_seq,omitempty"`
	LeaderAgg  int64  `json:"leader_aggregates,omitempty"`
	LeaderVer  int64  `json:"leader_versions,omitempty"`
	LeaderRes  int64  `json:"leader_resolvable,omitempty"`
}

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

	slots := make([]int, 0, len(st.Slots))
	for k := range st.Slots {
		s, err := strconv.Atoi(k)
		if err != nil {
			continue
		}
		slots = append(slots, s)
	}
	sort.Ints(slots)
	if *limit > 0 && len(slots) > *limit {
		slots = slots[:*limit]
	}
	if !*quiet && !*asJSON {
		fmt.Printf("answered by %s — comparing %d slots across %d nodes\n", st.Node, len(slots), len(st.Peers))
	}

	var (
		mu   sync.Mutex
		bad  = []divergence{} // empty, not nil: -json must print [] rather than null
		wg   sync.WaitGroup
		sem  = make(chan struct{}, *jobs)
		seen = map[string]int{} // kind -> count
	)
	for _, s := range slots {
		p := st.Slots[strconv.Itoa(s)]
		wg.Add(1)
		sem <- struct{}{}
		go func(slot int, p placement) {
			defer wg.Done()
			defer func() { <-sem }()
			found := compare(hc, adminOf, slot, p)
			mu.Lock()
			defer mu.Unlock()
			for _, d := range found {
				seen[d.Kind]++
			}
			bad = append(bad, found...)
		}(s, p)
	}
	wg.Wait()

	sort.Slice(bad, func(i, j int) bool {
		if bad[i].Kind != bad[j].Kind {
			return bad[i].Kind < bad[j].Kind
		}
		return bad[i].Slot < bad[j].Slot
	})

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

// compare asks one slot's leader and every replica for their local view and
// reports the holders that disagree with the leader. A node that has not opened
// the slot is reported as not-loaded rather than as a mismatch: it holds no
// copy to be wrong about, and the fetch loop will open it.
func compare(hc *http.Client, adminOf map[string]string, slot int, p placement) []divergence {
	ld, ok := getDescribe(hc, adminOf[p.Leader], slot)
	if !ok || !ld.Loaded {
		return nil // no reference to compare against
	}
	holders := append([]string{p.Leader}, p.Replicas...)
	var out []divergence
	for _, h := range holders {
		if h == p.Leader {
			continue
		}
		rd, ok := getDescribe(hc, adminOf[h], slot)
		if !ok {
			out = append(out, divergence{Slot: int64(slot), Node: h, Kind: "unreachable"})
			continue
		}
		if !rd.Loaded {
			out = append(out, divergence{Slot: int64(slot), Node: h, Kind: "not-loaded"})
			continue
		}
		if rd.Aggregates == ld.Aggregates && rd.Versions == ld.Versions && rd.Resolvable == ld.Resolvable {
			continue
		}
		kind := "behind" // still catching up: the LEO says so
		if rd.LastSeq >= ld.LastSeq {
			kind = "diverged" // same LEO, different directory: fetching cannot fix it
		}
		out = append(out, divergence{
			Slot: int64(slot), Node: h, Kind: kind,
			LastSeq: rd.LastSeq, Aggregates: rd.Aggregates, Versions: rd.Versions, Resolvable: rd.Resolvable,
			LeaderSeq: ld.LastSeq, LeaderAgg: ld.Aggregates, LeaderVer: ld.Versions, LeaderRes: ld.Resolvable,
		})
	}
	return out
}

func getDescribe(hc *http.Client, admin string, slot int) (describe, bool) {
	var d describe
	if admin == "" {
		return d, false
	}
	resp, err := hc.Get(urlOr(admin, fmt.Sprintf("/admin/slots/%d/describe", slot)))
	if err != nil {
		return d, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return d, false
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&d); err != nil {
		return d, false
	}
	return d, true
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
