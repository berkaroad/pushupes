// Package health holds the cluster's slot-copy comparison — the walk that
// cmd/slotcheck runs — as one implementation shared with the console. The
// standard is deliberately narrow: the leader's loaded copy of a slot is the
// reference, and every other holder of that slot is classified against it by
// its local view (LEO plus the aggregate directory's triple). A copy can be
// structurally short while looking in-sync from the outside (its WAL holds
// bytes the directory never adopted), which no other surface reports; that is
// what this walk finds.
package health

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

// The classification kinds. The strings are the CLI's output vocabulary and
// the console's JSON field names — one spelling, forever.
const (
	KindDiverged    = "diverged"
	KindBehind      = "behind"
	KindNotLoaded   = "not-loaded"
	KindUnreachable = "unreachable"
)

// SlotPlacement is the slice of the table the walk needs: who leads a slot
// and who holds it.
type SlotPlacement struct {
	Leader   string
	Replicas []string
}

// Divergence is one holder that disagrees with its slot's leader. The JSON
// tags are the slotcheck -json output contract.
type Divergence struct {
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

// Result is every holder that disagreed with its leader, across the walk.
type Result []Divergence

// Summary folds the items into the counts the console card and the CLI
// summary line print. compared is the number of SLOTS the walk was asked
// about (the CLI's "slots compared" — slots skipped for lack of a loaded
// leader reference are still counted there, matching the historical shape).
type Summary struct {
	Compared int            `json:"compared"`
	Counts   map[string]int `json:"counts"`
}

func (r Result) Summary(compared int) Summary {
	s := Summary{Compared: compared, Counts: map[string]int{}}
	for _, d := range r {
		s.Counts[d.Kind]++
	}
	return s
}

// view is one node's answer on /admin/slots/{n}/describe.
type view struct {
	Slot       int64  `json:"slot"`
	Node       string `json:"node"`
	Role       string `json:"role"`
	Loaded     bool   `json:"loaded"`
	LastSeq    int64  `json:"last_seq"`
	Aggregates int64  `json:"aggregates"`
	Versions   int64  `json:"versions"`
	Resolvable int64  `json:"resolvable"`
}

func describe(hc *http.Client, admin string, slot int64) (view, bool) {
	var v view
	if admin == "" {
		return v, false
	}
	u := admin
	if len(u) > 0 && !hasScheme(u) {
		u = "http://" + u
	}
	resp, err := hc.Get(u + "/admin/slots/" + strconv.FormatInt(slot, 10) + "/describe")
	if err != nil {
		return v, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return v, false
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&v); err != nil {
		return v, false
	}
	return v, true
}

func hasScheme(s string) bool {
	for i := 0; i+2 < len(s); i++ {
		if s[i] == ':' && s[i+1] == '/' && s[i+2] == '/' {
			return true
		}
	}
	return false
}

// CompareSlot asks one slot's leader and every holder for their local view
// and reports the holders that disagree with the leader. A node that has not
// opened the slot is reported as not-loaded rather than as a mismatch: it
// holds no copy to be wrong about, and the fetch loop will open it. A slot
// whose leader does not answer with a loaded copy has no reference and
// yields nothing.
func CompareSlot(hc *http.Client, adminOf map[string]string, slot int64, p SlotPlacement) []Divergence {
	ld, ok := describe(hc, adminOf[p.Leader], slot)
	if !ok || !ld.Loaded {
		return nil // no reference to compare against
	}
	holders := append([]string{p.Leader}, p.Replicas...)
	var out []Divergence
	for _, h := range holders {
		if h == p.Leader {
			continue
		}
		rd, ok := describe(hc, adminOf[h], slot)
		if !ok {
			out = append(out, Divergence{Slot: slot, Node: h, Kind: KindUnreachable})
			continue
		}
		if !rd.Loaded {
			out = append(out, Divergence{Slot: slot, Node: h, Kind: KindNotLoaded})
			continue
		}
		if rd.Aggregates == ld.Aggregates && rd.Versions == ld.Versions && rd.Resolvable == ld.Resolvable {
			continue
		}
		kind := KindBehind // still catching up: the LEO says so
		if rd.LastSeq >= ld.LastSeq {
			kind = KindDiverged // same LEO, different directory: fetching cannot fix it
		}
		out = append(out, Divergence{
			Slot: slot, Node: h, Kind: kind,
			LastSeq: rd.LastSeq, Aggregates: rd.Aggregates, Versions: rd.Versions, Resolvable: rd.Resolvable,
			LeaderSeq: ld.LastSeq, LeaderAgg: ld.Aggregates, LeaderVer: ld.Versions, LeaderRes: ld.Resolvable,
		})
	}
	return out
}

// Scan walks every slot in parallel (jobs-wide) and collects the disagreements.
// adminOf maps node id -> admin HTTP addr (with or without scheme). The walk
// is pure HTTP GETs of the admin surface — any answering node can run it.
func Scan(ctx context.Context, adminOf map[string]string, slots map[int64]SlotPlacement, jobs int) Result {
	if ctx == nil {
		ctx = context.Background()
	}
	if jobs < 1 {
		jobs = 1
	}
	hc := &http.Client{Timeout: 15 * time.Second}
	ids := make([]int64, 0, len(slots))
	for s := range slots {
		ids = append(ids, s)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var (
		mu  sync.Mutex
		out Result
		wg  sync.WaitGroup
		sem = make(chan struct{}, jobs)
	)
	for _, s := range ids {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(slot int64, p SlotPlacement) {
			defer wg.Done()
			defer func() { <-sem }()
			found := CompareSlot(hc, adminOf, slot, p)
			if len(found) == 0 {
				return
			}
			mu.Lock()
			out = append(out, found...)
			mu.Unlock()
		}(s, slots[s])
	}
	wg.Wait()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Slot < out[j].Slot
	})
	return out
}
