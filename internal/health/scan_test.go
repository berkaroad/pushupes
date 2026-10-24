package health

// The scan is the slotcheck walk, lifted out of the CLI so the console card
// and the operator tool cannot disagree about what "healthy" means. The
// per-slot verdict is the whole standard: the leader's loaded copy is the
// reference, and every other holder is classified against it.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// describe answers as /admin/slots/{n}/describe does, for the fake nodes.
func describeJSON(t *testing.T, d map[string]any) string {
	t.Helper()
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func fakeAdmin(t *testing.T, views map[int]map[string]any) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/slots/", func(w http.ResponseWriter, r *http.Request) {
		var slot int
		if _, err := fmt.Sscanf(r.URL.Path, "/admin/slots/%d/describe", &slot); err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		v, ok := views[slot]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, describeJSON(t, v))
	})
	return httptest.NewServer(mux)
}

func d(slot int, node, role string, loaded bool, seq, agg, ver, res int64) map[string]any {
	return map[string]any{
		"slot": slot, "node": node, "role": role, "loaded": loaded,
		"last_seq": seq, "aggregates": agg, "versions": ver, "resolvable": res,
	}
}

func TestCompareSlotClassificationTable(t *testing.T) {
	// Ported whole from the CLI when the walk moved here: the classification
	// is the tool's whole value, so all outcomes stay pinned — a checker that
	// only ever reports "healthy" is worse than none.
	serve := func(slot int64, node string, loaded bool, seq, agg, ver, res int64) string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !strings.HasSuffix(r.URL.Path, "/describe") {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"slot":` + strconv.FormatInt(slot, 10) +
				`,"node":"` + node + `","loaded":` + strconv.FormatBool(loaded) +
				`,"last_seq":` + strconv.FormatInt(seq, 10) +
				`,"aggregates":` + strconv.FormatInt(agg, 10) +
				`,"versions":` + strconv.FormatInt(ver, 10) +
				`,"resolvable":` + strconv.FormatInt(res, 10) + `}`))
		}))
		t.Cleanup(srv.Close)
		return srv.URL
	}
	leader := serve(7, "node-1", true, 100, 1, 100, 100)

	cases := []struct {
		name          string
		loaded        bool
		seq, agg, ver int64
		res           int64
		wantKind      string // "" = no finding
	}{
		{name: "in sync", loaded: true, seq: 100, agg: 1, ver: 100, res: 100},
		{name: "a lagging copy is reported as behind, not as damage",
			loaded: true, seq: 90, agg: 1, ver: 90, res: 90, wantKind: KindBehind},
		{name: "the leader's LEO with a short directory is diverged",
			loaded: true, seq: 100, agg: 1, ver: 60, res: 60, wantKind: KindDiverged},
		{name: "ahead of the leader's LEO with a different directory is diverged too",
			loaded: true, seq: 120, agg: 2, ver: 120, res: 120, wantKind: KindDiverged},
		{name: "a copy this node never opened is not a mismatch", wantKind: KindNotLoaded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			adminOf := map[string]string{
				"node-1": leader,
				"node-2": serve(7, "node-2", tc.loaded, tc.seq, tc.agg, tc.ver, tc.res),
			}
			got := CompareSlot(http.DefaultClient, adminOf, 7,
				SlotPlacement{Leader: "node-1", Replicas: []string{"node-1", "node-2"}})
			if tc.wantKind == "" {
				if len(got) != 0 {
					t.Fatalf("want no finding, got %+v", got)
				}
				return
			}
			if len(got) != 1 || got[0].Kind != tc.wantKind {
				t.Fatalf("want one %q finding, got %+v", tc.wantKind, got)
			}
			if got[0].Node != "node-2" || got[0].Slot != 7 {
				t.Fatalf("finding names the wrong holder: %+v", got[0])
			}
		})
	}

	// A leader this node cannot read leaves nothing to compare against:
	// reporting every replica as diverged would be noise, not a finding.
	if got := CompareSlot(http.DefaultClient, map[string]string{"node-2": "127.0.0.1:1"}, 3,
		SlotPlacement{Leader: "node-1", Replicas: []string{"node-1", "node-2"}}); got != nil {
		t.Fatalf("want no findings, got %+v", got)
	}
}

func TestCompareSlotClassifiesEveryHolderAgainstTheLeader(t *testing.T) {
	type want struct {
		node, kind string
	}
	hc := &http.Client{Timeout: 5 * time.Second}
	defer func() { /* servers closed per case */ }()

	// A: a behind replica (fetch will fix it) and a diverged one (it won't:
	// same LEO, shorter directory).
	behind := fakeAdmin(t, map[int]map[string]any{7: d(7, "replica", "follower", true, 5, 1, 5, 5)})
	diverged := fakeAdmin(t, map[int]map[string]any{7: d(7, "replica", "follower", true, 10, 1, 9, 9)})
	leader := fakeAdmin(t, map[int]map[string]any{7: d(7, "leader", "leader", true, 10, 1, 10, 10)})
	notLoaded := fakeAdmin(t, map[int]map[string]any{7: d(7, "replica", "follower", false, 0, 0, 0, 0)})
	badSlot := fakeAdmin(t, map[int]map[string]any{})
	defer behind.Close()
	defer diverged.Close()
	defer leader.Close()
	defer notLoaded.Close()
	defer badSlot.Close()

	cases := []struct {
		admin string
		w     want
	}{
		{behind.URL, want{"replica", KindBehind}},
		{diverged.URL, want{"replica", KindDiverged}},
		{notLoaded.URL, want{"replica", KindNotLoaded}},
		{"http://127.0.0.1:1", want{"replica", KindUnreachable}},
	}
	for _, c := range cases {
		adminOf := map[string]string{"leader": leader.URL, "replica": c.admin}
		got := CompareSlot(hc, adminOf, 7, SlotPlacement{Leader: "leader", Replicas: []string{"leader", "replica"}})
		if len(got) != 1 || got[0].Kind != c.w.kind || got[0].Node != c.w.node {
			t.Fatalf("holder at %s classified as %+v, want one %s on %s", c.admin, got, c.w.kind, c.w.node)
		}
	}

	// The leader itself is never counted against itself, and a leader whose
	// copy is not loaded leaves the slot without a reference: nothing is
	// reported (compare only what answers with a loaded leader).
	adminOf := map[string]string{"leader": notLoaded.URL, "replica": diverged.URL}
	if got := CompareSlot(hc, adminOf, 7, SlotPlacement{Leader: "leader", Replicas: []string{"leader", "replica"}}); len(got) != 0 {
		t.Fatalf("a slot without a loaded leader reference reported %+v, want nothing", got)
	}
	adminOf = map[string]string{"leader": leader.URL, "replica": leader.URL}
	if got := CompareSlot(hc, adminOf, 7, SlotPlacement{Leader: "leader", Replicas: []string{"leader", "replica"}}); len(got) != 0 {
		t.Fatalf("a copy agreeing with its leader reported %+v, want nothing", got)
	}

	// badSlot: leader answers 404 for the slot -> no reference, skip.
	adminOf = map[string]string{"leader": badSlot.URL, "replica": diverged.URL}
	if got := CompareSlot(hc, adminOf, 7, SlotPlacement{Leader: "leader", Replicas: []string{"leader", "replica"}}); len(got) != 0 {
		t.Fatalf("a leader not serving the slot produced %+v, want a skipped comparison", got)
	}
}

func TestScanSummarisesCountsAndCompared(t *testing.T) {
	// One rich leader, four slot outcomes: clean / diverged / behind+
	// unreachable holders / not-loaded holder; plus a slot whose leader is
	// down (still COUNTED as compared by the walk — the total is the slots
	// asked about, matching the CLI's summary line).
	leaderSrv := fakeAdmin(t, map[int]map[string]any{
		0: d(0, "n1", "leader", true, 3, 1, 3, 3),
		1: d(1, "n1", "leader", true, 10, 1, 10, 10),
		2: d(2, "n1", "leader", true, 10, 1, 10, 10),
		3: d(3, "n1", "leader", true, 4, 1, 4, 4),
	})
	defer leaderSrv.Close()
	clean := fakeAdmin(t, map[int]map[string]any{0: d(0, "n2", "follower", true, 3, 1, 3, 3)})
	defer clean.Close()
	divged := fakeAdmin(t, map[int]map[string]any{1: d(1, "n2", "follower", true, 10, 1, 9, 9)})
	defer divged.Close()
	bahn := fakeAdmin(t, map[int]map[string]any{2: d(2, "n2", "follower", true, 6, 1, 6, 6)})
	defer bahn.Close()
	nl := fakeAdmin(t, map[int]map[string]any{3: d(3, "n2", "follower", false, 0, 0, 0, 0)})
	defer nl.Close()

	slots := map[int64]SlotPlacement{
		0: {Leader: "n1", Replicas: []string{"n1", "n2"}},
		1: {Leader: "n1", Replicas: []string{"n1", "n2"}},
		2: {Leader: "n1", Replicas: []string{"n1", "n2"}},
		3: {Leader: "n1", Replicas: []string{"n1", "n2"}},
	}
	adminOf := map[string]string{"n1": leaderSrv.URL, "n2": ""}
	// Per-slot holder admins: Scan takes adminOf by NODE; this fixture needs
	// one admin per holder PER slot, so drive it through the node table the
	// same way the real cluster does — one fake per node cannot vary by slot,
	// so compose: n2's fake answers all four slots with the right shapes.
	_ = adminOf
	_ = clean
	n2 := fakeAdmin(t, map[int]map[string]any{
		0: d(0, "n2", "follower", true, 3, 1, 3, 3),
		1: d(1, "n2", "follower", true, 10, 1, 9, 9), // diverged
		2: d(2, "n2", "follower", true, 6, 1, 6, 6),  // behind
		3: d(3, "n2", "follower", false, 0, 0, 0, 0), // not-loaded
	})
	defer n2.Close()

	res := Scan(nil, map[string]string{"n1": leaderSrv.URL, "n2": n2.URL}, slots, 4)
	sum := res.Summary(len(slots))
	if sum.Counts[KindDiverged] != 1 || sum.Counts[KindBehind] != 1 || sum.Counts[KindNotLoaded] != 1 || sum.Counts[KindUnreachable] != 0 {
		t.Fatalf("counts %+v, want diverged=1 behind=1 not-loaded=1", sum.Counts)
	}
	if sum.Compared != 4 {
		t.Fatalf("compared = %d, want 4", sum.Compared)
	}
	if len(res) != 3 {
		t.Fatalf("items %+v, want the three holders that disagree", res)
	}
}
