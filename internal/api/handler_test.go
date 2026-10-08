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

package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/berkaroad/pushupes/internal/cluster"
	"github.com/berkaroad/pushupes/internal/data"
	"github.com/berkaroad/pushupes/internal/storage"
)

// decodeBody decodes an error response body.
func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response body %q is not JSON: %v", rec.Body.String(), err)
	}
	return body
}

// TestRefuseNotControllerCarriesController pins the shape of a follower's
// refusal of a controller-only command: it is a 429 with the same err_id
// (1005) the admin plane already used for this case, and it names the
// controller — its node id and admin address — both in the message and as
// fields, so a client can retry the command against the right node without
// parsing the prose.
func TestRefuseNotControllerCarriesController(t *testing.T) {
	rec := httptest.NewRecorder()
	handled := refuseNotController(rec, &cluster.NotControllerError{
		LeaderID:  "node-2",
		AdminAddr: "http://127.0.0.1:8092",
	})
	if !handled {
		t.Fatal("a NotControllerError must be rendered as a refusal")
	}
	if rec.Code != http.StatusTooEarly {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusTooEarly)
	}
	body := decodeBody(t, rec)
	if body["err_id"] != float64(data.ErrIDNotLeader) {
		t.Fatalf("err_id = %v, want %d", body["err_id"], data.ErrIDNotLeader)
	}
	if body["controller"] != "node-2" || body["controller_admin_addr"] != "http://127.0.0.1:8092" {
		t.Fatalf("refusal does not carry the controller: %v", body)
	}
	msg, _ := body["error"].(string)
	if !strings.Contains(msg, "node-2") || !strings.Contains(msg, "http://127.0.0.1:8092") {
		t.Fatalf("refusal message must name the controller id and address: %q", msg)
	}
}

// TestRefuseNotControllerIgnoresOtherErrors makes sure the helper only claims
// the errors it owns: anything else stays with the caller's own handler (and
// nothing is written by the helper).
func TestRefuseNotControllerIgnoresOtherErrors(t *testing.T) {
	rec := httptest.NewRecorder()
	if refuseNotController(rec, errors.New("unknown target node \"nope\"")) {
		t.Fatal("a plain error must not be rendered as a controller refusal")
	}
	if rec.Body.Len() != 0 || rec.Code != http.StatusOK {
		t.Fatalf("helper wrote a response for a foreign error: code=%d body=%q", rec.Code, rec.Body.String())
	}
}

// TestAdminWriteHandlersRefuseWithoutController drives the real mux + handler
// path (not just the renderer) for every admin command that mutates replicated
// state. The engine here has no Raft node — the "no controller known" end of
// the same rule — and the point is that the handlers answer with the
// actionable refusal rather than a generic 500 and never touch the data plane.
func TestAdminWriteHandlersRefuseWithoutController(t *testing.T) {
	st, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	eng := cluster.NewEngine(nil, st, "node-9", nil)
	srv := New(eng, st)

	cases := []struct {
		name string
		path string
		body string
	}{
		{"migrate", "/admin/slots/0/migrate", `{"to_node":"node-2"}`},
		{"remove-replica", "/admin/slots/0/remove-replica", `{"node":"node-2"}`},
		{"plan", "/admin/cluster/plan", ``},
		{"replica-policy", "/admin/cluster/replica-policy", `{"policy":"high"}`},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		if rec.Code != http.StatusTooEarly {
			t.Fatalf("%s: status = %d (%s), want %d", tc.name, rec.Code, rec.Body.String(), http.StatusTooEarly)
		}
		body := decodeBody(t, rec)
		if body["err_id"] != float64(data.ErrIDNotLeader) {
			t.Fatalf("%s: err_id = %v, want %d", tc.name, body["err_id"], data.ErrIDNotLeader)
		}
		if msg, _ := body["error"].(string); !strings.Contains(msg, "not the controller") {
			t.Fatalf("%s: refusal message %q is not actionable", tc.name, msg)
		}
	}
}

// TestAdminStatsMergesFlushAndRepl pins the merged diagnostics endpoint: one
// read answers both sub-systems' views with their own numbers, ?worst=N is
// accepted, and the two endpoints it replaced are gone.
func TestAdminStatsMergesFlushAndRepl(t *testing.T) {
	st, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	srv := New(cluster.NewEngine(nil, st, "node-9", nil), st)

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/admin/stats?worst=4", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d (%s)", rr.Code, rr.Body.String())
	}
	body := decodeBody(t, rr)
	if body["node"] != "node-9" {
		t.Fatalf("node = %v, want node-9", body["node"])
	}
	flush, _ := body["flush"].(map[string]any)
	if flush == nil {
		t.Fatalf("no flush section in %v", body)
	}
	for _, k := range []string{"dirty", "oldest_unflushed_age_s", "busy_locked", "fsync_count",
		"fsync_new_bytes_avg", "fsync_file_size_max", "queue_units", "inflight", "fsync_svc_p99_ms"} {
		if _, ok := flush[k]; !ok {
			t.Fatalf("flush section missing %q: %v", k, flush)
		}
	}
	// The ack chain is the third section of the merged view: its steps and the
	// replication-side terms behind them are part of the admin surface.
	ack, _ := body["ack"].(map[string]any)
	if ack == nil {
		t.Fatalf("no ack section in %v", body)
	}
	for _, k := range []string{"appends", "calls", "success", "redirects", "fence_route", "wal_land",
		"hw_wait", "total", "waiters_now", "hw_stalls", "hw_lag_records",
		"report_rtt", "progress_handle", "wake_lag", "worst"} {
		if _, ok := ack[k]; !ok {
			t.Fatalf("ack section missing %q: %v", k, ack)
		}
	}
	repl, _ := body["repl"].(map[string]any)
	if repl == nil {
		t.Fatalf("no repl section in %v", body)
	}
	for _, k := range []string{"node", "tracked_slots", "stuck_slots", "stuck_oldest_age_s", "peers", "mfetch", "fetch"} {
		if _, ok := repl[k]; !ok {
			t.Fatalf("repl section missing %q: %v", k, repl)
		}
	}
	mfetch, _ := repl["mfetch"].(map[string]any)
	if mfetch == nil {
		t.Fatalf("no mfetch section: %v", repl)
	}
	for _, k := range []string{"rounds", "parked_rounds", "ended_by_wake", "ended_by_deadline", "parked_now"} {
		if _, ok := mfetch[k]; !ok {
			t.Fatalf("mfetch section missing %q: %v", k, mfetch)
		}
	}

	// The split endpoints are gone; /admin/stats is the one view.
	for _, p := range []string{"/admin/flushstats", "/admin/replstats"} {
		rr := httptest.NewRecorder()
		srv.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, p, nil))
		if rr.Code != http.StatusNotFound {
			t.Fatalf("%s: status = %d, want 404 (merged into /admin/stats)", p, rr.Code)
		}
	}
}
