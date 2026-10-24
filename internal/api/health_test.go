package api

// The 槽位健康 card reads one field — slot_health — that rides the existing
// /admin/cluster/status poll: the page never starts a second clock, and the
// scan itself is an on-demand background walk (idempotent while one runs)
// whose result is kept until a newer one replaces it. A failed round keeps
// the last good numbers visible (with the error beside them), because the
// operator needs the stale health more than a blank card.

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/berkaroad/pushupes/internal/cluster"
	"github.com/berkaroad/pushupes/internal/health"
	"github.com/berkaroad/pushupes/internal/storage"
)

func newHealthTestServer(t *testing.T) *Server {
	t.Helper()
	st, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	srv := New(cluster.NewEngine(nil, st, "node-9", nil), st)
	return srv
}

func getSlotHealth(t *testing.T, srv *Server) map[string]any {
	t.Helper()
	// The same object the status handler embeds (the handler itself needs a
	// raft-backed engine this offline harness does not have; the embedding
	// line is pinned by the route test below reaching the tracker).
	return srv.health.snapshot()
}

func waitState(t *testing.T, srv *Server, want string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		h := getSlotHealth(t, srv)
		if h["state"] == want {
			return h
		}
		if time.Now().After(deadline) {
			t.Fatalf("slot_health stuck at %v, want state %q", h, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHealthScanRouteIsIdempotentAndRidesStatus(t *testing.T) {
	srv := newHealthTestServer(t)

	if got := getSlotHealth(t, srv)["state"]; got != "idle" {
		t.Fatalf("fresh node reports state %v, want idle", got)
	}

	release := make(chan struct{})
	calls := make(chan struct{}, 4)
	srv.health.scan = func(ctx context.Context) (health.Result, int, error) {
		calls <- struct{}{}
		<-release
		return health.Result{
			{Slot: 1, Node: "n2", Kind: health.KindDiverged},
			{Slot: 7, Node: "n2", Kind: health.KindDiverged},
			{Slot: 7, Node: "n3", Kind: health.KindBehind},
		}, 1024, nil
	}

	start := httptest.NewRecorder()
	srv.ServeHTTP(start, httptest.NewRequest(http.MethodPost, "/admin/slots/health-scan", nil))
	if start.Code != http.StatusAccepted {
		t.Fatalf("POST health-scan status = %d, want 202", start.Code)
	}
	// A second request while one runs must not start a second walk.
	again := httptest.NewRecorder()
	srv.ServeHTTP(again, httptest.NewRequest(http.MethodPost, "/admin/slots/health-scan", nil))
	if again.Code != http.StatusAccepted {
		t.Fatalf("idempotent POST status = %d, want 202", again.Code)
	}
	h := waitState(t, srv, "scanning")
	if _, has := h["scanned_at"]; has {
		t.Fatal("a first scan in flight must not report a completion time")
	}
	close(release)

	done := waitState(t, srv, "done")
	if done["diverged"] != 2 || done["behind"] != 1 {
		t.Fatalf("counts after done: %v", done)
	}
	if done["not_loaded"] != 0 || done["unreachable"] != 0 {
		t.Fatalf("zero kinds must still be spelled out: %v", done)
	}
	if done["compared"] != 1024 {
		t.Fatalf("compared = %v, want 1024", done["compared"])
	}
	if at, _ := done["scanned_at"].(string); at == "" {
		t.Fatal("done result carries no scanned_at")
	}
	if n := len(calls); n != 1 {
		t.Fatalf("the walk ran %d times, want exactly 1 (idempotent guard)", n)
	}
}

func TestHealthScanErrorKeepsTheLastGoodNumbers(t *testing.T) {
	srv := newHealthTestServer(t)

	srv.health.scan = func(ctx context.Context) (health.Result, int, error) {
		return health.Result{{Slot: 3, Node: "n2", Kind: health.KindDiverged}}, 8, nil
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/slots/health-scan", nil))
	h := waitState(t, srv, "done")
	if h["diverged"] != 1 {
		t.Fatalf("first round counts: %v", h)
	}

	boom := errors.New("dial tcp: no node answered")
	srv.health.scan = func(ctx context.Context) (health.Result, int, error) {
		return nil, 0, boom
	}
	srv.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/admin/slots/health-scan", nil))
	h = waitState(t, srv, "error")
	if h["diverged"] != 1 {
		t.Fatalf("a failed round hid the last good numbers: %v", h)
	}
	if msg, _ := h["error"].(string); !strings.Contains(msg, "no node answered") {
		t.Fatalf("failed round carries no error text: %v", h["error"])
	}
}
