package api

// The on-demand slot-health scan behind the console's 槽位健康 card. The
// walk itself is internal/health — the same standard cmd/slotcheck prints —
// so the page and the operator tool report the same numbers by construction.
// The trigger is POST /admin/slots/health-scan (idempotent while a scan runs;
// the console re-triggers once a minute and the guard is the backstop), the
// result rides the existing /admin/cluster/status poll — no second clock on
// the page — and a failed round keeps the last good numbers, marked with its
// error, because stale health still beats a blank card.

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/berkaroad/pushupes/internal/health"
)

// healthScanTimeout bounds one walk: 1024+ slots x a few local describes is
// seconds of work; anything beyond minutes is a node wedged, and the walk
// must not live past its usefulness.
const healthScanTimeout = 2 * time.Minute

type healthTracker struct {
	scan func(ctx context.Context) (health.Result, int, error)

	mu       sync.Mutex
	running  bool
	state    string // "", scanning, done, error
	compared int
	counts   map[string]int
	at       time.Time
	lastErr  string
}

// trigger starts one walk unless one is already in flight. It reports whether
// this call started it; the other answer ("already scanning") is the idempotent
// no-op the console leans on when several poll ticks race a slow walk.
func (t *healthTracker) trigger() bool {
	t.mu.Lock()
	if t.running {
		t.mu.Unlock()
		return false
	}
	t.running = true
	t.state = "scanning"
	t.mu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), healthScanTimeout)
		defer cancel()
		res, compared, err := t.scan(ctx)
		t.mu.Lock()
		defer t.mu.Unlock()
		t.running = false
		if err != nil {
			// state error KEEPS the previous counts: the numbers on the card
			// stay the last known truth while the card says the check failed.
			t.state = "error"
			t.lastErr = err.Error()
			return
		}
		t.counts = res.Summary(compared).Counts
		t.compared = compared
		t.at = time.Now().UTC()
		t.state = "done"
		t.lastErr = ""
	}()
	return true
}

// snapshot is the slot_health object of /admin/cluster/status. All four
// kinds are always spelled out once any result exists (the card distinguishes
// "healthy zero" from "not yet measured" by the state and the presence of
// scanned_at, not by a missing key).
func (t *healthTracker) snapshot() map[string]any {
	t.mu.Lock()
	defer t.mu.Unlock()
	state := t.state
	if state == "" {
		state = "idle"
	}
	out := map[string]any{"state": state}
	if !t.at.IsZero() {
		out["scanned_at"] = t.at.Format(time.RFC3339)
		out["compared"] = t.compared
		for kind, key := range map[string]string{
			health.KindDiverged:    "diverged",
			health.KindBehind:      "behind",
			health.KindNotLoaded:   "not_loaded",
			health.KindUnreachable: "unreachable",
		} {
			out[key] = t.counts[kind]
		}
	}
	if state == "error" {
		out["error"] = t.lastErr
	}
	return out
}

// engineScan walks the cluster the responding node can see: the replicated
// table for the placement view, every member's admin addr for the describes.
func (s *Server) engineScan(ctx context.Context) (health.Result, int, error) {
	tbl := s.Engine.TableSnapshot()
	adminOf := make(map[string]string, len(tbl.Peers))
	for id, p := range tbl.Peers {
		adminOf[id] = p.AdminAddr
	}
	slots := make(map[int64]health.SlotPlacement, len(tbl.Slots))
	for n, p := range tbl.Slots {
		slots[int64(n)] = health.SlotPlacement{Leader: p.Leader, Replicas: append([]string(nil), p.Replicas...)}
	}
	return health.Scan(ctx, adminOf, slots, 8), len(slots), nil
}

func (s *Server) handleHealthScan(w http.ResponseWriter, r *http.Request) {
	started := s.health.trigger()
	writeJSON(w, http.StatusAccepted, map[string]any{
		"started": started,
		"state":   "scanning",
	})
}
