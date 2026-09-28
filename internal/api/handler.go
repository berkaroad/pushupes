// Package api exposes the pushupes HTTP surface: the inter-node
// replication/migration endpoints and the admin API. Client event
// reads/writes live on the gRPC data plane (internal/grpcapi), not here.
// Routing follows DESIGN.md section 6.
package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"pushupes/internal/cluster"
	"pushupes/internal/data"
	"pushupes/internal/storage"
)

// Server wires HTTP routes onto the engine + store.
type Server struct {
	Engine *cluster.Engine
	Store  *storage.Store
	mux    *http.ServeMux
}

// New builds the HTTP handler tree.
func New(eng *cluster.Engine, store *storage.Store) *Server {
	s := &Server{Engine: eng, Store: store, mux: http.NewServeMux()}

	s.mux.HandleFunc("GET /healthz", s.handleHealth)

	// pprof rides the admin plane (no separate port): the profile is a
	// debugging surface for operators, mounted on DefaultServeMux by the
	// net/http/pprof blank import in main.
	s.mux.Handle("GET /debug/pprof/", http.DefaultServeMux)

	// management (the client event plane is gRPC; see internal/grpcapi)
	s.mux.HandleFunc("GET /v1/slots/{slot}/describe", s.handleSlotDescribe)

	// internal (inter-node) endpoints
	s.mux.HandleFunc("POST /internal/mfetch", s.handleMFetch)
	s.mux.HandleFunc("POST /internal/replicate", s.handleReplicate)
	s.mux.HandleFunc("POST /internal/leo", s.handleLEO)
	s.mux.HandleFunc("POST /internal/replica-progress", s.handleReplicaProgress)
	s.mux.HandleFunc("POST /internal/migrate/segments", s.handleMigrateSegments)
	s.mux.HandleFunc("POST /internal/migrate/snapshot", s.handleMigrateSnapshot)

	// admin
	s.mux.HandleFunc("GET /admin/cluster/status", s.handleClusterStatus)
	s.mux.HandleFunc("GET /admin/writes", s.handleWrites)
	s.mux.HandleFunc("POST /admin/slots/{slot}/migrate", s.handleMigrate)
	s.mux.HandleFunc("POST /admin/cluster/plan", s.handlePlan)

	return s
}

// ServeHTTP makes Server an http.Handler. CORS is permissive: the API is
// unauthenticated and the console must poll per-node counters (status
// "writes") from any browser origin to derive live slot write rates.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// The 4096-slot status is ~350KB of highly repetitive JSON; gzip cuts
	// it ~20x and browsers fetch transparently advertise the encoding.
	if strings.HasPrefix(r.URL.Path, "/admin/") && strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		gz := gzip.NewWriter(w)
		defer gz.Close()
		w.Header().Set("Content-Encoding", "gzip")
		s.mux.ServeHTTP(&gzipWriter{ResponseWriter: w, gz: gz}, r)
		return
	}
	s.mux.ServeHTTP(w, r)
}

// gzipWriter redirects body writes into a gzip stream.
type gzipWriter struct {
	http.ResponseWriter
	gz *gzip.Writer
}

func (g *gzipWriter) Write(b []byte) (int, error) { return g.gz.Write(b) }

func (s *Server) handleSlotDescribe(w http.ResponseWriter, r *http.Request) {
	slot, err := strconv.ParseInt(r.PathValue("slot"), 10, 32)
	if err != nil {
		writeErr(w, http.StatusBadRequest, data.ErrIDBadRequest, "bad slot")
		return
	}
	sl, err := s.Store.Slot(int32(slot))
	if err != nil {
		writeErr(w, http.StatusNotFound, data.ErrIDBadRequest, err.Error())
		return
	}
	tbl := s.Engine.TableSnapshot()
	p := tbl.Slots[int32(slot)]
	writeJSON(w, http.StatusOK, map[string]any{
		"slot":        slot,
		"last_seq":    sl.LastSeq(),
		"hw":          s.Engine.HW(int32(slot)),
		"segments":    sl.SegmentCount(),
		"total_bytes": sl.TotalSize(),
		"placement":   p,
		"isr":         s.Engine.ISR(int32(slot)),
		"writes":      s.Store.WriteCount(int32(slot)),
	})
}

// ---- internal endpoints -------------------------------------------------------

// handleMFetch serves one multiplexed replica fetch round (all slots a
// follower takes from this leader, long-polled as a unit).
func (s *Server) handleMFetch(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 16<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, data.ErrIDBadRequest, "bad mfetch request")
		return
	}
	req, err := cluster.DecodeMRequest(raw)
	if err != nil {
		writeErr(w, http.StatusBadRequest, data.ErrIDBadRequest, "bad mfetch request")
		return
	}
	resp, err := s.Engine.HandleMFetch(*req)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, 0, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(cluster.EncodeMResponse(resp))
}

// handleLEO answers a slot LEO probe.
func (s *Server) handleLEO(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Slot int32 `json:"slot"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, data.ErrIDBadRequest, "bad leo request")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"leo": s.Store.LastSeqOf(req.Slot)})
}

// handleReplicaProgress accepts follower LEO reports on the slot leader —
// one slot via {slot, leo}, or a bulk list via {items:[{slot, from_seq}]}
// (from_seq-1 is the LEO; what the fetch sessions post after productive
// rounds).
func (s *Server) handleReplicaProgress(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Follower string              `json:"follower"`
		Slot     int32               `json:"slot"`
		LEO      uint64              `json:"leo"`
		Items    []cluster.FetchItem `json:"items"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, data.ErrIDBadRequest, "bad progress request")
		return
	}
	if len(req.Items) > 0 {
		for _, it := range req.Items {
			if it.FromSeq > 0 {
				s.Engine.NoteReplicaProgress(it.Slot, req.Follower, it.FromSeq-1)
			}
		}
	} else if req.Follower != "" {
		s.Engine.NoteReplicaProgress(req.Slot, req.Follower, req.LEO)
	}
	w.WriteHeader(http.StatusOK)
}

// handleReplicate serves /internal/replicate: a migration forwarding push.
func (s *Server) handleReplicate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Slot    int32  `json:"slot"`
		Seq     uint64 `json:"seq"`
		Payload []byte `json:"payload"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, data.ErrIDBadRequest, "bad replicate request")
		return
	}
	if err := s.Engine.HandleReplicate(req.Slot, req.Seq, req.Payload); err != nil {
		writeErr(w, http.StatusInternalServerError, 0, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleMigrateSegments accepts pushed sealed segments.
func (s *Server) handleMigrateSegments(w http.ResponseWriter, r *http.Request) {
	var req cluster.PushSegmentsRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 512<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, data.ErrIDBadRequest, "bad segments payload: "+err.Error())
		return
	}
	if err := s.Engine.HandlePushSegments(req); err != nil {
		writeErr(w, http.StatusInternalServerError, 0, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleMigrateSnapshot makes this node (the source) push its sealed segments
// of a slot to the given target node.
func (s *Server) handleMigrateSnapshot(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Slot   int32  `json:"slot"`
		ToNode string `json:"to_node"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, data.ErrIDBadRequest, "bad snapshot request")
		return
	}
	if err := s.Engine.HandleSnapshotCommand(r.Context(), req.Slot, req.ToNode); err != nil {
		writeErr(w, http.StatusInternalServerError, 0, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ---- admin ---------------------------------------------------------------------

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	tbl := s.Engine.TableSnapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"node":       s.Engine.Self(),
		"raft":       s.Engine.RaftStats(),
		"peers":      tbl.Peers,
		"slots":      tbl.Slots,
		"slot_count": tbl.SlotCount,
		// durable writes per slot since this process started; clients diff
		// successive snapshots to derive write rates (msg/sec).
		"writes": s.Engine.WriteCounts(),
	})
}

// handleWrites is the light-rate poll target: just the per-slot durable
// counters (the array the console diffs every 2s), without the 4096-entry
// slot table that rides /admin/cluster/status. Clients that poll rates
// already hold the table from the slower status refresh.
func (s *Server) handleWrites(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"node":       s.Engine.Self(),
		"slot_count": s.Store.SlotCount,
		"writes":     s.Engine.WriteCounts(),
	})
}

func (s *Server) handleMigrate(w http.ResponseWriter, r *http.Request) {
	slot, err := strconv.ParseInt(r.PathValue("slot"), 10, 32)
	if err != nil {
		writeErr(w, http.StatusBadRequest, data.ErrIDBadRequest, "bad slot")
		return
	}
	var req struct {
		ToNode string `json:"to_node"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ToNode == "" {
		writeErr(w, http.StatusBadRequest, data.ErrIDBadRequest, "to_node required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60_000_000_000) // 60s
	defer cancel()
	if err := s.Engine.StartMigration(ctx, int32(slot), req.ToNode); err != nil {
		writeErr(w, http.StatusInternalServerError, 0, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handlePlan triggers an initial/rebalanced slot plan (controller only).
func (s *Server) handlePlan(w http.ResponseWriter, r *http.Request) {
	if err := s.Engine.SubmitCommand(&cluster.Command{Op: cluster.OpPlanSlots}); err != nil {
		if errors.Is(err, cluster.ErrNotLeader) {
			writeErr(w, http.StatusTooEarly, data.ErrIDNotLeader, "not controller")
			return
		}
		writeErr(w, http.StatusInternalServerError, 0, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, "ok")
}

// ---- helpers ---------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, code int, v any) {
	// Marshal to a buffer FIRST: a body we cannot encode must surface as a
	// 500, not a half-written 200 with an empty payload (silently corrupt
	// reads are far worse than an error).
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		http.Error(w, `{"error":"response encode failed"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(bytes.TrimRight(buf.Bytes(), "\n"))
}

func writeErr(w http.ResponseWriter, code, errID int, msg string) {
	writeJSON(w, code, map[string]any{"error": msg, "err_id": errID})
}
