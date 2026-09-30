// Package api exposes the pushupes admin HTTP surface: management,
// diagnostics (pprof) and the slot-describe view. Client event
// reads/writes live on the gRPC data plane (internal/grpcapi); node-to-
// node replication/migration traffic lives on the peer-port PeerService
// gRPC plane (internal/cluster peersvc.go). Routing follows DESIGN.md §6.
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

	// admin
	s.mux.HandleFunc("GET /admin/cluster/status", s.handleClusterStatus)
	s.mux.HandleFunc("GET /admin/writes", s.handleWrites)
	s.mux.HandleFunc("GET /admin/slots/{slot}/describe", s.handleSlotDescribe)
	s.mux.HandleFunc("GET /admin/slots/{slot}/streams", s.handleSlotStreams)
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

// handleSlotStreams lists a slot's event streams — aggregate id plus the
// version of its latest committed record — out of the slot's in-memory index.
// The index is built when the slot is opened, from record frame heads only
// (data.DecodeRecordMeta), so answering reads no WAL file at all; a slot this
// node has never opened is reported as loaded=false instead of being opened
// here, which would walk every segment of it (storage.StreamPage has the
// reasoning). Pages are cursor walked: ?after=<aggregate_id>&limit=<n>.
func (s *Server) handleSlotStreams(w http.ResponseWriter, r *http.Request) {
	slot, err := strconv.ParseInt(r.PathValue("slot"), 10, 32)
	if err != nil {
		writeErr(w, http.StatusBadRequest, data.ErrIDBadRequest, "bad slot")
		return
	}
	limit := storage.DefaultStreamPage
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > storage.MaxStreamPage {
			writeErr(w, http.StatusBadRequest, data.ErrIDBadRequest, "bad limit")
			return
		}
		limit = n
	}
	page, err := s.Store.StreamPage(int32(slot), r.URL.Query().Get("after"), limit)
	if err != nil {
		writeErr(w, http.StatusNotFound, data.ErrIDBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"node":       s.Engine.Self(),
		"slot":       page.Slot,
		"loaded":     page.Loaded,
		"total":      page.Total,
		"streams":    page.Streams,
		"next_after": page.NextAfter,
	})
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

// handleWrites is the light per-slot poll target, without the 4096-entry slot
// table that rides /admin/cluster/status: the durable counters the console
// diffs every 2s to derive write rates, plus the per-slot gauges it shows as
// columns (WAL bytes, event stream count). Clients that poll it already hold
// the table from the slower status refresh.
func (s *Server) handleWrites(w http.ResponseWriter, r *http.Request) {
	bytesOnDisk, streams := s.Store.SlotGauges()
	writeJSON(w, http.StatusOK, map[string]any{
		"node":       s.Engine.Self(),
		"slot_count": s.Store.SlotCount,
		"writes":     s.Engine.WriteCounts(),
		// Console gauges, one entry per slot id: WAL bytes on disk and event
		// stream count. A slot this node has not loaded reads as zero instead
		// of being opened (that would scan every segment of it) — the console
		// takes the answer from whichever node holds the slot.
		"bytes":   bytesOnDisk,
		"streams": streams,
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
