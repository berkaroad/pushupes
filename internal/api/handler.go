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
	s.mux.HandleFunc("POST /admin/slots/{slot}/remove-replica", s.handleRemoveReplica)
	s.mux.HandleFunc("POST /admin/cluster/plan", s.handlePlan)
	s.mux.HandleFunc("GET /admin/cluster/nodes", s.handleMembers)
	s.mux.HandleFunc("POST /admin/cluster/nodes", s.handleAddMember)
	s.mux.HandleFunc("DELETE /admin/cluster/nodes/{id}", s.handleRemoveMember)

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
	// The whole slot table is a few hundred KB of highly repetitive JSON at the
	// default slot count; gzip cuts
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

// handleSlotDescribe answers with THIS node's view of one slot. Every number is
// local, which is why the response says which node answered and what role it
// plays: hw/isr only exist on the slot's leader (they are tracked from the
// replica progress reports it receives), and last_seq/segments/total_bytes come
// from the local copy of the slot. The console therefore asks the holder (leader
// first) rather than whichever node it happens to be proxied to.
//
// The slot is deliberately NOT opened here: Slot() creates the directory on a
// node that does not hold the slot and walks every segment of it, and a read-only
// view has no business doing either. A cold slot reports loaded=false with
// zeroed numbers instead.
func (s *Server) handleSlotDescribe(w http.ResponseWriter, r *http.Request) {
	slot, err := strconv.ParseInt(r.PathValue("slot"), 10, 32)
	if err != nil {
		writeErr(w, http.StatusBadRequest, data.ErrIDBadRequest, "bad slot")
		return
	}
	sl, err := s.Store.SlotIfLoaded(int32(slot))
	if err != nil {
		writeErr(w, http.StatusNotFound, data.ErrIDBadRequest, err.Error())
		return
	}
	tbl := s.Engine.TableSnapshot()
	p := tbl.Slots[int32(slot)]
	self := s.Engine.Self()
	role := "none"
	switch {
	case p.Leader == self:
		role = "leader"
	default:
		for _, rep := range p.Replicas {
			if rep == self {
				role = "replica"
				break
			}
		}
	}
	var lastSeq, totalBytes int64
	var segments int
	if sl != nil {
		lastSeq = int64(sl.LastSeq())
		segments = sl.SegmentCount()
		totalBytes = sl.TotalSize()
	}
	// pending_drop_at: 0, or the unix second at which THIS node will
	// automatically drop its local copy of the slot (the post-migration
	// retention window). The copy is no longer part of the slot's replica
	// set, so it is surplus data waiting to be cleaned up.
	pendingDropAt := int64(0)
	if t, ok := s.Engine.PendingDrop(int32(slot)); ok {
		pendingDropAt = t.Unix()
	}
	// The copy's digest — the same sums the leader rebalancer and the
	// migration fence compare: how many aggregates the slot holds, the sum of
	// their claimed versions, and the sum of the seqs the directory can
	// actually resolve. A copy whose last_seq sits at the leader's while
	// versions/resolvable are lower is structurally short: its WAL holds
	// records its aggregate directory never adopted, so those records cannot
	// be read on this node. Surfacing it here is what makes that visible
	// without a leader round to notice it (see storage.Slot.SlotDigest).
	digestAgg, digestVers, digestResolvable := s.Store.SlotDigest(int32(slot))
	writeJSON(w, http.StatusOK, map[string]any{
		"slot":        slot,
		"node":        self,
		"role":        role,
		"loaded":      sl != nil,
		"last_seq":    lastSeq,
		"hw":          s.Engine.HW(int32(slot)),
		"segments":    segments,
		"total_bytes": totalBytes,
		"placement":   p,
		"isr":         s.Engine.ISR(int32(slot)),
		"writes":      s.Store.WriteCount(int32(slot)),
		"aggregates":  digestAgg,
		"versions":    digestVers,
		"resolvable":  digestResolvable,
		// A non-zero value means this node has queued the automatic
		// post-migration cleanup of its local copy of the slot.
		"pending_drop_at": pendingDropAt,
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

// handleClusterStatus answers from THIS node's view. controller_* is the
// machine-readable redirect the console follows to pin its traffic to the
// Raft leader: id + admin address resolved from the replicated peer directory
// (empty while no leader is elected or the leader has not registered yet).
func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	tbl := s.Engine.TableSnapshot()
	leaderID, leaderAdmin := s.Engine.Controller()
	writeJSON(w, http.StatusOK, map[string]any{
		"node":       s.Engine.Self(),
		"raft":       s.Engine.RaftStats(),
		"peers":      tbl.Peers,
		"slots":      tbl.Slots,
		"slot_count": tbl.SlotCount,
		// replica_factor is the per-slot copy count the controller derives from
		// the member count (cluster.ReplicaCountForMembers): it is not a knob,
		// so this is the only place an operator can read it back.
		"replica_factor":        tbl.Replicas,
		"controller":            leaderID,
		"controller_admin_addr": leaderAdmin,
		// durable writes per slot since this process started; clients diff
		// successive snapshots to derive write rates (msg/sec).
		"writes": s.Engine.WriteCounts(),
	})
}

// handleWrites is the light per-slot poll target, without the whole slot
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
		// Post-migration cleanup queue of THIS node: 0, or the unix second at
		// which this node's local copy of the slot is due to be dropped. It is
		// per-node by nature (only the node holding the copy knows), so the
		// console asks every node and marks the replica whose copy is on its
		// way out. Same shape as bytes/streams.
		"dropping": s.Engine.PendingDrops(),
	})
}

// handleMigrate stages a hot migration. It is a controller-only (Raft leader)
// command: a follower refuses it with the controller's identity instead of
// forwarding, so the console reads status, takes the controller's admin
// address and posts here directly.
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
		if refuseNotController(w, err) {
			return
		}
		writeErr(w, http.StatusInternalServerError, 0, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleRemoveReplica reclaims one member from a slot's replica set — the
// counterpart of the admission handleMigrate performs for a target outside the
// set. A committed migration now reclaims its own surplus former-source copy
// automatically, so this endpoint is the OPERATOR-facing tool kept for manual
// cleanup: it is the fallback when an automatic reclaim could not run, the
// rollback for a staged admission, and the remedy for any other surplus.
// Like handleMigrate it is a controller-only command, so it must
// be posted to the Raft leader; a follower refuses it with the controller's
// node id and admin address instead of forwarding it. Its error codes match
// handleMigrate's. The
// removed node's local copy is deliberately NOT deleted — an operator drops it.
func (s *Server) handleRemoveReplica(w http.ResponseWriter, r *http.Request) {
	slot, err := strconv.ParseInt(r.PathValue("slot"), 10, 32)
	if err != nil {
		writeErr(w, http.StatusBadRequest, data.ErrIDBadRequest, "bad slot")
		return
	}
	var req struct {
		Node string `json:"node"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Node == "" {
		writeErr(w, http.StatusBadRequest, data.ErrIDBadRequest, "node required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30_000_000_000) // 30s
	defer cancel()
	if err := s.Engine.RemoveReplica(ctx, int32(slot), req.Node); err != nil {
		if refuseNotController(w, err) {
			return
		}
		writeErr(w, http.StatusInternalServerError, 0, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handlePlan triggers an initial/rebalanced slot plan (controller only). A
// follower refuses with the controller's node id and admin address (the client
// retries there) rather than forwarding it.
func (s *Server) handlePlan(w http.ResponseWriter, r *http.Request) {
	if err := s.Engine.SubmitCommand(&cluster.Command{Op: cluster.OpPlanSlots}); err != nil {
		if refuseNotController(w, err) {
			return
		}
		writeErr(w, http.StatusInternalServerError, 0, err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleMembers lists the cluster's raft membership. It is local view, like
// the rest of the admin surface: a node that has just been added shows up here
// once the configuration change has reached this node's log.
func (s *Server) handleMembers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"node":    s.Engine.Self(),
		"members": s.Engine.Members(),
	})
}

// handleAddMember adds a node to the running cluster. Controller-only: a
// follower refuses with the controller's node id and admin address (the client
// retries there) — nothing is forwarded, exactly like the other cluster
// commands.
//
//	POST /admin/cluster/nodes  {"id":"node-4","peer_addr":"10.0.0.4:8394"}
//
// Only the peer (consensus) address is given: the node announces its admin and
// client addresses itself once it is a member.
func (s *Server) handleAddMember(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID       string `json:"id"`
		PeerAddr string `json:"peer_addr"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" || req.PeerAddr == "" {
		writeErr(w, http.StatusBadRequest, data.ErrIDBadRequest, "id and peer_addr required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), cluster.AddMemberTimeout+5_000_000_000)
	defer cancel()
	if err := s.Engine.AddMember(ctx, req.ID, req.PeerAddr); err != nil {
		if refuseNotController(w, err) {
			return
		}
		if errors.Is(err, cluster.ErrBadMember) {
			writeErr(w, http.StatusBadRequest, data.ErrIDBadRequest, err.Error())
			return
		}
		writeErr(w, http.StatusInternalServerError, 0, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"node": s.Engine.Self(), "member": req.ID, "change": "added"})
}

// handleRemoveMember drops a node from the running cluster. Controller-only,
// like handleAddMember. The node keeps serving its slots until the
// configuration change reaches it (the consensus layer waits for it to
// acknowledge the entry), then the controller moves their leadership to live
// replicas and the rebalancer refills the replica sets.
//
//	DELETE /admin/cluster/nodes/node-4
func (s *Server) handleRemoveMember(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeErr(w, http.StatusBadRequest, data.ErrIDBadRequest, "id required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), cluster.AddMemberTimeout+5_000_000_000)
	defer cancel()
	if err := s.Engine.RemoveMember(ctx, id); err != nil {
		if refuseNotController(w, err) {
			return
		}
		if errors.Is(err, cluster.ErrBadMember) {
			writeErr(w, http.StatusBadRequest, data.ErrIDBadRequest, err.Error())
			return
		}
		writeErr(w, http.StatusInternalServerError, 0, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"node": s.Engine.Self(), "member": id, "change": "removed"})
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

// refuseNotController renders a controller-only command that a FOLLOWER
// refused, and reports whether it handled the error. Nothing is forwarded: the
// response instead says which node is the controller — its node id and admin
// address — so the caller can retry the command directly against it. The two
// extra fields are the machine-readable form of the same fact (a client should
// not have to parse the prose to find the node to retry on); the status code
// matches the one handlePlan has always used for this case.
func refuseNotController(w http.ResponseWriter, err error) bool {
	var nc *cluster.NotControllerError
	if !errors.As(err, &nc) {
		return false
	}
	writeJSON(w, http.StatusTooEarly, map[string]any{
		"error":                 nc.Error(),
		"err_id":                data.ErrIDNotLeader,
		"controller":            nc.LeaderID,
		"controller_admin_addr": nc.AdminAddr,
	})
	return true
}
