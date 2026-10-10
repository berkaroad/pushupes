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

// Package grpcapi serves the client-facing data plane (event writes and
// queries) over gRPC on its own port. Event traffic is gRPC-only: the HTTP
// port carries inter-node replication and admin endpoints exclusively.
// Routing, idempotency, version checks, HW-bounded reads and MOVED/ASK
// redirects all keep the semantics from DESIGN.md.
package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/berkaroad/pushupes/internal/cluster"
	"github.com/berkaroad/pushupes/internal/data"
	"github.com/berkaroad/pushupes/internal/lease"
	"github.com/berkaroad/pushupes/internal/storage"
	pushupesv1 "github.com/berkaroad/pushupes/pkg/grpcapi/pushupes/v1"
)

// Server implements pushupes.v1.EventService.
type Server struct {
	pushupesv1.UnimplementedEventServiceServer

	engine *cluster.Engine
	store  *storage.Store
	logger *slog.Logger

	// selfClientAddr is this node's client-plane address ("host:port"),
	// the fallback entry PrefetchRoutes answers for slots the node cannot
	// route elsewhere (unassigned, or a leader that has not announced a
	// client address yet) — the same local-serve rule SubmitBatch applies.
	selfClientAddr string

	// batchParallelism caps how many slots one BatchAppend executes
	// concurrently (see BatchAppend).
	batchParallelism int

	// leaders pools EventService clients for cross-node read proxies
	// (this node holds neither the slot nor a replica of it).
	mu      sync.Mutex
	leaders map[string]pushupesv1.EventServiceClient
}

// DefaultBatchSlotParallelism is the concurrent-slot cap one BatchAppend
// fans out to; -batch-slot-parallelism raises or lowers it per node.
const DefaultBatchSlotParallelism = 100

// NewServer wires the gRPC facade onto the engine + store. A
// batchParallelism of zero or less means "the default cap" (callers that do
// not tune the knob pass 0). selfClientAddr is this node's client-plane
// address (scheme-free "host:port"), used by PrefetchRoutes' fallback.
func NewServer(eng *cluster.Engine, store *storage.Store, logger *slog.Logger, batchParallelism int, selfClientAddr string) *Server {
	// A nil logger must not panic a handler half-way through: the read path
	// logs (and continues) on a proxy failure, and a panic there replaces a
	// "proxy dial failed" warning with a crash that hides the real reason.
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if batchParallelism <= 0 {
		batchParallelism = DefaultBatchSlotParallelism
	}
	return &Server{engine: eng, store: store, logger: logger,
		selfClientAddr:   cluster.HostPort(selfClientAddr),
		batchParallelism: batchParallelism,
		leaders:          map[string]pushupesv1.EventServiceClient{}}
}

// Register adds the service to a grpc.Server.
func (s *Server) Register(g *grpc.Server) {
	pushupesv1.RegisterEventServiceServer(g, s)
}

// leaderClient dials (lazily, keep-alive) the leader's gRPC data plane.
func (s *Server) leaderClient(addr string) (pushupesv1.EventServiceClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.leaders[addr]; ok {
		return c, nil
	}
	// routing-table addresses carry a scheme; gRPC dials plain host:port
	conn, err := grpc.NewClient(cluster.HostPort(addr), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c := pushupesv1.NewEventServiceClient(conn)
	s.leaders[addr] = c
	return c, nil
}

// ---- Write ------------------------------------------------------------------

// The client plane has a single write entry, BatchAppend; a single write is a
// batch of one record. toRecord/s0fail below carry the per-record validation
// every write runs before touching the engine.

// toRecord converts one request into the domain record, answering nil plus a
// fail response when the request itself is invalid (the 1002 rule a rejected
// record applies before it reaches the engine).
func toRecord(req *pushupesv1.AppendRequest) (*data.EventRecord, *pushupesv1.AppendResponse) {
	if req.AggregateId == "" || req.CommandId == "" || len(req.Events) == 0 {
		return nil, s0fail("aggregate_id, command_id and at least one event are required")
	}
	rec := &data.EventRecord{
		AggregateID: req.AggregateId,
		Version:     req.Version,
		UnixTime:    req.UnixTime,
		CommandID:   req.CommandId,
	}
	rec.Events = make([]data.Event, len(req.Events))
	for i, ev := range req.Events {
		rec.Events[i] = data.Event{Type: ev.Type, Body: ev.Body}
	}
	if rec.UnixTime == 0 {
		rec.UnixTime = time.Now().Unix()
	}
	return rec, nil
}

// s0fail builds the pre-engine 1002 fail (slot 0, no node): the shape of a
// record rejected by argument validation.
func s0fail(msg string) *pushupesv1.AppendResponse {
	return &pushupesv1.AppendResponse{
		Status:  pushupesv1.AppendResponse_STATUS_FAIL,
		ErrId:   data.ErrIDBadRequest,
		Message: msg,
	}
}

// batchSlot executes one slot group of a BatchAppend: convert + validate the
// requests (bad ones answer 1002 and never enter the engine — the batch rule
// that a rejected record does not execute extends to invalid input), then run
// the rest through Engine.SubmitBatch, which lands them in request order and
// waits for the slot's watermark ONCE for the whole group. A slot-level
// redirect answers every record of the group (routing is per slot).
func (s *Server) batchSlot(ctx context.Context, slot int32, reqs []*pushupesv1.AppendRequest) []*pushupesv1.AppendResponse {
	out := make([]*pushupesv1.AppendResponse, len(reqs))
	recs := make([]*data.EventRecord, len(reqs))
	var live []int
	for i, q := range reqs {
		rec, failResp := toRecord(q)
		if failResp != nil {
			out[i] = failResp
			continue
		}
		recs[i] = rec
		live = append(live, i)
	}
	if len(live) == 0 {
		return out
	}
	group := make([]*data.EventRecord, len(live))
	for k, i := range live {
		group[k] = recs[i]
	}
	resps, err := s.engine.SubmitBatch(ctx, slot, group)
	if err != nil {
		// routing/internal outcome for the SLOT: every live record shares it
		var failResp *pushupesv1.AppendResponse
		var redir *cluster.RedirectError
		switch {
		case errors.As(err, &redir):
			failResp = s.fail(redir.Kind, "redirect", 0, redir.Slot, cluster.HostPort(redir.Addr))
		case errors.Is(err, data.ErrSlotNotLocal):
			failResp = s.fail(data.ErrIDSlotNotLocal, "slot not local", 0, 0, "")
		default:
			failResp = s.fail(0, fmt.Sprintf("append: %v", err), 0, 0, "")
		}
		for _, i := range live {
			out[i] = failResp
		}
		return out
	}
	for k, i := range live {
		out[i] = convertAppend(resps[k])
	}
	return out
}

// ---- BatchAppend -------------------------------------------------------------

// BatchAppend appends records in one call — the only write entry on the
// client plane, so a single write is a batch of one record. Each record gets its own
// result (success/exists/fail) in the SAME ORDER as the request. The batch
// must carry distinct aggregate_ids: every record sharing an aggregate_id
// with another record in the same batch fails (1002) and none of them is
// executed — a stream that would be written twice in one batch has no
// well-defined order, and partial execution could land one of the two and
// fail the other, leaving the retry semantics murky. Every other aggregate
// executes normally. An empty batch returns empty results.
//
// Execution is slot-parallel and slot-serial: records are grouped by their
// routing slot, one worker per slot runs them in request order through the
// same write path as the engine's single append (write fence, routing,
// idempotency, version check, HW wait), and distinct
// slots run concurrently under the server's shared cap
// (-batch-slot-parallelism, default DefaultBatchSlotParallelism). The slot WAL lock already
// serialises appends to one slot; keeping the HW waits serial per slot as
// well is what stops two batch records from racing to observe a watermark
// for the other's seq.
func (s *Server) BatchAppend(ctx context.Context, req *pushupesv1.BatchAppendRequest) (*pushupesv1.BatchAppendResponse, error) {
	// One lease for the whole request: its inner AppendRequest event bodies
	// alias the same receive buffer, and this handler is where they reach
	// the WAL.
	defer lease.Release(req)

	n := len(req.Records)
	out := &pushupesv1.BatchAppendResponse{Results: make([]*pushupesv1.BatchAppendResult, n)}
	if n == 0 {
		return out, nil
	}

	// Duplicate-aggregate detection first (map iteration order is random,
	// so detection must not depend on execution order): every record whose
	// aggregate_id appears more than once in the batch fails without
	// executing.
	counts := make(map[string]int, n)
	for _, r := range req.Records {
		counts[r.AggregateId]++
	}

	// Group the executable records by slot; positions index the response.
	type slotGroup struct {
		slot int32
		idx  []int
	}
	var groups []*slotGroup
	bySlot := make(map[int32]*slotGroup, 16)
	for i, r := range req.Records {
		if counts[r.AggregateId] > 1 {
			out.Results[i] = &pushupesv1.BatchAppendResult{
				AggregateId: r.AggregateId,
				Response: s.fail(data.ErrIDBadRequest,
					fmt.Sprintf("duplicate aggregate_id in batch: %s", r.AggregateId), 0, 0, ""),
			}
			continue
		}
		slot := s.store.SlotOf(r.AggregateId)
		g := bySlot[slot]
		if g == nil {
			g = &slotGroup{slot: slot}
			bySlot[slot] = g
			groups = append(groups, g)
		}
		g.idx = append(g.idx, i)
	}

	// Fan out per slot; the semaphore caps concurrent slots across the
	// batch. Each slot group executes as ONE SubmitBatch: serial land in
	// request order + one merged watermark wait for the group.
	sem := make(chan struct{}, s.batchParallelism)
	var wg sync.WaitGroup
	for _, g := range groups {
		wg.Add(1)
		go func(g *slotGroup) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			sub := make([]*pushupesv1.AppendRequest, len(g.idx))
			for k, i := range g.idx {
				sub[k] = req.Records[i]
			}
			resps := s.batchSlot(ctx, g.slot, sub)
			for k, i := range g.idx {
				out.Results[i] = &pushupesv1.BatchAppendResult{
					AggregateId: req.Records[i].AggregateId,
					Response:    resps[k],
				}
			}
		}(g)
	}
	wg.Wait()
	return out, nil
}

// ---- PrefetchRoutes ----------------------------------------------------------

// PrefetchRoutes answers the whole slot->node write-routing table from this
// node's Raft-replicated placement view: any node serves it (no forwarding,
// no leader requirement), and it never touches the WAL. The answer is a
// routing HINT — write correctness is defined by the MOVED/ASK self-heal in
// BatchAppend responses, not by this table's freshness — so clients call it
// once at startup, refresh it periodically, and patch the row a redirect
// names in between.
func (s *Server) PrefetchRoutes(ctx context.Context, req *pushupesv1.PrefetchRoutesRequest) (*pushupesv1.PrefetchRoutesResponse, error) {
	rows := s.engine.SlotRouteTable()
	out := &pushupesv1.PrefetchRoutesResponse{
		// slot_node_index is dense over slots: its length IS the cluster's
		// slot count, which the client reads back from here instead of
		// hard-coding a constant it has no business knowing.
		SlotNodeIndex: make([]int32, len(rows)),
	}
	// Addresses de-duplicate in first-appearance order; the fallback entry
	// (unassigned slot, or a leader whose registration has not landed) is
	// this node's own client addr — the same local-serve rule SubmitBatch
	// applies, so the array has no holes and needs no sentinel handling.
	seen := map[string]int32{}
	add := func(addr string) int32 {
		if i, ok := seen[addr]; ok {
			return i
		}
		i := int32(len(out.NodeClientAddrs))
		seen[addr] = i
		out.NodeClientAddrs = append(out.NodeClientAddrs, addr)
		return i
	}
	for slot, addr := range rows {
		if addr == "" {
			addr = s.selfClientAddr
		}
		out.SlotNodeIndex[slot] = add(cluster.HostPort(addr))
	}
	return out, nil
}

func convertAppend(r *data.AppendResponse) *pushupesv1.AppendResponse {
	out := &pushupesv1.AppendResponse{
		Status:         appendStatus(r.Status),
		ErrId:          uint32(r.ErrID),
		Message:        r.Err,
		CurrentVersion: r.CurrentVersion,
		Seq:            r.Seq,
		Slot:           r.Slot,
		Node:           r.Node,
	}
	if r.Record != nil {
		// the stored record is a plain EventRecord: raw bytes end to end,
		// no JSON-plane body wrapping on the gRPC surface
		out.Record = recordToProto(r.Record, r.Seq)
	}
	return out
}

func appendStatus(st string) pushupesv1.AppendResponse_Status {
	switch st {
	case data.StatusSuccess:
		return pushupesv1.AppendResponse_STATUS_SUCCESS
	case data.StatusExists:
		return pushupesv1.AppendResponse_STATUS_EXISTS
	default:
		return pushupesv1.AppendResponse_STATUS_FAIL
	}
}

func (s *Server) fail(errID int, msg string, currentVersion uint32, slot int32, node string) *pushupesv1.AppendResponse {
	return &pushupesv1.AppendResponse{
		Status:         pushupesv1.AppendResponse_STATUS_FAIL,
		ErrId:          uint32(errID),
		Message:        msg,
		CurrentVersion: currentVersion,
		Slot:           slot,
		Node:           node,
	}
}

// ---- ReadStream ---------------------------------------------------------------

func (s *Server) ReadStream(ctx context.Context, req *pushupesv1.ReadStreamRequest) (*pushupesv1.ReadStreamResponse, error) {
	if req.AggregateId == "" {
		return nil, status.Error(codes.InvalidArgument, "aggregate_id required")
	}
	from := req.FromVersion
	if from == 0 {
		from = 1
	}
	slot := s.store.SlotOf(req.AggregateId)

	// This node holds neither the slot nor a replica of it: proxy the read
	// to the slot leader's gRPC data plane.
	if addr := s.engine.ReadProxyAddr(slot); addr != "" {
		client, err := s.leaderClient(addr)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "read proxy dial: %v", err)
		}
		return client.ReadStream(ctx, &pushupesv1.ReadStreamRequest{
			AggregateId: req.AggregateId, FromVersion: from, Limit: req.Limit,
		})
	}

	// A leader owns its log: it reads up to its own durable LEO. Bounding a
	// leader read by the replication high watermark would hide records it has
	// already accepted — and therefore already acknowledged — because
	// the HW can lag the LEO whenever a replica falls behind, turning a
	// complete stream into a SILENT short read.
	//
	// A replica's local read is bounded by its own durable LEO as well. The
	// watermark is leader-side bookkeeping, so HW() reports it only for a
	// slot this node currently leads and 0 for every other node — a replica
	// (including one that led this slot before stepping down) falls through
	// to its local LEO and returns everything its own log holds, never a
	// frozen former-leader watermark.
	hw := uint64(0)
	if !s.engine.Leads(slot) {
		hw = s.engine.HW(slot) // 0 unless this node leads the slot
		if hw == 0 {
			hw = s.store.LastSeqOf(slot) // bound by the durable local LEO
		}
	}
	recs, seqs, err := s.store.ReadAggregate(req.AggregateId, from, req.Limit, hw)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read: %v", err)
	}
	out := &pushupesv1.ReadStreamResponse{Records: make([]*pushupesv1.Record, len(recs))}
	for i, rec := range recs {
		var sq uint64
		if i < len(seqs) {
			sq = seqs[i]
		}
		out.Records[i] = recordToProto(rec, sq)
	}
	out.NextVersion = from + uint32(len(recs))
	out.LastVersion = out.NextVersion - 1
	if len(recs) == 0 {
		out.NextVersion, out.LastVersion = from, from-1
	}
	return out, nil
}

// ---- ReadTails -----------------------------------------------------------------

// ReadTails answers the latest version of many aggregates in one call: the bulk
// form of "what is this stream's tail", which a resume scan needs per stream.
//
// Two things keep it from being N single probes wearing a trench coat:
//
//  1. The ownership decision is made ONCE PER SLOT, not once per aggregate:
//     routing is per slot, so one lookup per slot covers every aggregate that
//     shares it (LeaderReplica, a single table-lock read).
//  2. Aggregates this node can serve locally are answered in one storage pass;
//     only the ones it holds neither slot nor replica for are forwarded, and
//     those are grouped by destination so each peer gets ONE RPC carrying all
//     of its aggregates.
//
// The visibility bound is ReadStream's: a node answering locally reports what
// its own durable LEO covers. Server-side we never bound by the replication
// watermark, because this node is a replica for every slot it can answer from
// and a stale watermark would silently under-report.
func (s *Server) ReadTails(ctx context.Context, req *pushupesv1.ReadTailsRequest) (*pushupesv1.ReadTailsResponse, error) {
	out := &pushupesv1.ReadTailsResponse{Versions: make([]uint32, len(req.AggregateIds))}
	if len(req.AggregateIds) == 0 {
		return out, nil
	}

	// Group by slot first: the slot decides routing, so every aggregate that
	// shares a slot shares a destination.
	bySlot := map[int32][]int{}
	slots := make([]int32, 0, len(req.AggregateIds))
	for i, id := range req.AggregateIds {
		if id == "" {
			continue // answered with 0
		}
		slot := s.store.SlotOf(id)
		if _, ok := bySlot[slot]; !ok {
			slots = append(slots, slot)
		}
		bySlot[slot] = append(bySlot[slot], i)
	}

	// One ownership lookup per slot, and a peer grouping so each destination
	// gets a single forwarded RPC.
	type fwdBatch struct {
		idx  []int // positions in req.AggregateIds
		ids  []string
		addr string
	}
	fwd := map[string]*fwdBatch{}
	var fwdOrder []string
	var localIdx []int

	for _, slot := range slots {
		idx := bySlot[slot]
		if addr := s.engine.ReadProxyAddr(slot); addr == "" {
			localIdx = append(localIdx, idx...)
			continue
		} else {
			b := fwd[addr]
			if b == nil {
				b = &fwdBatch{addr: addr}
				fwd[addr] = b
				fwdOrder = append(fwdOrder, addr)
			}
			for _, i := range idx {
				b.idx = append(b.idx, i)
				b.ids = append(b.ids, req.AggregateIds[i])
			}
		}
	}

	// Local answers: one slot lookup per aggregate inside the store, but no
	// routing clone and no extra RPC.
	for _, i := range localIdx {
		v, err := s.store.TailVersionOf(req.AggregateIds[i], 0)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "read tails: %v", err)
		}
		out.Versions[i] = v
	}

	// Forwarded answers: one RPC per destination, carrying all of its
	// aggregates. A failed destination leaves its slots at 0 (the caller's
	// contract: 0 means "nothing visible here"), matching the per-aggregate
	// probe which also answered 0 on a transport error.
	for _, addr := range fwdOrder {
		b := fwd[addr]
		client, err := s.leaderClient(addr)
		if err != nil {
			s.logger.Warn("read tails: proxy dial failed; reporting 0 for its aggregates", "addr", addr, "error", err)
			continue
		}
		sub, err := client.ReadTails(ctx, &pushupesv1.ReadTailsRequest{AggregateIds: b.ids})
		if err != nil {
			s.logger.Warn("read tails: proxy call failed; reporting 0 for its aggregates", "addr", addr, "error", err)
			continue
		}
		if len(sub.Versions) != len(b.ids) {
			s.logger.Warn("read tails: proxy returned a mismatched length; reporting 0 for its aggregates",
				"addr", addr, "want", len(b.ids), "got", len(sub.Versions))
			continue
		}
		for n, i := range b.idx {
			out.Versions[i] = sub.Versions[n]
		}
	}
	return out, nil
}

// ---- ReadByCommand --------------------------------------------------------------

func (s *Server) ReadByCommand(ctx context.Context, req *pushupesv1.ReadByCommandRequest) (*pushupesv1.ReadByCommandResponse, error) {
	if req.AggregateId == "" || req.CommandId == "" {
		return nil, status.Error(codes.InvalidArgument, "aggregate_id and command_id required")
	}
	slot := s.store.SlotOf(req.AggregateId)
	if addr := s.engine.ReadProxyAddr(slot); addr != "" {
		client, err := s.leaderClient(addr)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "read proxy dial: %v", err)
		}
		return client.ReadByCommand(ctx, &pushupesv1.ReadByCommandRequest{
			AggregateId: req.AggregateId, CommandId: req.CommandId,
		})
	}
	rec, seq, err := s.store.RecordByCommand(req.AggregateId, req.CommandId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "by-command: %v", err)
	}
	if rec == nil {
		return &pushupesv1.ReadByCommandResponse{Found: false}, nil
	}
	return &pushupesv1.ReadByCommandResponse{Found: true, Record: recordToProto(rec, seq)}, nil
}

// ---- ReadVersionByTime ----------------------------------------------------------

// ReadVersionByTime reports the point-in-time version anchor of one aggregate:
// the highest version whose record is stamped at or before the requested
// unix_time, or 0 when nothing visible predates it. The client then replays
// history from that version with ReadStream.
//
// Routing and visibility are ReadStream's: a node holding neither the slot
// nor a replica proxies to the slot leader's client plane, and a local answer
// is bounded by the node's own durable LEO (never a watermark — the same
// silent-short-read reasoning as ReadStream).
func (s *Server) ReadVersionByTime(ctx context.Context, req *pushupesv1.ReadVersionByTimeRequest) (*pushupesv1.ReadVersionByTimeResponse, error) {
	if req.AggregateId == "" {
		return nil, status.Error(codes.InvalidArgument, "aggregate_id required")
	}
	slot := s.store.SlotOf(req.AggregateId)
	if addr := s.engine.ReadProxyAddr(slot); addr != "" {
		client, err := s.leaderClient(addr)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "read proxy dial: %v", err)
		}
		return client.ReadVersionByTime(ctx, &pushupesv1.ReadVersionByTimeRequest{
			AggregateId: req.AggregateId, UnixTime: req.UnixTime,
		})
	}
	hw := uint64(0)
	if !s.engine.Leads(slot) {
		hw = s.engine.HW(slot) // 0 unless this node leads the slot
		if hw == 0 {
			hw = s.store.LastSeqOf(slot) // bound by the durable local LEO
		}
	}
	v, err := s.store.VersionAtOrBeforeTime(req.AggregateId, req.UnixTime, hw)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "version by time: %v", err)
	}
	return &pushupesv1.ReadVersionByTimeResponse{Version: v}, nil
}

// ---- record conversion -----------------------------------------------------

// recordToProto maps a stored record straight to its proto form: bodies are
// raw bytes end to end, no JSON wrapping on this plane.
func recordToProto(r *data.EventRecord, seq uint64) *pushupesv1.Record {
	out := &pushupesv1.Record{
		AggregateId: r.AggregateID,
		Version:     r.Version,
		UnixTime:    r.UnixTime,
		CommandId:   r.CommandID,
		Seq:         seq,
	}
	out.Events = make([]*pushupesv1.Event, len(r.Events))
	for i, ev := range r.Events {
		out.Events[i] = &pushupesv1.Event{Type: ev.Type, Body: ev.Body}
	}
	return out
}
