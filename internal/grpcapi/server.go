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
	"runtime"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

	// batchJobs feeds the persistent slot-worker pool that executes every
	// BatchAppend's slot groups (see BatchAppend).
	batchJobs chan *batchJob
}

// batchPoolWorkers sizes the persistent slot-worker pool that executes
// every BatchAppend's slot groups: sixteen times the process CPU width
// (16 × runtime.GOMAXPROCS(0)), so the fan-out follows the machine the
// node runs on. A worker holds its slot for the whole SubmitBatch —
// mostly parked on the merged watermark wait, holding no CPU — so a pool
// the width of the CPU would queue waits that previously ran overlapped.
// Measured under multi-writer batch load, sixteen CPUs' worth of workers
// keeps the pool from throttling the load while still bounding concurrent
// WAL writers process-wide, and the
// long-lived workers keep their grown execution stacks warm across
// batches.
var batchPoolWorkers = 16 * runtime.GOMAXPROCS(0)

// batchJob is one slot group handed to the pool: the worker executes
// batchSlot over the group's record positions, fills their results, and
// signals the waiting BatchAppend handler.
type batchJob struct {
	s    *Server
	ctx  context.Context
	req  *pushupesv1.BatchAppendRequest
	out  []*pushupesv1.BatchAppendResult
	slot int32
	idx  []int
	done *sync.WaitGroup
}

func (j *batchJob) run() {
	defer j.done.Done()
	sub := make([]*pushupesv1.AppendRequest, len(j.idx))
	for k, i := range j.idx {
		sub[k] = j.req.Records[i]
	}
	resps := j.s.batchSlot(j.ctx, j.slot, sub)
	for k, i := range j.idx {
		j.out[i] = &pushupesv1.BatchAppendResult{
			AggregateId: j.req.Records[i].AggregateId,
			Response:    resps[k],
		}
	}
}

// batchWorker is one pool worker: it takes slot groups until the process
// ends (the pool is per-server and lives as long as the node).
func (s *Server) batchWorker() {
	for job := range s.batchJobs {
		job.run()
	}
}

// NewServer wires the gRPC facade onto the engine + store. selfClientAddr is
// this node's client-plane address (scheme-free "host:port"), used by
// PrefetchRoutes' fallback.
func NewServer(eng *cluster.Engine, store *storage.Store, logger *slog.Logger, selfClientAddr string) *Server {
	// A nil logger must not panic a handler half-way through: startup
	// validation here keeps the field safe for any handler that grows a
	// log line later.
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	s := &Server{engine: eng, store: store, logger: logger,
		selfClientAddr: cluster.HostPort(selfClientAddr),
		batchJobs:      make(chan *batchJob),
	}
	for i := 0; i < batchPoolWorkers; i++ {
		go s.batchWorker()
	}
	return s
}

// Register adds the service to a grpc.Server.
func (s *Server) Register(g *grpc.Server) {
	pushupesv1.RegisterEventServiceServer(g, s)
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
// slots run concurrently under the server's shared worker pool of
// batchPoolWorkers workers (16 × runtime.GOMAXPROCS(0): workers spend most
// of a slot group parked on the merged watermark wait, so the pool is
// wider than the CPU to keep waits overlapped; it bounds concurrent WAL
// writers process-wide and keeps its stacks warm across batches). The
// slot WAL lock already
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

	// Hand each slot group to the worker pool. A group executes as ONE
	// SubmitBatch: serial land in request order + one merged watermark wait
	// for the group. The blocking send queues groups in request order;
	// concurrent in-flight slots across all batches are exactly the pool
	// size.
	var wg sync.WaitGroup
	wg.Add(len(groups))
	for _, g := range groups {
		s.batchJobs <- &batchJob{s: s, ctx: ctx, req: req, out: out.Results, slot: g.slot, idx: g.idx, done: &wg}
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

// Reads redirect in-band exactly like writes: the node that can serve a
// slot — its leader, or a replica whose sync with the current epoch is
// proven — answers; every other node hands back err_id 1003 (MOVED) with
// the authoritative slot and the leader's client-plane node, and the
// client refreshes its routing row and retries. Nothing is relayed server
// to server: at data-plane scale the relay would double every read's
// handler cost and open one pooled connection per (entry, leader) pair
// the cluster never needs.

func (s *Server) ReadStream(ctx context.Context, req *pushupesv1.ReadStreamRequest) (*pushupesv1.ReadStreamResponse, error) {
	if req.AggregateId == "" {
		return nil, status.Error(codes.InvalidArgument, "aggregate_id required")
	}
	from := req.FromVersion
	if from == 0 {
		from = 1
	}
	slot := s.store.SlotOf(req.AggregateId)

	// Read admission: redirect when this node cannot be trusted to answer
	// locally (a non-holder, or a replica whose sync with the slot's current
	// leader is not yet proven); serve locally when it can — the leader up to
	// its own LEO, a synced replica up to the watermark it synced at, and a
	// synced-but-nothing-confirmed replica the empty answer. See
	// Engine.LocalRead for the rule and its reasoning. A node that cannot
	// serve answers the redirect itself — MOVED, no relay.
	uptoSeq, serveEmpty, redirect, _ := s.engine.LocalRead(slot)
	if redirect != "" {
		return &pushupesv1.ReadStreamResponse{
			ErrId: uint32(data.ErrIDSlotNotLocal), Slot: slot, Node: redirect,
		}, nil
	}
	var (
		recs []*data.EventRecord
		seqs []uint64
		err  error
	)
	if !serveEmpty {
		recs, seqs, err = s.store.ReadAggregate(req.AggregateId, from, req.Limit, uptoSeq)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "read: %v", err)
		}
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
// The answer shape is BatchAppend's: one result per requested aggregate, in
// request order, each echoing its aggregate_id and carrying its own outcome —
// a version when this node serves that slot, or that slot's own MOVED
// redirect. Routing and redirecting are PER ENTRY, so one call never lets an
// unreachable destination zero-fill another aggregate's answer; the fan-out
// (group by leader, re-send redirected entries after patching) lives in the
// client library.
//
// Two things keep the local half cheap:
//
//  1. The ownership decision is made ONCE PER SLOT, not once per aggregate:
//     routing is per slot, so one lookup per slot covers every aggregate that
//     shares it (a single table-lock read per distinct slot).
//  2. Aggregates this node can serve locally are answered in one storage
//     pass per entry, each bounded by that slot's admission ceiling.
//
// The visibility bound is ReadStream's: a leader reports what its own durable
// LEO covers, a proven replica what its synced watermark covers, and an
// unproven holder redirects rather than guesses.
func (s *Server) ReadTails(ctx context.Context, req *pushupesv1.ReadTailsRequest) (*pushupesv1.ReadTailsResponse, error) {
	out := &pushupesv1.ReadTailsResponse{Results: make([]*pushupesv1.ReadTailsResult, len(req.AggregateIds))}
	if len(req.AggregateIds) == 0 {
		return out, nil
	}

	// One admission lookup per distinct slot, then fill each entry from it.
	type verdict struct {
		serve      bool
		bound      uint64 // local visibility ceiling (0 = the leader's own LEO)
		serveEmpty bool
		redirect   string
		slot       int32
	}
	seen := map[int32]*verdict{}

	for i, id := range req.AggregateIds {
		res := &pushupesv1.ReadTailsResult{AggregateId: id}
		out.Results[i] = res
		if id == "" {
			continue // answered with version 0, same as before
		}
		slot := s.store.SlotOf(id)
		v := seen[slot]
		if v == nil {
			uptoSeq, serveEmpty, redirect, _ := s.engine.LocalRead(slot)
			v = &verdict{serve: redirect == "", bound: uptoSeq, serveEmpty: serveEmpty, redirect: redirect, slot: slot}
			seen[slot] = v
		}
		if !v.serve {
			res.ErrId = uint32(data.ErrIDSlotNotLocal)
			res.Slot = v.slot
			res.Node = v.redirect
			continue
		}
		if v.serveEmpty {
			continue // admitted but nothing confirmed: version 0
		}
		version, err := s.store.TailVersionOf(id, v.bound)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "read tails: %v", err)
		}
		res.Version = version
	}
	return out, nil
}

// ---- ReadByCommand --------------------------------------------------------------

func (s *Server) ReadByCommand(ctx context.Context, req *pushupesv1.ReadByCommandRequest) (*pushupesv1.ReadByCommandResponse, error) {
	if req.AggregateId == "" || req.CommandId == "" {
		return nil, status.Error(codes.InvalidArgument, "aggregate_id and command_id required")
	}
	slot := s.store.SlotOf(req.AggregateId)
	uptoSeq, serveEmpty, redirect, _ := s.engine.LocalRead(slot)
	if redirect != "" {
		return &pushupesv1.ReadByCommandResponse{
			ErrId: uint32(data.ErrIDSlotNotLocal), Slot: slot, Node: redirect,
		}, nil
	}
	var (
		rec *data.EventRecord
		seq uint64
		err error
	)
	if !serveEmpty {
		// the store's 0 convention is unbounded (the leader path); a synced
		// replica's ceiling is its watermark, and a record past it is NOT
		// confirmed here — report not-found rather than leak it.
		bound := uptoSeq
		if bound == 0 {
			bound = s.store.LastSeqOf(slot)
		}
		rec, seq, err = s.store.RecordByCommand(req.AggregateId, req.CommandId)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "by-command: %v", err)
		}
		if rec != nil && seq > bound {
			rec = nil
		}
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
// nor a proven-in-sync replica of it answers the MOVED redirect itself, and
// a local answer is bounded by the node's own durable LEO (never a
// watermark — the same silent-short-read reasoning as ReadStream).
func (s *Server) ReadVersionByTime(ctx context.Context, req *pushupesv1.ReadVersionByTimeRequest) (*pushupesv1.ReadVersionByTimeResponse, error) {
	if req.AggregateId == "" {
		return nil, status.Error(codes.InvalidArgument, "aggregate_id required")
	}
	slot := s.store.SlotOf(req.AggregateId)
	uptoSeq, serveEmpty, redirect, _ := s.engine.LocalRead(slot)
	if redirect != "" {
		return &pushupesv1.ReadVersionByTimeResponse{
			ErrId: uint32(data.ErrIDSlotNotLocal), Slot: slot, Node: redirect,
		}, nil
	}
	var err error
	v := uint32(0)
	if !serveEmpty {
		v, err = s.store.VersionAtOrBeforeTime(req.AggregateId, req.UnixTime, uptoSeq)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "version by time: %v", err)
		}
	}
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
