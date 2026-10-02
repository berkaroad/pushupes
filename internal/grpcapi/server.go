// Package grpcapi serves the client-facing data plane (event writes and
// queries) over gRPC on its own port. Event traffic is gRPC-only: the HTTP
// port carries inter-node replication and admin endpoints exclusively.
// Routing, idempotency, version checks, HW-bounded reads and MOVED/ASK
// redirects all keep the semantics from DESIGN.md.
package grpcapi

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"pushupes/internal/cluster"
	"pushupes/internal/data"
	pushupesv1 "pushupes/internal/grpcapi/pushupes/v1"
	"pushupes/internal/lease"
	"pushupes/internal/storage"
)

// Server implements pushupes.v1.EventService.
type Server struct {
	pushupesv1.UnimplementedEventServiceServer

	engine *cluster.Engine
	store  *storage.Store
	logger *logrus.Entry

	// leaders pools EventService clients for cross-node read proxies
	// (this node holds neither the slot nor a replica of it).
	mu      sync.Mutex
	leaders map[string]pushupesv1.EventServiceClient
}

// NewServer wires the gRPC facade onto the engine + store.
func NewServer(eng *cluster.Engine, store *storage.Store, logger *logrus.Entry) *Server {
	return &Server{engine: eng, store: store, logger: logger,
		leaders: map[string]pushupesv1.EventServiceClient{}}
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

// ---- Append -----------------------------------------------------------------

func (s *Server) Append(ctx context.Context, req *pushupesv1.AppendRequest) (*pushupesv1.AppendResponse, error) {
	// The codec aliases the event bodies into the receive buffer: the lease
	// covers this handler, which is where the bytes reach the WAL.
	defer lease.Release(req)
	if req.AggregateId == "" || req.CommandId == "" || len(req.Events) == 0 {
		return s.fail(data.ErrIDBadRequest, "aggregate_id, command_id and at least one event are required", 0, 0, ""), nil
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

	resp, err := s.engine.SubmitAppend(ctx, rec)
	if err != nil {
		var redir *cluster.RedirectError
		if errors.As(err, &redir) {
			return s.fail(redir.Kind, "redirect", 0, redir.Slot, cluster.HostPort(redir.Addr)), nil
		}
		if errors.Is(err, data.ErrSlotNotLocal) {
			return s.fail(data.ErrIDSlotNotLocal, "slot not local", 0, 0, ""), nil
		}
		return nil, status.Errorf(codes.Internal, "append: %v", err)
	}
	return convertAppend(resp), nil
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
//  1. The ownership decision is made ONCE PER SLOT, not once per aggregate.
//     ReadProxyAddr clones the whole slot table, so calling it per aggregate
//     would repeat the expensive part of an already expensive scan.
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
			s.logger.WithError(err).WithField("addr", addr).
				Warn("read tails: proxy dial failed; reporting 0 for its aggregates")
			continue
		}
		sub, err := client.ReadTails(ctx, &pushupesv1.ReadTailsRequest{AggregateIds: b.ids})
		if err != nil {
			s.logger.WithError(err).WithField("addr", addr).
				Warn("read tails: proxy call failed; reporting 0 for its aggregates")
			continue
		}
		if len(sub.Versions) != len(b.ids) {
			s.logger.WithFields(map[string]any{"addr": addr, "want": len(b.ids), "got": len(sub.Versions)}).
				Warn("read tails: proxy returned a mismatched length; reporting 0 for its aggregates")
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
