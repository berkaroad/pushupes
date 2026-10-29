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

// PeerService: the unified inter-node data plane over gRPC.
//
// All node-to-node traffic — replica fetch, LEO progress, migration
// forwarding/segments/snapshot, liveness probes and address registration —
// is served by this gRPC service ON THE RAFT PEER PORT. The peer listener
// demuxes connections by their first byte: 'P' (the HTTP/2 client preface
// "PRI * HTTP/2.0" starts with it) is handed to the gRPC server, every
// other byte to the Raft transport. Operators still configure exactly one
// port per node; the admin port keeps only management + pprof.
//
// The client side (peerClient) caches one lazily dialed grpc.ClientConn
// per peer address, so a fetch session and a liveness probe against the
// same leader share one HTTP/2 connection.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/berkaroad/pushupes/internal/config"
	pushupesv1 "github.com/berkaroad/pushupes/internal/grpcapi/pushupes/v1"
	"github.com/berkaroad/pushupes/internal/lease"
)

const (
	// grpcMagicByte is the first byte of the HTTP/2 preface ("PRI ...")
	// that every gRPC connection starts with. Raft's wire protocol begins
	// with an RPC version byte (0), so the classification never collides.
	grpcMagicByte = byte('P')
)

// peerServer adapts Engine onto pushupesv1.PeerServiceServer. The adapter
// is stateless: every RPC delegates to the same Engine logic the old HTTP
// internal endpoints drove.
// peerServer implements BOTH halves of the inter-node surface. They are
// separate services in the proto because they answer different questions —
// membership and layout vs slot content — but one node has one code path for
// each RPC, so a single type satisfies both interfaces and ServePeer registers
// it twice.
type peerServer struct {
	pushupesv1.UnimplementedControllerPeerServiceServer
	pushupesv1.UnimplementedDataPeerServiceServer
	e *Engine
}

// NewPeerServer returns the gRPC implementation of the inter-node plane.
func (e *Engine) NewPeerServer() *peerServer {
	return &peerServer{e: e}
}

func (s *peerServer) Ping(context.Context, *pushupesv1.PingRequest) (*pushupesv1.PingResponse, error) {
	return &pushupesv1.PingResponse{Node: s.e.self}, nil
}

// ReportUnreachable takes one observer's evidence that a node stopped answering
// at the transport level. It is a witness, not a verdict: only the controller
// acts, and only on a quorum (see recordUnreachable); a report that lands on a
// follower is dropped, because the observer keeps reporting while the failure
// lasts and the liveness sweep remains the fallback.
func (s *peerServer) ReportUnreachable(_ context.Context, req *pushupesv1.ReportUnreachableRequest) (*pushupesv1.ReportUnreachableResponse, error) {
	s.e.recordUnreachable(req.Reporter, req.Suspect)
	return &pushupesv1.ReportUnreachableResponse{}, nil
}

func (s *peerServer) Register(_ context.Context, req *pushupesv1.RegisterRequest) (*pushupesv1.RegisterResponse, error) {
	reg := &Registration{ID: req.Id, AdminAddr: req.AdminAddr, ClientAddr: req.ClientAddr, Role: req.GetRole()}
	if err := s.e.acceptRegistration(reg); err != nil {
		return nil, err
	}
	return &pushupesv1.RegisterResponse{}, nil
}

func (s *peerServer) Adopt(ctx context.Context, req *pushupesv1.AdoptRequest) (*pushupesv1.AdoptResponse, error) {
	if err := s.e.acceptAdoption(ctx, &Adoption{ID: req.Id, PeerAddr: req.PeerAddr, Role: req.GetRole()}); err != nil {
		// A clear refusal travels as InvalidArgument: the offering node's
		// joiner loop treats it as fatal (exit) instead of rotating to the
		// next seed. Everything else — no leader yet, relay unreachable —
		// stays retryable.
		if errors.Is(err, ErrBadMember) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, err
	}
	return &pushupesv1.AdoptResponse{}, nil
}

func (s *peerServer) MFetch(ctx context.Context, req *pushupesv1.MFetchRequest) (*pushupesv1.MFetchResponse, error) {
	internal := &MFetchRequest{Follower: req.Follower, WaitMS: req.WaitMs, Slots: req.Slots, FromSeqs: req.FromSeqs, Sweep: req.Sweep}
	resp, err := s.e.HandleMFetchCtx(ctx, *internal)
	if err == nil && ctx.Err() != nil {
		// The round produced an answer, but the caller was already gone: the
		// response cannot reach it either. (A handler that gave up on a
		// cancelled context returns an error itself, but a round that had data
		// ready returns normally — and that is the common shape on a busy
		// leader, so this must be checked too.)
		s.e.noteFollowerSessionLost(req.Follower, req.Slots)
	}
	if err != nil {
		// The follower's long poll ended without an answer: its connection is
		// gone (a clean round returns a response). The slots it was reporting
		// for must stop waiting on it at once — see noteFollowerSessionLost.
		s.e.noteFollowerSessionLost(req.Follower, req.Slots)
		return nil, err
	}
	return &pushupesv1.MFetchResponse{
		Follower: resp.Follower,
		Items:    protoItems(resp.Items),
		HwSlots:  resp.HwSlots,
		HwValues: resp.HwValues,
	}, nil
}

func (s *peerServer) ReplicaProgress(_ context.Context, req *pushupesv1.ReplicaProgressRequest) (*pushupesv1.ReplicaProgressResponse, error) {
	t0 := time.Now()
	prog := progressBatch{follower: req.Follower, stamp: req.Stamp, now: time.Now()}
	for i, slot := range req.Slots {
		if i < len(req.FromSeqs) && slot >= 0 && req.FromSeqs[i] > 0 {
			prog.add(slot, req.FromSeqs[i]-1)
		}
	}
	prog.apply(s.e)
	// The leader's half of the acknowledgement hop: what a follower's report
	// costs before the parked calls can see the new watermark.
	s.e.ack.progressHandle.observe(time.Since(t0))
	// Zero-lag read-admission echo: the post-report watermark of EVERY slot
	// this report covered rides back on the same response (see the proto
	// comment). The append that got acknowledged by exactly this report is
	// visible to THIS replica's local reads on return — no fetch round in
	// between. Slots without a repl entry answer 0 (a leader that just took
	// the slot over: the replica re-proves on the next echo or fetch ledger).
	// Both arrays are built in the same iteration: the request's parallel
	// arrays can be ragged (a malformed entry skipped), and truncating one
	// after the fact would mis-align the pairs the follower indexes by slot.
	out := &pushupesv1.ReplicaProgressResponse{}
	s.e.replMu.Lock()
	for i, slot := range req.Slots {
		if i >= len(req.FromSeqs) || slot < 0 || req.FromSeqs[i] == 0 {
			continue // not a position the batch recorded
		}
		hw := uint64(0)
		if sr := s.e.repl[slot]; sr != nil {
			hw = sr.hw
		}
		out.HwSlots = append(out.HwSlots, slot)
		out.HwValues = append(out.HwValues, hw)
	}
	s.e.replMu.Unlock()
	return out, nil
}

func (s *peerServer) Replicate(_ context.Context, req *pushupesv1.ReplicateRequest) (*pushupesv1.ReplicateResponse, error) {
	// req.Payload aliases the receive buffer until the lease is released, which
	// has to be after the append it feeds.
	defer lease.Release(req)
	if err := s.e.HandleReplicate(req.Slot, req.Seq, req.Payload); err != nil {
		return nil, err
	}
	return &pushupesv1.ReplicateResponse{}, nil
}

// ReplicateAggregate streams one aggregate's records out of a slot so a
// generation migration can replay them into the slot the new segment routes them
// to (DESIGN §4.5.8). The caller pulls: the DESTINATION asks the source, which
// keeps the flow control on the side that has to store the bytes.
//
// Records leave encoded and in version order, and the cursor is the aggregate
// version — not a slot offset — because that is the only identity that survives
// the re-routing: one source slot's aggregates scatter across several
// destination slots, and a replay that resumed on a byte offset would be
// meaningless on the other side. The last chunk carries NextVersion = 0.
func (s *peerServer) ReplicateAggregate(req *pushupesv1.ReplicateAggregateRequest, stream pushupesv1.DataPeerService_ReplicateAggregateServer) error {
	const defRecords, defBytes = 256, 1 << 20
	maxRecords := int(req.GetMaxRecords())
	if maxRecords <= 0 {
		maxRecords = defRecords
	}
	maxBytes := int(req.GetMaxBytes())
	if maxBytes <= 0 {
		maxBytes = defBytes
	}
	from := uint32(req.GetFromVersion())
	if from == 0 {
		from = 1
	}
	for {
		recs, _, err := s.e.store.ReadAggregate(req.GetSlot(), req.GetAggregateId(), from, uint64(maxRecords), req.GetUptoSeq())
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			return nil // nothing (left) to replay: the aggregate is done
		}
		chunk := &pushupesv1.ReplicateAggregateResponse{}
		size := 0
		var buf []byte
		for i, rec := range recs {
			buf = rec.EncodeBinary(buf[:0])
			if i > 0 && size+len(buf) > maxBytes {
				recs = recs[:i] // the byte budget ends the chunk here
				break
			}
			size += len(buf)
			chunk.Records = append(chunk.Records, append([]byte(nil), buf...))
		}
		// The cursor always advances past what was sent: the caller asks again and
		// learns the aggregate is finished from an EMPTY stream, not from a guess
		// about short reads (a short chunk can also come from the byte budget).
		chunk.Versions = uint32(len(chunk.Records))
		last := recs[len(chunk.Records)-1].Version
		chunk.NextVersion = last + 1
		if err := stream.Send(chunk); err != nil {
			return err
		}
		from = last + 1
	}
}

// SlotStreams lists a slot's aggregates (paged), so a generation migration can
// walk the source slot without holding the whole aggregate directory in memory:
// a slot at the calibration target holds ~1e7 streams.
func (s *peerServer) SlotStreams(_ context.Context, req *pushupesv1.SlotStreamsRequest) (*pushupesv1.SlotStreamsResponse, error) {
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 512
	}
	page, err := s.e.store.StreamPage(req.GetSlot(), req.GetAfter(), limit)
	if err != nil {
		return nil, err
	}
	out := &pushupesv1.SlotStreamsResponse{NextAfter: page.NextAfter}
	for _, st := range page.Streams {
		out.Aggregates = append(out.Aggregates, st.AggregateID)
	}
	return out, nil
}

// ReplayInto makes THIS node pull one aggregate from a source slot and land it in
// a local slot (the destination side of a generation migration).
//
// The pull direction matters: the destination owns the write and therefore the
// pacing, and the source needs no knowledge of the new layout. The replay is
// idempotent per (aggregate, version) — see replayChunkInto — so a call that
// fails half way can simply be repeated.
func (s *peerServer) ReplayInto(ctx context.Context, req *pushupesv1.ReplayIntoRequest) (*pushupesv1.ReplayIntoResponse, error) {
	if req.GetSrcAddr() == "" || req.GetAggregateId() == "" {
		return nil, fmt.Errorf("replay_into: src_addr and aggregate_id are required")
	}
	cli, err := s.e.peers.data(req.GetSrcAddr())
	if err != nil {
		return nil, fmt.Errorf("replay_into: dial %s: %w", req.GetSrcAddr(), err)
	}
	stream, err := cli.ReplicateAggregate(ctx, &pushupesv1.ReplicateAggregateRequest{
		Slot:        req.GetSrcSlot(),
		AggregateId: req.GetAggregateId(),
		FromVersion: req.GetFromVersion(),
	})
	if err != nil {
		return nil, fmt.Errorf("replay_into: open stream from %s: %w", req.GetSrcAddr(), err)
	}
	landed := 0
	next := req.GetFromVersion()
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("replay_into: recv from %s: %w", req.GetSrcAddr(), err)
		}
		n, err := s.e.replayChunkInto(req.GetDstSlot(), chunk.GetRecords())
		if err != nil {
			return nil, err
		}
		landed += n
		if chunk.GetNextVersion() != 0 {
			next = chunk.GetNextVersion()
		}
	}
	return &pushupesv1.ReplayIntoResponse{Landed: uint32(landed), NextVersion: next}, nil
}

func (s *peerServer) SlotLeo(_ context.Context, req *pushupesv1.SlotLeoRequest) (*pushupesv1.SlotLeoResponse, error) {
	leo, aggregates, versions, resolvable := s.e.HandleSlotLeo(req.Slot)
	resp := &pushupesv1.SlotLeoResponse{
		Leo: leo, Aggregates: aggregates, Versions: versions, Resolvable: resolvable,
	}
	if req.Flush {
		// The fence's durability probe: make the tail durable before answering.
		dur, err := s.e.HandleSlotFlush(req.Slot)
		if err != nil {
			return nil, err
		}
		resp.DurableLeo = dur
	}
	return resp, nil
}

// FreezeSlot holds a slot's write gate closed until ThawSlot. The freeze waits
// for the writes already in flight, so the LEO it returns is the log the caller
// can replay from without racing anyone.
func (s *peerServer) FreezeSlot(ctx context.Context, req *pushupesv1.FreezeSlotRequest) (*pushupesv1.FreezeSlotResponse, error) {
	if !s.e.Leads(req.GetSlot()) {
		return nil, fmt.Errorf("freeze slot %d: this node does not lead it", req.GetSlot())
	}
	if err := s.e.acquireSlotFence(ctx, req.GetSlot()); err != nil {
		return nil, err
	}
	return &pushupesv1.FreezeSlotResponse{Leo: s.e.store.LastSeqOf(req.GetSlot())}, nil
}

// ThawSlot releases a frozen slot — but only once this node's APPLIED table has
// moved on. Releasing first would let the writers that were blocked on the gate
// wake up, read their still-local view ("I lead this slot"), and append to the
// old segment: those records are acknowledged and then thrown away by the
// retirement. The data plane applies the switch a beat after the Raft commit, so
// the controller retries this until it succeeds.
func (s *peerServer) ThawSlot(_ context.Context, req *pushupesv1.ThawSlotRequest) (*pushupesv1.ThawSlotResponse, error) {
	if p, ok := s.e.slotPlacement(req.GetSlot()); ok && p.Leader == s.e.self {
		return nil, fmt.Errorf("thaw slot %d: this node still leads it (the layout switch has not been applied here yet)", req.GetSlot())
	}
	s.e.releaseSlotFence(req.GetSlot())
	return &pushupesv1.ThawSlotResponse{}, nil
}

func (s *peerServer) FenceSlot(ctx context.Context, req *pushupesv1.FenceSlotRequest) (*pushupesv1.FenceSlotResponse, error) {
	leo, err := s.e.HandleFenceSlot(ctx, req.Slot, req.GetTarget())
	if err != nil {
		return nil, err
	}
	return &pushupesv1.FenceSlotResponse{Leo: leo}, nil
}

// SlotLeader answers this node's local placement view of a slot (leader +
// epoch) — the migration source's fence-release gate (see fence.go).
func (s *peerServer) SlotLeader(_ context.Context, req *pushupesv1.SlotLeaderRequest) (*pushupesv1.SlotLeaderResponse, error) {
	leader, epoch := s.e.HandleSlotLeader(req.Slot)
	return &pushupesv1.SlotLeaderResponse{Leader: leader, Epoch: epoch}, nil
}

// DropSlot discards this node's local copy of a slot. The migration fence calls
// it on a target whose streams are inconsistent with its LEO, because the
// increment that would repair it cannot be shipped (the seqs are spent) — the
// copy is rebuilt from the source instead.
func (s *peerServer) DropSlot(_ context.Context, req *pushupesv1.DropSlotRequest) (*pushupesv1.DropSlotResponse, error) {
	if err := s.e.HandleDropSlot(req.Slot, req.Node); err != nil {
		return nil, err
	}
	return &pushupesv1.DropSlotResponse{}, nil
}

// ---- message conversion ------------------------------------------------------

// fetchItemsFromProto converts response items (all fields).
func fetchItemsFromProto(ps []*pushupesv1.FetchItem) []FetchItem {
	out := make([]FetchItem, 0, len(ps))
	for _, p := range ps {
		out = append(out, FetchItem{
			Slot: p.Slot, FromSeq: p.FromSeq, NextSeq: p.NextSeq, Payload: p.Payload,
		})
	}
	return out
}

func protoItems(is []FetchItem) []*pushupesv1.FetchItem {
	out := make([]*pushupesv1.FetchItem, 0, len(is))
	for _, it := range is {
		out = append(out, &pushupesv1.FetchItem{
			Slot: it.Slot, FromSeq: it.FromSeq, NextSeq: it.NextSeq, Payload: it.Payload,
		})
	}
	return out
}

// ---- client side: cached connections per peer address ------------------------

// peerClient holds one lazily dialed grpc.ClientConn per peer base
// address (canonical scheme-carrying string; dial strips the scheme).
// Keepalive catches half-dead peers within seconds so fetch sessions fall
// to the backoff path quickly instead of hanging on a dead TCP session.
type peerClient struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func newPeerClient() *peerClient { return &peerClient{conns: map[string]*grpc.ClientConn{}} }

// conn returns the cached connection to addr, dialing on first use. Both
// service halves ride it: the split is about which RPCs you call, not about
// opening two connections to the same node.
func (pc *peerClient) conn(addr string) (*grpc.ClientConn, error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if c, ok := pc.conns[addr]; ok {
		return c, nil
	}
	conn, err := grpc.NewClient(HostPort(addr),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(config.PeerMaxMsgBytes), grpc.MaxCallSendMsgSize(config.PeerMaxMsgBytes)),
	)
	if err != nil {
		return nil, err
	}
	pc.conns[addr] = conn
	return conn, nil
}

// controller is the control-plane stub: membership, layout feed, liveness.
func (pc *peerClient) controller(addr string) (pushupesv1.ControllerPeerServiceClient, error) {
	conn, err := pc.conn(addr)
	if err != nil {
		return nil, err
	}
	return pushupesv1.NewControllerPeerServiceClient(conn), nil
}

// data is the data-plane stub: replica fetch, migration fences, slot content.
func (pc *peerClient) data(addr string) (pushupesv1.DataPeerServiceClient, error) {
	conn, err := pc.conn(addr)
	if err != nil {
		return nil, err
	}
	return pushupesv1.NewDataPeerServiceClient(conn), nil
}

// reset drops the cached connection to addr. A ClientConn carries the backoff
// it accumulated while the peer was gone — gRPC retries a failed transport with
// exponential backoff up to its two-minute ceiling — so the first RPC after a
// long outage can keep failing for a minute after the peer is back. Callers that
// have fresh evidence the peer is up again (a self-announcement, a listening
// peer port) drop the connection here so the next RPC redials instead of
// waiting out that backoff.
func (pc *peerClient) reset(addr string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if c, ok := pc.conns[addr]; ok {
		_ = c.Close()
		delete(pc.conns, addr)
	}
}

func (pc *peerClient) close() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	for _, c := range pc.conns {
		_ = c.Close()
	}
	pc.conns = map[string]*grpc.ClientConn{}
}

// ---- engine convenience wrappers (the whole peer-plane client surface) -------

// peerAddr is the unified inter-node address of a node: the Raft peer
// endpoint, which also serves PeerService. All peer-plane call sites dial
// this — the admin port no longer carries any node-to-node traffic.
func (e *Engine) peerAddr(nodeID string) string {
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	return e.table.Peers[nodeID].PeerAddr
}

// peerController resolves the CONTROL-plane stub for a peer address
// ("" is a hard error: every peer-plane call site must have a registered peer).
func (e *Engine) peerController(addr string) (pushupesv1.ControllerPeerServiceClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("peer plane: no address for target")
	}
	return e.peers.controller(addr)
}

// peerData resolves the DATA-plane stub for a peer address.
func (e *Engine) peerData(addr string) (pushupesv1.DataPeerServiceClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("peer plane: no address for target")
	}
	return e.peers.data(addr)
}

// resetPeerConn forgets the cached connection to addr (see peerClient.reset):
// the next peer-plane call dials fresh. Used where the caller has evidence the
// peer just came back, so a stale connection carries nothing but old backoff.
func (e *Engine) resetPeerConn(addr string) {
	if addr != "" {
		e.peers.reset(addr)
	}
}

func (e *Engine) peerMFetch(ctx context.Context, addr string, req *MFetchRequest) (*MFetchResponse, func(), error) {
	c, err := e.peerData(addr)
	if err != nil {
		return nil, nil, err
	}
	presp, err := c.MFetch(ctx, &pushupesv1.MFetchRequest{
		Follower: req.Follower, WaitMs: req.WaitMS, Slots: req.Slots, FromSeqs: req.FromSeqs, Sweep: req.Sweep,
	})
	if err != nil {
		return nil, nil, err
	}
	// The items' payloads alias presp's receive buffer, so the lease has to be
	// released by the caller once the payloads have been written to the WAL.
	release := func() { lease.Release(presp) }
	return &MFetchResponse{
		Follower: presp.Follower,
		Items:    fetchItemsFromProto(presp.Items),
		HwSlots:  presp.HwSlots,
		HwValues: presp.HwValues,
	}, release, nil
}

func (e *Engine) peerReplicate(ctx context.Context, addr string, slot int32, seq uint64, payload []byte) error {
	c, err := e.peerData(addr)
	if err != nil {
		return err
	}
	_, err = c.Replicate(ctx, &pushupesv1.ReplicateRequest{Slot: slot, Seq: seq, Payload: payload})
	return err
}

func (e *Engine) peerProgress(ctx context.Context, addr, follower string, slots []int32, froms []uint64, stamp bool) error {
	c, err := e.peerData(addr)
	if err != nil {
		return err
	}
	resp, err := c.ReplicaProgress(ctx, &pushupesv1.ReplicaProgressRequest{Follower: follower, Slots: slots, FromSeqs: froms, Stamp: stamp})
	if err != nil {
		return err
	}
	// Zero-lag read admission: the leader's post-report watermarks ride back on
	// this very response — the report that completed an append's ack carries
	// the watermark that append needs. applyHwLedger is epoch-keyed, so a
	// stale or ghost round (a demoted leader answering late) cannot poison it;
	// an empty ledger from an old binary is simply a no-op.
	e.applyHwLedger(addr, resp.GetHwSlots(), resp.GetHwValues())
	return nil
}

func (e *Engine) peerLeo(ctx context.Context, addr string, slot int32) (uint64, error) {
	leo, _, _, _, err := e.peerSlotState(ctx, addr, slot)
	return leo, err
}

// slotState is a peer's answer to the catch-up probe: the slot LEO plus the
// directory summary (see storage.SlotDigest) that lets the caller compare the
// peer's streams against its own.
type slotState struct {
	LEO        uint64
	Aggregates uint64
	Versions   uint64
	Resolvable uint64
}

// peerSlotState asks a peer for one slot's LEO and directory summary in a single
// round trip.
func (e *Engine) peerSlotState(ctx context.Context, addr string, slot int32) (uint64, uint64, uint64, uint64, error) {
	c, err := e.peerData(addr)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	resp, err := c.SlotLeo(ctx, &pushupesv1.SlotLeoRequest{Slot: slot})
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return resp.Leo, resp.Aggregates, resp.Versions, resp.Resolvable, nil
}

// remoteSlotState is peerSlotState with the standard probe timeout.
func (e *Engine) remoteSlotState(ctx context.Context, addr string, slot int32) (slotState, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	leo, agg, ver, res, err := e.peerSlotState(cctx, addr, slot)
	if err != nil {
		return slotState{}, err
	}
	return slotState{LEO: leo, Aggregates: agg, Versions: ver, Resolvable: res}, nil
}

// peerFence asks a peer (the migration source) to take the commit fence for a
// slot and returns the frozen LEO it confirmed the target holds.
func (e *Engine) peerFence(ctx context.Context, addr string, slot int32, target string) (uint64, error) {
	c, err := e.peerData(addr)
	if err != nil {
		return 0, err
	}
	resp, err := c.FenceSlot(ctx, &pushupesv1.FenceSlotRequest{Slot: slot, Target: target})
	if err != nil {
		return 0, err
	}
	return resp.Leo, nil
}

// peerSlotLeader asks a peer for its LOCAL placement view of one slot: the
// leader it believes in and the epoch. Used by the migration source to gate the
// fence release on the target having applied the leader move.
func (e *Engine) peerSlotLeader(ctx context.Context, addr string, slot int32) (string, int64, error) {
	c, err := e.peerData(addr)
	if err != nil {
		return "", 0, err
	}
	resp, err := c.SlotLeader(ctx, &pushupesv1.SlotLeaderRequest{Slot: slot})
	if err != nil {
		return "", 0, err
	}
	return resp.Leader, resp.Epoch, nil
}

func (e *Engine) peerPing(ctx context.Context, addr string) error {
	c, err := e.peerController(addr)
	if err != nil {
		return err
	}
	_, err = c.Ping(ctx, &pushupesv1.PingRequest{})
	return err
}

// ClosePeers releases cached client connections (process shutdown).
func (e *Engine) ClosePeers() { e.peers.close() }

// ---- serving PeerService on the demuxed peer-port channel --------------------

// ServePeer mounts PeerService on the gRPC surface of the peer listener
// (the demuxed 'P' connections). Returns the server so shutdown can stop
// it; Serve exits on its own when the listener closes.
func (e *Engine) ServePeer(d net.Listener) *grpc.Server {
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(config.PeerMaxMsgBytes),
		grpc.MaxSendMsgSize(config.PeerMaxMsgBytes),
		// The client keeps idle peer conns warm with 10s pings
		// (PermitWithoutStream); relax the default 5min enforcement so the
		// server does not GOAWAY "too_many_pings" on them.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
	)
	impl := e.NewPeerServer()
	pushupesv1.RegisterControllerPeerServiceServer(srv, impl)
	pushupesv1.RegisterDataPeerServiceServer(srv, impl)
	go func() { _ = srv.Serve(d) }()
	return srv
}

// SlotLeos answers the durable LEO of many slots (see the proto comment): the
// controller's failover pick needs to know which replica actually holds what was
// acknowledged. Slots this node does not hold answer 0.
func (s *peerServer) SlotLeos(_ context.Context, req *pushupesv1.SlotLeosRequest) (*pushupesv1.SlotLeosResponse, error) {
	resp := &pushupesv1.SlotLeosResponse{Slots: req.Slots, Leos: make([]uint64, len(req.Slots))}
	for i, slot := range req.Slots {
		resp.Leos[i] = s.e.HandleLEO(slot)
	}
	return resp, nil
}

// SlotSizes answers the on-disk bytes and the seq ledger of many slots (see
// the proto comment): the cluster storage gauges count each slot's leader copy,
// and only the holder can report its size and its count. The seq book is the
// copy's LEO — the same ledger its durable-write counter advances.
func (s *peerServer) SlotSizes(_ context.Context, req *pushupesv1.SlotSizesRequest) (*pushupesv1.SlotSizesResponse, error) {
	resp := &pushupesv1.SlotSizesResponse{Slots: req.Slots, Bytes: make([]uint64, len(req.Slots)), Seqs: make([]uint64, len(req.Slots))}
	for i, slot := range req.Slots {
		resp.Bytes[i] = uint64(s.e.SlotSize(slot))
		resp.Seqs[i] = s.e.HandleLEO(slot)
	}
	return resp, nil
}

// WatchLayout is the data-plane node's feed (DESIGN §12, layoutfeed.go): the
// caller runs no Raft group and takes the replicated metadata from here. Only
// the controller serves frames — any other voter answers a redirect frame
// naming where to go — and the stream ends when the caller falls too far
// behind to keep in a bounded buffer, which is the caller's cue to reconnect
// and re-sync from its applied index.
func (s *peerServer) WatchLayout(req *pushupesv1.WatchLayoutRequest, stream grpc.ServerStreamingServer[pushupesv1.WatchLayoutResponse]) error {
	if req.GetLearnerId() == "" {
		return fmt.Errorf("cluster: WatchLayout needs a learner id")
	}
	return s.e.layoutFeed().serve(stream.Context(), layoutRequest{
		LearnerID: req.GetLearnerId(),
		FromIndex: req.GetFromIndex(),
		FromTerm:  req.GetFromTerm(),
		Topology:  req.GetTopology(),
	}, func(f *layoutFrame) error { return stream.Send(toPBFrame(f)) })
}

// toPBFrame puts one feed step on the wire. kind/entry_kind are carried as
// numbers so the two sides can grow without a shared enum.
func toPBFrame(f *layoutFrame) *pushupesv1.WatchLayoutResponse {
	out := &pushupesv1.WatchLayoutResponse{
		Kind:           f.Kind,
		Index:          f.Index,
		Term:           f.Term,
		Data:           f.Data,
		CommitIndex:    f.CommitIndex,
		LeaderId:       f.LeaderID,
		LeaderPeerAddr: f.LeaderPeer,
		Topology:       f.Topology,
		FirstIndex:     f.FirstIndex,
		EntryKind:      uint32(f.EntryKind),
		Reason:         f.Reason,
	}
	if len(f.Voters) > 0 {
		out.Voters = make([]*pushupesv1.LayoutVoter, 0, len(f.Voters))
		for _, v := range f.Voters {
			out.Voters = append(out.Voters, &pushupesv1.LayoutVoter{Id: v.ID, Addr: v.Addr})
		}
	}
	return out
}

// peerSlotSizes asks a peer for the on-disk bytes and the seq ledger of the
// given slots — one round trip feeds both cluster storage folds.
func (e *Engine) peerSlotSizes(ctx context.Context, addr string, slots []int32) (bytes, seqs []uint64, err error) {
	c, err := e.peerData(addr)
	if err != nil {
		return nil, nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, config.StorageQueryTimeout)
	defer cancel()
	resp, err := c.SlotSizes(cctx, &pushupesv1.SlotSizesRequest{Slots: slots})
	if err != nil {
		return nil, nil, err
	}
	if len(resp.Bytes) != len(slots) || len(resp.Seqs) != len(slots) {
		return nil, nil, fmt.Errorf("slot sizes: got %d/%d answers for %d slots", len(resp.Bytes), len(resp.Seqs), len(slots))
	}
	return resp.Bytes, resp.Seqs, nil
}

// peerSlotLeos asks a peer for the durable LEOs of the given slots.
func (e *Engine) peerSlotLeos(ctx context.Context, addr string, slots []int32) ([]uint64, error) {
	c, err := e.peerData(addr)
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, config.PeerRPCTimeout)
	defer cancel()
	resp, err := c.SlotLeos(cctx, &pushupesv1.SlotLeosRequest{Slots: slots})
	if err != nil {
		return nil, err
	}
	if len(resp.Leos) != len(slots) {
		return nil, fmt.Errorf("slot leos: got %d answers for %d slots", len(resp.Leos), len(slots))
	}
	return resp.Leos, nil
}
