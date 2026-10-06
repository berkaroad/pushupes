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
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	pushupesv1 "pushupes/internal/grpcapi/pushupes/v1"
	"pushupes/internal/lease"
)

const (
	// grpcMagicByte is the first byte of the HTTP/2 preface ("PRI ...")
	// that every gRPC connection starts with. Raft's wire protocol begins
	// with an RPC version byte (0), so the classification never collides.
	grpcMagicByte = byte('P')

	peerRPCTimeout = 30 * time.Second
	// failoverPickTimeout bounds ONE round of "what is your LEO for these
	// slots?" asked of the failover candidates. The pick is best effort (a
	// candidate that does not answer just drops out of the running), so it gets
	// a healthy-peer budget rather than the general peer RPC timeout — a
	// failover must not wait 30s for a dying node's answer.
	failoverPickTimeout = 1 * time.Second
	peerPingTimeout     = 2 * time.Second
	// storageQueryTimeout bounds ONE round of "what are your slot sizes?"
	// (the cluster storage gauge, see runStorageSampler). Shorter than the
	// general peer RPC budget: a peer that does not answer costs the gauge one
	// sample, not the whole refresh.
	storageQueryTimeout = 2 * time.Second

	// peerMaxMsgBytes bounds gRPC messages on the peer plane. Snapshot
	// segments stream in <=4MiB chunks and fetch rounds cap at
	// maxPayloadBytes (32MiB), so a 64MiB ceiling covers every message
	// with headroom while keeping a malicious peer from pinning memory.
	peerMaxMsgBytes = 64 << 20
)

// peerServer adapts Engine onto pushupesv1.PeerServiceServer. The adapter
// is stateless: every RPC delegates to the same Engine logic the old HTTP
// internal endpoints drove.
type peerServer struct {
	pushupesv1.UnimplementedPeerServiceServer
	e *Engine
}

// NewPeerServer returns the gRPC implementation of the inter-node plane.
func (e *Engine) NewPeerServer() pushupesv1.PeerServiceServer {
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
	reg := &Registration{ID: req.Id, AdminAddr: req.AdminAddr, ClientAddr: req.ClientAddr}
	if err := s.e.acceptRegistration(reg); err != nil {
		return nil, err
	}
	return &pushupesv1.RegisterResponse{}, nil
}

func (s *peerServer) Adopt(ctx context.Context, req *pushupesv1.AdoptRequest) (*pushupesv1.AdoptResponse, error) {
	if err := s.e.acceptAdoption(ctx, &Adoption{ID: req.Id, PeerAddr: req.PeerAddr}); err != nil {
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
	return &pushupesv1.MFetchResponse{Follower: resp.Follower, Items: protoItems(resp.Items)}, nil
}

func (s *peerServer) ReplicaProgress(_ context.Context, req *pushupesv1.ReplicaProgressRequest) (*pushupesv1.ReplicaProgressResponse, error) {
	prog := progressBatch{follower: req.Follower, stamp: req.Stamp, now: time.Now()}
	for i, slot := range req.Slots {
		if i < len(req.FromSeqs) && slot >= 0 && req.FromSeqs[i] > 0 {
			prog.add(slot, req.FromSeqs[i]-1)
		}
	}
	prog.apply(s.e)
	return &pushupesv1.ReplicaProgressResponse{}, nil
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

func (s *peerServer) SlotLeo(_ context.Context, req *pushupesv1.SlotLeoRequest) (*pushupesv1.SlotLeoResponse, error) {
	leo, aggregates, versions, resolvable := s.e.HandleSlotLeo(req.Slot)
	return &pushupesv1.SlotLeoResponse{
		Leo: leo, Aggregates: aggregates, Versions: versions, Resolvable: resolvable,
	}, nil
}

func (s *peerServer) FenceSlot(ctx context.Context, req *pushupesv1.FenceSlotRequest) (*pushupesv1.FenceSlotResponse, error) {
	leo, err := s.e.HandleFenceSlot(ctx, req.Slot)
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

func (s *peerServer) PushSegments(stream pushupesv1.PeerService_PushSegmentsServer) error {
	if err := s.e.writeSegments(stream); err != nil {
		return err
	}
	return stream.SendAndClose(&pushupesv1.PushSegmentsResponse{})
}

func (s *peerServer) TriggerSnapshot(ctx context.Context, req *pushupesv1.TriggerSnapshotRequest) (*pushupesv1.TriggerSnapshotResponse, error) {
	if err := s.e.HandleSnapshotCommand(ctx, req.Slot, req.ToNode); err != nil {
		return nil, err
	}
	return &pushupesv1.TriggerSnapshotResponse{}, nil
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

func (pc *peerClient) client(addr string) (pushupesv1.PeerServiceClient, error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if c, ok := pc.conns[addr]; ok {
		return pushupesv1.NewPeerServiceClient(c), nil
	}
	conn, err := grpc.NewClient(HostPort(addr),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(peerMaxMsgBytes), grpc.MaxCallSendMsgSize(peerMaxMsgBytes)),
	)
	if err != nil {
		return nil, err
	}
	pc.conns[addr] = conn
	return pushupesv1.NewPeerServiceClient(conn), nil
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

// peerRPC resolves a client for a peer address ("" is a hard error: every
// peer-plane call site must have a registered peer first).
func (e *Engine) peerRPC(addr string) (pushupesv1.PeerServiceClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("peer plane: no address for target")
	}
	return e.peers.client(addr)
}

func (e *Engine) peerMFetch(ctx context.Context, addr string, req *MFetchRequest) (*MFetchResponse, func(), error) {
	c, err := e.peerRPC(addr)
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
	return &MFetchResponse{Follower: presp.Follower, Items: fetchItemsFromProto(presp.Items)}, release, nil
}

func (e *Engine) peerReplicate(ctx context.Context, addr string, slot int32, seq uint64, payload []byte) error {
	c, err := e.peerRPC(addr)
	if err != nil {
		return err
	}
	_, err = c.Replicate(ctx, &pushupesv1.ReplicateRequest{Slot: slot, Seq: seq, Payload: payload})
	return err
}

func (e *Engine) peerProgress(ctx context.Context, addr, follower string, slots []int32, froms []uint64, stamp bool) error {
	c, err := e.peerRPC(addr)
	if err != nil {
		return err
	}
	_, err = c.ReplicaProgress(ctx, &pushupesv1.ReplicaProgressRequest{Follower: follower, Slots: slots, FromSeqs: froms, Stamp: stamp})
	return err
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
	c, err := e.peerRPC(addr)
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
func (e *Engine) peerFence(ctx context.Context, addr string, slot int32) (uint64, error) {
	c, err := e.peerRPC(addr)
	if err != nil {
		return 0, err
	}
	resp, err := c.FenceSlot(ctx, &pushupesv1.FenceSlotRequest{Slot: slot})
	if err != nil {
		return 0, err
	}
	return resp.Leo, nil
}

// peerSlotLeader asks a peer for its LOCAL placement view of one slot: the
// leader it believes in and the epoch. Used by the migration source to gate the
// fence release on the target having applied the leader move.
func (e *Engine) peerSlotLeader(ctx context.Context, addr string, slot int32) (string, int64, error) {
	c, err := e.peerRPC(addr)
	if err != nil {
		return "", 0, err
	}
	resp, err := c.SlotLeader(ctx, &pushupesv1.SlotLeaderRequest{Slot: slot})
	if err != nil {
		return "", 0, err
	}
	return resp.Leader, resp.Epoch, nil
}

// peerOpenPush starts a PushSegments client stream to addr. The caller
// sends the bounded chunks and closes with CloseAndRecv.
func (e *Engine) peerOpenPush(ctx context.Context, addr string) (pushupesv1.PeerService_PushSegmentsClient, error) {
	c, err := e.peerRPC(addr)
	if err != nil {
		return nil, err
	}
	return c.PushSegments(ctx)
}

func (e *Engine) peerTriggerSnapshot(ctx context.Context, addr string, slot int32, toNode string) error {
	c, err := e.peerRPC(addr)
	if err != nil {
		return err
	}
	_, err = c.TriggerSnapshot(ctx, &pushupesv1.TriggerSnapshotRequest{Slot: slot, ToNode: toNode})
	return err
}

func (e *Engine) peerPing(ctx context.Context, addr string) error {
	c, err := e.peerRPC(addr)
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
		grpc.MaxRecvMsgSize(peerMaxMsgBytes),
		grpc.MaxSendMsgSize(peerMaxMsgBytes),
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
	pushupesv1.RegisterPeerServiceServer(srv, e.NewPeerServer())
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

// SlotSizes answers the on-disk bytes of many slots (see the proto comment): the
// cluster storage gauge counts each slot's leader copy, and only the holder can
// report its size.
func (s *peerServer) SlotSizes(_ context.Context, req *pushupesv1.SlotSizesRequest) (*pushupesv1.SlotSizesResponse, error) {
	resp := &pushupesv1.SlotSizesResponse{Slots: req.Slots, Bytes: make([]uint64, len(req.Slots))}
	for i, slot := range req.Slots {
		resp.Bytes[i] = uint64(s.e.SlotSize(slot))
	}
	return resp, nil
}

// peerSlotSizes asks a peer for the on-disk bytes of the given slots.
func (e *Engine) peerSlotSizes(ctx context.Context, addr string, slots []int32) ([]uint64, error) {
	c, err := e.peerRPC(addr)
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, storageQueryTimeout)
	defer cancel()
	resp, err := c.SlotSizes(cctx, &pushupesv1.SlotSizesRequest{Slots: slots})
	if err != nil {
		return nil, err
	}
	if len(resp.Bytes) != len(slots) {
		return nil, fmt.Errorf("slot sizes: got %d answers for %d slots", len(resp.Bytes), len(slots))
	}
	return resp.Bytes, nil
}

// peerSlotLeos asks a peer for the durable LEOs of the given slots.
func (e *Engine) peerSlotLeos(ctx context.Context, addr string, slots []int32) ([]uint64, error) {
	c, err := e.peerRPC(addr)
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, peerRPCTimeout)
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
