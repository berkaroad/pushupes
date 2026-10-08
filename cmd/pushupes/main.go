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

// Command pushupes runs one pushupes node: the append-only slot WAL,
// the Raft control plane, the leader-follower replication and hot-migration
// data plane, and the HTTP surface (client / inter-node / admin).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"pushupes/internal/api"
	"pushupes/internal/cluster"
	"pushupes/internal/data"
	"pushupes/internal/raft"
	"pushupes/internal/storage"

	"google.golang.org/grpc"

	"pushupes/internal/grpcapi"
	"pushupes/internal/payloadcodec"
)

// defaultGrpcMaxMsgBytes is the client-plane gRPC message cap: gRPC's own
// default (4 MiB). Operators raise it with -grpc-max-msg-size when batches
// carry more records than fit in 4 MiB.
const defaultGrpcMaxMsgBytes = 4 << 20

func main() {
	var (
		nodeID     = flag.String("node", envOr("PUSHUPES_NODE", "node-1"), "node id (env PUSHUPES_NODE)")
		adminAddr  = flag.String("admin", envOr("PUSHUPES_ADMIN", "http://127.0.0.1:8091"), "HTTP admin listen address: admin API + pprof (env PUSHUPES_ADMIN); events and all inter-node traffic use -client and -peer")
		clientAddr = flag.String("client", envOr("PUSHUPES_CLIENT", "http://127.0.0.1:8591"), "gRPC data-plane listen address: event writes and queries (env PUSHUPES_CLIENT)")
		peerAddr   = flag.String("peer", envOr("PUSHUPES_PEER", "http://127.0.0.1:8391"), "Raft transport + peer gRPC listen address: all node-to-node traffic (env PUSHUPES_PEER)")
		dataDir    = flag.String("data", envOr("PUSHUPES_DATA", "./data"), "data directory (env PUSHUPES_DATA)")
		peers      = flag.String("peers", envOr("PUSHUPES_PEERS", ""), "comma list (env PUSHUPES_PEERS) of id=http://host:peerport (admin/client addrs are self-registered; legacy id:peerport:adminport:clientport also accepted)")
		joinAddr   = flag.String("join", envOr("PUSHUPES_JOIN", ""), "peer address of one specific cluster member (host:peerport) to offer this node to on start. Optional: a node that does not lead its -peers list joins automatically and offers itself through the configured members in rotation — pass -join to name one instead")
		adoptTo    = flag.Duration("adopt-interval", envDurationOr("PUSHUPES_ADOPT_INTERVAL", cluster.DefaultAdoptInterval),
			"how often a node that is not a cluster member yet retries offering itself through the members it was configured with (-join when given, else -peers) (env PUSHUPES_ADOPT_INTERVAL)")
		// The slot count is a permanent layout decision (routing, placement,
		// migration granularity), so it is not a runtime knob — it is fixed at
		// data.DefaultSlotCount. See DESIGN §7.2.
		slotCount = data.DefaultSlotCount
		// The replica policy tier (low / medium / high) behind the per-slot
		// replica factor. This flag only SEEDS a brand-new cluster: the policy
		// lives in the replicated slot table, so on an existing cluster the
		// table's value wins and the admin endpoint
		// (POST /admin/cluster/replica-policy) is the entry point for every
		// change. See DESIGN §4.
		replicaPolicy = flag.String("replica-policy", envOr("PUSHUPES_REPLICA_POLICY", string(cluster.DefaultReplicaPolicy)),
			"replica policy tier for a NEW cluster: low (1 copy per slot), medium (2), high (fault tolerance + 1); applied on the table's first plan only — an existing cluster keeps the policy the admin endpoint last set (env PUSHUPES_REPLICA_POLICY)")
		segmentB = byteSize(storage.DefaultSegmentBytes)
		flushN   = flag.Int64("flush-messages", 1000, "fsync every N records (0 disables)")
		flushD   = flag.Duration("flush-interval", 5*time.Second, "fsync every interval (0 disables)")
		// How long a woken fetch round waits for the rest of the write burst
		// before answering. Every acknowledged append pays it once (the round
		// carrying its record pays it), and it buys rounds: 0 answers as soon
		// as the first slot has data, at the cost of more rounds and reports.
		fetchSettleD = flag.Duration("fetch-settle", envDurationOr("PUSHUPES_FETCH_SETTLE", cluster.DefaultFetchSettle),
			"fetch round coalescing window after a data wake (0 answers at once)")
		dropAfter = flag.Duration("drop-after", envDurationOr("PUSHUPES_DROP_AFTER", cluster.DefaultDropRetention),
			"post-migration retention: how long the FORMER SOURCE keeps its local copy of a migrated slot before dropping it (positive = that window, 0 = the default 30s; negative is rejected; the cleanup cannot be disabled — env PUSHUPES_DROP_AFTER)")
		rebalanceInterval = flag.Duration("rebalance-interval", envDurationOr("PUSHUPES_REBALANCE_INTERVAL", cluster.DefaultRebalanceInterval),
			"how often the controller checks the leader layout against the slot ring and hands deviated slots back (a node that died returns to its own slots after failover; 0 disables the rebalancer; negative is rejected — env PUSHUPES_REBALANCE_INTERVAL)")
		rebalanceBatch = flag.Int("rebalance-batch", envIntOr("PUSHUPES_REBALANCE_BATCH", cluster.DefaultRebalanceBatch),
			"max leader hand-overs one rebalance round executes, run serially (smaller = gentler write-latency noise, larger = faster convergence; 0 disables the rebalancer; negative is rejected — env PUSHUPES_REBALANCE_BATCH)")
		// The client plane keeps gRPC's stock 4MiB message cap by default.
		// BatchAppend asks for one batch to fit inside the cap, so the
		// operator raises it together with typical batch size (a 4MiB cap
		// carries roughly 4000 records of 1KiB bodies).
		grpcMaxMsg = byteSize(envIntOr("PUSHUPES_GRPC_MAX_MSG_SIZE", defaultGrpcMaxMsgBytes))
		batchPar   = flag.Int("batch-slot-parallelism", envIntOr("PUSHUPES_BATCH_SLOT_PARALLELISM", grpcapi.DefaultBatchSlotParallelism),
			"max slots one BatchAppend executes concurrently (different slots take independent fences and WALs; a wide batch spanning the ring lands at most this many concurrent WAL writers — env PUSHUPES_BATCH_SLOT_PARALLELISM)")
		bootstrap     = flag.Bool("bootstrap", false, "write this cluster's initial configuration from this node. By default the node whose id leads -peers does that and every other configured node starts as a seed that offers itself through the running cluster (env PUSHUPES_BOOTSTRAP not read: this is a startup decision, not a tunable)")
		raftHeartbeat = flag.Duration("raft-heartbeat-timeout", envDurationOr("PUSHUPES_RAFT_HEARTBEAT_TIMEOUT", raft.DefaultHeartbeatTimeout),
			"consensus heartbeat interval (env PUSHUPES_RAFT_HEARTBEAT_TIMEOUT). A leader whose heartbeats do not arrive in time is voted out, so on a host that can delay the consensus event loop raise this together with -raft-election-timeout")
		raftElection = flag.Duration("raft-election-timeout", envDurationOr("PUSHUPES_RAFT_ELECTION_TIMEOUT", raft.DefaultElectionTimeout),
			"consensus election timeout: how long a follower waits for a heartbeat before standing for election (env PUSHUPES_RAFT_ELECTION_TIMEOUT). At least 2x -raft-heartbeat-timeout; raise both when the cluster votes leaders out under load")
		raftFlush = flag.Duration("raft-flush-interval", envDurationOr("PUSHUPES_RAFT_FLUSH_INTERVAL", cluster.DefaultRaftFlushInterval),
			"consensus WAL group-commit window: appends arriving within it share a single fsync (env PUSHUPES_RAFT_FLUSH_INTERVAL)")
		raftSegB = byteSize(envIntOr("PUSHUPES_RAFT_SEGMENT_BYTES", cluster.DefaultRaftSegmentBytes))
	)
	// -segment-bytes takes a size, not just a byte count: 268435456 or 256MiB.
	flag.Var(&segmentB, "segment-bytes", "WAL segment roll `size` (bytes, or a suffix like 256MiB/1GiB); a multiple of 64MiB, at most 2GiB")
	flag.Var(&grpcMaxMsg, "grpc-max-msg-size", "client-plane gRPC max message `size` in bytes (suffixes like 16MiB accepted; the gRPC default 4MiB applies when unset)")
	flag.Var(&raftSegB, "raft-segment-bytes", "consensus WAL segment roll `size` (bytes, or a suffix like 64MiB)")
	flag.Parse()

	// gRPC's stock buffer pool zeroes every buffer it hands out and its size
	// classes are too sparse for event payloads (a 100KiB message would take a
	// 1MiB buffer and pay a 1MiB memclr). Our codec then keeps the large bytes
	// fields out of the copy path in both directions. Both must be installed
	// before anything creates a client or server.
	payloadcodec.InstallBufferPool()
	payloadcodec.InstallCodec()

	base := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger := base.With("node", *nodeID)

	if err := run(*nodeID, *adminAddr, *clientAddr, *peerAddr, *dataDir, *peers, *joinAddr, *adoptTo, slotCount, *replicaPolicy, int64(segmentB), int64(grpcMaxMsg), *batchPar, *flushN, *flushD, *dropAfter, *fetchSettleD, *rebalanceInterval, *rebalanceBatch, *bootstrap, *raftFlush, *raftHeartbeat, *raftElection, int64(raftSegB), logger); err != nil {
		logger.Error("pushupes exited with error", "error", err)
		os.Exit(1)
	}
}

func run(nodeID, adminAddr, clientAddr, peerAddr, dataDir, peersCSV, joinAddr string, adoptInterval time.Duration, slotCount int, replicaPolicy string, segmentBytes, grpcMaxMsgBytes int64, batchParallelism int, flushN int64, flushD, dropAfter, fetchSettle time.Duration, rebalanceInterval time.Duration, rebalanceBatch int, bootstrap bool, raftFlush, raftHeartbeat, raftElection time.Duration, raftSegBytes int64, logger *slog.Logger) error {
	// Canonical form for every stored address: scheme required. A bare
	// host:port gets the default "http://" prefix; an explicit protocol
	// is honoured as passed. TCP-level uses (listen/dial) strip it again.
	adminAddr = cluster.NormalizeAddr(adminAddr)
	clientAddr = cluster.NormalizeAddr(clientAddr)
	peerAddr = cluster.NormalizeAddr(peerAddr)
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}

	if err := storage.ValidateSegmentBytes(segmentBytes); err != nil {
		return err
	}
	// The batch slot cap sizes a semaphore; a zero/negative value would make
	// every BatchAppend block forever (fail fast at startup; the default is
	// grpcapi.DefaultBatchSlotParallelism).
	if batchParallelism <= 0 {
		return fmt.Errorf("-batch-slot-parallelism %d must be positive", batchParallelism)
	}
	// The client-plane message cap must be positive; a zero/negative value
	// would break every gRPC call on the data plane (fail fast at startup).
	if grpcMaxMsgBytes <= 0 {
		return fmt.Errorf("-grpc-max-msg-size %s must be positive", humanBytes(grpcMaxMsgBytes))
	}
	// Post-migration retention: 0 means "the default", a negative window is
	// rejected here (fail fast). There is no way to switch the cleanup off.
	dropRetention, err := cluster.ResolveDropRetention(dropAfter)
	if err != nil {
		return err
	}
	logger.Info("post-migration cleanup: the former source drops its local copy after this retention",
		"drop_after", dropRetention.String())
	store, err := storage.OpenStore(dataDir, int32(slotCount), segmentBytes, storage.FlushPolicy{
		IntervalMessages: flushN,
		Interval:         flushD,
	})
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	peers, err := parsePeers(peersCSV)
	if err != nil {
		return err
	}
	selfPeer := cluster.Peer{ID: nodeID, PeerAddr: peerAddr, AdminAddr: adminAddr, ClientAddr: clientAddr}
	if !containsPeer(peers, nodeID) {
		peers = append(peers, selfPeer)
	}
	// Who authors the cluster's configuration, and who offers itself?
	//
	// Membership is written on first start, so exactly ONE node may write it:
	// the node that leads the configured list (the operator's -bootstrap
	// overrides that). Every other configured node starts as a SEED — its
	// -peers is only the list of members it offers itself through, and its
	// configuration arrives from the cluster it joins.
	//
	// The rule matters because a node that bootstraps its own copy writes an
	// entry at index 1 that competes with the running cluster's (same index,
	// same term, different content), and the nodes holding each version form
	// their own raft group, each able to commit on its own — a cluster grown by
	// starting the new member with the bigger -peers list used to split exactly
	// that way. With this rule, "start the new node with -peers naming the
	// existing members" is all an operator has to do: it joins, and -join is
	// only needed to point at one specific member.
	seeding := !bootstrapEligible(nodeID, peers, bootstrap)
	targets := joinTargets(nodeID, peers, joinAddr)
	if seeding {
		if strings.TrimSpace(joinAddr) == "" {
			logger.Warn(fmt.Sprintf("%s does not lead -peers: starting as a joiner (offering itself through the configured members) instead of writing an initial configuration. To author a brand-new cluster from this node, pass -bootstrap", nodeID),
				"seeds", targets, "first_id", canonicalFirstPeer(peers))
		} else {
			logger.Info("starting as a joiner: offering itself through the configured members", "targets", targets)
		}
	}

	// The engine is the Applier the FSM calls into; it also needs the Raft
	// node to submit commands. Build the engine first with a nil node, then
	// hand it to NewNode and wire the node back in.
	eng := cluster.NewEngine(nil, store, nodeID, logger)
	eng.SetDropRetention(dropRetention)
	// -replica-policy is a seed, not a live knob: an invalid value is a
	// startup error (same fail-fast rule as -drop-after), and a valid one is
	// applied by the controller ONLY on a brand-new cluster's first plan. An
	// existing cluster keeps the policy stored in the replicated table — the
	// admin endpoint is the entry point for every change after that.
	bootPolicy, err := cluster.NormalizeReplicaPolicy(replicaPolicy)
	if err != nil {
		return fmt.Errorf("-replica-policy: %w", err)
	}
	eng.SetBootPolicy(bootPolicy)
	// The per-slot replica factor follows from that policy tier and the member
	// count, re-derived by the controller every round — so a cluster that
	// grows at runtime raises its own copy count under the high tier, while
	// low/medium hold theirs.
	// Leader rebalance: 0 on either knob is the operator's off switch; a
	// negative value is a startup error (same fail-fast rule as -drop-after).
	if rebalanceInterval < 0 {
		return fmt.Errorf("-rebalance-interval %s must not be negative (positive = the check cadence, 0 = the rebalancer is off)", rebalanceInterval)
	}
	if rebalanceBatch < 0 {
		return fmt.Errorf("-rebalance-batch %d must not be negative (positive = hand-overs per round, 0 = the rebalancer is off)", rebalanceBatch)
	}
	eng.SetRebalanceConfig(rebalanceInterval, rebalanceBatch)
	if fetchSettle < 0 {
		return fmt.Errorf("-fetch-settle %s must not be negative", fetchSettle)
	}
	eng.SetFetchSettle(fetchSettle)
	if rebalanceInterval > 0 && rebalanceBatch > 0 {
		logger.Info("leader rebalance: the controller hands deviated slots back to the ring layout",
			"interval", rebalanceInterval.String(), "batch", rebalanceBatch)
	} else {
		logger.Info("leader rebalance disabled (-rebalance-interval or -rebalance-batch is 0)")
	}

	node, peerGRPC, err := cluster.NewNode(cluster.Config{
		NodeID:           nodeID,
		PeerAddr:         peerAddr,
		AdminAddr:        adminAddr,
		ClientAddr:       clientAddr,
		DataDir:          filepath.Join(dataDir, "cluster"),
		Peers:            peers,
		Seed:             seeding,
		FlushInterval:    raftFlush,
		SegmentBytes:     raftSegBytes,
		HeartbeatTimeout: raftHeartbeat,
		ElectionTimeout:  raftElection,
	}, eng, logger)
	if err != nil {
		return fmt.Errorf("raft node: %w", err)
	}
	defer node.Close()
	defer eng.ClosePeers()
	eng.SetNode(node)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Background replica fetch + controller reconcile loops.
	eng.Start(ctx)

	// Data-plane address self-discovery + inter-node plane: PeerService
	// (gRPC) is served on the peer port beside Raft (demuxed by first
	// byte); announce our admin/client addresses until they land in the
	// routing table.
	peerSrv := eng.ServePeer(peerGRPC)
	defer peerSrv.Stop()
	go eng.NewRegisterAnnouncer().Run(ctx)

	// Runtime join: a node that does not author the cluster's configuration
	// (see above) keeps offering itself through the configured members until
	// the leader has added it. A node that is already a member — the author
	// itself, or a joiner that has caught up — sends nothing.
	if seeding {
		go eng.NewJoiner(targets, adoptInterval).Run(ctx)
	}

	srv := api.NewServer(cluster.HostPort(adminAddr), api.New(eng, store), logger)
	errCh := make(chan error, 2)
	go func() { errCh <- srv.ListenAndServe() }()

	// gRPC data plane (event writes + queries) on its own port. The message
	// cap is operator-configurable (-grpc-max-msg-size, default 4MiB): a
	// BatchAppend must fit one batch inside it, both directions.
	grpcOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(int(grpcMaxMsgBytes)),
		grpc.MaxSendMsgSize(int(grpcMaxMsgBytes)),
	}
	grpcSrv := grpc.NewServer(grpcOpts...)
	grpcapi.NewServer(eng, store, logger, batchParallelism).Register(grpcSrv)
	grpcLis, err := net.Listen("tcp", cluster.HostPort(clientAddr))
	if err != nil {
		return fmt.Errorf("client listen %s: %w", clientAddr, err)
	}
	go func() { errCh <- grpcSrv.Serve(grpcLis) }()

	// The replica count is not a field here: it is derived from the member
	// count by the controller and logged when it changes.
	logger.Info("pushupes node started",
		"admin", adminAddr, "client", clientAddr, "peer", peerAddr, "slots", slotCount)

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("admin server: %w", err)
		}
	}

	// graceful drain: stop accepting, flush the WAL, leave Raft
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	grpcSrv.GracefulStop()
	return srv.Shutdown(shutdownCtx)
}

// parsePeers parses the bootstrap/join peer directory. Token grammar:
//
//	id=http://host:peerport  /  http://host:peerport  /  host:peerport
//	(the address doubles as the node id when no id= is given; the http://
//	scheme is configuration notation — PeerAddr itself stays host:port,
//	which is what the Raft transport dials)
//	id:peerport:adminport:clientport   (legacy; host defaults to 127.0.0.1)
//	id:host:peerport:adminport:clientport (legacy)
//
// The one-port form is the primary surface: a peer entry only needs its
// Raft address reachable — the admin/client addresses are self-announced
// into the routing table over the registration protocol (see
// internal/cluster/register.go), so operators do not have to wire three
// ports per node across machines. Without an explicit id, ip:port doubles
// as the node id. Legacy multi-port forms are still accepted (used by the
// dev cluster script); those addresses seed the table before registration
// patches them.
func parsePeers(csv string) ([]cluster.Peer, error) {
	if strings.TrimSpace(csv) == "" {
		return nil, nil
	}
	var out []cluster.Peer
	for _, tok := range strings.Split(csv, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		// optional node-id= prefix
		var id string
		if eq := strings.Index(tok, "="); eq >= 0 {
			id = tok[:eq]
			tok = tok[eq+1:]
			if id == "" {
				return nil, fmt.Errorf("bad peer %q: empty node id", tok)
			}
		}
		host, port, err := net.SplitHostPort(strings.TrimPrefix(tok, "http://"))
		if err == nil && host != "" {
			n, perr := strconv.Atoi(port)
			if perr != nil || n < 1 || n > 65535 {
				return nil, fmt.Errorf("bad peer %q: port %q is not 1-65535", tok, port)
			}
			if id == "" {
				id = net.JoinHostPort(host, port)
			}
			out = append(out, cluster.Peer{ID: id, PeerAddr: cluster.NormalizeAddr(net.JoinHostPort(host, port))})
			continue
		}
		if id != "" {
			return nil, fmt.Errorf("bad peer %q (want id=http://host:peerport)", tok)
		}
		// legacy 4/5-segment forms
		parts := strings.Split(tok, ":")
		var ports []string
		var phost string
		switch len(parts) {
		case 4:
			if id == "" {
				id = parts[0]
			}
			phost, ports = "127.0.0.1", parts[1:]
		case 5:
			if id == "" {
				id = parts[0]
			}
			phost, ports = parts[1], parts[2:]
		default:
			return nil, fmt.Errorf("bad peer %q (want [id=]ip:peerport)", tok)
		}
		if id == "" || phost == "" {
			return nil, fmt.Errorf("bad peer %q: empty id or host", tok)
		}
		for j, p := range ports {
			n, aerr := strconv.Atoi(p)
			if aerr != nil || n < 1 || n > 65535 {
				return nil, fmt.Errorf("bad peer %q: port %q (segment %d) is not 1-65535", tok, p, j+1)
			}
		}
		out = append(out, cluster.Peer{
			ID:         id,
			PeerAddr:   cluster.NormalizeAddr(phost + ":" + ports[0]),
			AdminAddr:  cluster.NormalizeAddr(phost + ":" + ports[1]),
			ClientAddr: cluster.NormalizeAddr(phost + ":" + ports[2]),
		})
	}
	return out, nil
}

func containsPeer(peers []cluster.Peer, id string) bool {
	for _, p := range peers {
		if p.ID == id {
			return true
		}
	}
	return false
}

// canonicalFirstPeer is the id that leads the configured list (the smallest,
// the same order raft's voter set uses) and therefore authors the cluster's
// configuration unless the operator says otherwise. "" for an empty list.
func canonicalFirstPeer(peers []cluster.Peer) string {
	first := ""
	for _, p := range peers {
		if p.ID != "" && (first == "" || p.ID < first) {
			first = p.ID
		}
	}
	return first
}

// bootstrapEligible reports whether this node writes the cluster's initial
// configuration. Exactly one node may: the one leading -peers, or whoever the
// operator marked with -bootstrap. A second author would write a competing
// entry at index 1 (same term, same index, different content) and split the
// cluster into raft groups that each commit on their own.
func bootstrapEligible(nodeID string, peers []cluster.Peer, explicit bool) bool {
	if explicit {
		return true
	}
	return canonicalFirstPeer(peers) == nodeID
}

// joinTargets lists the member addresses this node offers itself through: an
// explicit -join address when given, otherwise the configured members (never
// itself). Empty means the node has nobody to ask.
func joinTargets(nodeID string, peers []cluster.Peer, joinAddr string) []string {
	if a := strings.TrimSpace(joinAddr); a != "" {
		return []string{a}
	}
	out := make([]string, 0, len(peers))
	for _, p := range peers {
		if p.ID == nodeID || p.PeerAddr == "" {
			continue
		}
		out = append(out, p.PeerAddr)
	}
	return out
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// envIntOr reads an integer from the environment, falling back to def. A
// malformed or non-integer value is fatal rather than silently defaulting:
// a typo in a tuning knob should be loud.
func envIntOr(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid %s=%q: %v (want an integer like 8, or 0 to disable)\n", k, v, err)
		os.Exit(2)
	}
	return n
}

// envDurationOr reads a Go duration (30s, 2m, 1h) from the environment,
// falling back to def. A malformed value is fatal instead of silently falling
// back to the default: an operator typo in a knob that decides when data gets
// deleted must not be hidden.
func envDurationOr(k string, def time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid %s=%q: %v (want a duration like 30s, 2m, 0 to disable)\n", k, v, err)
		os.Exit(2)
	}
	return d
}

// byteSize is a flag.Value for -segment-bytes: it accepts a plain byte count
// (268435456) or a suffixed size (256MiB, 1GiB), so the flag can be written the
// way the sizing rule is stated. Ki/Mi/Gi are powers of two, K/M/G are powers
// of ten.
type byteSize int64

func (b *byteSize) String() string { return humanBytes(int64(*b)) }

func (b *byteSize) Set(s string) error {
	n, err := parseByteSize(s)
	if err != nil {
		return err
	}
	*b = byteSize(n)
	return nil
}

func parseByteSize(s string) (int64, error) {
	t := strings.TrimSpace(s)
	if t == "" {
		return 0, fmt.Errorf("empty size")
	}
	num, mult := t, int64(1)
	for _, suf := range []struct {
		name string
		mult int64
	}{
		{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40},
		{"KB", 1000}, {"MB", 1000 * 1000}, {"GB", 1000 * 1000 * 1000},
	} {
		if len(t) > len(suf.name) && strings.EqualFold(t[len(t)-len(suf.name):], suf.name) {
			num, mult = t[:len(t)-len(suf.name)], suf.mult
			break
		}
	}
	v, err := strconv.ParseInt(strings.TrimSpace(num), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("not a size: %q (use a byte count like 268435456, or a suffix like 256MiB or 1GiB)", s)
	}
	if v < 0 {
		return 0, fmt.Errorf("size must not be negative: %q", s)
	}
	return v * mult, nil
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<30 && n%(1<<30) == 0:
		return fmt.Sprintf("%dGiB", n>>30)
	case n >= 1<<20 && n%(1<<20) == 0:
		return fmt.Sprintf("%dMiB", n>>20)
	default:
		return strconv.FormatInt(n, 10)
	}
}
