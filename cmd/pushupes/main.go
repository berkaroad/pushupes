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

	"github.com/berkaroad/pushupes/internal/api"
	"github.com/berkaroad/pushupes/internal/cluster"
	"github.com/berkaroad/pushupes/internal/data"
	"github.com/berkaroad/pushupes/internal/raft"
	"github.com/berkaroad/pushupes/internal/storage"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/berkaroad/pushupes/internal/grpcapi"
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
		peers      = flag.String("peers", envOr("PUSHUPES_PEERS", ""), "the cluster AUTHOR's initial Raft configuration document, nothing else: comma list of id=http://host:peerport (bare host:peerport and the legacy id:peerport:adminport:clientport also accepted; admin/client addrs are self-registered). Passing it makes THIS node the author, and it must name itself in the list; every other node joins through -join (mutually exclusive) (env PUSHUPES_PEERS)")
		joinAddr   = flag.String("join", envOr("PUSHUPES_JOIN", ""), "comma list of one or more running cluster members (host:peerport, optional id=) this node offers ITSELF to until admitted — the only way a node joins: -join plus the intended -role. Voter joiners land in the raft configuration, learners in the directory; the node's own -peer is the address authority. Mutually exclusive with -peers (env PUSHUPES_JOIN)")
		adoptTo    = flag.Duration("adopt-interval", envDurationOr("PUSHUPES_ADOPT_INTERVAL", cluster.DefaultAdoptInterval),
			"how often a node that is not a cluster member yet retries offering itself through the -join targets, tried in rotation (env PUSHUPES_ADOPT_INTERVAL)")
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
			"replica policy tier for a NEW cluster: low (1 copy per slot), medium (2, the default), high (3 copies per slot), or a positive number used as the factor directly; applied on the table's first plan only — an existing cluster keeps the policy the admin endpoint last set (env PUSHUPES_REPLICA_POLICY)")
		segmentB = byteSize(envByteSizeOr("PUSHUPES_SEGMENT_BYTES", storage.DefaultSegmentBytes))
		flushN   = flag.Int64("flush-messages", envInt64Or("PUSHUPES_FLUSH_MESSAGES", 1000), "fsync every N records (0 disables) (env PUSHUPES_FLUSH_MESSAGES)")
		flushD   = flag.Duration("flush-interval", envDurationOr("PUSHUPES_FLUSH_INTERVAL", storage.DefaultFlushInterval), "fsync every interval (0 disables) (env PUSHUPES_FLUSH_INTERVAL)")
		flushPar = flag.Int("flush-parallelism", envIntOr("PUSHUPES_FLUSH_PARALLELISM", 1), "segment fsyncs allowed in flight at once, the flush parallelism (1 = one at a time) (env PUSHUPES_FLUSH_PARALLELISM)")
		// How long a woken fetch round waits for the rest of the write burst
		// before answering. Every acknowledged append pays it once (the round
		// carrying its record pays it), and it buys rounds: 0 answers as soon
		// as the first slot has data, at the cost of more rounds and reports.
		fetchSettleD = flag.Duration("fetch-settle", envDurationOr("PUSHUPES_FETCH_SETTLE", cluster.DefaultFetchSettle),
			"fetch round coalescing window after a data wake (0 answers at once) (env PUSHUPES_FETCH_SETTLE)")
		dropAfter = flag.Duration("drop-after", envDurationOr("PUSHUPES_DROP_AFTER", cluster.DefaultDropRetention),
			"post-migration retention: how long the FORMER SOURCE keeps its local copy of a migrated slot before dropping it (positive = that window, 0 = the default 30s; negative is rejected; the cleanup cannot be disabled — env PUSHUPES_DROP_AFTER)")
		rebalanceInterval = flag.Duration("rebalance-interval", envDurationOr("PUSHUPES_REBALANCE_INTERVAL", cluster.DefaultRebalanceInterval),
			"how often the controller checks the leader layout against the slot ring and hands deviated slots back (a node that died returns to its own slots after failover; 0 disables the rebalancer; negative is rejected — env PUSHUPES_REBALANCE_INTERVAL)")
		startupGrace = flag.Duration("startup-grace", envDurationOr("PUSHUPES_STARTUP_GRACE", cluster.DefaultStartupGrace),
			"how long a control node's first start waits for the data nodes the directory already knows to come up before it starts treating an unanswered peer as failed, and keeps the first rebalance rounds out of that window (a whole-cluster restart whose data nodes start a few seconds late keeps its leader layout instead of piling every slot onto the first node that answers; 0 = render verdicts from the first round; negative is rejected — env PUSHUPES_STARTUP_GRACE)")
		// The client plane keeps gRPC's stock 4MiB message cap by default.
		// BatchAppend asks for one batch to fit inside the cap, so the
		// operator raises it together with typical batch size (a 4MiB cap
		// carries roughly 4000 records of 1KiB bodies).
		grpcMaxMsg    = byteSize(envByteSizeOr("PUSHUPES_GRPC_MAX_MSG_SIZE", defaultGrpcMaxMsgBytes))
		bootstrap     = flag.Bool("bootstrap", false, "author this cluster's initial configuration from this node. With -peers the author is the node whose id leads the list unless -bootstrap names another; -bootstrap alone (no -peers) authors a single-voter configuration this node can grow with -join voters later. A learner never authors, and -bootstrap is refused together with -join (env PUSHUPES_BOOTSTRAP not read: this is a startup decision, not a tunable)")
		role          = flag.String("role", envOr("PUSHUPES_ROLE", ""), "this node's place in the cluster, required: voter (control plane — votes in the consensus group and hosts NO slot replicas) or learner (data plane — receives the replicated layout from the control plane without voting). The smallest deployment is one control node plus one data node (env PUSHUPES_ROLE)")
		raftHeartbeat = flag.Duration("raft-heartbeat-timeout", envDurationOr("PUSHUPES_RAFT_HEARTBEAT_TIMEOUT", raft.DefaultHeartbeatTimeout),
			"consensus heartbeat interval (env PUSHUPES_RAFT_HEARTBEAT_TIMEOUT). A leader whose heartbeats do not arrive in time is voted out, so on a host that can delay the consensus event loop raise this together with -raft-election-timeout")
		raftElection = flag.Duration("raft-election-timeout", envDurationOr("PUSHUPES_RAFT_ELECTION_TIMEOUT", raft.DefaultElectionTimeout),
			"consensus election timeout: how long a follower waits for a heartbeat before standing for election (env PUSHUPES_RAFT_ELECTION_TIMEOUT). At least 2x -raft-heartbeat-timeout; raise both when the cluster votes leaders out under load")
		raftFlush = flag.Duration("raft-flush-interval", envDurationOr("PUSHUPES_RAFT_FLUSH_INTERVAL", cluster.DefaultRaftFlushInterval),
			"consensus WAL group-commit window: appends arriving within it share a single fsync (env PUSHUPES_RAFT_FLUSH_INTERVAL)")
		raftSegB = byteSize(envByteSizeOr("PUSHUPES_RAFT_SEGMENT_BYTES", cluster.DefaultRaftSegmentBytes))
	)
	// -segment-bytes takes a size, not just a byte count: 268435456 or 256MiB.
	flag.Var(&segmentB, "segment-bytes", "WAL segment roll `size` (bytes, or a suffix like 256MiB/1GiB); a multiple of 64MiB, at most 2GiB (env PUSHUPES_SEGMENT_BYTES)")
	flag.Var(&grpcMaxMsg, "grpc-max-msg-size", "client-plane gRPC max message `size` in bytes (suffixes like 16MiB accepted; the gRPC default 4MiB applies when unset) (env PUSHUPES_GRPC_MAX_MSG_SIZE)")
	flag.Var(&raftSegB, "raft-segment-bytes", "consensus WAL segment roll `size` (bytes, or a suffix like 64MiB) (env PUSHUPES_RAFT_SEGMENT_BYTES)")
	flag.Parse()

	base := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger := base.With("node", *nodeID)

	if err := run(*nodeID, *adminAddr, *clientAddr, *peerAddr, *dataDir, *peers, *joinAddr, *role, *adoptTo, slotCount, *replicaPolicy, int64(segmentB), int64(grpcMaxMsg), *flushN, *flushD, *flushPar, *dropAfter, *fetchSettleD, *rebalanceInterval, *startupGrace, *bootstrap, *raftFlush, *raftHeartbeat, *raftElection, int64(raftSegB), logger); err != nil {
		logger.Error("pushupes exited with error", "error", err)
		os.Exit(1)
	}
}

func run(nodeID, adminAddr, clientAddr, peerAddr, dataDir, peersCSV, joinAddr, role string, adoptInterval time.Duration, slotCount int, replicaPolicy string, segmentBytes, grpcMaxMsgBytes int64, flushN int64, flushD time.Duration, flushPar int, dropAfter, fetchSettle, rebalanceInterval, startupGrace time.Duration, bootstrap bool, raftFlush, raftHeartbeat, raftElection time.Duration, raftSegBytes int64, logger *slog.Logger) error {
	nodeRole, err := cluster.NormalizeRole(role)
	if err != nil {
		return err
	}
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
	if flushPar < 1 {
		return fmt.Errorf("-flush-parallelism %d must be at least 1 (one segment fsync in flight)", flushPar)
	}
	store, err := storage.OpenStore(dataDir, int32(slotCount), segmentBytes, storage.FlushPolicy{
		IntervalMessages: flushN,
		Interval:         flushD,
		Parallelism:      flushPar,
	})
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()
	logger.Info("segment fsync parallelism", "in_flight", flushPar,
		"note", "1 keeps one fsync at a time; more lets independent segments overlap on the device")

	// Membership comes first: authorship is a startup decision, and every
	// wrong combination fails the process before any port is bound.
	selfPeer := cluster.Peer{ID: nodeID, PeerAddr: peerAddr, AdminAddr: adminAddr, ClientAddr: clientAddr}
	author, peers, targets, err := membershipPlan(nodeID, peersCSV, joinAddr, nodeRole, bootstrap, selfPeer)
	if err != nil {
		return err
	}
	seeding := !author
	if seeding {
		logger.Info("starting as a joiner: offering itself through the -join members",
			"role", nodeRole, "node", nodeID, "targets", targets)
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
	// The per-slot replica factor follows from the policy — low 1 / medium 2 /
	// high 3, or a plain number for a direct factor — as
	// MIN(data-node count, copies asked for), re-derived by the controller
	// every round. Membership changes move the ring's seats, not the factor.
	// Leader rebalance: interval 0 is the operator's off switch; a negative
	// value is a startup error (same fail-fast rule as -drop-after). Commits
	// are strictly serial inside the loop, so this cadence paces convergence.
	if rebalanceInterval < 0 {
		return fmt.Errorf("-rebalance-interval %s must not be negative (positive = the check cadence, 0 = the rebalancer is off)", rebalanceInterval)
	}
	eng.SetRebalanceConfig(rebalanceInterval)
	if startupGrace < 0 {
		return fmt.Errorf("-startup-grace %s must not be negative (positive = how long the first rebalance waits for the registered data nodes to come up, 0 = no wait)", startupGrace)
	}
	eng.SetStartupGrace(startupGrace)
	if startupGrace > 0 {
		logger.Info("start-up grace: a control node's first start holds its liveness verdict for the data nodes the directory already knows until they answer once, and keeps the first rebalance rounds out of that window",
			"grace", startupGrace.String())
	} else {
		logger.Info("start-up grace disabled (-startup-grace is 0): liveness verdicts render from the first round")
	}
	if fetchSettle < 0 {
		return fmt.Errorf("-fetch-settle %s must not be negative", fetchSettle)
	}
	eng.SetFetchSettle(fetchSettle)
	if rebalanceInterval > 0 {
		logger.Info("leader rebalance: the controller hands deviated slots back to the ring layout, one commit per round (serial)",
			"interval", rebalanceInterval.String())
	} else {
		logger.Info("leader rebalance disabled (-rebalance-interval is 0)")
	}

	node, peerGRPC, err := cluster.NewNode(cluster.Config{
		NodeID:           nodeID,
		PeerAddr:         peerAddr,
		AdminAddr:        adminAddr,
		ClientAddr:       clientAddr,
		Role:             nodeRole,
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
	// routing table. A voter joiner's configured peer list is empty by
	// design (its configuration arrives from the leader's log), so the
	// -join targets are what its first announcements go through.
	peerSrv := eng.ServePeer(peerGRPC)
	defer peerSrv.Stop()
	go eng.NewRegisterAnnouncer(targets...).Run(ctx)

	// Runtime join: every non-author node — voter joiner AND learner alike —
	// offers itself through the -join list until the leader admits it (into
	// the raft configuration or the directory, by its own -role). A clear
	// refusal is fatal: the id or the address belongs to somebody else and
	// no retry fixes that, so the process exits (fail-fast).
	var joinErr chan error
	if seeding {
		joinErr = make(chan error, 1)
		go func() { joinErr <- eng.NewJoiner(targets, adoptInterval, nodeRole).Run(ctx) }()
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
		// Liveness pings, both directions, on the client surface. The
		// enforcement policy exists so the server does NOT GOAWAY the
		// pkg/client's 5s idle pings (the default 5m min would kill healthy
		// connections whose only crime is keeping themselves warm). The
		// server-side params detect a client whose network died hard — its
		// in-flight batches then fail fast and free the handler instead of
		// holding a half-open stream until the call timeout.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
	}
	grpcSrv := grpc.NewServer(grpcOpts...)
	grpcapi.NewServer(eng, store, logger, clientAddr).Register(grpcSrv)
	grpcLis, err := net.Listen("tcp", cluster.HostPort(clientAddr))
	if err != nil {
		return fmt.Errorf("client listen %s: %w", clientAddr, err)
	}
	go func() { errCh <- grpcSrv.Serve(grpcLis) }()

	// The replica count is not a field here: it is derived from the member
	// count by the controller and logged when it changes.
	logger.Info("pushupes node started",
		"admin", adminAddr, "client", clientAddr, "peer", peerAddr, "slots", slotCount)

	// Serve until a signal, a server error, or a fatal join refusal. The join
	// answer arrives ONCE: the admitted (nil) case clears the channel and the
	// loop keeps serving — only a refusal ends the process.
serve:
	for {
		select {
		case <-ctx.Done():
			logger.Info("shutdown signal received")
			break serve
		case err := <-errCh:
			if err != nil {
				return fmt.Errorf("admin server: %w", err)
			}
			break serve
		case err := <-joinErr:
			if err != nil {
				return err
			}
			logger.Info("join admitted; serving as a cluster member")
			joinErr = nil
		}
	}

	// graceful drain: stop accepting, flush the WAL, leave Raft
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	grpcSrv.GracefulStop()
	return srv.Shutdown(shutdownCtx)
}

// membershipPlan turns the two membership flags into the node's place at
// startup: it AUTHORS the cluster's initial raft configuration (-peers is its
// document; nothing else may carry it), or it is a JOINER offering itself
// through the -join list until a leader admits it (DESIGN §12.3: a voter into
// the raft configuration, a learner into the directory — the single CLI
// expression of "add a node"). Every mismatch is a startup error, fail-fast:
//
//   - -peers and -join are mutually exclusive; -peers must name this node and
//     only the list's leading id (or an explicit -bootstrap) may author;
//   - a learner must have -join targets (it offers itself to the control
//     plane and the list doubles as its layout-feed targets);
//   - a voter with neither flag is a mistake unless -bootstrap names it the
//     author of a single-voter configuration.
func membershipPlan(nodeID, peersCSV, joinCSV, role string, bootstrap bool, self cluster.Peer) (author bool, peers []cluster.Peer, targets []string, err error) {
	peers, err = parsePeers(peersCSV)
	if err != nil {
		return
	}
	joinList, err := parsePeers(joinCSV)
	if err != nil {
		return
	}
	if len(peers) > 0 && len(joinList) > 0 {
		err = fmt.Errorf("-join and -peers are mutually exclusive: -peers is the document of the node that AUTHORS the initial configuration; every other node — voter joiner or learner — joins by offering itself through -join")
		return
	}
	for _, p := range joinList {
		if p.ID == nodeID {
			err = fmt.Errorf("-join names this node itself (%s); a join target must be a member of the cluster being joined", nodeID)
			return
		}
		targets = append(targets, p.PeerAddr)
	}
	switch role {
	case cluster.RoleLearner:
		if bootstrap {
			err = fmt.Errorf("-bootstrap authors a raft configuration; a learner never votes and cannot author one")
			return
		}
		if len(targets) == 0 {
			err = fmt.Errorf("-role learner needs -join naming the control plane (comma list of host:peerport) to offer itself to")
			return
		}
		// The learner's -join list doubles as its feed target list: the
		// layout subscription dials them until a leader is known (see
		// learnernode.go dialTargets).
		peers = joinList
	case cluster.RoleVoter:
		switch {
		case len(peers) > 0:
			if !containsPeer(peers, nodeID) {
				err = fmt.Errorf("-peers must name this node (%s): only a member of the authored configuration can author it; a node that is not a voter yet joins with -join", nodeID)
				return
			}
			if !bootstrapEligible(nodeID, peers, bootstrap) {
				err = fmt.Errorf("only the author passes -peers: this node (%s) does not lead the list (leading id %s); start it with -join naming the cluster it joins, or pass -bootstrap if it is meant to author", nodeID, canonicalFirstPeer(peers))
				return
			}
			author = true
		case bootstrap:
			peers = []cluster.Peer{self}
			author = true
		}
		// A voter JOINER carries NO seed list: the raft node starts as a seed
		// with an empty membership and accepts the leader's messages until the
		// configuration arrives from the leader's log (an id-less -join entry
		// must not become a pretended voter set — the ids would not even match
		// the leader's own id, and its vote requests would be refused).
	}
	if !author && len(targets) == 0 {
		err = fmt.Errorf("a voter authors a cluster (-peers naming itself, or -bootstrap alone) or joins one (-join naming its members); this node was given neither")
		return
	}
	return
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

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
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

// envInt64Or reads an integer sized in records from the environment,
// falling back to def. A malformed or non-integer value is fatal rather
// than silently defaulting: a typo in a tuning knob should be loud.
func envIntOr(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid %s=%q: %v (want an integer like 4)\n", k, v, err)
		os.Exit(2)
	}
	return n
}

func envInt64Or(k string, def int64) int64 {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid %s=%q: %v (want an integer like 1000, or 0 to disable)\n", k, v, err)
		os.Exit(2)
	}
	return n
}

// envByteSizeOr reads a byte size (-segment-bytes, -grpc-max-msg-size,
// -raft-segment-bytes) from the environment, falling back to def. It accepts
// the same forms as the flags themselves — a plain byte count or a suffixed
// size (256MiB, 1GiB) — so an operator can write the env var the way the flag
// is documented; a malformed value is fatal like the other env readers.
func envByteSizeOr(k string, def int64) int64 {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := parseByteSize(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid %s=%q: %v (want bytes or a size like 256MiB/1GiB)\n", k, v, err)
		os.Exit(2)
	}
	return n
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
