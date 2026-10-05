// Command pushupes runs one pushupes node: the append-only slot WAL,
// the Raft control plane, the leader-follower replication and hot-migration
// data plane, and the HTTP surface (client / inter-node / admin).
package main

import (
	"context"
	"flag"
	"fmt"
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
	"pushupes/internal/storage"

	"github.com/sirupsen/logrus"
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
		// The slot count is a permanent layout decision (routing, placement,
		// migration granularity), so it is not a runtime knob — it is fixed at
		// data.DefaultSlotCount. See DESIGN §7.2.
		slotCount   = data.DefaultSlotCount
		replication = flag.Int("replication-factor", 2, "replicas per slot")
		segmentB    = byteSize(storage.DefaultSegmentBytes)
		flushN      = flag.Int64("flush-messages", 1000, "fsync every N records (0 disables)")
		flushD      = flag.Duration("flush-interval", 5*time.Second, "fsync every interval (0 disables)")
		dropAfter   = flag.Duration("drop-after", envDurationOr("PUSHUPES_DROP_AFTER", cluster.DefaultDropRetention),
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
		bootstrap  = flag.Bool("bootstrap", false, "accepted for compatibility: membership is static and written on first start, so this is a no-op")
		raftFlush  = flag.Duration("raft-flush-interval", envDurationOr("PUSHUPES_RAFT_FLUSH_INTERVAL", cluster.DefaultRaftFlushInterval),
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

	base := logrus.New()
	base.SetLevel(logrus.InfoLevel)
	logger := logrus.NewEntry(base).WithField("node", *nodeID)

	if err := run(*nodeID, *adminAddr, *clientAddr, *peerAddr, *dataDir, *peers, slotCount, *replication, int64(segmentB), int64(grpcMaxMsg), *flushN, *flushD, *dropAfter, *rebalanceInterval, *rebalanceBatch, *bootstrap, *raftFlush, int64(raftSegB), logger); err != nil {
		logger.WithError(err).Fatal("pushupes exited with error")
	}
}

func run(nodeID, adminAddr, clientAddr, peerAddr, dataDir, peersCSV string, slotCount, replicationFactor int, segmentBytes, grpcMaxMsgBytes int64, flushN int64, flushD, dropAfter time.Duration, rebalanceInterval time.Duration, rebalanceBatch int, bootstrap bool, raftFlush time.Duration, raftSegBytes int64, logger *logrus.Entry) error {
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
	logger.WithField("drop_after", dropRetention.String()).
		Info("post-migration cleanup: the former source drops its local copy after this retention")
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

	// The engine is the Applier the FSM calls into; it also needs the Raft
	// node to submit commands. Build the engine first with a nil node, then
	// hand it to NewNode and wire the node back in.
	eng := cluster.NewEngine(nil, store, nodeID, logger)
	eng.SetDropRetention(dropRetention)
	// Leader rebalance: 0 on either knob is the operator's off switch; a
	// negative value is a startup error (same fail-fast rule as -drop-after).
	if rebalanceInterval < 0 {
		return fmt.Errorf("-rebalance-interval %s must not be negative (positive = the check cadence, 0 = the rebalancer is off)", rebalanceInterval)
	}
	if rebalanceBatch < 0 {
		return fmt.Errorf("-rebalance-batch %d must not be negative (positive = hand-overs per round, 0 = the rebalancer is off)", rebalanceBatch)
	}
	eng.SetRebalanceConfig(rebalanceInterval, rebalanceBatch)
	if rebalanceInterval > 0 && rebalanceBatch > 0 {
		logger.WithFields(map[string]any{
			"interval": rebalanceInterval.String(), "batch": rebalanceBatch,
		}).Info("leader rebalance: the controller hands deviated slots back to the ring layout")
	} else {
		logger.Info("leader rebalance disabled (-rebalance-interval or -rebalance-batch is 0)")
	}

	node, peerGRPC, err := cluster.NewNode(cluster.Config{
		NodeID:        nodeID,
		PeerAddr:      peerAddr,
		AdminAddr:     adminAddr,
		ClientAddr:    clientAddr,
		DataDir:       filepath.Join(dataDir, "cluster"),
		Bootstrap:     bootstrap,
		Peers:         peers,
		FlushInterval: raftFlush,
		SegmentBytes:  raftSegBytes,
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
	grpcapi.NewServer(eng, store, logger).Register(grpcSrv)
	grpcLis, err := net.Listen("tcp", cluster.HostPort(clientAddr))
	if err != nil {
		return fmt.Errorf("client listen %s: %w", clientAddr, err)
	}
	go func() { errCh <- grpcSrv.Serve(grpcLis) }()

	logger.WithFields(map[string]any{
		"admin": adminAddr, "client": clientAddr, "peer": peerAddr, "slots": slotCount, "replicas": replicationFactor,
	}).Info("pushupes node started")

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
