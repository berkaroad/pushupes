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
)

func main() {
	var (
		nodeID      = flag.String("node", envOr("PUSHUPES_NODE", "node-1"), "node id")
		adminAddr   = flag.String("admin", envOr("PUSHUPES_ADMIN", "http://127.0.0.1:8091"), "HTTP listen address: inter-node replication + admin + pprof")
		clientAddr  = flag.String("client", envOr("PUSHUPES_CLIENT", "http://127.0.0.1:8591"), "gRPC data-plane listen address (event writes + queries)")
		peerAddr    = flag.String("peer", envOr("PUSHUPES_PEER", "http://127.0.0.1:8391"), "Raft transport listen address (peer-to-peer)")
		dataDir     = flag.String("data", envOr("PUSHUPES_DATA", "./data"), "data directory")
		peers       = flag.String("peers", envOr("PUSHUPES_PEERS", ""), "comma list of id=http://host:peerport (admin/client addrs are self-registered; legacy id:peerport:adminport:clientport also accepted)")
		slotCount   = flag.Int("slots", data.DefaultSlotCount, "fixed slot count")
		replication = flag.Int("replication-factor", 2, "replicas per slot")
		segmentB    = flag.Int64("segment-bytes", storage.DefaultSegmentBytes, "WAL segment roll size in bytes")
		acksDefault = flag.String("acks", "leader", "default acks for appends: leader|all|none")
		flushN      = flag.Int64("flush-messages", 1000, "fsync every N records (0 disables)")
		flushD      = flag.Duration("flush-interval", 5*time.Second, "fsync every interval (0 disables)")
		bootstrap   = flag.Bool("bootstrap", false, "form a new cluster from -peers when no Raft state exists")
	)
	flag.Parse()

	base := logrus.New()
	base.SetLevel(logrus.InfoLevel)
	logger := logrus.NewEntry(base).WithField("node", *nodeID)

	if err := run(*nodeID, *adminAddr, *clientAddr, *peerAddr, *dataDir, *peers, *slotCount, *replication, *segmentB, *acksDefault, *flushN, *flushD, *bootstrap, logger); err != nil {
		logger.WithError(err).Fatal("pushupes exited with error")
	}
}

func run(nodeID, adminAddr, clientAddr, peerAddr, dataDir, peersCSV string, slotCount, replicationFactor int, segmentBytes int64, acksDefault string, flushN int64, flushD time.Duration, bootstrap bool, logger *logrus.Entry) error {
	// Canonical form for every stored address: scheme required. A bare
	// host:port gets the default "http://" prefix; an explicit protocol
	// is honoured as passed. TCP-level uses (listen/dial) strip it again.
	adminAddr = cluster.NormalizeAddr(adminAddr)
	clientAddr = cluster.NormalizeAddr(clientAddr)
	peerAddr = cluster.NormalizeAddr(peerAddr)
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}

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
	eng := cluster.NewEngine(nil, store, nodeID, acksDefault, logger)

	node, peerGRPC, err := cluster.NewNode(cluster.Config{
		NodeID:     nodeID,
		PeerAddr:   peerAddr,
		AdminAddr:  adminAddr,
		ClientAddr: clientAddr,
		DataDir:    filepath.Join(dataDir, "cluster"),
		Bootstrap:  bootstrap,
		Peers:      peers,
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

	// gRPC data plane (event writes + queries) on its own port.
	grpcSrv := grpc.NewServer()
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
