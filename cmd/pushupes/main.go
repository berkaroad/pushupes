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

	"github.com/berkaroad/pushupes/internal/api"
	"github.com/berkaroad/pushupes/internal/cluster"
	"github.com/berkaroad/pushupes/internal/config"
	"github.com/berkaroad/pushupes/internal/raft"
	"github.com/berkaroad/pushupes/internal/storage"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/berkaroad/pushupes/internal/grpcapi"
)

// defaultConfig is the shipped startup configuration: the knobs internal/config
// owns come from config.Default, and the ones a layer owns come from that
// layer — the storage knobs and the slot count from internal/storage, the
// replica policy tier from cluster and the three consensus knobs
// (WAL window, WAL segment size, heartbeat/election timings) from the consensus
// package. Each default therefore has one definition, in the package
// whose contract it belongs to.
func defaultConfig() *config.Config {
	cfg := config.Default()
	cfg.SegmentBytes = storage.DefaultSegmentBytes
	cfg.FlushMessages = storage.DefaultFlushMessages
	cfg.FlushInterval = storage.DefaultFlushInterval
	cfg.FlushParallelism = storage.DefaultFlushParallelism
	cfg.SlotCount = storage.DefaultSlotCount
	cfg.ReplicaPolicy = string(cluster.DefaultReplicaPolicy)
	cfg.RaftFlushInterval = raft.DefaultFlushInterval
	cfg.RaftSegmentBytes = raft.DefaultSegmentBytes
	cfg.RaftHeartbeatTimeout = raft.DefaultHeartbeatTimeout
	cfg.RaftElectionTimeout = raft.DefaultElectionTimeout
	return cfg
}

func main() {
	cfg := defaultConfig()
	cfg.Register(flag.CommandLine)
	flag.Parse()

	base := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger := base.With("node", cfg.NodeID)

	if err := run(cfg, logger); err != nil {
		logger.Error("pushupes exited with error", "error", err)
		os.Exit(1)
	}
}

func run(cfg *config.Config, logger *slog.Logger) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	nodeRole, err := cluster.NormalizeRole(cfg.Role)
	if err != nil {
		return err
	}
	// Canonical form for every stored address: scheme required. A bare
	// host:port gets the default "http://" prefix; an explicit protocol
	// is honoured as passed. TCP-level uses (listen/dial) strip it again.
	cfg.AdminAddr = cluster.NormalizeAddr(cfg.AdminAddr)
	cfg.ClientAddr = cluster.NormalizeAddr(cfg.ClientAddr)
	cfg.PeerAddr = cluster.NormalizeAddr(cfg.PeerAddr)
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return err
	}

	if err := storage.ValidateSegmentBytes(cfg.SegmentBytes); err != nil {
		return err
	}
	// Post-migration retention: 0 means "the default", a negative window is
	// rejected here (fail fast). There is no way to switch the cleanup off.
	dropRetention, err := cluster.ResolveDropRetention(cfg.DropAfter)
	if err != nil {
		return err
	}
	logger.Info("post-migration cleanup: the former source drops its local copy after this retention",
		"drop_after", dropRetention.String())
	store, err := storage.OpenStore(cfg.DataDir, int32(cfg.SlotCount), cfg.SegmentBytes, storage.FlushPolicy{
		IntervalMessages: cfg.FlushMessages,
		Interval:         cfg.FlushInterval,
		Parallelism:      cfg.FlushParallelism,
	})
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()
	logger.Info("segment fsync parallelism", "in_flight", cfg.FlushParallelism,
		"note", "1 keeps one fsync at a time; more lets independent segments overlap on the device")

	// Membership comes first: authorship is a startup decision, and every
	// wrong combination fails the process before any port is bound.
	selfPeer := cluster.Peer{ID: cfg.NodeID, PeerAddr: cfg.PeerAddr, AdminAddr: cfg.AdminAddr, ClientAddr: cfg.ClientAddr}
	author, peers, targets, err := membershipPlan(cfg.NodeID, cfg.Peers, cfg.Join, nodeRole, cfg.Bootstrap, selfPeer)
	if err != nil {
		return err
	}
	seeding := !author
	if seeding {
		logger.Info("starting as a joiner: offering itself through the -join members",
			"role", nodeRole, "node", cfg.NodeID, "targets", targets)
	}

	// The engine is the Applier the FSM calls into; it also needs the Raft
	// node to submit commands. Build the engine first with a nil node, then
	// hand it to NewNode and wire the node back in.
	eng := cluster.NewEngine(nil, store, cfg.NodeID, logger)
	eng.SetDropRetention(dropRetention)
	// -replica-policy is a seed, not a live knob: an invalid value is a
	// startup error (same fail-fast rule as -drop-after), and a valid one is
	// applied by the controller ONLY on a brand-new cluster's first plan. An
	// existing cluster keeps the policy stored in the replicated table — the
	// admin endpoint is the entry point for every change after that.
	bootPolicy, err := cluster.NormalizeReplicaPolicy(cfg.ReplicaPolicy)
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
	eng.SetRebalanceConfig(cfg.RebalanceInterval)
	eng.SetStartupGrace(cfg.StartupGrace)
	if cfg.StartupGrace > 0 {
		logger.Info("start-up grace: a control node's first start holds its liveness verdict for the data nodes the directory already knows until they answer once, and keeps the first rebalance rounds out of that window",
			"grace", cfg.StartupGrace.String())
	} else {
		logger.Info("start-up grace disabled (-startup-grace is 0): liveness verdicts render from the first round")
	}
	eng.SetFetchSettle(cfg.FetchSettle)
	if cfg.RebalanceInterval > 0 {
		logger.Info("leader rebalance: the controller hands deviated slots back to the ring layout, one commit per round (serial)",
			"interval", cfg.RebalanceInterval.String())
	} else {
		logger.Info("leader rebalance disabled (-rebalance-interval is 0)")
	}

	node, peerGRPC, err := cluster.NewNode(cluster.Config{
		NodeID:           cfg.NodeID,
		PeerAddr:         cfg.PeerAddr,
		AdminAddr:        cfg.AdminAddr,
		ClientAddr:       cfg.ClientAddr,
		Role:             nodeRole,
		DataDir:          filepath.Join(cfg.DataDir, "cluster"),
		Peers:            peers,
		Seed:             seeding,
		FlushInterval:    cfg.RaftFlushInterval,
		SegmentBytes:     cfg.RaftSegmentBytes,
		HeartbeatTimeout: cfg.RaftHeartbeatTimeout,
		ElectionTimeout:  cfg.RaftElectionTimeout,
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
	// the raft configuration or the directory, by its own -cfg.Role). A clear
	// refusal is fatal: the id or the address belongs to somebody else and
	// no retry fixes that, so the process exits (fail-fast).
	var joinErr chan error
	if seeding {
		joinErr = make(chan error, 1)
		go func() { joinErr <- eng.NewJoiner(targets, cfg.AdoptInterval, nodeRole).Run(ctx) }()
	}

	srv := api.NewServer(cluster.HostPort(cfg.AdminAddr), api.New(eng, store), logger)
	errCh := make(chan error, 2)
	go func() { errCh <- srv.ListenAndServe() }()

	// gRPC data plane (event writes + queries) on its own port. The message
	// cap is operator-configurable (-grpc-max-msg-size, default 4MiB): a
	// BatchAppend must fit one batch inside it, both directions.
	grpcOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(int(cfg.GRPCMaxMsgBytes)),
		grpc.MaxSendMsgSize(int(cfg.GRPCMaxMsgBytes)),
		// Liveness pings, both directions, on the client surface. The
		// enforcement policy exists so the server does NOT GOAWAY the
		// pkg/client's 5s idle pings (the default 5m min would kill healthy
		// connections whose only crime is keeping themselves warm). The
		// server-side params detect a client whose network died hard — its
		// in-flight batches then fail fast and free the handler instead of
		// holding a half-open stream until the call timeout.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             config.GRPCKeepaliveMinTime,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    config.GRPCKeepaliveTime,
			Timeout: config.GRPCKeepaliveTimeout,
		}),
	}
	grpcSrv := grpc.NewServer(grpcOpts...)
	grpcapi.NewServer(eng, store, logger, cfg.ClientAddr).Register(grpcSrv)
	grpcLis, err := net.Listen("tcp", cluster.HostPort(cfg.ClientAddr))
	if err != nil {
		return fmt.Errorf("client listen %s: %w", cfg.ClientAddr, err)
	}
	go func() { errCh <- grpcSrv.Serve(grpcLis) }()

	// The replica count is not a field here: it is derived from the member
	// count by the controller and logged when it changes.
	logger.Info("pushupes node started",
		"admin", cfg.AdminAddr, "client", cfg.ClientAddr, "peer", cfg.PeerAddr, "slots", int(cfg.SlotCount))

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
	shutdownCtx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
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
