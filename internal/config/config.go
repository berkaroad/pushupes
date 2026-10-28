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

// Package config holds pushupes's startup configuration and the runtime tuning
// of the layers that do not own an on-disk contract: every operator-facing
// default, the cluster / gateway / admin tuning constants that are not knobs,
// and the worker counts derived from the process CPU width. The packages read
// these from here instead of declaring their own copies, so a knob has one
// home: this file is the list an operator (and a reviewer) reads to know what
// the service ships with.
//
// It imports only the standard library, so every internal package may depend
// on it.
//
// A layer that owns an on-disk or on-wire contract keeps its own numbers in its
// own config.go — internal/raft/config.go for the consensus engine,
// internal/storage/config.go for the WAL, segment and index formats — so a
// change to a layer's contract shows up as a diff of exactly one file. Those
// layers are the single definition of their defaults, and the command seeds the
// fields below from them: the two raft timings from raft.DefaultHeartbeatTimeout
// / raft.DefaultElectionTimeout, the replica policy tier from
// cluster.DefaultReplicaPolicy, and the storage knobs plus the slot count from
// internal/storage.
package config

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Topology and path defaults
// ---------------------------------------------------------------------------

const (
	// DefaultNodeID, DefaultDataDir and the three listen addresses are the
	// shipped startup defaults (-node, -data, -admin, -client, -peer).
	DefaultNodeID     = "node-1"
	DefaultDataDir    = "./data"
	DefaultAdminAddr  = "http://127.0.0.1:8091"
	DefaultClientAddr = "http://127.0.0.1:8591"
	DefaultPeerAddr   = "http://127.0.0.1:8391"
)

// ---------------------------------------------------------------------------
// Replication, migration and membership: shipped defaults
// ---------------------------------------------------------------------------

const (
	// FetchSettle is the fetch round's coalescing window (see
	// SetFetchSettle): how long a woken round waits for the rest of the same
	// write burst before it scans and answers. Measured on the 3-node bench
	// (1KiB, conns 4, batch 100, mirrored 2ms/200us/0 sweep): answering at
	// once is WORSE than coalescing (fewer slots per round leaves the tail of a
	// burst to a later round: hw_wait 6.42ms at 0 vs 4.60ms at 200us against
	// 5.70ms at 2ms), so the window earns its keep -- but 2ms is overpaid, at
	// 37 slots per round against 29 for the same acknowledgement latency.
	FetchSettle = 200 * time.Microsecond

	// DropAfter is how long a migrated slot's former source keeps its
	// local copy before dropping it. Long enough to survive an immediately noticed
	// bad migration, short enough that surplus copies do not linger; operators
	// tune it with -drop-after.
	DropAfter = 30 * time.Second

	// RebalanceInterval is how often the controller re-checks the
	// ring layout. The layout only ever needs healing after a failover or a
	// late-joined replica catches up; a fast tick matters because the batch
	// (not the tick) is the throughput knob the operator thinks about.
	RebalanceInterval = 2 * time.Second

	// StartupGrace is how long a control node's first start holds the
	// liveness verdict for registered data nodes that have not answered a probe
	// yet (-startup-grace). On a whole-cluster restart the layout on disk is
	// already the intended one while the data nodes themselves are still coming
	// up; without the wait the sweep marks every lagging node down within
	// seconds and the failover piles its slots onto the first node that answers
	// — measured on a staged restart: 1024 slots on one data node 3.1s in, then
	// ~34 minutes of serial hand-backs to undo. The wait ends early as soon as
	// every covered node has answered; a node that stays silent past it is
	// judged exactly as before.
	StartupGrace = 90 * time.Second

	// AdoptInterval is how often a node that is not a member yet retries
	// offering itself through -join.
	AdoptInterval = 3 * time.Second
	// AddMemberTimeout bounds one runtime membership change.
	AddMemberTimeout = 30 * time.Second
)

// ---------------------------------------------------------------------------
// Consensus knobs of the cluster layer
// ---------------------------------------------------------------------------

const (
	// The consensus engine's own timings and defaults (heartbeat, election,
	// WAL group-commit window and segment size) live in internal/raft/config.go.
	// The values below are the cluster layer's: the zero-value fallbacks of
	// cluster.Config — how long one committed batch may take to apply, when a
	// snapshot is taken (entries and cadence) and how many entries stay behind
	// it.
	RaftApplyTimeout             = 10 * time.Second
	RaftSnapshotThreshold uint64 = 1024
	RaftSnapshotInterval         = 30 * time.Second
	RaftTrailingLogs      uint64 = 256
)

// ---------------------------------------------------------------------------
// Client plane, admin plane and process lifetime
// ---------------------------------------------------------------------------

const (
	// GRPCMaxMsgBytes is the client-plane gRPC message cap (-grpc-max-msg-size):
	// gRPC's own stock 4MiB. One BatchAppend has to fit inside it in both
	// directions, so it moves together with typical batch size.
	GRPCMaxMsgBytes int64 = 4 << 20

	// Client-plane keepalive: the enforcement policy exists so the server does
	// not GOAWAY a client's 5s idle pings, and the server parameters detect a
	// client whose network died hard.
	GRPCKeepaliveMinTime = 5 * time.Second
	GRPCKeepaliveTime    = 30 * time.Second
	GRPCKeepaliveTimeout = 10 * time.Second

	// ShutdownTimeout bounds the graceful drain: stop accepting, flush the
	// WAL, leave Raft (and the admin server's own Shutdown call).
	ShutdownTimeout = 10 * time.Second
)

// ---------------------------------------------------------------------------
// Replica fetch, watermark and confirmation windows
// ---------------------------------------------------------------------------

const (
	// FetchBaseInterval is the replica fetch cadence on an idle slot;
	// FetchMaxBackoff caps the exponential backoff a failing fetch walks up to.
	FetchBaseInterval  = 100 * time.Millisecond
	FetchMaxBackoff    = 2 * time.Second
	FetchWait          = 2 * time.Second  // follower-requested long-poll budget
	FetchSlack         = 1 * time.Second  // client-side timeout margin
	FetchMaxWait       = 5 * time.Second  // leader-side cap on a requested wait
	FetchSweepInterval = 2 * time.Second  // stamp all positions at least this often
	ISRStaleAfter      = 10 * time.Second // must exceed FetchWait+FetchMaxBackoff
	// CommitFloorWait bounds how long a leader holds a write back while its
	// copy catches up to the slot's commit floor (see awaitCommitFloor). The
	// catch-up is one fetch round in the normal case; the bound only exists so
	// a stalled fetch degrades to the old behaviour instead of wedging writes.
	CommitFloorWait = 2 * time.Second
	HWWaitTimeout   = 10 * time.Second // the write-ack watermark deadline
	// FetchTailRecords is how close to a slot's head a follower counts as
	// ACK-CRITICAL: a position within this many records is served before the
	// round's bulk backlog share. It is a record count (not bytes) because the
	// property being protected is "the follower is one round's production
	// behind", and a writer produces records, not bytes; a tail's byte size is
	// still bounded by the round budget's per-slot read cap.
	FetchTailRecords = 64
	// MaxEarlyReportsPerRound caps how many ACK-CRITICAL positions a round
	// reports the moment they land (see fetchRound): each is one small RPC, so
	// the cap bounds a round whose tails are unusually many without ever
	// letting the urgent ones wait for the round's bulk apply.
	MaxEarlyReportsPerRound = 16
	// MaxPayloadBytes caps one long-poll response: during a backlog the
	// session streams item by item across rounds instead of building a
	// gigabyte JSON blob of every slot at once.
	MaxPayloadBytes = 32 << 20

	// SeatBuildGrace bounds how long a seat counts as "being built" for a leader
	// that takes the slot over mid-catch-up (see takeover). A copy that is still
	// fetching after this window is treated as an ordinary member — the promise
	// must not stay relaxed for a seat that is simply slow, and a fetch that long
	// means something else is wrong.
	SeatBuildGrace = time.Minute
	// EmptyLeaderConfirmWindow is how long a slot must keep reading as an
	// empty-shell leader before writes are refused: a copy being (re)built can
	// briefly read empty while its seats still report the previous holder's
	// history — the same transient that confirmDiverged guards against.
	EmptyLeaderConfirmWindow = 2 * time.Second
	// SessionLossRecheck throttles the watermark re-derivation a session loss
	// triggers (see noteFollowerSessionLost): at most one per interval per follower.
	SessionLossRecheck = 200 * time.Millisecond
	// RingCatchUpWindow bounds how long a follower's ring-catch-up report keeps the
	// other sessions at half budget: a session that stopped reporting (its node went
	// away, the slots moved) must not throttle them forever.
	RingCatchUpWindow = 30 * time.Second
	// RefusedConfirmWindow is how long a peer's peer port must keep REFUSING
	// connections before the controller calls the process gone. A refused connect is
	// the strongest liveness signal there is — nothing is listening — but a process
	// that just restarted refuses too while it binds its port (measured: under a
	// second), so a single refusal must not be a verdict: the whole first
	// whole-cluster wave that made every lagging data node look dead came from
	// acting one probe too early. Past the window the verdict is kept by wall clock
	// rather than by the strike count, so a round that runs long (a slow peer's
	// probe timeout) does not stretch it.
	RefusedConfirmWindow  = 3 * time.Second
	LivenessFailThreshold = 3
	PlanSettleMaxRounds   = 2
	// DialRefusedTimeout bounds the fast "is anything listening" dial. A refused
	// connection comes back immediately; only a black-holed address (a firewall
	// dropping SYN) waits this long.
	DialRefusedTimeout = 300 * time.Millisecond
	// StorageSampleInterval is how stale the cluster storage gauge may get before
	// a reader of /admin/cluster/status triggers a background refresh. The sum
	// needs one peer round trip per other member, so it is never computed inline:
	// the status answers from the last sample, and the read that found the sample
	// stale schedules the next one. A console polling the page therefore keeps the
	// gauge fresh; with no reader at all, an idle node samples nothing.
	StorageSampleInterval = 2 * time.Second

	// UnreachableQuorum is how many DISTINCT observers must report the same
	// node within UnreachableWindow before it is marked down ahead of the probe
	// threshold. Two is the smallest number that cannot be a single broken
	// link; it is deliberately not a majority: the witnesses are the nodes
	// actively fetching from the suspect, which is every other member in
	// practice, and waiting for a majority would give back the latency the
	// scheme exists to remove.
	UnreachableQuorum = 2
	// UnreachableWindow is how long a witness counts, and how often one
	// observer may report the same suspect.
	UnreachableWindow = time.Second
	// UnreachableReportAfter is how many CONSECUTIVE transport failures make a
	// session's observer speak up: one failure can be a blip (a table-driven
	// session restart, a hiccup), two in a row at fetch cadence cannot.
	UnreachableReportAfter = 2
)

// ---------------------------------------------------------------------------
// Leader rebalance, drain and reclaim rounds
// ---------------------------------------------------------------------------

const (
	// RebalanceCommitsPerRound is how many layout commits one controller round
	// executes: exactly one. A commit's fence freezes one slot's writes, so a
	// strictly serial commit loop keeps the frozen-slot count at one and the
	// write-latency noise bounded to a single slot; convergence is paced by
	// the round interval instead (a node that led 512 slots — half of the
	// 1024-slot table, the most any one node leads under the two-replica
	// ring — returns in 512 rounds, and a factor decrease reclaims one
	// surplus seat per round).
	RebalanceCommitsPerRound = 1
	// DrainCommitsPerRound widens the serial loop while any member is
	// marked draining: the commits STILL run one by one inside the round
	// (the fence only ever freezes one slot at a time), but a drain waits
	// one round per hand-over instead of one tick per hand-over — even a
	// node leading the worst-case 512 slots (half of the 1024-slot table,
	// the most any one node leads under the two-replica ring) drains in
	// seconds, not hours.
	DrainCommitsPerRound = 64
	// ReclaimBudget bounds one reclaim sweep. The sweep retires every surplus
	// seat it can inside the budget and the next round continues, so a factor
	// decrease on a large table converges in a round or two instead of one seat
	// per round (1024 slots at one seat per interval is about half an hour). It must
	// stay under the round interval so a sweep cannot run back to back with the
	// next round.
	ReclaimBudget = 1 * time.Second
	// ReclaimProbeConcurrency bounds the copy checks in flight in one sweep. Each
	// probe is a peer round trip, so the sweep must not open one per surplus seat.
	ReclaimProbeConcurrency = 16
	RebalanceRetryBase      = RebalanceInterval
	RebalanceRetryMax       = time.Minute
	// DrainTick is the pace a scale-down forces. The rebalancer normally sleeps
	// when the operator turned it off (interval 0) — but a drain mark is the
	// operator asking the cluster to MOVE data, and nobody else does that work:
	// stopping the loop here would leave every DELETE parked in "draining"
	// forever on a rebalance-off cluster. So a drain outranks the switch, and
	// outranks the configured interval when that one is slower: the rounds of a
	// handover run back to back at DrainTick until the node is empty.
	DrainTick = 2 * time.Second
	// DivergedConfirmWindow is how long a copy must keep reading as diverged (same
	// log end offset, different directory) before a round acts on it. A copy's
	// directory lags its log end offset for a moment while an apply batch lands,
	// and that reads exactly like a fork: measured on a rejoining node, two slots
	// read diverged with no repair action, and the same data read clean a minute
	// later. Dropping on one reading would make the node pull the whole slot again
	// for nothing.
	DivergedConfirmWindow = 2 * time.Second
)

// ---------------------------------------------------------------------------
// Migration fence
// ---------------------------------------------------------------------------

const (
	// FenceDrainTimeout bounds waiting for in-flight appends to finish while
	// the fence is being taken. A write already accepted holds the fence for
	// its whole local append (including the watermark wait), so this
	// must exceed a healthy wait and stays far below the 10s client deadline.
	FenceDrainTimeout = 2 * time.Second
	// FenceCatchUpTimeout bounds how long the source holds the fence waiting
	// for the target to confirm the frozen tail. The common case is "already
	// caught up" (the best-effort mirror and the fetch loop keep the target
	// within a few ms) and returns almost immediately; this is the fallback.
	FenceCatchUpTimeout = 3 * time.Second
	// FenceDurableTimeout bounds the target's flush. The fence holds the slot's
	// writes for its duration, so a target that cannot get the tail on disk
	// fails the hand-over (release + backoff) rather than wedging the slot.
	FenceDurableTimeout = 3 * time.Second
	// FenceMaxHold is the source-side watchdog: a fence is force-released
	// after this long even if no controller action ever arrives, so a crashed
	// controller cannot block a slot's writes forever.
	FenceMaxHold = 8 * time.Second
	// FenceRPCTimeout is the controller-side bound on the FenceSlot call: it
	// covers the drain + catch-up on the source plus one RPC margin.
	FenceRPCTimeout = FenceDrainTimeout + FenceCatchUpTimeout + time.Second
	// FencePushBatch caps one tail-push read (frames are streamed in batches).
	FencePushBatch = 4 << 20
	// FenceTargetGateTimeout bounds how long the source keeps a COMMITTED
	// fence's writers blocked while waiting for the migration target to apply
	// the leader move (see holdFenceUntilTargetLeader). The target applies it
	// within a Raft round trip (a few ms); this is the fallback when the target
	// is slow or unreachable, and stays well inside the 10s client deadline and
	// the fence's own watchdog (FenceMaxHold).
	FenceTargetGateTimeout = 1 * time.Second
	// FenceTargetGatePoll is the interval between two target-readiness probes.
	FenceTargetGatePoll = 2 * time.Millisecond
	// FenceTargetGateRPCTimeout bounds one readiness probe (a hung target must
	// not stretch the gate past its deadline).
	FenceTargetGateRPCTimeout = 500 * time.Millisecond
)

// ---------------------------------------------------------------------------
// Peer plane: accept, dial, RPC and message bounds
// ---------------------------------------------------------------------------

const (
	// PeerPeekTimeout bounds how long the demux waits for the first byte that
	// tells a gRPC connection from a Raft frame; PeerAcceptQueue is how many
	// accepted connections may wait for that classification, and
	// PeerDialTimeout bounds one outbound peer dial.
	PeerPeekTimeout = 10 * time.Second
	PeerAcceptQueue = 64
	PeerDialTimeout = 2 * time.Second
	// PeerRPCTimeout is the general peer RPC budget (progress probes, layout
	// queries, table commands) on one node-to-node gRPC connection.
	PeerRPCTimeout = 30 * time.Second
	// FailoverPickTimeout bounds ONE round of "what is your LEO for these
	// slots?" asked of the failover candidates. The pick is best effort (a
	// candidate that does not answer just drops out of the running), so it gets
	// a healthy-peer budget rather than the general peer RPC timeout — a
	// failover must not wait 30s for a dying node's answer.
	FailoverPickTimeout = 1 * time.Second
	// PeerPingTimeout bounds one liveness probe (PeerPing) of a peer.
	PeerPingTimeout = 2 * time.Second
	// StorageQueryTimeout bounds ONE round of "what are your slot sizes?"
	// (the cluster storage gauge, see runStorageSampler). Shorter than the
	// general peer RPC budget: a peer that does not answer costs the gauge one
	// sample, not the whole refresh.
	StorageQueryTimeout = 2 * time.Second
	// PeerMaxMsgBytes bounds gRPC messages on the peer plane. Snapshot
	// segments stream in <=4MiB chunks and fetch rounds cap at
	// MaxPayloadBytes (32MiB), so a 64MiB ceiling covers every message
	// with headroom while keeping a malicious peer from pinning memory.
	PeerMaxMsgBytes = 64 << 20
)

// ---------------------------------------------------------------------------
// Registration, layout feed and per-slot bookkeeping
// ---------------------------------------------------------------------------

const (
	// RegisterRetryFast and RegisterRetrySteady pace the address-announcement
	// retries: fast while the node just started and its registration is
	// expected to land, steady once it is up. RegisterRPCTimeout bounds one
	// announcement RPC.
	RegisterRetryFast   = time.Second
	RegisterRetrySteady = 10 * time.Second
	RegisterRPCTimeout  = 5 * time.Second
	// LayoutApplyFlush bounds how often the resume point is written: often
	// enough that a restart re-reads a recent index, rare enough that a burst
	// of table commands is not a burst of fsyncs. A crash that loses the last
	// few applies costs a replay of entries this node may already have applied
	// — safe because every command is deterministic and idempotent (see
	// layoutfeed.go).
	LayoutApplyFlush = 64
	// LayoutArchiveReset is how long a subscription must have lived before it
	// counts as "this address works" and the reconnect backoff resets.
	LayoutArchiveReset = 30 * time.Second
	// LayoutDialTimeout bounds one dial of a layout-feed target.
	LayoutDialTimeout = 10 * time.Second
	// LayoutBufFrames bounds one subscriber's buffer, in frames. A tick pushes at
	// most a batch of entries plus a commit, so this holds several ticks of
	// backlog: enough to absorb a learner that is merely busy, small enough that a
	// learner which stopped reading is cut (and re-syncs) instead of growing the
	// leader's heap.
	LayoutBufFrames = 256
	// MaxLayoutBatch bounds how many entries one fill sends a subscriber: the
	// same order as the consensus layer's append batch, because the learner is
	// asked to keep up with the same log the voters do.
	MaxLayoutBatch = 64
	// HandoverStageTimeout bounds the table command that stages a handover (the
	// target joins the replica set). The catch-up itself is not bounded here: the
	// target pulls at whatever rate the fetch path gives it, and the controller
	// commits on a later round once the target holds the source's log.
	HandoverStageTimeout = 5 * time.Second
	// OwnerRefreshInterval bounds how long a placement change may go unstamped;
	// the apply path signals the loop directly, so this is the safety net for a
	// WAL appearing without a table change (first write into a fresh seat).
	OwnerRefreshInterval = 2 * time.Second
)

// ---------------------------------------------------------------------------
// Admin and health surfaces
// ---------------------------------------------------------------------------

const (
	// HealthScanTimeout bounds one walk: 1024+ slots x a few local describes is
	// seconds of work; anything beyond minutes is a node wedged, and the walk
	// must not live past its usefulness.
	HealthScanTimeout = 2 * time.Minute

	// HealthClientTimeout bounds one admin request the health walk makes to a
	// node, and AdminReadHeaderTimeout bounds reading one admin request's
	// headers on the serving side.
	HealthClientTimeout    = 15 * time.Second
	AdminReadHeaderTimeout = 10 * time.Second

	// AdminMigrateTimeout and AdminRemoveReplicaTimeout bound the two long
	// admin actions; AdminAddMemberMargin is the slack added on top of
	// AddMemberTimeout for the node-add request.
	AdminMigrateTimeout       = 60 * time.Second
	AdminRemoveReplicaTimeout = 30 * time.Second
	AdminAddMemberMargin      = 5 * time.Second

	// StatsWorstLimit is the largest ?worst=N /admin/stats accepts: the
	// per-slot list it names (the N most-starved slots by watermark lag) is a
	// debugging view, and the page it renders is bounded accordingly.
	StatsWorstLimit = 1024
)

// ---------------------------------------------------------------------------
// Process width: the worker counts follow runtime.GOMAXPROCS(0)
// ---------------------------------------------------------------------------

const (
	// Per-CPU factors behind the worker counts below.
	BatchPoolWorkersPerCPU = 16
	ApplyWorkersPerCPU     = 2
)

var (
	// Procs is the runtime's own view of this process's CPU width: the same
	// measure the pools below use, so inside a container it reflects the
	// enforced quota rather than the host's core count, and a node does not
	// fan out a worker per host core.
	Procs = runtime.GOMAXPROCS(0)

	// BatchPoolWorkers sizes the persistent slot-worker pool that executes
	// every BatchAppend's slot groups: sixteen times the CPU width, so the
	// fan-out follows the machine the node runs on. A worker holds its slot
	// for the whole SubmitBatch — mostly parked on the merged watermark wait,
	// holding no CPU — so a pool the width of the CPU would queue waits that
	// previously ran overlapped. Measured under multi-writer batch load,
	// sixteen CPUs' worth of workers keeps the pool from throttling the load
	// while still bounding concurrent WAL writers process-wide, and the
	// long-lived workers keep their grown execution stacks warm across
	// batches.
	BatchPoolWorkers = BatchPoolWorkersPerCPU * Procs

	// ApplyWorkers and ApplyParallelMin size the fetch round's apply pool: the
	// round's items are different slots, so they are applied concurrently
	// while one slot's records stay in seq order inside its item. Below
	// ApplyParallelMin a pool costs more than it saves.
	ApplyWorkers     = ApplyWorkersPerCPU * Procs
	ApplyParallelMin = Procs

	// HWWaitTick is waitForHW's safety net. The watermark normally advances on a
	// replica report, which kicks the gate; the transitions that produce no report
	// at all — a replica going stale (ISRStaleAfter with no further reports) or a
	// peer being marked down — are why a parked wait still re-checks on a tick.
	// It replaces a 2ms poll: 40x less work with the same worst case on those
	// paths, and no poll interval at all on the common one. A variable so a test
	// can push it out of the way and prove the wake comes from the event.
	HWWaitTick = 50 * time.Millisecond
)

// ---------------------------------------------------------------------------
// Startup configuration: one operator-facing knob per field
// ---------------------------------------------------------------------------

// Config is one node's startup configuration: every operator-facing knob and
// its shipped default. Default returns the defaults, Register binds them to a
// flag set (with the PUSHUPES_* environment behind each flag) and Validate
// applies the fail-fast rules before anything is opened or bound.
type Config struct {
	NodeID     string
	AdminAddr  string
	ClientAddr string
	PeerAddr   string
	DataDir    string
	Peers      string
	Join       string
	Role       string
	Bootstrap  bool

	AdoptInterval     time.Duration
	FetchSettle       time.Duration
	DropAfter         time.Duration
	RebalanceInterval time.Duration
	StartupGrace      time.Duration

	GRPCMaxMsgBytes int64

	// The fields below take their shipped values from the package that owns
	// the contract: SegmentBytes / FlushMessages / FlushInterval /
	// FlushParallelism / SlotCount from internal/storage, ReplicaPolicy from
	// cluster.DefaultReplicaPolicy, and the raft knobs from the consensus
	// package. Default leaves them
	// zero; the command seeds them before the flags are registered.
	SlotCount        int32
	ReplicaPolicy    string
	SegmentBytes     int64
	FlushMessages    int64
	FlushInterval    time.Duration
	FlushParallelism int

	RaftFlushInterval    time.Duration
	RaftSegmentBytes     int64
	RaftHeartbeatTimeout time.Duration
	RaftElectionTimeout  time.Duration
}

// Default is the shipped configuration for the knobs this package owns. The
// layer-seeded fields (see Config) stay zero here: the command fills them from
// internal/storage, internal/raft, internal/cluster and internal/data before it
// registers the flags, so each of those values keeps one definition.
func Default() *Config {
	return &Config{
		NodeID:     DefaultNodeID,
		AdminAddr:  DefaultAdminAddr,
		ClientAddr: DefaultClientAddr,
		PeerAddr:   DefaultPeerAddr,
		DataDir:    DefaultDataDir,

		AdoptInterval:     AdoptInterval,
		FetchSettle:       FetchSettle,
		DropAfter:         DropAfter,
		RebalanceInterval: RebalanceInterval,
		StartupGrace:      StartupGrace,

		GRPCMaxMsgBytes: GRPCMaxMsgBytes,
	}
}

// Register binds every knob to fs. Each flag's default is the field's current
// value, overridden by its PUSHUPES_* environment variable when that is set, so
// the environment works exactly like the flag and a malformed value is fatal
// instead of silently falling back.
func (c *Config) Register(fs *flag.FlagSet) {
	fs.StringVar(&c.NodeID, "node", envOr("PUSHUPES_NODE", c.NodeID), "node id (env PUSHUPES_NODE)")
	fs.StringVar(&c.AdminAddr, "admin", envOr("PUSHUPES_ADMIN", c.AdminAddr), "HTTP admin listen address: admin API + pprof (env PUSHUPES_ADMIN); events and all inter-node traffic use -client and -peer")
	fs.StringVar(&c.ClientAddr, "client", envOr("PUSHUPES_CLIENT", c.ClientAddr), "gRPC data-plane listen address: event writes and queries (env PUSHUPES_CLIENT)")
	fs.StringVar(&c.PeerAddr, "peer", envOr("PUSHUPES_PEER", c.PeerAddr), "Raft transport + peer gRPC listen address: all node-to-node traffic (env PUSHUPES_PEER)")
	fs.StringVar(&c.DataDir, "data", envOr("PUSHUPES_DATA", c.DataDir), "data directory (env PUSHUPES_DATA)")
	fs.StringVar(&c.Peers, "peers", envOr("PUSHUPES_PEERS", c.Peers), "the cluster AUTHOR's initial Raft configuration document, nothing else: comma list of id=http://host:peerport (bare host:peerport and the legacy id:peerport:adminport:clientport also accepted; admin/client addrs are self-registered). Passing it makes THIS node the author, and it must name itself in the list; every other node joins through -join (mutually exclusive) (env PUSHUPES_PEERS)")
	fs.StringVar(&c.Join, "join", envOr("PUSHUPES_JOIN", c.Join), "comma list of one or more running cluster members (host:peerport, optional id=) this node offers ITSELF to until admitted — the only way a node joins: -join plus the intended -role. Voter joiners land in the raft configuration, learners in the directory; the node's own -peer is the address authority. Mutually exclusive with -peers (env PUSHUPES_JOIN)")
	fs.BoolVar(&c.Bootstrap, "bootstrap", c.Bootstrap, "author this cluster's initial configuration from this node. With -peers the author is the node whose id leads the list unless -bootstrap names another; -bootstrap alone (no -peers) authors a single-voter configuration this node can grow with -join voters later. A learner never authors, and -bootstrap is refused together with -join (env PUSHUPES_BOOTSTRAP not read: this is a startup decision, not a tunable)")
	fs.StringVar(&c.Role, "role", envOr("PUSHUPES_ROLE", c.Role), "this node's place in the cluster, required: voter (control plane — votes in the consensus group and hosts NO slot replicas) or learner (data plane — receives the replicated layout from the control plane without voting). The smallest deployment is one control node plus one data node (env PUSHUPES_ROLE)")
	fs.DurationVar(&c.AdoptInterval, "adopt-interval", envDurationOr("PUSHUPES_ADOPT_INTERVAL", c.AdoptInterval),
		"how often a node that is not a cluster member yet retries offering itself through the -join targets, tried in rotation (env PUSHUPES_ADOPT_INTERVAL)")
	fs.StringVar(&c.ReplicaPolicy, "replica-policy", envOr("PUSHUPES_REPLICA_POLICY", c.ReplicaPolicy),
		"replica policy tier for a NEW cluster: low (1 copy per slot), medium (2, the default), high (3 copies per slot), or a positive number used as the factor directly; applied on the table's first plan only — an existing cluster keeps the policy the admin endpoint last set (env PUSHUPES_REPLICA_POLICY)")
	c.SegmentBytes = envByteSizeOr("PUSHUPES_SEGMENT_BYTES", c.SegmentBytes)
	fs.Var(sizeFlag{&c.SegmentBytes}, "segment-bytes", "WAL segment roll `size` (bytes, or a suffix like 256MiB/1GiB); a power of two from 256MiB up to 2GiB (256MiB, 512MiB, 1GiB, 2GiB) (env PUSHUPES_SEGMENT_BYTES)")
	fs.Int64Var(&c.FlushMessages, "flush-messages", envInt64Or("PUSHUPES_FLUSH_MESSAGES", c.FlushMessages), "fsync every N records (0 disables) (env PUSHUPES_FLUSH_MESSAGES)")
	fs.DurationVar(&c.FlushInterval, "flush-interval", envDurationOr("PUSHUPES_FLUSH_INTERVAL", c.FlushInterval), "fsync every interval (0 disables) (env PUSHUPES_FLUSH_INTERVAL)")
	fs.IntVar(&c.FlushParallelism, "flush-parallelism", envIntOr("PUSHUPES_FLUSH_PARALLELISM", c.FlushParallelism), "segment fsyncs allowed in flight at once, the flush parallelism (1 = one at a time) (env PUSHUPES_FLUSH_PARALLELISM)")
	fs.DurationVar(&c.FetchSettle, "fetch-settle", envDurationOr("PUSHUPES_FETCH_SETTLE", c.FetchSettle),
		"fetch round coalescing window after a data wake (0 answers at once) (env PUSHUPES_FETCH_SETTLE)")
	fs.DurationVar(&c.DropAfter, "drop-after", envDurationOr("PUSHUPES_DROP_AFTER", c.DropAfter),
		"post-migration retention: how long the FORMER SOURCE keeps its local copy of a migrated slot before dropping it (positive = that window, 0 = the default 30s; negative is rejected; the cleanup cannot be disabled — env PUSHUPES_DROP_AFTER)")
	fs.DurationVar(&c.RebalanceInterval, "rebalance-interval", envDurationOr("PUSHUPES_REBALANCE_INTERVAL", c.RebalanceInterval),
		"how often the controller checks the leader layout against the slot ring and hands deviated slots back (a node that died returns to its own slots after failover; 0 disables the rebalancer; negative is rejected — env PUSHUPES_REBALANCE_INTERVAL)")
	fs.DurationVar(&c.StartupGrace, "startup-grace", envDurationOr("PUSHUPES_STARTUP_GRACE", c.StartupGrace),
		"how long a control node's first start waits for the data nodes the directory already knows to come up before it starts treating an unanswered peer as failed, and keeps the first rebalance rounds out of that window (a whole-cluster restart whose data nodes start a few seconds late keeps its leader layout instead of piling every slot onto the first node that answers; 0 = render verdicts from the first round; negative is rejected — env PUSHUPES_STARTUP_GRACE)")
	c.GRPCMaxMsgBytes = envByteSizeOr("PUSHUPES_GRPC_MAX_MSG_SIZE", c.GRPCMaxMsgBytes)
	fs.Var(sizeFlag{&c.GRPCMaxMsgBytes}, "grpc-max-msg-size", "client-plane gRPC max message `size` in bytes (suffixes like 16MiB accepted; the gRPC default 4MiB applies when unset) (env PUSHUPES_GRPC_MAX_MSG_SIZE)")
	fs.DurationVar(&c.RaftHeartbeatTimeout, "raft-heartbeat-timeout", envDurationOr("PUSHUPES_RAFT_HEARTBEAT_TIMEOUT", c.RaftHeartbeatTimeout),
		"consensus heartbeat interval (env PUSHUPES_RAFT_HEARTBEAT_TIMEOUT). A leader whose heartbeats do not arrive in time is voted out, so on a host that can delay the consensus event loop raise this together with -raft-election-timeout")
	fs.DurationVar(&c.RaftElectionTimeout, "raft-election-timeout", envDurationOr("PUSHUPES_RAFT_ELECTION_TIMEOUT", c.RaftElectionTimeout),
		"consensus election timeout: how long a follower waits for a heartbeat before standing for election (env PUSHUPES_RAFT_ELECTION_TIMEOUT). At least 2x -raft-heartbeat-timeout; raise both when the cluster votes leaders out under load")
	fs.DurationVar(&c.RaftFlushInterval, "raft-flush-interval", envDurationOr("PUSHUPES_RAFT_FLUSH_INTERVAL", c.RaftFlushInterval),
		"consensus WAL group-commit window: appends arriving within it share a single fsync (env PUSHUPES_RAFT_FLUSH_INTERVAL)")
	c.RaftSegmentBytes = envByteSizeOr("PUSHUPES_RAFT_SEGMENT_BYTES", c.RaftSegmentBytes)
	fs.Var(sizeFlag{&c.RaftSegmentBytes}, "raft-segment-bytes", "consensus WAL segment roll `size` (bytes, or a suffix like 64MiB) (env PUSHUPES_RAFT_SEGMENT_BYTES)")
}

// Validate applies the fail-fast rules that only need the values themselves; it
// runs on the seeded configuration (see Default), so the storage-backed fields
// hold their shipped values by then.
// The rules that need a package's vocabulary stay with it: the segment sizing
// rule is storage.ValidateSegmentBytes, the drop retention and the replica
// policy tier are resolved by the cluster layer.
func (c *Config) Validate() error {
	if c.SlotCount <= 0 {
		return fmt.Errorf("slot count %d must be positive", c.SlotCount)
	}
	// A zero/negative client-plane message cap would break every gRPC call on
	// the data plane.
	if c.GRPCMaxMsgBytes <= 0 {
		return fmt.Errorf("-grpc-max-msg-size %s must be positive", humanBytes(c.GRPCMaxMsgBytes))
	}
	if c.FlushParallelism < 1 {
		return fmt.Errorf("-flush-parallelism %d must be at least 1 (one segment fsync in flight)", c.FlushParallelism)
	}
	if c.RebalanceInterval < 0 {
		return fmt.Errorf("-rebalance-interval %s must not be negative (positive = the check cadence, 0 = the rebalancer is off)", c.RebalanceInterval)
	}
	if c.StartupGrace < 0 {
		return fmt.Errorf("-startup-grace %s must not be negative (positive = how long the first rebalance waits for the registered data nodes to come up, 0 = no wait)", c.StartupGrace)
	}
	if c.FetchSettle < 0 {
		return fmt.Errorf("-fetch-settle %s must not be negative", c.FetchSettle)
	}
	if c.RaftSegmentBytes <= 0 {
		return fmt.Errorf("-raft-segment-bytes %s must be positive", humanBytes(c.RaftSegmentBytes))
	}
	return nil
}

// ---------------------------------------------------------------------------
// Environment and size parsing behind the flags
// ---------------------------------------------------------------------------

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

// envIntOr reads an integer from the environment, falling back to def. A
// malformed value is fatal rather than silently defaulting: a typo in a tuning
// knob should be loud.
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

// envInt64Or reads an integer sized in records from the environment, falling
// back to def.
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
// size (256MiB, 1GiB).
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

// sizeFlag is the flag.Value behind -segment-bytes, -grpc-max-msg-size and
// -raft-segment-bytes: it accepts a plain byte count (268435456) or a suffixed
// size (256MiB, 1GiB), so the flag can be written the way the sizing rule is
// stated. Ki/Mi/Gi are powers of two, K/M/G are powers of ten.
type sizeFlag struct{ p *int64 }

func (s sizeFlag) String() string {
	if s.p == nil { // the zero instance flag uses to detect a default
		return "0"
	}
	return humanBytes(*s.p)
}

func (s sizeFlag) Set(v string) error {
	n, err := parseByteSize(v)
	if err != nil {
		return err
	}
	*s.p = n
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
