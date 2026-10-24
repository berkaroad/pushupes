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

package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/berkaroad/pushupes/internal/data"
	pushupesv1 "github.com/berkaroad/pushupes/internal/grpcapi/pushupes/v1"
	"github.com/berkaroad/pushupes/internal/storage"
)

type epochHW struct {
	epoch int64 // placement epoch the sample was taken at
	hw    uint64
}

// Engine is the application-level cluster state: it owns the slot table
// (replicated through Raft via the Applier interface), routes client appends
// to slot leaders, runs the follower fetch loop, and tracks ISR/high
// watermark per slot. Event data never enters Raft — it is replicated by
// a dedicated seq-based fetch protocol.
type Engine struct {
	node  consensus
	store *storage.Store
	// One drain-driver seat (balance.go): the disabled RunRebalancer loop
	// claims it when it starts over a marked table, wakeDrainRunner claims it
	// for a drain marked after the loop returned. Whoever holds it runs the
	// handover rounds; exactly one driver exists per drain.
	drainRunMu      sync.Mutex
	drainDriverBusy bool
	// hub is this node's data-plane feed (layoutfeed.go): the subscriptions
	// that keep role=learner nodes in step with the replicated metadata. Built
	// on first use because it reads the consensus timing off `node`, which is
	// attached after construction.
	hubOnce sync.Once
	hub     *layoutHub

	// planPoolSeen/planPoolWaited carry the settle gate across controller
	// rounds (see poolSettled): the last data-plane pool observed while it was
	// still changing, and how many rounds the plan has been deferred for it.
	// Only the controller's own loop touches them.
	planPoolSeen   []string
	planPoolWaited int
	table          *Table
	tableMu        sync.RWMutex
	logger         *slog.Logger
	peers          *peerClient // cached PeerService connections, keyed by peer addr
	self           string

	// replication bookkeeping per slot, only meaningful on slot leaders
	replMu sync.Mutex
	repl   map[int32]*slotRepl

	// flow is this node's flow-control throttle (see flowcontrol.go): a
	// node-level config plus per-slot-leader token buckets. Zero value =
	// unlimited, which is the default every node starts in.
	flow flowControl
	diag replDiag // data-plane tallies reported by ReplStats
	ack  ackDiag  // per-step ack-chain timings reported by AckStats
	// fetchSettle is how long a fetch round sleeps after a data wake before
	// scanning, so one round answers every slot the burst touched instead of
	// the first one to land (see SetFetchSettle).
	fetchSettle time.Duration
	// traceAgg is the forensic ack filter (PUSHUPES_TRACE_AGG): non-empty
	// logs every acknowledgement of a matching aggregate (see traceAck).
	traceAgg string

	// hwGates wakes the calls parked in waitForHW when a slot's watermark
	// moves (see hwwake.go). Indexed by slot id, read-only after construction.
	hwGates []hwGate

	// hwPing is the aggregated watermark-advance signal for parked fetch
	// rounds — a generation BROADCAST (one channel per generation under a
	// mutex, the hwGate shape): every parked round holds the current
	// generation and kickHW CLOSES it, waking all parked rounds at once; a
	// single buffered channel would let one round drain the wake and starve
	// the rest (a stolen wake). A woken round re-scans its own armed handles
	// and answers early with the fresh ledger, so a replica's read-admission
	// proof never trails the acked watermark by a report round (see
	// HandleMFetchCtx). Guarded by hwPingMu.
	hwPingMu sync.Mutex
	hwPing   chan struct{} // closed by kickHW; the next waiter arms a fresh one

	// rotation cursor for the mfetch payload budget
	fetchRot atomic.Uint64

	// fetch round buffers (sync.Pool): a session round allocates its
	// rotation order and parked set fresh every time; a hot leader
	// serves thousands of rounds per second and these were the
	// dominant malloc traffic in the profile.
	orderPool    sync.Pool // []int, cap >= followed slots
	parkedPool   sync.Pool // []parkedEntry
	progressPool sync.Pool // *progressBatch
	scanPool     sync.Pool // *fetchScan (per-position "has data" + wake handle + LEO)
	fetchBudget  int64     // bytes one fetch round may carry (maxPayloadBytes; tests shrink it to pin the visit order)
	// fetchRingMu guards fetchRingBusy: the followers that last reported being
	// behind on a ring seat. While one is, the other sessions are served at
	// half the round budget — that node's catch-up is what a layout change
	// waits on, the rest is redundancy. The ack-critical tail leads the round
	// outside the budget class, so a live append is never slowed by this.
	fetchRingMu   sync.Mutex
	fetchRingBusy map[string]time.Time
	// divSeen holds the last diverged reading of a copy a round has not acted
	// on yet (see confirmDiverged). rebMu in balance.go guards it with the
	// retry maps.
	divSeen         map[int32]rebalanceAttempt
	divergedConfirm time.Duration // window before a diverged reading may drop a copy
	// emptySeen holds the first sightings of an empty-shell leader reading
	// (see leaderEmptyShellWithHistory); rebMu guards it with divSeen.
	// Once past the window the entry STAYS (the refusal is steady-state, not
	// one-shot); clearing happens when the shell fills or the evidence goes.
	emptySeen          map[int32]time.Time
	emptyLeaderConfirm time.Duration // window before an empty-shell reading may refuse
	fetchScratchPool   sync.Pool     // *fetchScratch (per-round positions + report)

	// ownership bitmap: which slots this node leads, rebuilt only when
	// the Raft table version advances. Per-item map lookups under the
	// fetch hot loop (~1.4k followed slots per round) were a top CPU
	// contributor on the write path; a word-test per slot is free.
	ownMu sync.Mutex
	own   atomic.Pointer[ownership]
	// tableGen bumps with every applied table change (holds tableMu).
	tableGen uint64

	// owner fence (owner.go): pendingOwners is the set of slots whose local
	// copy is unproven for the live table (leader service refused with
	// fail/1007 until adopted, or until the copy is removed from disk by
	// the operator) — the verdict a stranger-dir admission stamp renders
	// on record-less data; every other member slot is proven by its
	// placement and stamped.
	ownerMu       sync.Mutex
	pendingOwners map[int32]ownerReason
	ownerKick     chan struct{}

	// migration commit fence: one preallocated write gate per slot (a writer
	// must be able to register its in-flight append without racing the fence
	// object's creation) plus the set of slots currently fenced, so a table
	// change can release the fence (see fence.go).
	fences   []slotFence
	fenceMu  sync.Mutex
	fenceSet map[int32]bool

	// slots whose local log forked from the leader's at a seq (the fetch loop
	// quarantines them instead of failing the whole multiplexed session).
	divMu    sync.Mutex
	diverged map[int32]string
	// rebuildTarget records the in-flight rebuild of a diverged copy: the
	// leader's tail as of the discard (the seq a full refetch must reach
	// before the quarantine lifts). The latch used to be permanent — a forked
	// copy stopped reporting (silently leaving the ISR, one replica short
	// forever); now the discard + refetch + tail-arrival closes it. Guarded
	// by divMu with the latch it belongs to.
	rebuildTarget map[int32]uint64
	// rebuildAttempts counts whole-history refetches per slot (poisoned
	// sources re-form the chain break; see rebuildDivergedCopy's cap).
	rebuildAttempts map[int32]int

	// slots this node led as of the previous table walk, so syncMigrationState
	// can tell a leadership GAIN (drop the previous term's stale follower
	// positions; see advanceHW) from a steady state.
	ledMu   sync.Mutex
	ledPrev map[int32]bool

	sessMu   sync.Mutex
	sessions map[string]*fetchSession
	// sessKick wakes replicaLoop to reconcile sessions when the table
	// changes (a leader move makes a follower follow a new leader; the
	// reconciliation rides the kick immediately, so the new leader's watermark
	// gets a replica report within one RPC round trip instead of waiting for
	// any polling cadence).
	sessKick chan struct{}

	// read-admission ledger (replica side): slot -> the latest high watermark
	// this node has SEEN from that slot's leader (via each fetch round's HW
	// ledger). A replica serves a slot's reads locally only once its durable
	// LEO reaches the bound — until then the slot's confirmed data may outrun
	// this copy's log and a local read would be a short read, so the read
	// redirects to the leader (see LocalRead). Samples are keyed by the
	// placement EPOCH they were taken at: an epoch change (a failover, a
	// migration commit) invalidates every sample taken under the old leader
	// and forces re-sync against the new one. Reset on process start — a
	// restarted replica re-proves its sync from the first ledger round, which
	// costs a few seconds of redirect-and-retry reads and no correctness.
	hwMu   sync.Mutex
	hwSeen map[int32]epochHW

	// controller liveness: consecutive health-probe failures per peer.
	// A single failure is not enough — a peer still booting (HTTP not yet
	// listening) would otherwise be failed over instantly on join.
	failMu     sync.Mutex
	failStreak map[string]int

	// storageMu guards gauge, the cluster storage sample every node refreshes in
	// the background (any node serves /admin/cluster/status — runStorageSampler).
	// storageKick schedules a refresh; a status read finding the gauge stale
	// sends it, so sampling runs only while something reads the gauge.
	storageMu   sync.Mutex
	gauge       storageGauge
	storageKick chan struct{}

	// unreach is the controller's witness table for the unreachable reports
	// observers send (suspect -> reporter -> when). See recordUnreachable.
	unreachMu sync.Mutex
	unreach   map[string]map[string]time.Time

	// leader rebalance (balance.go): how often the controller re-checks the
	// ring layout and how many leader hand-overs one round may execute.
	// Set from the -rebalance-interval flag.
	rebalanceInterval time.Duration

	// bootPolicy is the -replica-policy seed: it applies to the FIRST plan
	// of a brand-new cluster (an unreplicated table), once per process.
	// After that the policy lives in the replicated table and the only way to
	// change it is the admin endpoint (SetReplicaPolicy) — a restart of an
	// existing node never re-seeds it.
	bootPolicy ReplicaPolicy

	// retry backoff for hand-overs that do not stick (balance.go): one record
	// per slot, keyed on the move's endpoints. Without it a target that cannot
	// converge is repaired (a full slot rebuild) once per round, forever.
	rebMu    sync.Mutex
	rebRetry map[int32]rebalanceAttempt

	// retry backoff for surplus-seat reclaims (balance.go): also per slot, keyed
	// on the seat being dropped. A seat whose kept copies are still catching up
	// costs one digest round trip per seat per round until it converges.
	seatRetry map[int32]rebalanceAttempt

	// seated records when each replica seat first appeared in a slot's set as
	// seen by THIS node (the table is replicated, so every node observes the
	// same transitions — including one that is not leading the slot).
	// syncSeats uses it to tell, at a takeover, a seat that is still being built
	// from one that was already an established member of the promise.
	seated map[seatKey]time.Time

	// goneSess records, per follower, when its fetch long poll last ended
	// without an answer — i.e. its connection died (see
	// noteFollowerSessionLost). Reports that predate that moment stop feeding the
	// high watermark: a replica that is no longer being served must not pin a
	// slot's writes. Cleared as soon as the follower reports again.
	goneSess map[string]time.Time

	// post-migration local cleanup: slots whose local copy this node is
	// scheduled to drop after the retention window (slot -> schedule), plus
	// this node's replica-set membership as of the previous table walk (the
	// transition member -> not-a-member is what arms a countdown). Exposed to
	// the admin plane so the console can show which copy of a slot is on its
	// way out. See localdrop.go.
	dropMu    sync.Mutex
	dropAfter time.Duration
	// hwWait is the watermark deadline an acknowledged append waits on
	// before fail/1005 (default defaultHWWaitTimeout; a batch group pays it
	// ONCE — see SubmitBatch). Tests shorten it.
	hwWait       time.Duration
	dropGen      uint64
	pendingDrops map[int32]pendingDrop
	// dropWake nudges this node's single drop sweeper — ONE timer per node
	// instead of one per queued slot (a factor decrease queues thousands of
	// copies at once). dropSweeper records that the goroutine is running.
	dropWake    chan struct{}
	dropSweeper bool
	// dropSweeperN counts how many sweeper goroutines this node has armed. There
	// is deliberately ONE per node; tests pin that arming many schedules at once
	// still starts exactly one.
	dropSweeperN int
	replicaOf    map[int32]bool

	// The slots THIS controller is driving through StartMigration right now.
	// The controller round uses it to tell a migration it owns from one a
	// controller that is gone left staged: a slot's migrating_out state is
	// replicated and outlives the goroutine that staged it, so without this
	// the orphan is never revisited. See reconcileOrphanMigrations.
	migMu     sync.Mutex
	migrating map[int32]struct{}
}

// slotRepl tracks follower LEOs and the high watermark for one slot's
// replication group.
type slotRepl struct {
	// Parallel per-follower arrays (replica factor is 2-3: a linear scan
	// beats two string-keyed maps — no hash, no GC write barriers on the
	// fetch hot path, which re-reports every followed slot each round).
	// The leader's own LEO is never stored: it is store.LastSeqOf(slot).
	node   []string
	leo    []uint64
	lastOK []time.Time
	hw     uint64
	// hwAt is when hw last advanced. The age of a stuck slot's hwAt is how long
	// its appends have been waiting for an acknowledgement (see ReplStats).
	hwAt time.Time

	// seats is the slot's replica set as of the last table walk while this node
	// led it, and building lists the seats that APPEARED in that set since —
	// copies being built (a re-layout's new seats) that have not reached this
	// leader's LEO since. They do not gate the watermark: a seat that just
	// joined has nothing to acknowledge yet, and letting its empty log pin HW
	// would block every append to that slot until it finished fetching (the
	// same stall the migration target's exclusion fixes, generalized to the
	// seats a re-layout adds). The mark clears the moment the seat's LEO
	// reaches the leader's LEO; from then on it is an ordinary ISR member and
	// gates again.
	//
	// The seats present when this node TOOK THE SLOT OVER are deliberately NOT
	// marked: they are the set the write confirmation already rests on, and a
	// replica of that set which is behind must keep holding the watermark back
	// — that gating is the acknowledged-write promise, not a stall to fix.
	seats    []string
	building []string
}

// seatKey identifies one replica seat: a node's membership of one slot's set.
type seatKey struct {
	slot int32
	node string
}

// seatBuildGrace bounds how long a seat counts as "being built" for a leader
// that takes the slot over mid-catch-up (see takeover). A copy that is still
// fetching after this window is treated as an ordinary member — the promise
// must not stay relaxed for a seat that is simply slow, and a fetch that long
// means something else is wrong.
const seatBuildGrace = time.Minute

// takeover starts this leader's view of a slot it just gained. The seats present
// are recorded (they are the set the promise rests on) and so are the "building"
// marks: a seat seated within seatBuildGrace is a copy that is plausibly still
// fetching, and the new leader has no leadership history to tell it apart from
// an established member — so it must NOT gate this leader's writes (that is what
// turns a hand-over of a slot with a seat mid-catch-up into a full-watermark
// stall: every write waits for a whole slot transfer). A seat that is already
// caught up clears its mark on its first report, within one fetch round.
func (sr *slotRepl) takeover(seats, young []string) {
	sr.seats = append(sr.seats[:0], seats...)
	sr.building = append(sr.building[:0], young...)
}

// noteSeatAges refreshes, for every slot and seat in the table, the moment this
// node first saw that seat. Seats that left their set are dropped, and a node
// that comes back is a fresh seat again.
func (e *Engine) noteSeatAges() {
	now := time.Now()
	e.replMu.Lock()
	e.tableMu.RLock()
	next := make(map[seatKey]time.Time, len(e.table.Slots)*2)
	for s, p := range e.table.Slots {
		for _, r := range p.Replicas {
			k := seatKey{s, r}
			if at, ok := e.seated[k]; ok {
				next[k] = at
				continue
			}
			next[k] = now
		}
	}
	e.tableMu.RUnlock()
	e.seated = next
	e.replMu.Unlock()
}

// youngSeats returns the seats of a slot that were seated recently enough to
// still be fetching (see takeover). Caller holds replMu.
func (e *Engine) youngSeats(slot int32, seats []string, now time.Time) []string {
	var out []string
	for _, s := range seats {
		if at, ok := e.seated[seatKey{slot, s}]; ok && now.Sub(at) < seatBuildGrace {
			out = append(out, s)
		}
	}
	return out
}

// syncSeats folds the slot's current replica set into this leader's view: the
// seats that appeared since the last walk become "building" and the ones that
// left the set lose their mark.
func (sr *slotRepl) syncSeats(now []string) {
	// Seats that left the set are no longer copies to build (and a seat that
	// comes back later is a fresh transition: it gets marked again).
	if len(sr.building) > 0 {
		kept := sr.building[:0]
		for _, b := range sr.building {
			if seatListHas(now, b) {
				kept = append(kept, b)
			}
		}
		sr.building = kept
	}
	for _, n := range now {
		if !seatListHas(sr.seats, n) && !sr.isBuilding(n) {
			sr.building = append(sr.building, n)
		}
	}
	sr.seats = append(sr.seats[:0], now...)
}

// isBuilding reports whether the seat is still being built (its entry may not
// even exist yet: a seat is marked before its first fetch report arrives).
func (sr *slotRepl) isBuilding(node string) bool { return seatListHas(sr.building, node) }

// dropBuilding clears a seat's mark: it has caught up, so it is an ordinary
// in-sync replica from now on and gates the watermark like every other one.
func (sr *slotRepl) dropBuilding(node string) {
	if len(sr.building) == 0 {
		return
	}
	kept := sr.building[:0]
	for _, b := range sr.building {
		if b != node {
			kept = append(kept, b)
		}
	}
	sr.building = kept
}

// seatListHas reports whether a seat list holds a node (the lists hold at most
// the replica factor plus one or two seats a re-layout is adding).
func seatListHas(list []string, id string) bool {
	for _, v := range list {
		if v == id {
			return true
		}
	}
	return false
}

// find returns the array index of one follower, or -1.
func (sr *slotRepl) find(node string) int {
	for i, n := range sr.node {
		if n == node {
			return i
		}
	}
	return -1
}

// touch records (leo, now) for one follower, creating its entry when new.
func (sr *slotRepl) touch(node string, leo uint64, now time.Time) {
	i := sr.find(node)
	if i < 0 {
		sr.node = append(sr.node, node)
		sr.leo = append(sr.leo, leo)
		sr.lastOK = append(sr.lastOK, now)
		return
	}
	sr.leo[i] = leo
	sr.lastOK[i] = now
}

// setLastOK overrides a follower's freshness stamp (test/ops helper).
func (sr *slotRepl) setLastOK(node string, t time.Time) {
	if i := sr.find(node); i >= 0 {
		sr.lastOK[i] = t
	}
}

// fetchSession is one follower's multiplexed fetch loop against one slot
// leader: a single goroutine long-polls PeerService.MFetch carrying the
// follower positions of every slot this node replicates from that leader.
// One session per leader => O(nodes) connections instead of O(followed slots).
type fetchSession struct {
	leader string
	slots  atomic.Value // []int32, refreshed by sessionLoop
	stop   chan struct{}
	// lastSweep bounds when unchanged positions were last stamped: the
	// liveness of frozen slots rides the periodic sweep round (time-based
	// because idle rounds each last the full long-poll wait, so a count
	// cadence would sweep every N*fetchWait and brush isrStaleAfter).
	lastSweep time.Time
	// unreachable counts CONSECUTIVE transport-level failures of this session's
	// rounds (a dead leader's connection), and lastReport throttles the
	// witnesses this session sends the controller: one per suspect per
	// unreachableWindow, so a leader that stays dead does not turn its
	// followers into a report stream (see reportLeaderUnreachable). Touched only
	// by this session's own goroutine.
	unreachable int
	lastReport  time.Time
}

// Fetch cadence per session: a productive round restarts immediately
// (base gap), an idle one doubles the sleep up to maxBackoff. The leader
// long-poll (wait) absorbs quiet periods inside one request, so the
// whole idle cycle (wait+maxBackoff) must stay inside the ISR staleness
// window for replicas to remain in-sync.
const (
	fetchBaseInterval = 100 * time.Millisecond
	fetchMaxBackoff   = 2 * time.Second
	fetchWait         = 2 * time.Second // follower-requested long-poll budget
	fetchSlack        = 1 * time.Second // client-side timeout margin
	fetchMaxWait      = 5 * time.Second // leader-side cap on a requested wait
	// DefaultFetchSettle is the fetch round's coalescing window (see
	// SetFetchSettle): how long a woken round waits for the rest of the same
	// write burst before it scans and answers. Measured on the 3-node bench
	// (1KiB, conns 4, batch 100, mirrored 2ms/200us/0 sweep): answering at
	// once is WORSE than coalescing (fewer slots per round leaves the tail of a
	// burst to a later round: hw_wait 6.42ms at 0 vs 4.60ms at 200us against
	// 5.70ms at 2ms), so the window earns its keep -- but 2ms is overpaid, at
	// 37 slots per round against 29 for the same acknowledgement latency.
	DefaultFetchSettle = 200 * time.Microsecond
	fetchSweepInterval = 2 * time.Second  // stamp all positions at least this often
	isrStaleAfter      = 10 * time.Second // must exceed fetchWait+fetchMaxBackoff
	// commitFloorWait bounds how long a leader holds a write back while its
	// copy catches up to the slot's commit floor (see awaitCommitFloor). The
	// catch-up is one fetch round in the normal case; the bound only exists so
	// a stalled fetch degrades to the old behaviour instead of wedging writes.
	commitFloorWait      = 2 * time.Second
	defaultHWWaitTimeout = 10 * time.Second // the write-ack watermark deadline

	// fetchTailRecords is how close to a slot's head a follower counts as
	// ACK-CRITICAL: a position within this many records is served before the
	// round's bulk backlog share. It is a record count (not bytes) because the
	// property being protected is "the follower is one round's production
	// behind", and a writer produces records, not bytes; a tail's byte size is
	// still bounded by the round budget's per-slot read cap.
	fetchTailRecords = 64
	// maxEarlyReportsPerRound caps how many ACK-CRITICAL positions a round
	// reports the moment they land (see fetchRound): each is one small RPC, so
	// the cap bounds a round whose tails are unusually many without ever
	// letting the urgent ones wait for the round's bulk apply.
	maxEarlyReportsPerRound = 16
	// maxPayloadBytes caps one long-poll response: during a backlog the
	// session streams item by item across rounds instead of building a
	// gigabyte JSON blob of every slot at once.
	maxPayloadBytes = 32 << 20
)

// Applying one fetch round: the round's items are different slots, so they are
// applied concurrently (a small pool) while one slot's records stay in seq order
// inside its item — the store writes one durable frame per record, and the
// device serves concurrent writers far better than a single serial one. The
// round's positions are reported once, after it lands (see the call site in
// fetchRound).
//
// The pool scales with the CPU budget this process actually has. GOMAXPROCS(0)
// is the runtime's own view of it — the same measure the store's open workers
// use — so inside a container it reflects the enforced quota rather than the
// host's core count, and a node does not fan out a worker per host core.
var (
	applyWorkers     = 2 * runtime.GOMAXPROCS(0) // slots applied concurrently within one round
	applyParallelMin = runtime.GOMAXPROCS(0)     // below this, a pool costs more than it saves
)

// NewEngine wires the cluster to the local storage. The Raft node may be
// attached later with SetNode (they reference each other).
func NewEngine(node consensus, store *storage.Store, self string, logger *slog.Logger) *Engine {
	traceAgg := os.Getenv("PUSHUPES_TRACE_AGG")
	e := &Engine{
		node:  node,
		store: store,
		self:  self,
		// The table starts on the medium policy — the default tier — and the
		// replica count follows from it for a single-member cluster. A
		// brand-new cluster re-derives the factor from the startup seed every
		// round; an existing cluster keeps whatever policy the admin endpoint
		// last set (it lives in the replicated table, not in the engine).
		table:  NewTableWithPolicy(store.SlotCount, DefaultReplicaPolicy),
		logger: logger,
		// One HTTP/2 connection per leader carries every multiplexed
		// long-poll fetch round plus progress/replication/leo probes —
		// gRPC multiplexes them over the single cached conn (the old HTTP
		// transport needed a large idle-conn pool to avoid re-dial churn).
		peers:           newPeerClient(),
		repl:            map[int32]*slotRepl{},
		sessions:        map[string]*fetchSession{},
		sessKick:        make(chan struct{}, 1),
		storageKick:     make(chan struct{}, 1),
		failStreak:      map[string]int{},
		ledPrev:         map[int32]bool{},
		fenceSet:        map[int32]bool{},
		diverged:        map[int32]string{},
		rebuildTarget:   map[int32]uint64{},
		rebuildAttempts: map[int32]int{},
		pendingOwners:   map[int32]ownerReason{},
		ownerKick:       make(chan struct{}, 1),
		hwSeen:          map[int32]epochHW{},
		migrating:       map[int32]struct{}{},
		// post-migration cleanup: the former source keeps its copy for this
		// long once it sees the hand-over committed (-drop-after).
		dropAfter: DefaultDropRetention,
		// how long an acknowledged append waits for the slot's high
		// watermark before fail/1005 (the record stays in the leader's WAL;
		// a client retry converges on exists). A batch pays this deadline
		// once per slot group, not once per record (SubmitBatch's merged
		// wait). Tests shorten it.
		hwWait: defaultHWWaitTimeout,
		// leader rebalance: enabled by default, tuned with -rebalance-interval
		// (an interval of 0 turns it off).
		rebalanceInterval: DefaultRebalanceInterval,
		// per-slot ack-chain tallies, indexed by slot id (read-only after this)
		ack:         ackDiag{slots: make([]slotAck, store.SlotCount)},
		hwGates:     make([]hwGate, store.SlotCount),
		fetchSettle: DefaultFetchSettle,
		traceAgg:    traceAgg,
	}
	e.orderPool.New = func() any { return make([]int, 0, 64) }
	e.parkedPool.New = func() any { return make([]parkedEntry, 0, 64) }
	e.progressPool.New = func() any { return &progressBatch{} }
	e.scanPool.New = func() any { return &fetchScan{} }
	e.fetchBudget = maxPayloadBytes
	e.divergedConfirm = divergedConfirmWindow
	e.emptyLeaderConfirm = emptyLeaderConfirmWindow
	e.fetchScratchPool.New = func() any { return &fetchScratch{} }
	e.fences = make([]slotFence, store.SlotCount)
	if e.logger == nil {
		l := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
		e.logger = l.With("component", "cluster")
	}
	// The round-apply shape, resolved once (see the constants): how many slots a
	// fetch round writes concurrently, when the pool kicks in, and when progress
	// is posted mid-round. One line per node so an operator can see what the
	// process actually chose on its machine.
	e.logger.Info("fetch round apply pool",
		"apply_workers", applyWorkers, "apply_parallel_min", applyParallelMin)
	return e
}

// ---- Applier (Raft state machine callbacks) --------------------------------

// ApplyCommand implements cluster.Applier: deterministic table mutations.
func (e *Engine) ApplyCommand(cmd []byte) ([]byte, error) {
	c, err := DecodeCommand(cmd)
	if err != nil {
		return nil, err
	}
	e.tableMu.Lock()
	err = e.table.Apply(c)
	e.tableGen++
	e.tableMu.Unlock()
	if err != nil {
		return nil, err
	}
	e.syncMigrationState()
	// A table change that takes a node out of a slot's picture (marked down, out
	// of the directory, or dropped from a replica set) may be holding that
	// slot's watermark back: re-derive it now rather than waiting for the next
	// unrelated progress report to notice.
	switch c.Op {
	case OpMarkDown, OpLeaveNode, OpSlotRemoveReplica:
		e.releaseWatermarksFor(c.NodeID)
	}
	// The apply-path half of the owner fence's promise (owner.go): a slot
	// this commit hands to self carries its record from THIS moment — the
	// new leader never serves, waits, or reads unproven while a fence tick
	// catches up.
	e.stampAppliedSlots(c)
	return nil, nil
}

// releaseWatermarksFor re-derives the high watermark of every slot that names
// the node (as leader or replica). advanceHW skips positions that are down or no
// longer in the set, so the watermarks such a position was pinning move up now.
func (e *Engine) releaseWatermarksFor(node string) {
	if node == "" {
		return
	}
	e.tableMu.RLock()
	var slots []int32
	for s, p := range e.table.Slots {
		if p.Leader == node || replicaListHas(p.Replicas, node) {
			slots = append(slots, s)
		}
	}
	e.tableMu.RUnlock()
	for _, s := range slots {
		e.advanceHW(s)
	}
}

// SnapshotState implements cluster.Applier (binary: ~350KB JSON -> a few KB
// for every slot; snapshots ride the control plane).
func (e *Engine) SnapshotState() ([]byte, error) {
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	return e.table.EncodeTableBinary(), nil
}

// ResetState implements cluster.Applier: back to an empty table, as a node
// that has been told to rebuild from nothing must be (see learnernode.go).
// The slot count stays this node's own — the commands that follow carry the
// cluster's actual layout.
func (e *Engine) ResetState() error {
	tbl := NewTableWithPolicy(e.store.SlotCount, DefaultReplicaPolicy)
	e.tableMu.Lock()
	e.table = tbl
	e.tableGen++
	e.tableMu.Unlock()
	e.syncMigrationState()
	return nil
}

// RestoreState implements cluster.Applier; accepts the binary snapshot form.
func (e *Engine) RestoreState(b []byte) error {
	tbl, err := DecodeTableBinary(b)
	if err != nil {
		return err
	}
	e.tableMu.Lock()
	e.table = tbl
	e.tableGen++
	e.tableMu.Unlock()
	e.syncMigrationState()
	return nil
}

// WriteCounts snapshots this node's per-slot durable write counters; the
// admin API pairs them with the table so clients can derive write rates.
func (e *Engine) WriteCounts() []uint64 { return e.store.WriteCounts() }

// WriteByteCounts snapshots this node's per-slot durable write byte counters,
// index-aligned with WriteCounts: the admin API ships both, so clients diff
// successive pairs into the message rate and the byte rate of one window.
func (e *Engine) WriteByteCounts() []uint64 { return e.store.WriteByteCounts() }

// ClientWriteCounts / ClientWriteByteCounts snapshot the client-origin
// counters (what batch appends landed HERE as the slots' leader; catch-up
// paths never touch them), index-aligned with WriteCounts. The console
// writes its 写入速度 cards and node charts from these — WriteCounts would
// also count migration/replication traffic, which the operator is not
// asking about.
func (e *Engine) ClientWriteCounts() []uint64 { return e.store.ClientWriteCounts() }

func (e *Engine) ClientWriteByteCounts() []uint64 { return e.store.ClientWriteByteCounts() }

// TableSnapshot returns a copy of the current table (for the admin API).
func (e *Engine) TableSnapshot() *Table {
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	return e.table.Clone()
}

// leaderCopyMissingWithHistory reports a leader whose local copy of the slot is
// absent while the slot has handover history (epoch >= 2). Epoch 1 is a slot the
// plan just created — it legitimately starts empty; anything at epoch 2 has
// already been moved once, so an empty copy means this node is missing the
// history, not creating it.
func (e *Engine) leaderCopyMissingWithHistory(slot int32) bool {
	if e.store.SlotPresent(slot) {
		return false
	}
	p, ok := e.slotPlacement(slot)
	return ok && p.Leader == e.self && p.Epoch >= 2
}

// emptyLeaderConfirmWindow is how long a slot must keep reading as an
// empty-shell leader before writes are refused: a copy being (re)built can
// briefly read empty while its seats still report the previous holder's
// history — the same transient that confirmDiverged guards against.
const emptyLeaderConfirmWindow = 2 * time.Second

// leaderEmptyShellWithHistory reports the empty-shell sibling of
// leaderCopyMissingWithHistory: this node leads a slot that has moved before
// (epoch >= 2), its local copy EXISTS but holds zero records, and a seat in
// the CURRENT replica set has reported history (LEO > 0) on the leader-side
// progress ledger. That is the live-cluster Oct-24 shape: a former migration
// source whose copy was dropped mid-run, re-granted as a fresh empty seat,
// then handed leadership by a floorless commit (offline-source leader_move
// or a failover pin) — SlotPresent is true so the missing-copy guard never
// fires, and the empty directory answers "current version 0" to every client,
// the adopt-and-rewrite storm the missing-copy guard exists to refuse.
// The evidence is the seat ledger every fetch round already feeds: no new
// RPC, no new protocol. The reading must be confirmed across
// emptyLeaderConfirmWindow before it refuses; it holds until the shell fills
// or the seat evidence clears.
func (e *Engine) leaderEmptyShellWithHistory(slot int32) bool {
	if !e.store.SlotPresent(slot) || e.store.LastSeqOf(slot) != 0 {
		e.clearEmptyShell(slot) // the shell filled (or vanished): keep no verdict
		return false
	}
	p, ok := e.slotPlacement(slot)
	if !ok || p.Leader != e.self || p.Epoch < 2 {
		return false // not ours to vouch for, or a plan-new slot starting empty
	}
	if !e.seatHasLedHistory(slot, p.Replicas) {
		e.clearEmptyShell(slot) // nobody reports history: an empty start is legitimate
		return false
	}
	return e.confirmEmptyShell(slot, time.Now())
}

// seatHasLedHistory reports whether any CURRENT replica (not this node) has
// reported a durable LEO above zero on the leader-side progress ledger.
func (e *Engine) seatHasLedHistory(slot int32, replicas []string) bool {
	e.replMu.Lock()
	defer e.replMu.Unlock()
	sr := e.repl[slot]
	if sr == nil {
		return false
	}
	for j, n := range sr.node {
		if n == e.self || sr.leo[j] == 0 {
			continue
		}
		for _, r := range replicas {
			if r == n {
				return true
			}
		}
	}
	return false
}

// confirmEmptyShell records this sighting and reports whether the reading has
// held across the window. Unlike confirmDiverged (one-shot, it ends by
// dropping the copy) a confirmed verdict STAYS: every later write asks again
// and gets the same answer until clearEmptyShell forgets the sighting.
func (e *Engine) confirmEmptyShell(slot int32, now time.Time) bool {
	e.rebMu.Lock()
	defer e.rebMu.Unlock()
	if e.emptySeen == nil {
		e.emptySeen = map[int32]time.Time{}
	}
	first, ok := e.emptySeen[slot]
	if !ok {
		e.emptySeen[slot] = now
		return false
	}
	return now.Sub(first) >= e.emptyLeaderConfirm
}

// clearEmptyShell forgets an empty-shell sighting (the copy holds records
// again, or no seat contests the empty): a later emptying confirms afresh.
// Cheap no-op on the hot path: the map holds only currently-contested slots.
func (e *Engine) clearEmptyShell(slot int32) {
	e.rebMu.Lock()
	delete(e.emptySeen, slot)
	e.rebMu.Unlock()
}

// slotPlacement reads ONE slot's placement under the table lock (TableSnapshot
// clones the whole table and must never be called from the write path).
func (e *Engine) slotPlacement(slot int32) (Placement, bool) {
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	p, ok := e.table.Slots[slot]
	if !ok {
		return Placement{}, false
	}
	return *p, true
}

// commitFloor is this slot's frozen-tail floor from the replicated table
// (0 when the slot never moved through a commit fence).
func (e *Engine) commitFloor(slot int32) uint64 {
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	return e.table.CommitLEO[slot]
}

// awaitCommitFloor holds a write back until this copy has reached the slot's
// commit floor. The floor is the tail the fence froze and confirmed on the
// TARGET at handover: if this node leads the slot yet its own log still sits
// below it, the fetch has not caught up and answering the client would refuse
// versions the previous leader already acknowledged (the residual 1001) or,
// worse, reuse their seqs. Bounded; on timeout the write is served anyway and
// the anomaly is logged, so a stuck fetch can never wedge the slot.
func (e *Engine) awaitCommitFloor(slot int32) {
	floor := e.commitFloor(slot)
	if floor == 0 || e.store.LastSeqOf(slot) >= floor {
		return
	}
	e.logger.Info("slot commit floor: holding the write until the local copy catches up",
		"slot", slot, "floor", floor, "leo", e.store.LastSeqOf(slot), "op", "commit_floor_wait")
	deadline := time.Now().Add(commitFloorWait)
	for {
		if e.store.LastSeqOf(slot) >= floor {
			return
		}
		if time.Now().After(deadline) {
			e.logger.Warn("slot commit floor: local log is still below the confirmed tail; serving the write anyway",
				"slot", slot, "floor", floor, "leo", e.store.LastSeqOf(slot), "op", "commit_floor")
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// HandleSlotLeader answers a peer's query for THIS node's local placement view
// of a slot: the leader it currently believes in ("" when unassigned) and the
// placement epoch. It is the migration source's fence-release gate (fence.go):
// the source only stops blocking (and starts redirecting) once the target
// reports the slot's leader is the target itself at an epoch not older than the
// committed one — so a client is never bounced between a source that has
// applied the move and a target that has not.
func (e *Engine) HandleSlotLeader(slot int32) (string, int64) {
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	if p, ok := e.table.Slots[slot]; ok {
		return p.Leader, p.Epoch
	}
	return "", 0
}

// ---- Routing ---------------------------------------------------------------

// SlotOf routes an aggregate to its slot.
func (e *Engine) SlotOf(aggregateID string) int32 { return e.store.SlotOf(aggregateID) }

// syncMigrationState drives the post-handover cleanup countdown (localdrop.go)
// from the table's replica-set membership.
//
// The countdown follows REPLICA-SET MEMBERSHIP, not the migration itself: this
// node arms one for a slot exactly when a table change takes it OUT of that
// slot's replica set while it holds a local copy. That is the only moment a
// copy becomes surplus, and it makes both directions right:
//
//   - an out-of-set migration admits the target (set at factor+1) and the
//     post-commit reclaim removes the former source — the source leaves the
//     set, so its countdown starts here;
//   - an in-set leader hand-over removes nobody: the former source stays a
//     replica (still part of the RF), so nothing is armed, no "queued" state
//     is published and no data is ever dropped for it;
//   - a slot this node is back on (rollback, a later failover, a re-join)
//     cancels any schedule it has: the data is kept.
//
// The opposite (arming on the hand-over commit) is what produced both reported
// symptoms: a node that was still a replica of the slot had its copy deleted
// after the retention, and — because a schedule armed by an earlier hand-over
// was silently reused — a later hand-over's copy could be deleted the moment
// the earlier window expired, i.e. "immediately".
func (e *Engine) syncMigrationState() {
	// Every table change re-decides the owner fence: a promotion must be
	// judged (and stamped, or fenced) now, not at the next slow tick.
	e.kickOwnerFence()
	prev := e.replicaSnapshot()
	fenced := e.fencedSlots()
	fencedSet := make(map[int32]bool, len(fenced))
	for _, s := range fenced {
		fencedSet[s] = true
	}
	// placement view of every currently fenced slot: after the scan, a fence is
	// either kept (still migrating out under this node), released (aborted /
	// rolled back to stable under this node) or GATED (committed to a new
	// leader that must apply the move before clients are redirected into it).
	type leaderEpoch struct {
		leader string
		epoch  int64
	}
	after := make(map[int32]leaderEpoch, len(fenced))

	e.tableMu.RLock()
	member := make(map[int32]bool, len(e.table.Slots))
	fenceOK := make(map[int32]bool, 4) // slots a held fence is still valid for
	var surplus []int32                // left the replica set while holding a copy: arm a countdown
	for s, p := range e.table.Slots {
		if fencedSet[s] {
			after[s] = leaderEpoch{p.Leader, p.Epoch}
		}
		// A fence is only meaningful while this node still leads a slot that
		// is still migrating out. The moment the commit (leader_move) or an
		// abort (rollback to stable) is applied, the fence must be released so
		// the blocked writers proceed (and get redirected, or are served by
		// the still-leading source).
		if p.State == SlotMigratingOut && p.Leader == e.self {
			fenceOK[s] = true
		}
		inSet := replicaListHas(p.Replicas, e.self)
		if inSet {
			member[s] = true
		}
		if prev[s] && !inSet {
			surplus = append(surplus, s)
		}
	}
	e.tableMu.RUnlock()

	// Release any fence whose migration no longer owns this node as leader —
	// but, for a COMMITTED move (the slot's leader is now a different node),
	// first gate the release on that target having applied the move itself:
	// this node applying it says nothing about the target, and until the target
	// applies it a client asking the target is sent back here while a client
	// asking here is sent to the target (the MOVED/ASK ping-pong that failed
	// clients with a small redirect budget). An abort (this node still leads)
	// or an unassigned slot releases at once, as before.
	for _, s := range fenced {
		if fenceOK[s] {
			continue
		}
		if pl, ok := after[s]; ok && pl.leader != "" && pl.leader != e.self {
			e.holdFenceUntilTargetLeader(s, e.self, pl.leader, pl.epoch)
			continue
		}
		e.releaseSlotFence(s)
	}

	e.setReplicaMembership(member)
	// Leadership bookkeeping: a slot this node just STARTED leading carries
	// the previous term's follower positions in e.repl. Their leos are as of
	// the old leader's last progress sample — often far below this node's own
	// LEO — and they stay "fresh" for isrStaleAfter, so advanceHW's min would
	// freeze the watermark below the leader's LEO until each follower happens
	// to report again. That freeze is what blocked every append to
	// the 10s watermark wait deadline around a migration. Dropping the bookkeeping makes
	// the new leader count only live reports; the followers re-report within
	// one fetch round and the watermark jumps straight to the true min.
	e.tableMu.RLock()
	led := make(map[int32]bool, len(e.table.Slots))
	ledSeats := make(map[int32][]string, 8) // replica sets of the slots THIS node leads
	for s, p := range e.table.Slots {
		if p.Leader == e.self {
			led[s] = true
			// Copy: the walk is consumed later (under replMu) and a table
			// command compacts replica sets in place (removeString).
			ledSeats[s] = append([]string(nil), p.Replicas...)
		}
	}
	e.tableMu.RUnlock()
	// Seat ages are node-global (not leadership-scoped): a leader that takes a
	// slot over needs them for seats it never tracked itself.
	e.noteSeatAges()
	e.ledMu.Lock()
	prevLed := e.ledPrev
	e.ledPrev = led
	e.ledMu.Unlock()
	e.replMu.Lock()
	for s := range led {
		sr := e.repl[s]
		if !prevLed[s] {
			// Just took the slot over: start from a clean slate of positions (the
			// previous term's are stale) while recording the seats we inherit and
			// the ones still being built (see slotRepl.takeover).
			sr = newSlotRepl()
			e.repl[s] = sr
			sr.takeover(ledSeats[s], e.youngSeats(s, ledSeats[s], time.Now()))
			continue
		}
		if sr == nil {
			continue // no report has arrived yet: nothing to mark against
		}
		sr.syncSeats(ledSeats[s])
	}
	e.replMu.Unlock()

	// A copy this node is on again is not surplus: cancel whatever was queued
	// for it (the pending map holds at most a handful of slots).
	for _, s := range e.pendingSlotIDs() {
		if e.onSlot(s) {
			e.cancelPendingDrop(s)
		}
	}
	for _, s := range surplus {
		e.startDropCountdown(s)
	}

	// The table changed: a follower may now follow a different leader. Wake the
	// session loop so it re-groups immediately instead of on its next 1s tick —
	// otherwise the new leader's watermark waits a second for its
	// first replica report after every leader move.
	e.kickSessions()
}

// SubmitAppend routes a client append to the slot leader (executing locally
// when we are it) and defines what "write succeeded" means for the whole
// cluster: an append is acknowledged with success only once the slot's high
// watermark covers it, i.e. every in-sync replica has the record durable in
// its own WAL. A node may then die without any acknowledged record being
// lost (see localAppend / waitForHW).
//
// The call takes the slot's migration write fence first: while a migration
// commit fence is held (the source of a live migration, in the window between
// the frozen LEO and the leader move, see fence.go), the append BLOCKS until
// the fence is released and is then re-evaluated against the table — it is
// never failed by the fence, and it is redirected to the new leader once the
// move is applied.
func (e *Engine) SubmitAppend(ctx context.Context, rec *data.EventRecord) (resp *data.AppendResponse, err error) {
	start := time.Now()
	slot := e.SlotOf(rec.AggregateID)
	leave := e.enterWriteFence(slot)
	defer leave()
	e.awaitLocalDemotion(slot)
	fenced := time.Now()
	e.noteCall(start, fenced, 1, false)
	defer func() {
		redir := redirected(err)
		e.noteCallEnd(start, redir)
		if !redir {
			e.noteOutcome(resp)
		}
	}()
	return e.submitAppendLocked(ctx, rec, slot)
}

// controlOnlyRefusal answers the append THIS node must not serve: a slot the
// table does not assign and a node the directory knows as a pure control
// plane. The classic cluster serves an unassigned slot locally (single-node
// shape), but a voter's WAL growing a seq space no layout knows is exactly
// the contamination the role split exists to keep off the control plane —
// the plan simply has not run yet (no data-plane member registered).
//
// Returns nil when this node may serve the slot.
func (e *Engine) controlOnlyRefusal(slot int32) *data.AppendResponse {
	e.tableMu.RLock()
	p, in := e.table.Peers[e.self]
	pureControl := in && p.PureControl()
	e.tableMu.RUnlock()
	if !pureControl {
		return nil
	}
	return &data.AppendResponse{
		Status: data.StatusFail,
		ErrID:  data.ErrIDNotLeader,
		Err:    "cluster has no data-plane members; this node is control-plane only",
		Slot:   slot,
	}
}

// submitAppendLocked is the routing/execution half of SubmitAppend, run while
// the slot's write-fence registration is held (so the fence drain accounts for
// this append).
func (e *Engine) submitAppendLocked(ctx context.Context, rec *data.EventRecord, slot int32) (*data.AppendResponse, error) {

	// Copy the routing fields under the lock: the placement is mutated in
	// place by the Raft apply loop (OpSlotState/OpLeaderMove), so holding the
	// *Placement past RUnlock raced a concurrent rollback.
	e.tableMu.RLock()
	pp, ok := e.table.Slots[slot]
	var pState SlotState
	var pLeader string
	if ok {
		pState, pLeader = pp.State, pp.Leader
	}
	e.tableMu.RUnlock()
	if !ok {
		if refusal := e.controlOnlyRefusal(slot); refusal != nil {
			return refusal, nil
		}
		// single-node / unassigned: serve locally
		return e.localAppend(slot, rec)
	}

	switch {
	case pState == SlotMigratingOut && pLeader == e.self:
		// The source stays leader for the whole commit window and keeps
		// serving writes: they queue behind the commit fence and are released
		// (or redirected) the moment the leader move is applied. There is no
		// mirroring step — the target is an ordinary replica of this slot and
		// the fetch path carries every record to it.
		return e.localAppend(slot, rec)
	case pState == SlotMigratingOut:
		// someone else leads a migrating slot: the leader handles forwarding
		addr := e.clientAddr(pLeader)
		return nil, &RedirectError{Kind: data.ErrIDMigrating, Slot: slot, Node: pLeader, Addr: addr}
	case pLeader == e.self:
		e.awaitCommitFloor(slot)
		return e.localAppend(slot, rec)
	default:
		addr := e.clientAddr(pLeader)
		if addr == "" {
			return nil, data.ErrSlotNotLocal
		}
		return nil, &RedirectError{Kind: data.ErrIDSlotNotLocal, Slot: slot, Node: pLeader, Addr: addr}
	}
}

// RedirectError signals MOVED (1003) / ASK (1004) / NOT_LEADER (1005).
// Slot is the routed slot so the response can tell the client which
// routing entry to refresh.
type RedirectError struct {
	Kind int
	Slot int32
	Node string
	Addr string
}

func (r *RedirectError) Error() string {
	return fmt.Sprintf("redirect kind=%d node=%s addr=%s", r.Kind, r.Node, r.Addr)
}

// clientAddr is the client-facing data-plane address of a peer: the
// target MOVED/ASK redirects and cross-node read redirects point at.
func (e *Engine) clientAddr(nodeID string) string {
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	return e.table.Peers[nodeID].ClientAddr
}

// SlotRouteTable snapshots the write-routing table for PrefetchRoutes: one
// entry per slot in slot order (length = the table's slot count), carrying
// the slot leader's client-plane addr. An entry is "" when the slot is
// unassigned or its leader has not announced a client address yet — the
// caller decides the fallback (the answering node itself, matching
// SubmitBatch's local-serve rule for such slots).
func (e *Engine) SlotRouteTable() []string {
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	out := make([]string, e.table.SlotCount)
	for s := range out {
		if p, ok := e.table.Slots[int32(s)]; ok && p.Leader != "" {
			out[s] = e.table.Peers[p.Leader].ClientAddr
		}
	}
	return out
}

// localAppend runs the business rules against the local WAL, then waits for
// the slot's high watermark to cover the record before reporting success:
// an append acknowledged without that wait could vanish if the leader died
// before its replicas pulled the record.
func (e *Engine) localAppend(slot int32, rec *data.EventRecord) (*data.AppendResponse, error) {
	// Flow control (one node-wide token budget shared by every slot this
	// node leads): a refused append answers fail/1006 without touching the
	// WAL. Only the leader copy pays — this function only runs when THIS
	// node serves the slot's writes.
	if ok, bucket := e.takeFor(slot); !ok {
		return &data.AppendResponse{
			Status: data.StatusFail,
			ErrID:  data.ErrIDFlowControl,
			Err:    "FlowControl",
			Slot:   slot,
		}, nil
	} else if bucket != nil {
		resp, err := e.localAppendThrottled(slot, rec)
		if err != nil {
			// The WAL write itself failed: nothing landed, refund.
			flowRefund(bucket)
			return resp, err
		}
		if resp != nil && resp.Status == data.StatusFail &&
			(resp.ErrID == data.ErrIDVersionConflict || resp.ErrID == data.ErrIDBadRequest || resp.ErrID == data.ErrIDSlotUnproven) {
			// The token paid for a record that never landed on the leader's
			// log (a business-rule fail): give it back. A watermark timeout
			// (fail/1005) keeps its charge — the record IS durable above —
			// and so does EXISTS, per the operator's rule for idempotent
			// retries.
			flowRefund(bucket)
		}
		return resp, err
	}
	return e.localAppendThrottled(slot, rec)
}

func (e *Engine) localAppendThrottled(slot int32, rec *data.EventRecord) (*data.AppendResponse, error) {
	if s := e.slotAckOf(slot); s != nil {
		s.appends.Add(1)
	}
	tLand := time.Now()
	resp, err := e.landLocalAppend(slot, rec)
	e.noteLand(slot, time.Since(tLand))
	if err != nil || resp.Status != data.StatusSuccess {
		return resp, err
	}
	// success: wait for the watermark so the acknowledged record survives a
	// leader crash (see the function comment).
	tWait := time.Now()
	hw, werr := e.waitForHW(context.Background(), slot, resp.Seq, e.hwWait)
	e.noteWait(slot, time.Since(tWait), e.hwLagRecords(slot, hw), werr != nil)
	if werr != nil {
		resp.Status = data.StatusFail
		resp.ErrID = data.ErrIDNotLeader
		resp.Err = werr.Error()
		return resp, nil
	}
	return resp, nil
}

// landLocalAppend applies the business rules to the local WAL WITHOUT the
// watermark wait; the caller decides when and how long to wait (a single
// append waits for its own seq, a batch waits once for the group's max —
// see SubmitBatch). The returned response for a success carries the assigned
// seq; exists carries the stored record; fail carries the wire error.
// traceAck is the forensic hook the handover investigations turn on with
// PUSHUPES_TRACE_AGG=<prefix>: every acknowledgement of a matching aggregate is
// logged with this node, the seq it took and the version it confirmed. An
// acknowledged version that no copy holds afterwards is provable from these
// lines plus the survivors' WALs, without guessing at client bookkeeping.
// Off (one map lookup on a nil string) unless the env var is set.
func (e *Engine) traceAck(slot int32, rec *data.EventRecord, seq uint64, kind string) {
	if e.traceAgg == "" {
		return
	}
	// Volume is bounded by the HANDOVER window, not the write rate: only an
	// acknowledgement taken while this node holds the slot's write fence, or
	// while the slot is mid-migration in this node's applied table, can be
	// the one a successor fails to inherit.
	fenced := false
	if f := e.fenceFor(slot); f != nil {
		f.mu.Lock()
		fenced = f.closed
		f.mu.Unlock()
	}
	var state SlotState
	if p, ok := e.slotPlacement(slot); ok {
		state = p.State
	}
	if !fenced && (state == "" || state == SlotStable) {
		return
	}
	e.logger.Info("append acknowledged during handover", "node", e.self, "slot", slot,
		"agg", rec.AggregateID, "version", rec.Version, "seq", seq,
		"kind", kind, "fenced", fenced, "state", state, "op", "ack_trace")
}

func (e *Engine) landLocalAppend(slot int32, rec *data.EventRecord) (*data.AppendResponse, error) {
	// Owner fence (owner.go): a copy the table cannot vouch for stays out of
	// service. Appending here would write into a seq space the client was
	// never told about — the contamination the fence exists to refuse — so
	// the fence exists to refuse — so the refusal answers before the WAL, and an operator resolves it
	// explicitly (adopt, or remove the copy on disk).
	if e.slotPending(slot) {
		return &data.AppendResponse{
			Status: data.StatusFail,
			ErrID:  data.ErrIDSlotUnproven,
			Err:    "SlotUnproven",
			Slot:   slot,
		}, nil
	}
	// A leader with NO local copy of a slot that has already moved (epoch >= 2)
	// must not answer the client's version at all. Its directory starts empty,
	// so the CAS would report "current version 0" and the client would adopt
	// that — rewriting a slot whose history lives elsewhere into a fresh
	// version-1 stream (the dg22 storm: hundreds of 1001s and a copy that
	// crawled 0→6 frames while the real chain was at ~550). Answering a
	// retryable NOT_LEADER instead keeps the client's ledger intact until the
	// copy materializes; the move is re-planned if it never does.
	if e.leaderCopyMissingWithHistory(slot) {
		e.logger.Warn("append: this node leads the slot but holds no copy of it (handover history); answering retryable",
			"slot", slot, "agg", rec.AggregateID, "op", "leader_without_copy")
		return &data.AppendResponse{
			Status: data.StatusFail,
			ErrID:  data.ErrIDNotLeader,
			Err:    "replica catching up",
			Slot:   slot,
		}, nil
	}
	// The empty-shell sibling: the copy exists but holds nothing while an
	// in-set seat reports history this leader never saw (a floorless handover
	// pinned leadership onto a re-granted empty seat). Answering "current
	// version 0" from the shell invites the same adopt-and-rewrite the
	// missing-copy guard refuses, so the refusal shape is shared.
	if e.leaderEmptyShellWithHistory(slot) {
		e.logger.Warn("append: this node leads the slot but its copy is empty while a seat reports history; answering retryable",
			"slot", slot, "agg", rec.AggregateID, "op", "leader_empty_shell")
		return &data.AppendResponse{
			Status: data.StatusFail,
			ErrID:  data.ErrIDNotLeader,
			Err:    "replica catching up",
			Slot:   slot,
		}, nil
	}
	out, err := e.store.Append(rec)
	if err != nil {
		return nil, err
	}
	resp := &data.AppendResponse{Status: out.Status, ErrID: out.ErrID, Seq: out.Seq, Slot: slot}
	switch out.Status {
	case data.StatusFail:
		if out.ErrID == data.ErrIDVersionConflict {
			resp.CurrentVersion = out.CurrentVersion
			resp.Err = "version conflict"
			// Diagnostic (cheap, conflict-only): which copy refused, what it
			// believes the chain tail is, and whether its own WAL/s directory
			// disagree — separates a stale-but-honest tail (a migration target
			// that has not caught the acked record) from a directory stall.
			e.logger.Warn("append version conflict: responder's chain is short of the client's ledger",
				"slot", slot, "agg", rec.AggregateID, "sent", rec.Version,
				"refused_tail", out.CurrentVersion,
				"dir_version", e.store.VersionOf(slot, rec.AggregateID),
				"leo", e.store.LastSeqOf(slot), "op", "1001_diag")
		} else {
			resp.Err = "bad request"
		}
		return resp, nil
	case data.StatusExists:
		resp.Record = out.Record // stored record, raw bytes end to end
		e.traceAck(slot, rec, out.Seq, "exists")
		return resp, nil
	}
	e.traceAck(slot, rec, out.Seq, "success")
	// Success carries status/seq only: the caller already holds the record it
	// sent, and echoing a 100KiB body back doubled the bytes on the wire per
	// append (see DESIGN.md §6). EXISTS still returns the stored record, which
	// is the one case where the caller cannot know it.
	return resp, nil
}

// SubmitBatch appends a group of records that ALL route to the given slot, in
// request order, answering each record individually. The group enters the
// slot's write fence ONCE and reads the routing table once — both are
// slot-level state, so per-record lookups in the single path were a batch tax,
// not a requirement.
//
// The headline difference from N single appends is the MERGED watermark wait:
// every record lands (serially, under the slot's WAL lock, preserving WAL
// order = request order), then the group waits for the slot's high watermark
// ONCE, against the largest seq the group wrote. Records are judged against
// the watermark observed when the wait ends: on timeout, the prefix the
// watermark does cover is still acknowledged success (its durability promise is
// met), only the records above it fail/1005 — durable in the leader's WAL,
// retried by the client under the same command_id, converging on exists. A
// stalled replica set therefore costs the group ONE timeout, not one per
// record, and never re-fails records the watermark already covers.
//
// A routing outcome that is not local (MOVED/ASK/NOT_LEADER for the slot)
// fails the WHOLE group the same way: it is slot-level state, every record
// shares the answer. Migration forwarding stays per record (best effort).
func (e *Engine) SubmitBatch(ctx context.Context, slot int32, recs []*data.EventRecord) (out []*data.AppendResponse, err error) {
	if len(recs) == 0 {
		return nil, nil
	}
	start := time.Now()
	leave := e.enterWriteFence(slot)
	defer leave()
	e.awaitLocalDemotion(slot)
	fenced := time.Now()
	e.noteCall(start, fenced, len(recs), true)
	defer func() {
		redir := redirected(err)
		e.noteCallEnd(start, redir)
		if !redir {
			// batchLocal has already demoted the records above the watermark
			// deadline, so these are the settled statuses.
			for _, r := range out {
				e.noteOutcome(r)
			}
		}
	}()

	// One routing snapshot for the group (same copy-under-lock rule as
	// submitAppendLocked: the placement is mutated in place by the apply loop).
	e.tableMu.RLock()
	pp, ok := e.table.Slots[slot]
	var pState SlotState
	var pLeader string
	if ok {
		pState, pLeader = pp.State, pp.Leader
	}
	e.tableMu.RUnlock()

	switch {
	case !ok:
		if refusal := e.controlOnlyRefusal(slot); refusal != nil {
			// One answer per record (they are independent wire responses).
			out := make([]*data.AppendResponse, len(recs))
			for i := range out {
				r := *refusal
				out[i] = &r
			}
			return out, nil
		}
		// single-node / unassigned: serve locally
		return e.batchLocal(ctx, slot, recs)
	case pState == SlotMigratingOut && pLeader != e.self:
		// A migrating slot another node leads: the commit window is theirs.
		return nil, &RedirectError{Kind: data.ErrIDMigrating, Slot: slot, Node: pLeader, Addr: e.clientAddr(pLeader)}
	case pLeader == e.self:
		e.awaitCommitFloor(slot)
		return e.batchLocal(ctx, slot, recs)
	default:
		addr := e.clientAddr(pLeader)
		if addr == "" {
			return nil, data.ErrSlotNotLocal
		}
		return nil, &RedirectError{Kind: data.ErrIDSlotNotLocal, Slot: slot, Node: pLeader, Addr: addr}
	}
}

// batchLocal is the local-execution half of SubmitBatch: land every record in
// request order, mirror accepted ones to a migration target when set, then
// wait for the watermark once against the group's max seq.
func (e *Engine) batchLocal(ctx context.Context, slot int32, recs []*data.EventRecord) ([]*data.AppendResponse, error) {
	out := make([]*data.AppendResponse, len(recs))
	if s := e.slotAckOf(slot); s != nil {
		s.appends.Add(uint64(len(recs)))
	}
	var maxSeq uint64
	tLand := time.Now()
	// Flow control runs PER RECORD here, in request order, against the
	// node's shared bucket: the group is not atomic (the operator's rule),
	// so accepted records land and a refusal fails only that record and
	// everything after it — those never touch the WAL. Records that did
	// land keep the ordinary merged watermark wait below. A record refused
	// at position k also stops the take loop, so a burst cannot drain the
	// bucket on records that will not execute anyway.
	throttled := false
	for i, rec := range recs {
		if throttled {
			out[i] = &data.AppendResponse{Status: data.StatusFail, ErrID: data.ErrIDFlowControl, Err: "FlowControl", Slot: slot}
			continue
		}
		ok, bucket := e.takeFor(slot)
		if !ok {
			throttled = true
			out[i] = &data.AppendResponse{Status: data.StatusFail, ErrID: data.ErrIDFlowControl, Err: "FlowControl", Slot: slot}
			continue
		}
		resp, err := e.landLocalAppend(slot, rec)
		if err != nil {
			flowRefund(bucket)
			return nil, err
		}
		out[i] = resp
		if resp.Status == data.StatusFail &&
			(resp.ErrID == data.ErrIDVersionConflict || resp.ErrID == data.ErrIDBadRequest) {
			// Same refund rule as the single-append path: a business-rule
			// fail never consumed log space, so it must not consume budget.
			flowRefund(bucket)
		}
		if resp.Status == data.StatusSuccess && resp.Seq > maxSeq {
			maxSeq = resp.Seq
		}
	}
	e.noteLand(slot, time.Since(tLand))
	if maxSeq == 0 {
		return out, nil // nothing new landed: fail/exists only, no wait
	}
	tWait := time.Now()
	hw, werr := e.waitForHW(context.Background(), slot, maxSeq, e.hwWait)
	e.noteWait(slot, time.Since(tWait), e.hwLagRecords(slot, hw), werr != nil)
	if werr != nil {
		// The group's wait ran out. Judge every record against the watermark
		// waitForHW observed at the deadline: the prefix it covers keeps its
		// success (its durability promise IS met), only the records above it
		// fail/1005 — durable in the leader's WAL, retried by the client
		// under the same command_id, converging on exists. The timeout thus
		// costs the group ONE deadline, not one per record.
		for _, resp := range out {
			if resp.Status == data.StatusSuccess && resp.Seq > hw {
				resp.Status = data.StatusFail
				resp.ErrID = data.ErrIDNotLeader
				resp.Err = werr.Error()
			}
		}
	}
	return out, nil
}

// HandleReplicate serves PeerService.Replicate: a migration forwarding push.
// The payload is a single encoded record to be appended at a fixed seq.
func (e *Engine) HandleReplicate(slot int32, seq uint64, payload []byte) error {
	_, consumed, err := data.DecodeRecordMeta(payload)
	if err != nil {
		return err
	}
	if consumed != len(payload) {
		return fmt.Errorf("replicate: payload holds %d extra bytes", len(payload)-consumed)
	}
	return e.store.AppendFrameAtSeq(slot, seq, payload)
}

// ---- High watermark / ISR ----------------------------------------------------

// waitForHW blocks until the slot's high watermark covers seq, or timeout.
// It reports the watermark observed when it returns: on a timeout the caller
// can judge a GROUP of seqs against it (see SubmitBatchLocal's merged wait)
// instead of guessing.
func (e *Engine) waitForHW(ctx context.Context, slot int32, seq uint64, timeout time.Duration) (uint64, error) {
	if e.replicaCount(slot) <= 1 {
		return 0, nil // nothing to wait for
	}
	e.ack.waitersNow.Add(1)
	defer e.ack.waitersNow.Add(-1)
	deadline := time.Now().Add(timeout)
	// Event-driven: a replica report that moves the watermark kicks the slot's
	// gate, so the acknowledged append wakes on the movement itself instead of
	// on a poll interval. The tick is the safety net for transitions that
	// produce no report (a replica going stale, a peer marked down); see
	// hwWaitTick.
	tick := time.NewTicker(hwWaitTick)
	defer tick.Stop()
	for {
		// Same eligibility rule as the watermark min in advanceHW, computed
		// before the lock like advanceHW does: only a seat that IS an ack
		// holder may park an append. A seat left out of the min contributes
		// nothing to sr.hw, so waiting for its report waits for an event that
		// cannot move the watermark.
		target := e.migrationTargetOf(slot)
		members := e.tableReplicasOf(slot)
		e.replMu.Lock()
		sr := e.repl[slot]
		hw := uint64(0)
		var hwAt time.Time
		inSync := false
		if sr != nil {
			hw = sr.hw
			hwAt = sr.hwAt
			cutoff := time.Now().Add(-isrStaleAfter)
			leaderLEO := e.store.LastSeqOf(slot)
			for _, node := range members {
				if node == e.self {
					continue
				}
				i := sr.find(node)
				if i < 0 || !sr.lastOK[i].After(cutoff) {
					continue
				}
				// A replica whose connection died (or that the controller
				// marked down) is not being served: count it as out of ISR, so
				// the append falls back to "no in-sync replica to wait for"
				// instead of blocking on a watermark that was pinned by it.
				if e.sessionGone(node, sr.lastOK[i]) || e.peerDown(node) {
					continue
				}
				// Still being built: not an ack holder. On a low->medium flip
				// a slot's ONLY other copy is such a seat (its first report is
				// leo=0, and freshness alone made it read as in-isr), so every
				// append parked the full deadline behind the seat's catch-up:
				// the client's own call timeout then requeued the batch and
				// the idempotent retry answered exists (measured on a 3-node
				// flip with --batch 100: +100 exists per stalled window, and
				// hw_wait max pinned at the 10s deadline while every measured
				// term of the report round was under 30ms). The seat's own
				// report cadence is not the promise: it enters the ack set the
				// moment advanceHW lets it into the min (caught up).
				if sr.isBuilding(node) {
					continue
				}
				if node == target && sr.leo[i] < leaderLEO {
					continue // catching-up migration target: not an ack holder yet
				}
				inSync = true
				break
			}
		}
		// Take the wake channel before releasing replMu: an advance that
		// stores the watermark and kicks between this read and the park then
		// either closed this generation (we wake) or happened before it (the
		// watermark we just read already covers the record).
		var wake <-chan struct{}
		if g := e.hwGateOf(slot); g != nil {
			wake = g.wait()
		}
		e.replMu.Unlock()
		if !inSync {
			// No in-sync replica to wait for: the durability promise is to
			// in-sync replicas only, so the append is acknowledged as durable
			// as of the leader's own log (same rule advanceHW applies when the
			// ISR shrinks). Reading the cached sr.hw here could block the full
			// timeout on a slot whose replicas never reported (or went stale),
			// which is a stall, not a safety gain.
			hw = e.store.LastSeqOf(slot)
		}
		if hw >= seq {
			// Only a watermark that came from a replica report says anything
			// about the wake path; the no-ISR fallback takes the leader's own
			// log and its stamp would be meaningless here.
			if inSync && !hwAt.IsZero() {
				e.ack.wakeLag.observe(time.Since(hwAt))
			}
			return hw, nil
		}
		if time.Now().After(deadline) {
			e.ack.stalls.Add(1)
			e.logHWStall(slot, seq, hw)
			return hw, fmt.Errorf("timeout waiting for high watermark (hw %d < seq %d)", hw, seq)
		}
		select {
		case <-wake: // a replica report moved the watermark
		case <-tick.C: // safety net: re-check stale/down transitions
		case <-ctx.Done():
			return hw, ctx.Err()
		}
	}
}

// logHWStall reports why an append ran into the watermark wait deadline: the
// slot's replica set, each replica's last reported LEO and how stale that
// report is, and the leader's own LEO. Without this a wedged watermark (the
// migration stall) only showed up as a client-side timeout.
func (e *Engine) logHWStall(slot int32, seq, hw uint64) {
	e.replMu.Lock()
	sr := e.repl[slot]
	type rep struct {
		Node     string `json:"node"`
		LEO      uint64 `json:"leo"`
		AgeS     int64  `json:"age_s"`
		InISR    bool   `json:"in_isr"`
		Building bool   `json:"building"`
	}
	var reps []rep
	if sr != nil {
		cutoff := time.Now().Add(-isrStaleAfter)
		now := time.Now()
		for i, node := range sr.node {
			reps = append(reps, rep{node, sr.leo[i], int64(now.Sub(sr.lastOK[i]).Seconds()),
				sr.lastOK[i].After(cutoff), sr.isBuilding(node)})
		}
	}
	e.replMu.Unlock()
	e.logger.Warn("watermark wait deadline hit: high watermark did not cover the append",
		"slot", slot, "seq", seq, "hw", hw,
		"leader_leo", e.store.LastSeqOf(slot),
		"replicas", e.tableReplicasOf(slot),
		"positions", reps,
		"target", e.migrationTargetOf(slot))
}

func (e *Engine) replicaCount(slot int32) int {
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	p, ok := e.table.Slots[slot]
	if !ok {
		return 1
	}
	return len(p.Replicas)
}

// peerDown reports whether the replicated directory has this peer marked down
// (the controller's liveness verdict): a node whose slots have already failed
// over must not hold this leader's watermark back either. Caller holds replMu.
// seatVouchable reports whether node holds a seat whose watermark sample the
// leader may VOUCH for read admission: reported inside the staleness window,
// connection alive, not marked down — advanceHW's in-sync set as seen from
// one follower. The ack gate waits on the min over exactly this set, so an
// acknowledged record sits at or below every vouchable seat's LEO: vouching
// the watermark to such a seat is sound, and its own durable head is then a
// safe local read bound (see readAdmission). A seat outside the window is
// NOT covered by the ack promise; its ledger sample gets clamped to the
// leader's durable LEO — the only bound an unvouchable copy satisfies by
// catching up in full — so it redirects until proven, never answers short.
// Caller holds replMu (peerDown nests tableMu; advanceHW parks in the same
// order).
func (e *Engine) seatVouchable(sr *slotRepl, node string) bool {
	i := sr.find(node)
	if i < 0 || !sr.lastOK[i].After(time.Now().Add(-isrStaleAfter)) {
		return false
	}
	if e.sessionGone(node, sr.lastOK[i]) {
		return false
	}
	return !e.peerDown(node)
}

func (e *Engine) peerDown(node string) bool {
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	return e.table.Peers[node].Down
}

// isr returns in-sync replicas: followers whose LEO is within the lag window,
// minus the seats this leader is still building (a re-layout's new copy is a
// replica of the slot but not yet an in-sync one — it is excluded from the
// watermark for exactly that reason, see slotRepl.building, and the console
// must not show it as an ack holder).
func (e *Engine) isr(slot int32) []string {
	e.replMu.Lock()
	defer e.replMu.Unlock()
	sr := e.repl[slot]
	if sr == nil {
		return nil
	}
	cutoff := time.Now().Add(-isrStaleAfter)
	var out []string
	for i, node := range sr.node {
		if !sr.lastOK[i].After(cutoff) || sr.isBuilding(node) {
			continue
		}
		if e.sessionGone(node, sr.lastOK[i]) || e.peerDown(node) {
			continue // not being served / marked down: not an in-sync replica
		}
		out = append(out, node)
	}
	return out
}

// sessionGone reports whether the position of a follower is one this leader is
// no longer serving: its fetch long poll ended without an answer after that
// position was last reported. Such a replica is not in sync any more, so it
// neither feeds the high watermark nor reads as ISR. Caller holds replMu.
func (e *Engine) sessionGone(follower string, lastReport time.Time) bool {
	gone, ok := e.goneSess[follower]
	return ok && gone.After(lastReport)
}

// noteFollowerSessionLost records that a follower's fetch long poll ended
// without an answer — its connection is gone — and re-derives the watermark of
// every slot it was reporting for, so appends already waiting on that replica's
// position stop waiting now.
//
// This is the fast half of "a replica that is not being served must not pin the
// writes": the transport says so the moment the connection dies, while the
// replicated liveness verdict (mark_down) needs consecutive probe rounds and the
// staleness cutoff (isrStaleAfter) is a ten-second window. Without it a killed
// node's frozen LEO pins every slot it replicated until its seats leave the
// table — observed as an eight-second 0 msg/s window in a batch workload, since
// one pinned slot stalls a whole batch (the batch waits for every slot's
// watermark).
//
// A clean round is not a loss: the handler answers and the follower re-posts.
// Only an error/cancellation lands here, and a follower that reconnects clears
// the mark with its next report (noteReplicaProgress), so a blip costs at most
// the interval until then.
func (e *Engine) noteFollowerSessionLost(follower string, slots []int32) {
	if follower == "" || follower == e.self {
		return
	}
	now := time.Now()
	e.replMu.Lock()
	if e.goneSess == nil {
		e.goneSess = map[string]time.Time{}
	}
	// Re-deriving a whole session's watermarks is O(slots) locks: a follower
	// that flaps must not pay for that per flap.
	verbose := !e.goneSess[follower].After(now.Add(-sessionLossRecheck))
	e.goneSess[follower] = now
	e.replMu.Unlock()
	if !verbose {
		return
	}
	if e.logger != nil {
		e.logger.Info("follower fetch session lost: its positions stop gating the watermark until it reports again",
			"follower", follower, "slots", len(slots))
	}
	for _, s := range slots {
		e.advanceHW(s)
	}
}

// sessionLossRecheck throttles the watermark re-derivation a session loss
// triggers (see noteFollowerSessionLost): at most one per interval per follower.
const sessionLossRecheck = 200 * time.Millisecond

// ---- Unreachable witnesses: followers' evidence, the controller's verdict -----
//
// A slot leader that is KILLED is visible to every node fetching from it the
// moment its connection dies — much earlier than the controller's own liveness
// sweep can conclude anything (three probe rounds, ~3s, during which its frozen
// LEO pins the watermark of every slot it replicated and its slots stay routed
// to a dead leader). So the observers report what they saw and the controller
// acts on a QUORUM of distinct witnesses instead of waiting out its threshold:
// one observer's broken link is not evidence, several observers' is.
//
// The cost is event-driven and tiny: one small unary RPC per observer per
// unreachableWindow (the connection is already pooled, the body is two node
// ids), then the one OpMarkDown the probe path would have submitted anyway. The
// sweep stays as the fallback — nobody may be watching a node that hosts no
// replicated slots, and a partitioned observer cannot report at all.
const (
	// unreachableQuorum is how many DISTINCT observers must report the same
	// node within unreachableWindow before it is marked down ahead of the probe
	// threshold. Two is the smallest number that cannot be a single broken
	// link; it is deliberately not a majority: the witnesses are the nodes
	// actively fetching from the suspect, which is every other member in
	// practice, and waiting for a majority would give back the latency the
	// scheme exists to remove.
	unreachableQuorum = 2
	// unreachableWindow is how long a witness counts, and how often one
	// observer may report the same suspect.
	unreachableWindow = time.Second
	// unreachableReportAfter is how many CONSECUTIVE transport failures make a
	// session's observer speak up: one failure can be a blip (a table-driven
	// session restart, a hiccup), two in a row at fetch cadence cannot.
	unreachableReportAfter = 2
)

// transportUnreachable reports whether a fetch round failed at the TRANSPORT
// level — the peer's connection is gone (a dead process, a broken link), which
// is evidence a node is unreachable. A deadline (a slow server), a cancellation
// (this node stopping the session) or an application/routing answer are not.
func transportUnreachable(err error) bool {
	if err == nil {
		return false
	}
	if code := status.Code(err); code == codes.Unavailable {
		return true
	}
	return status.Code(errors.Unwrap(err)) == codes.Unavailable
}

// reportLeaderUnreachable tells the controller that a slot leader this node
// fetches from has stopped answering at the transport level. Fire and forget: a
// lost report only costs a probe round, and the caller throttles per session.
func (e *Engine) reportLeaderUnreachable(suspect string) {
	if suspect == "" || suspect == e.self || e.node == nil {
		return
	}
	if e.node.IsLeader() {
		// We ARE the controller: the evidence is local. (A node never fetches
		// from itself, so in practice this is the path a follower takes when the
		// controller changed under it.)
		e.recordUnreachable(e.self, suspect)
		return
	}
	addr := e.peerAddr(e.node.LeaderID())
	if addr == "" {
		return
	}
	reporter := e.self
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), peerPingTimeout)
		defer cancel()
		c, err := e.peerRPC(addr)
		if err != nil {
			return
		}
		if _, err := c.ReportUnreachable(ctx, &pushupesv1.ReportUnreachableRequest{Reporter: reporter, Suspect: suspect}); err != nil && e.logger != nil {
			e.logger.Debug("unreachable report: controller did not take it (it will probe)", "suspect", suspect)
		}
	}()
}

// failoverPeer marks a peer down and moves the leadership of the slots it led to
// the FRESHEST live replica of each (§failoverLeader): a replica that is behind
// does not hold the records the dead leader acknowledged, so handing it
// leadership loses them silently. The pick is computed here (only the controller
// can ask the replicas for their offsets), travels in the command so every node
// applies the same verdict, and falls back to the table-only rule when a
// candidate cannot be reached.
func (e *Engine) failoverPeer(id string) {
	cmd := &Command{Op: OpMarkDown, NodeID: id}
	if leaders, tails := e.freshestReplicas(id); len(leaders) > 0 {
		cmd.NewLeaders = leaders
		cmd.NewLeaderLEO = tails
		e.loggerf("failover of %s: %d slot(s) pinned to the freshest live replica", id, len(leaders))
	}
	if err := e.submit(cmd); err != nil {
		e.loggerf("failover of %s: %v (the sweep retries)", id, err)
	}
}

// freshestReplicas asks the live replicas of every slot the node LED for their
// durable LEO and returns, per slot, the one holding the most. One batched query
// per candidate (a failover touches one node's slots, and its replicas are the
// same handful of nodes for all of them), so the cost is a single peer round
// trip per candidate — paid once, on a failover.
//
// Candidates are the slot's replicas minus the failed node, minus peers already
// marked down, and minus peers the controller has been failing to probe (they
// are on their way down and must not be handed a slot). A slot whose candidates
// cannot be reached, or where they tie, is left to the table rule; ties resolve
// to the replica-set order there, which is what the pick would have said anyway.
func (e *Engine) freshestReplicas(down string) (map[int32]string, map[int32]uint64) {
	tbl := e.TableSnapshot()
	slots := make([]int32, 0, 64)
	for s, p := range tbl.Slots {
		if p.Leader == down {
			slots = append(slots, s)
		}
	}
	if len(slots) == 0 {
		return nil, nil
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })

	// Per candidate: the slots it replicates and could take over + its probe
	// streak (a peer the sweep is already failing is not a candidate).
	type candidate struct {
		slots []int32
		pos   map[int32]int // slot -> index in the replica-set order (tie-break)
	}
	cands := map[string]*candidate{}
	e.failMu.Lock()
	streak := make(map[string]int, len(tbl.Peers))
	for id, n := range e.failStreak {
		streak[id] = n
	}
	e.failMu.Unlock()
	for _, s := range slots {
		p := tbl.Slots[s]
		for i, r := range p.Replicas {
			if r == down {
				continue
			}
			// Draining candidates are out of the freshness run: a LEO pick
			// that lands on a node the operator is removing defeats the
			// drain. The table fallback still ranks draining last among
			// live replicas, so a slot is never stranded by this filter.
			if peer, ok := tbl.Peers[r]; !ok || peer.Offline() || peer.Draining || streak[r] > 0 {
				continue
			}
			c := cands[r]
			if c == nil {
				c = &candidate{pos: map[int32]int{}}
				cands[r] = c
			}
			c.slots = append(c.slots, s)
			c.pos[s] = i
		}
	}
	if len(cands) == 0 {
		return nil, nil
	}
	// Candidates are asked in PARALLEL and under a short deadline: the pick is a
	// best-effort freshness probe on the failover path (a healthy peer answers in
	// a millisecond), so a hanging candidate must not hold up the failover — it
	// simply drops out of the running and the table rule covers its slots.
	ctx, cancel := context.WithTimeout(context.Background(), failoverPickTimeout)
	defer cancel()
	type answer struct {
		id   string
		leos []uint64
	}
	leos := make(map[string]map[int32]uint64, len(cands))
	reached := map[string]bool{}
	answers := make(chan answer, len(cands))
	var wg sync.WaitGroup
	for id, c := range cands {
		addr := e.peerAddr(id)
		if addr == "" {
			continue
		}
		wg.Add(1)
		go func(id, addr string, slots []int32) {
			defer wg.Done()
			got, err := e.peerSlotLeos(ctx, addr, slots)
			if err != nil {
				return // unreachable candidate: the table rule decides this slot
			}
			answers <- answer{id, got}
		}(id, addr, c.slots)
	}
	wg.Wait()
	close(answers)
	for a := range answers {
		reached[a.id] = true
		m := make(map[int32]uint64, len(cands[a.id].slots))
		for i, s := range cands[a.id].slots {
			if i < len(a.leos) {
				m[s] = a.leos[i]
			}
		}
		leos[a.id] = m
	}
	out := map[int32]string{}
	tails := map[int32]uint64{}
	for _, s := range slots {
		best, bestLEO := "", uint64(0)
		for id, c := range cands {
			if !reached[id] {
				continue
			}
			leo := leos[id][s]
			if best == "" || leo > bestLEO || (leo == bestLEO && c.pos[s] < cands[best].pos[s]) {
				best, bestLEO = id, leo
			}
		}
		if best != "" {
			out[s] = best
			// The probe's best sighting travels with the pick: failoverPeer
			// files it as the slot's commit floor, bounding even the slots
			// the table rule seats (replica-set order, LEO-agnostic).
			tails[s] = bestLEO
		}
	}
	return out, tails
}

// recordUnreachable folds one observer's report into the controller's witness
// table and, on a quorum within the window, marks the suspect down — the same
// OpMarkDown the probe sweep would submit, just earlier. Only the controller
// acts: a report that lands on a follower is dropped (the observer keeps
// reporting while the failure lasts, and the sweep is the fallback).
func (e *Engine) recordUnreachable(reporter, suspect string) {
	if reporter == "" || suspect == "" || reporter == suspect || e.node == nil || !e.node.IsLeader() {
		return
	}
	tbl := e.TableSnapshot()
	p, ok := tbl.Peers[suspect]
	if !ok || p.Down {
		return // unknown member, or already the controller's verdict
	}
	now := time.Now()
	e.unreachMu.Lock()
	if e.unreach == nil {
		e.unreach = map[string]map[string]time.Time{}
	}
	seen := e.unreach[suspect]
	if seen == nil {
		seen = map[string]time.Time{}
		e.unreach[suspect] = seen
	}
	for r, at := range seen {
		if now.Sub(at) > unreachableWindow {
			delete(seen, r) // a stale witness is not a witness
		}
	}
	seen[reporter] = now
	n := len(seen)
	acted := n >= unreachableQuorum
	if acted {
		delete(e.unreach, suspect)
	}
	e.unreachMu.Unlock()
	if !acted {
		return
	}
	e.loggerf("peer %s reported unreachable by %d observers at the transport level, marking it down ahead of the probe threshold", suspect, n)
	go e.failoverPeer(suspect)
}

// advanceHW recomputes the slot high watermark as the min LEO across the
// in-sync set (leader included). Called on every replica progress.
//
// A migration TARGET that is still catching up does not gate the watermark.
// It is an extra copy the migration is building (the admission grew the set to
// factor+1) and not yet one of the replicas the slot's durability promise
// rests on — the handover only commits once the gate proved it caught up.
// Counting its low LEO froze the watermark below the leader's LEO and made
// every append block to the 10s wait deadline: the seconds-long pause
// around a migration. The slot's ordinary replicas still gate normally, so
// the acknowledged write still rests on in-sync copies.
//
// A seat a RE-LAYOUT added is excluded on the same reasoning (see
// slotRepl.building): a member that just joined a grown replica set is a copy
// being built, not yet one the promise rests on, and it starts gating the
// moment it reaches this leader's LEO. A seat that was already in the set when
// this node took the slot over keeps gating even while behind — that is the
// promise, not a stall to fix.
func (e *Engine) advanceHW(slot int32) {
	target := e.migrationTargetOf(slot)

	e.replMu.Lock()
	defer e.replMu.Unlock()
	sr := e.repl[slot]
	if sr == nil {
		return
	}
	leaderLEO := e.store.LastSeqOf(slot)
	minLEO := leaderLEO
	cutoff := time.Now().Add(-isrStaleAfter)
	for _, node := range e.tableReplicasOf(slot) {
		if node == e.self {
			continue
		}
		i := sr.find(node)
		if i < 0 || !sr.lastOK[i].After(cutoff) {
			continue // unknown or out of ISR: excluded from HW computation
		}
		if e.sessionGone(node, sr.lastOK[i]) {
			continue // its connection died after that report: not in sync any more
		}
		if e.peerDown(node) {
			continue // the controller marked it down: leadership has moved off it
		}
		if sr.isBuilding(node) {
			if sr.leo[i] < leaderLEO {
				continue // still being built: not an ack holder yet
			}
			sr.dropBuilding(node) // caught up: an ordinary ISR member from now on
		}
		if node == target && sr.leo[i] < leaderLEO {
			continue // still catching up: not an ack holder yet
		}
		if l := sr.leo[i]; l < minLEO {
			minLEO = l
		}
	}
	if minLEO > sr.hw {
		sr.hw = minLEO
		sr.hwAt = time.Now()
		// Wake the calls parked in waitForHW: their acknowledgement is exactly
		// this movement, so they must not wait for a poll interval (the kick
		// is free when nobody is parked).
		e.kickHW(slot)
	}
}

// hwGateOf returns the slot's wake gate, or nil for a slot outside this store
// (a test store with a smaller slot count: no gate, so such a wait is driven
// by the safety tick alone).
func (e *Engine) hwGateOf(slot int32) *hwGate {
	if slot < 0 || int(slot) >= len(e.hwGates) {
		return nil
	}
	return &e.hwGates[slot]
}

// kickHW wakes the waiters parked on a slot's high watermark: the ack
// callers waiting on that slot, plus — through the engine-wide hwPing
// signal — any fetch round parked with armed handles, which re-scans, books
// the moved slots' fresh watermarks into its own response, and answers
// early. That second wake is the tight echo: an acknowledged watermark
// reaches the follower inside the round already waiting, not one report
// round later.
func (e *Engine) kickHW(slot int32) {
	if g := e.hwGateOf(slot); g != nil {
		g.kick()
	}
	e.hwPingMu.Lock()
	if e.hwPing != nil {
		close(e.hwPing)
		e.hwPing = nil // nothing armed: the next waiter creates the generation
	}
	e.hwPingMu.Unlock()
}

// hwPingWait returns the current wake generation for parked fetch rounds,
// creating one if none is armed (the hwGate discipline: a kick with no
// waiter allocates nothing). Callers take the handle BEFORE parking — a
// kick between the handle and the park closes what they hold, so the first
// scan sees it (no-lost-wake).
func (e *Engine) hwPingWait() chan struct{} {
	e.hwPingMu.Lock()
	defer e.hwPingMu.Unlock()
	if e.hwPing == nil {
		e.hwPing = make(chan struct{})
	}
	return e.hwPing
}

// reArmHW returns the slot's watermark-gate handle for a parked fetch entry:
// armed at park time and re-armed in the hwPing scan BEFORE each fresh
// watermark read — the same no-lost-wake order as the store handle in the
// data branch and as waitForHW's park.
func (e *Engine) reArmHW(slot int32) <-chan struct{} {
	if g := e.hwGateOf(slot); g != nil {
		return g.wait()
	}
	return nil
}

// ---- Replication diagnostics -------------------------------------------------
//
// The data plane's own view of itself, in the same spirit as the storage flush
// view: a cluster whose client appends are parked in waitForHW looks idle from
// the outside (nothing reading, no handler waiting, CPU near zero), so the
// numbers that say whether the pipeline is healthy are the ones only the
// pipeline can report — how many slots have a high watermark behind their own
// log and for how long, how the leader's parked fetch rounds ended, and what
// the follower side actually posted.

// replDiag counts fetch activity at both ends. Increments happen once per
// fetch round (tens to thousands per second), never per record.
type replDiag struct {
	mfetchRounds  atomic.Uint64 // leader: MFetch rounds entered
	mfetchServed  atomic.Uint64 // ... that returned payloads in phase 1
	mfetchParked  atomic.Uint64 // ... that parked waiting for data
	mfetchStarved atomic.Uint64 // ... that answered early: the fetch budget was spent
	mfetchByWake  atomic.Uint64 // ... whose park ended on a data wake
	mfetchByHW    atomic.Uint64 // ... whose park ended on an early watermark echo
	mfetchByDeadl atomic.Uint64 // ... whose park ended on the wait deadline
	mfetchByCtx   atomic.Uint64 // ... whose park ended because the caller went away
	// The catch-up priority's own view (DESIGN §5): how often a follower was
	// behind on a ring seat (its catch-up is what a layout change waits on) and
	// how often another session was therefore served at half the budget.
	mfetchRingCatchUp atomic.Uint64
	mfetchHalfBudget  atomic.Uint64
	parkedNow         atomic.Int64  // parked rounds right now
	fetchRounds       atomic.Uint64 // follower: fetch rounds issued
	fetchData         atomic.Uint64 // ... that landed payloads
	fetchBytes        atomic.Uint64 // payload bytes landed
	reportsSent       atomic.Uint64 // progress reports posted
	reportsSlots      atomic.Uint64 // slots covered by those reports
	lastRoundNS       atomic.Int64  // follower: when the last round returned (0 = never)
	lastReportNS      atomic.Int64  // follower: when the last report was posted (0 = never)
}

// ReplPeerView is the leader's view of one follower, summed over the slots this
// node leads: how stale its reports are and how far its log trails the leader's.
type ReplPeerView struct {
	Follower     string  `json:"follower"`
	Slots        int     `json:"slots"`
	InISRSlots   int     `json:"in_isr_slots"`
	OldestOKAgeS float64 `json:"oldest_ok_age_s"`
	WorstLEOLag  uint64  `json:"worst_leo_lag"`
}

// ReplMfetchView is the leader's fetch-round tally. EndedByDeadline against
// EndedByWake is the number that separates a burst-driven pipeline (rounds end
// because new records arrived) from one paced by the long-poll wait budget.
type ReplMfetchView struct {
	Rounds          uint64 `json:"rounds"`
	ServedRounds    uint64 `json:"served_rounds"`
	ParkedRounds    uint64 `json:"parked_rounds"`
	StarvedRounds   uint64 `json:"starved_rounds"`
	EndedByWake     uint64 `json:"ended_by_wake"`
	EndedByDeadline uint64 `json:"ended_by_deadline"`
	EndedByHW       uint64 `json:"ended_by_hw_echo"`
	EndedByCtx      uint64 `json:"ended_by_ctx"`
	ParkedNow       int64  `json:"parked_now"`
	// Catch-up priority (DESIGN §5): rounds where a follower was behind on a
	// seat the ring granted it, and rounds another session was served at half
	// the payload budget because of one.
	RingCatchUpRounds uint64 `json:"ring_catch_up_rounds"`
	HalfBudgetRounds  uint64 `json:"half_budget_rounds"`
}

// ReplFetchView is this node's own tally as a follower.
type ReplFetchView struct {
	Rounds         uint64  `json:"rounds"`
	DataRounds     uint64  `json:"data_rounds"`
	Bytes          uint64  `json:"bytes"`
	Reports        uint64  `json:"reports"`
	ReportSlots    uint64  `json:"report_slots"`
	LastRoundAgeS  float64 `json:"last_round_age_s"`
	LastReportAgeS float64 `json:"last_report_age_s"`
}

// ReplStuckSlot is one slot this node leads whose log runs ahead of its own
// high watermark: the appends in (HW, LEO] are waiting for a replica report
// before they can be acknowledged.
type ReplStuckSlot struct {
	Slot int32   `json:"slot"`
	LEO  uint64  `json:"leo"`
	HW   uint64  `json:"hw"`
	Gap  uint64  `json:"gap"`
	AgeS float64 `json:"age_s"`
}

// ReplView is the whole replication self-view.
type ReplView struct {
	Node            string          `json:"node"`
	TrackedSlots    int             `json:"tracked_slots"`
	StuckSlots      int             `json:"stuck_slots"`
	StuckMaxGap     uint64          `json:"stuck_max_gap"`
	StuckOldestAgeS float64         `json:"stuck_oldest_age_s"`
	Worst           []ReplStuckSlot `json:"worst"`
	Peers           []ReplPeerView  `json:"peers"`
	Mfetch          ReplMfetchView  `json:"mfetch"`
	Fetch           ReplFetchView   `json:"fetch"`
}

// ReplStats snapshots the replication data plane.
func (e *Engine) ReplStats() ReplView { return e.ReplStatsTop(0) }

// ReplStatsTop is ReplStats with the worstN most-starved slots listed (by how
// long their watermark has been behind their log), so a caller can go straight
// to the slots clients are blocked on instead of guessing which they are.
// Safe to call while the node serves: replMu is held only for the map walk.
func (e *Engine) ReplStatsTop(worstN int) ReplView {
	e.replMu.Lock()
	tracked := len(e.repl)
	stuck, maxGap, oldest, worst, peers := aggregateRepl(e.repl, e.store.LastSeqOf, time.Now(), worstN)
	e.replMu.Unlock()

	d := &e.diag
	now := time.Now()
	return ReplView{
		Node: e.self, TrackedSlots: tracked,
		StuckSlots: stuck, StuckMaxGap: maxGap, StuckOldestAgeS: oldest,
		Worst: worst, Peers: peers,
		Mfetch: ReplMfetchView{
			Rounds: d.mfetchRounds.Load(), ServedRounds: d.mfetchServed.Load(),
			ParkedRounds: d.mfetchParked.Load(), StarvedRounds: d.mfetchStarved.Load(),
			EndedByWake: d.mfetchByWake.Load(), EndedByDeadline: d.mfetchByDeadl.Load(),
			EndedByHW:  d.mfetchByHW.Load(),
			EndedByCtx: d.mfetchByCtx.Load(), ParkedNow: d.parkedNow.Load(),
			RingCatchUpRounds: d.mfetchRingCatchUp.Load(), HalfBudgetRounds: d.mfetchHalfBudget.Load(),
		},
		Fetch: ReplFetchView{
			Rounds: d.fetchRounds.Load(), DataRounds: d.fetchData.Load(),
			Bytes: d.fetchBytes.Load(), Reports: d.reportsSent.Load(),
			ReportSlots:    d.reportsSlots.Load(),
			LastRoundAgeS:  ageSeconds(d.lastRoundNS.Load(), now),
			LastReportAgeS: ageSeconds(d.lastReportNS.Load(), now),
		},
	}
}

// ageSeconds turns a stored unix-nano stamp into an age, with -1 for "never".
func ageSeconds(ns int64, now time.Time) float64 {
	if ns == 0 {
		return -1
	}
	return now.Sub(time.Unix(0, ns)).Seconds()
}

// aggregateRepl sums the per-slot replication state this node keeps for the
// slots it leads. A slot counts as stuck when its high watermark trails its own
// log: appends sitting at seqs above it cannot be acknowledged until a replica
// reports them, so the count, the largest gap and the age of the oldest are the
// leader-side measure of "how long have clients been waiting". Pure, so it can
// be tested against hand-built state.
func aggregateRepl(repl map[int32]*slotRepl, leoOf func(int32) uint64, now time.Time, worstN int) (stuck int, maxGap uint64, oldestS float64, worst []ReplStuckSlot, peers []ReplPeerView) {
	byPeer := map[string]*ReplPeerView{}
	for slot, sr := range repl {
		if sr == nil {
			continue
		}
		leo := leoOf(slot)
		if leo > sr.hw {
			stuck++
			if g := leo - sr.hw; g > maxGap {
				maxGap = g
			}
			age := 0.0
			if !sr.hwAt.IsZero() {
				age = now.Sub(sr.hwAt).Seconds()
				if age > oldestS {
					oldestS = age
				}
			}
			if worstN > 0 {
				worst = append(worst, ReplStuckSlot{Slot: slot, LEO: leo, HW: sr.hw, Gap: leo - sr.hw, AgeS: age})
			}
		}
		for i, node := range sr.node {
			if node == "" {
				continue
			}
			pv := byPeer[node]
			if pv == nil {
				pv = &ReplPeerView{Follower: node}
				byPeer[node] = pv
			}
			pv.Slots++
			okAge := now.Sub(sr.lastOK[i])
			if okAge <= isrStaleAfter {
				pv.InISRSlots++
			}
			if a := okAge.Seconds(); a > pv.OldestOKAgeS {
				pv.OldestOKAgeS = a
			}
			if l := sr.leo[i]; leo > l {
				if lag := leo - l; lag > pv.WorstLEOLag {
					pv.WorstLEOLag = lag
				}
			}
		}
	}
	for _, pv := range byPeer {
		peers = append(peers, *pv)
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].Follower < peers[j].Follower })
	if worstN > 0 && len(worst) > 1 {
		sort.Slice(worst, func(i, j int) bool { return worst[i].AgeS > worst[j].AgeS })
		if len(worst) > worstN {
			worst = worst[:worstN]
		}
	}
	return stuck, maxGap, oldestS, worst, peers
}

// newSlotRepl returns an empty per-slot replication record for a slot this node
// leads.
func newSlotRepl() *slotRepl { return &slotRepl{hwAt: time.Now()} }

// migrationTargetOf returns the node a slot is currently migrating to, or ""
// when it is not migrating_out.
func (e *Engine) migrationTargetOf(slot int32) string {
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	if p, ok := e.table.Slots[slot]; ok && p.State == SlotMigratingOut {
		return p.MigratingTo
	}
	return ""
}

func (e *Engine) tableReplicasOf(slot int32) []string {
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	return e.table.ReplicasOf(slot)
}

// HW returns the leader-tracked high watermark for a slot.
//
// The watermark is leader-side bookkeeping: it is advanced only from the
// progress reports a slot LEADER receives from its replicas, and nothing
// clears it when the node stops leading the slot — the value would be left
// frozen at the step-down point. Reporting that stale leftover would make a
// former leader's local read silently truncate at it (zero records for
// aggregates written after the step-down, a few short for aggregates that
// straddle it, and no error either way), so the watermark is answered only
// for a slot this node CURRENTLY leads. Everywhere else HW reports 0 — "no
// live watermark" — and the reader bounds the read by its own durable LEO.
func (e *Engine) HW(slot int32) uint64 {
	if !e.Leads(slot) {
		return 0
	}
	e.replMu.Lock()
	defer e.replMu.Unlock()
	if sr := e.repl[slot]; sr != nil {
		return sr.hw
	}
	return 0
}

// Leads reports whether this node currently leads the slot. Read handlers use
// it to decide whether a read may be capped at the replication high watermark:
// a LEADER owns its log and must be able to read its own writes (the HW is a
// follower-visibility bound), so capping there would hide acknowledged records.
func (e *Engine) Leads(slot int32) bool {
	if slot < 0 {
		return false
	}
	o := e.ownershipSnapshot()
	if int(slot>>6) >= len(o.words) {
		return false
	}
	return o.words[slot>>6]&(1<<(slot&63)) != 0
}

// Self returns this node's id.
func (e *Engine) Self() string { return e.self }

// SetNode attaches the Raft node (breaking the engine<->node cycle).
func (e *Engine) SetNode(node consensus) { e.node = node }

// layoutFeed is this node's data-plane feed hub (layoutfeed.go), built on
// first use: it reads the consensus timing and the node id off the Engine's
// node, which is attached after construction.
func (e *Engine) layoutFeed() *layoutHub {
	e.hubOnce.Do(func() { e.hub = newLayoutHub(e) })
	return e.hub
}

// SetBootPolicy records the -replica-policy seed for a brand-new cluster.
// The controller applies it exactly once — on the table's first round, while
// the table is still unplanned — and never again: from the first plan on,
// the policy lives in the replicated table and SetReplicaPolicy (the admin
// endpoint's entry) is the only way to change it.
func (e *Engine) SetBootPolicy(p ReplicaPolicy) { e.bootPolicy = p }

// SetReplicaPolicy is the cluster's policy entry point: it changes the
// replica strategy tier — low (one copy per slot), medium (two, the default),
// high (one copy on every data-plane node). The tier lives in the replicated
// slot table, so the change is one Raft command: it takes effect on every node
// and survives restarts. The factor the tier implies is derived at apply
// time as MIN(data-node count, copies the policy asks for), and the controller's
// next round converges the replica sets on the new factor — the same
// factor-change path a membership change drives (DESIGN §4). Submitting the
// tier the table already holds is a no-op. It returns the live tier and the
// factor in force.
func (e *Engine) SetReplicaPolicy(policy string) (ReplicaPolicy, int, error) {
	p, err := NormalizeReplicaPolicy(policy)
	if err != nil {
		return "", 0, err
	}
	if tbl := e.TableSnapshot(); tbl.Policy == p {
		return p, tbl.Replicas, nil
	}
	if err := e.submit(&Command{Op: OpSetPolicy, Policy: string(p)}); err != nil {
		return "", 0, err
	}
	tbl := e.TableSnapshot()
	return tbl.Policy, tbl.Replicas, nil
}

// SetFetchSettle sets the fetch round's coalescing window: after a parked round
// is woken by data, the leader waits this long before scanning, so a burst that
// scatters across slots is answered by ONE round instead of one round per slot.
// It trades acknowledgement latency (every acknowledged append pays it, since
// the round that carries the record pays it) against rounds and reports: 0
// answers as soon as the first slot has data.
func (e *Engine) SetFetchSettle(d time.Duration) {
	if d < 0 {
		d = 0
	}
	e.fetchSettle = d
	e.logger.Info("fetch round coalescing window", "settle", d.String())
}

// RaftStats exposes consensus counters for the admin API.
func (e *Engine) RaftStats() map[string]any {
	return e.node.Stats()
}

// ISR exposes the in-sync replica set (public form for the admin API).
func (e *Engine) ISR(slot int32) []string { return e.isr(slot) }

// SyncedLeaderSlots is the fleet gauge behind the console's 同步状态 card:
// (led, synced, seats) over the slots THIS node leads — the handler pairs it
// with slot_count so the console can say how many slots the WHOLE cluster has
// not converged on yet. A leading slot counts as synced only when BOTH the
// placement's replica set reaches the table's replica factor AND the in-sync
// copies reach it too: this leader (it owns the log, the same convention the
// ack promise uses — isr() lists followers only) plus every follower holding
// a fresh, non-building, still-served, not-down position (exactly isr()'s
// set). A seat the re-plan just seated is building — it COUNTS the placement
// but not the sync — so this gauge is precisely the 构建中 window the card
// names, and the low tier (factor 1) reads synced by construction: the
// leader alone satisfies both halves.
//
// slots whose replica SET is still short of the factor are NOT part of (led,
// synced) at all — they come back as seats (the 待就位 side of the card): the
// placement has to be completed by the layout first (serial re-plan after a
// scale-out), and counting that wait inside 同步中 made every node read
// "all of its slots lagging" for the whole re-plan window, data fully caught
// up or not (the shape that looked like a stuck sync).
//
// One lock hold for the whole sweep (replMu -> tableMu, the order noteSeatAges
// already uses): the poller runs it against every node every 2s, and a
// per-slot isr()/peerDown() walk would be thousands of lock round-trips.
func (e *Engine) SyncedLeaderSlots() (led, synced, seats int) {
	e.replMu.Lock()
	defer e.replMu.Unlock()
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	factor := e.table.Replicas
	cutoff := time.Now().Add(-isrStaleAfter)
	for s, p := range e.table.Slots {
		if p == nil || p.Leader != e.self {
			continue
		}
		if len(p.Replicas) < factor {
			seats++ // placement incomplete: 待就位, not 构建中
			continue
		}
		led++
		inSync := 1 // this leader owns the slot's log
		if sr := e.repl[s]; sr != nil {
			for i, node := range sr.node {
				if sr.lastOK[i].After(cutoff) && !sr.isBuilding(node) &&
					!e.sessionGone(node, sr.lastOK[i]) && !e.table.Peers[node].Down {
					inSync++
				}
			}
		}
		if inSync >= factor {
			synced++
		}
	}
	return led, synced, seats
}

// SubmitCommand is the exported Raft submission entry point (admin triggers
// like the slot re-plan). It is controller-only: on a follower it refuses with
// a *NotControllerError naming the controller (node id + admin address) —
// nothing is forwarded to the leader.
func (e *Engine) SubmitCommand(c *Command) error { return e.submit(c) }

// NoteReplicaProgress is called by the leader when a fetch response reports
// a follower's LEO.
func (e *Engine) NoteReplicaProgress(slot int32, follower string, leo uint64) {
	e.noteReplicaProgress(slot, follower, leo, time.Now())
}

// noteReplicaProgress records one progress sample and advances the HW.
// lastOK is always refreshed (a lagging-but-alive replica must stay in the
// ISR), but advanceHW — a second lock + replica scan — only runs when the
// slot's min-LEO could actually move: the LEO grew, or the replica (re-)
// joined the in-sync set. Fetch rounds re-report hundreds of steady slots
// every round; this is the short-circuit that makes those free.
func (e *Engine) noteReplicaProgress(slot int32, follower string, leo uint64, now time.Time) {
	e.replMu.Lock()
	sr := e.repl[slot]
	if sr == nil {
		sr = newSlotRepl()
		e.repl[slot] = sr
	}
	i := sr.find(follower)
	prev, wasFresh := uint64(0), false
	if i >= 0 {
		prev, wasFresh = sr.leo[i], now.Sub(sr.lastOK[i]) <= isrStaleAfter
	}
	sr.touch(follower, leo, now)
	// A report means this follower IS being served again: its earlier session
	// loss (if any) no longer excludes it from the watermark.
	delete(e.goneSess, follower)
	e.replMu.Unlock()
	if leo > prev || !wasFresh {
		e.advanceHW(slot)
	}
}

// progressBatch accumulates per-follower slot LEOs from one fetch round and
// applies them under a single replMu hold instead of one lock round-trip per
// item (the leader-side hot path: thousands of items per second).
type progressBatch struct {
	follower string
	// stamp=true refreshes the liveness last-seen for every position
	// (sweep rounds); with false (ordinary rounds) entries whose LEO did
	// not move are read, compared, skipped — no writes, no advanceHW.
	stamp   bool
	slots   []int32
	leos    []uint64
	now     time.Time
	changed []int32 // scratch: slots whose HW computation may move
}

func (b *progressBatch) add(slot int32, leo uint64) {
	b.slots = append(b.slots, slot)
	b.leos = append(b.leos, leo)
}

// reset returns the batch to a clean state for pool reuse. apply already
// truncates the position slices; this also drops the follower/now stamps.
func (b *progressBatch) reset() *progressBatch {
	b.follower = ""
	b.stamp = false
	b.slots = b.slots[:0]
	b.leos = b.leos[:0]
	b.changed = b.changed[:0]
	b.now = time.Time{}
	return b
}

// apply records every sample (lastOK always refreshed, one shared timestamp),
// then advances the HW once per slot whose in-sync min could have moved.
func (b *progressBatch) apply(e *Engine) {
	if len(b.slots) == 0 {
		return
	}
	changed := b.changed[:0]
	e.replMu.Lock()
	for i, slot := range b.slots {
		leo := b.leos[i]
		sr := e.repl[slot]
		if sr == nil {
			sr = newSlotRepl()
			e.repl[slot] = sr
		}
		j := sr.find(b.follower)
		if j < 0 { // new replica: record + (re)join the in-sync set
			sr.touch(b.follower, leo, b.now)
			changed = append(changed, slot)
			continue
		}
		if leo != sr.leo[j] {
			sr.touch(b.follower, leo, b.now)
			changed = append(changed, slot)
			continue
		}
		// LEO unchanged: refresh the liveness stamp only on sweep rounds
		// (steady followers re-report hundreds of frozen slots every
		// round; a full write per entry was the top CPU consumer). A
		// STALE replica re-reporting the same LEO must still re-join the
		// in-sync set — advanceHW runs when freshness flipped.
		wasFresh := b.now.Sub(sr.lastOK[j]) <= isrStaleAfter
		if b.stamp || !wasFresh {
			sr.lastOK[j] = b.now
			if !wasFresh {
				changed = append(changed, slot) // re-joined: HW may move
			} else if b.stamp && sr.isBuilding(b.follower) {
				// The sweep doubles as the building mark's RE-CHECK: a
				// re-plan can re-mark an alive, caught-up copy as a fresh
				// transition (syncSeats' seat-away-and-back churn on a
				// scale-out's serial re-plan), and an idle slot's LEO never
				// changes to trigger the ordinary advance. Without this the
				// mark — and the 0-synced reading of every churned slot —
				// stuck until the NEXT write to each slot. advanceHW makes
				// the real call: a mark clears only when the LEO reaches the
				// leader's, so a genuinely building seat just stays marked.
				changed = append(changed, slot)
			}
		}
	}
	e.replMu.Unlock()
	for _, slot := range changed {
		e.advanceHW(slot)
	}
	b.changed = changed
	b.slots = b.slots[:0]
	b.leos = b.leos[:0]
}

// ---- Replica fetch protocol (data plane) -------------------------------------

// FetchItem is one slot's share of a multiplexed fetch: the follower's next
// wanted seq (FromSeq; FromSeq-1 is its durable LEO and doubles as the
// progress report the leader records), and — on the response — the byte
// range + concatenated payload starting at that seq.
type FetchItem struct {
	Slot    int32  `json:"slot"`
	FromSeq uint64 `json:"from_seq"`
	NextSeq uint64 `json:"next_seq,omitempty"` // response: first seq after the batch
	Payload []byte `json:"payload,omitempty"`  // response: concatenated records
}

// MFetchRequest is one session round: a follower's positions across every
// slot led by this node, long-polled as a unit. Positions are packed
// parallel arrays (Slots[i] wanted from FromSeqs[i]): a session carries
// every followed slot each round and per-entry message objects dominated
// the wire and CPU cost under sustained writes.
type MFetchRequest struct {
	Follower string   `json:"follower"`
	WaitMS   int64    `json:"wait_ms,omitempty"`
	Slots    []int32  `json:"slots"`
	FromSeqs []uint64 `json:"from_seqs"`
	// Sweep is a liveness round: the leader refreshes last-seen stamps for
	// every reported position instead of only the ones that moved.
	Sweep bool `json:"sweep,omitempty"`
}

// MFetchResponse answers an MFetchRequest; only slots with data (or
// requested diagnostics) carry payload bytes.
type MFetchResponse struct {
	Follower string      `json:"follower"`
	Items    []FetchItem `json:"items"`
	// HwSlots[i]/HwValues[i]: this round's HW ledger for the requesting
	// follower (see the proto field comment): the leader-side watermark of
	// every slot the follower reported a position for, so a replica's
	// read-admission bound flows even on an idle round.
	HwSlots  []int32  `json:"hw_slots,omitempty"`
	HwValues []uint64 `json:"hw_values,omitempty"`
}

// Item finds the response entry for one slot (sparse responses omit empty
// slots, so index arithmetic on Items is not meaningful).
func (r *MFetchResponse) Item(slot int32) (FetchItem, bool) {
	for _, it := range r.Items {
		if it.Slot == slot {
			return it, true
		}
	}
	return FetchItem{}, false
}

// HandleMFetch serves one multiplexed fetch round on a node that leads the
// requested slots.
//
//  1. Progress piggyback: every position's FromSeq-1 is the follower's
//     durable LEO for that slot; the leader records replica progress from
//     the request itself — no separate report round trip on idle rounds.
//  2. Long-poll as a unit: when nothing is ready and WaitMS>0, the leader
//     holds the request on the store-wide wake bus (Slot advanceNotify
//     closes it on every append) instead of a per-slot select, and answers
//     as soon as ANY parked slot advances or the wait budget elapses. A
//     quiet cluster therefore pays ~1 request per leader-session per wait
//     window, not one per slot.
//
// The response is sparse: only slots that carried data appear; empty slots
// are implied by their absence. Slots this node does not lead are answered
// empty (failover is in flight); the session retries and the table
// reassigns them.
func (e *Engine) HandleMFetch(req MFetchRequest) (*MFetchResponse, error) {
	return e.HandleMFetchCtx(context.Background(), req)
}

// fetchScratch holds the follower's per-round slices: the reported positions
// (one per followed slot, ~11KB at the default slot count) and the progress report of a
// productive round. Both were allocated fresh every round — hundreds of rounds
// a second per followed leader — and both are dead as soon as the round's RPC
// returns, so one goroutine (the session loop) reuses them.
type fetchScratch struct {
	froms []uint64 // the round's reported positions, one per followed slot
}

func (f *fetchScratch) reset() *fetchScratch {
	f.froms = f.froms[:0]
	return f
}

func sizedU64(b []uint64, n int) []uint64 {
	if cap(b) < n {
		return make([]uint64, n)
	}
	return b[:n]
}

// fetchScan is one round's per-position answers from Store.ScanFetchState:
// whether the slot has data for the follower, and the wake handle to park on.
type fetchScan struct {
	slots []int32
	moved []bool
	wakes []<-chan struct{}
	// leos[i] is the slot's durable LEO at scan time: with froms[i] it says how
	// far behind the follower is, which is what separates an ACK-CRITICAL
	// position from bulk catch-up (see the visit order in HandleMFetchCtx).
	leos []uint64
	// order and bulk are the visit order's two backlog classes — the ring
	// seats and the rest — kept in SEPARATE scratch buffers (see
	// buildFetchOrder; the pooled order slice holds the round's visit order).
	order []int
	bulk  []int
}

func (f *fetchScan) resize(n int) {
	if cap(f.moved) < n {
		f.moved = make([]bool, n*2)
	}
	if cap(f.wakes) < n {
		f.wakes = make([]<-chan struct{}, n*2)
	}
	if cap(f.leos) < n {
		f.leos = make([]uint64, n*2)
	}
	f.moved = f.moved[:n]
	f.wakes = f.wakes[:n]
	f.leos = f.leos[:n]
	if cap(f.order) < n {
		f.order = make([]int, n*2)
	}
	f.order = f.order[:n]
	if cap(f.bulk) < n {
		f.bulk = make([]int, n*2)
	}
	f.bulk = f.bulk[:n]
}

// ackCritical reports whether position i is one a live append is waiting on:
// the slot has data for the follower and the follower is within
// fetchTailRecords of that slot's head. Those are the records whose delivery
// is what lifts a parked append's watermark, so the round serves them before
// the bulk (see the visit-order note in HandleMFetchCtx).
func (f *fetchScan) ackCritical(i int, froms []uint64) bool {
	if i >= len(f.moved) || i >= len(f.leos) || i >= len(froms) || !f.moved[i] {
		return false
	}
	from := froms[i]
	if from == 0 {
		return false
	}
	// moved implies leo >= from, so the gap is at least 1 and never underflows.
	return f.leos[i]+1-from <= fetchTailRecords
}

// reset clears the wake handles before the buffer goes back to the pool, so a
// recycled scan does not keep a store's channels alive.
func (f *fetchScan) reset() *fetchScan {
	for i := range f.wakes {
		f.wakes[i] = nil
	}
	f.moved, f.wakes = f.moved[:0], f.wakes[:0]
	return f
}

// parkedEntry is one slot waiting on the wake bus inside a fetch round.
type parkedEntry struct {
	slot int32
	from uint64
	h    <-chan struct{}
	// hwWake is the slot's watermark-gate handle, armed under the ledger
	// replMu hold right after that hold read the watermark it guards — the
	// no-lost-wake order (an advance before the hold shows in the value the
	// hold books; an advance after it closes the handle the hold arms, and
	// the hwPing scan books it and answers early: the tight echo). Re-armed
	// by the scan after every fire, before the fresh read.
	hwWake <-chan struct{}
}

// ackCritical reports whether a served item is an ACK-CRITICAL position: the
// leader serves those before the round's bulk (see the visit order in
// HandleMFetchCtx) because the follower's next report of that slot is what
// lifts a parked append's watermark. A tail carries at most fetchTailRecords
// records, which is what identifies it on this side. Unset NextSeq reads as
// "unknown", never as a tail.
func (it *FetchItem) ackCritical() bool {
	return it.NextSeq > it.FromSeq && it.NextSeq-it.FromSeq <= fetchTailRecords
}

// HandleMFetchCtx serves PeerService.MFetch: one multiplexed replica fetch
// round (all slots a follower takes from this leader, long-polled as a
// unit). ctx cancels the long-poll (client disconnect / deadline).
// ringCatchUpWindow bounds how long a follower's ring-catch-up report keeps the
// other sessions at half budget: a session that stopped reporting (its node went
// away, the slots moved) must not throttle them forever.
const ringCatchUpWindow = 30 * time.Second

// noteRingCatchUp records whether the follower reported being behind on a ring
// seat, which makes its catch-up the critical path of a layout change.
func (e *Engine) noteRingCatchUp(follower string, pending bool) {
	if follower == "" || follower == e.self {
		return
	}
	e.fetchRingMu.Lock()
	defer e.fetchRingMu.Unlock()
	if pending {
		if e.fetchRingBusy == nil {
			e.fetchRingBusy = map[string]time.Time{}
		}
		e.fetchRingBusy[follower] = time.Now()
		return
	}
	delete(e.fetchRingBusy, follower)
}

// ringCatchUpBusy reports whether another follower is currently behind on a
// ring seat, i.e. whether this round shares the budget with a layout change.
func (e *Engine) ringCatchUpBusy(self string) bool {
	e.fetchRingMu.Lock()
	defer e.fetchRingMu.Unlock()
	busy := false
	now := time.Now()
	for f, at := range e.fetchRingBusy {
		if now.Sub(at) > ringCatchUpWindow {
			delete(e.fetchRingBusy, f) // stale: the session stopped reporting
			continue
		}
		if f != self {
			busy = true
		}
	}
	return busy
}

// buildFetchOrder fills order with the round's visit sequence: ACK-critical
// positions first, then the follower's RING seats that have pending data,
// then the remaining bulk. The ring and bulk classes write SEPARATE scratch
// buffers and land in order one after the other, so order is always a
// permutation of [0, n) — every entry feeds straight into req.Slots, and a
// single shared buffer with a moving ring offset overwrites a placed bulk
// entry whenever the classes interleave, leaving a hole that copies the
// pooled buffer's stale residue (a previous, larger round's index) into the
// visit order. Returns whether any ring seat still waits (the layout's
// critical path).
func buildFetchOrder(n, rot int, order, ringScratch, bulkScratch []int, ack, ringMoved func(i int) bool) bool {
	head, ringN, bulkN := 0, 0, 0
	pending := false
	for k := 0; k < n; k++ {
		i := (k + rot) % n
		if ack(i) {
			order[head] = i
			head++
			continue
		}
		if ringMoved(i) {
			ringScratch[ringN] = i
			ringN++
			pending = true
			continue
		}
		bulkScratch[bulkN] = i
		bulkN++
	}
	copy(order[head:], ringScratch[:ringN])
	copy(order[head+ringN:], bulkScratch[:bulkN])
	return pending
}

func (e *Engine) HandleMFetchCtx(ctx context.Context, req MFetchRequest) (*MFetchResponse, error) {
	n := len(req.Slots)
	if n != len(req.FromSeqs) || n > 8192 {
		return nil, fmt.Errorf("mfetch: %d slots / %d positions invalid or over cap", n, len(req.FromSeqs))
	}
	t0 := time.Now()
	defer func() {
		e.logger.Debug("mfetch round", "self", e.self, "follower", req.Follower, "items", n,
			"ms", time.Since(t0).Milliseconds())
	}()
	waitMS := req.WaitMS

	// Round buffers come from pools; the rotation cursor keeps one slot's
	// sustained backlog from starving the rest under the payload budget.
	//
	// Parking is UNBOUNDED on purpose: a capped set would leave late
	// writes (to slots beyond the cap on an idle round) unservable until
	// the wait deadline, and the bus re-arm would spin on the closed
	// handle. Scanning every parked handle per bus fire is ~1.4k cheap
	// select-defaults — far cheaper than the old reflect.Select over
	// 1400+ cases per round.
	parked := e.parkedPool.Get().([]parkedEntry)[:0]

	// Capture the wake bus BEFORE any slot read: an append landing
	// between a parked slot's handle capture and the wait select would
	// otherwise close a bus handle we never observe (the same no-lost-wake
	// discipline as the per-slot handles). If it already fired during
	// phase 1, the first select iteration scans immediately.
	var bus <-chan struct{}
	if waitMS > 0 {
		bus = e.store.WakeBus()
	}

	// Ownership bitmap: rebuilt only when the Raft table version advances,
	// replacing the per-round map built under tableMu.
	own := e.ownershipSnapshot()

	// One batched scan for the whole round: per reported position it answers
	// "has data" and hands back the wake handle to park on. Asking the store
	// slot by slot cost two calls per slot and a read call even for the ~1300
	// empty ones, which at fetch-round rates is the leader's largest block of
	// own CPU at small body sizes.
	scan := e.scanPool.Get().(*fetchScan)
	scan.resize(n)
	e.store.ScanFetchState(req.Slots, req.FromSeqs, scan.moved, scan.wakes, scan.leos)

	// Visit order: the ACK-CRITICAL positions first, then the bulk. A position
	// the follower is at or near the head of is the one a live append is waiting
	// on — the follower's next report is what lifts that append's watermark —
	// while bulk catch-up moves up to 4MiB PER SLOT under this round's budget,
	// so a rotation over a few hundred catching-up seats answers only ~8 of them
	// per round and one lap can outlast the append's wait deadline even though
	// the replica is in ISR and reporting every round (measured: gap=1, in_isr,
	// building=false stalls repeating deadline after deadline on a cluster whose
	// per-slot logs run to tens of megabytes — the record had simply not been
	// delivered to that slot yet). A tail is a handful of records, so serving it
	// first costs the bulk almost nothing and bounds the appended record's
	// delivery by one round trip. Both classes keep the rotation order, so no
	// backlog slot can starve its peers.
	order := e.orderPool.Get().([]int)
	if cap(order) < n {
		order = make([]int, n*2)
	}
	order = order[:n]
	rot := int(e.fetchRot.Add(1)) % max(1, n)
	// Within the bulk, the follower's RING seats lead its factor seats. A ring
	// seat is a share of the layout the node has to carry — it joined, came
	// back after a failure, or the ring re-spread when a member left — and a
	// layout change waits for it; a factor seat is an extra copy, pure
	// redundancy, and can lag. Both classes keep the rotation order inside
	// themselves, so neither starves its peers.
	ringSeats := own.ring[req.Follower]
	ringPending := buildFetchOrder(n, rot, order, scan.order, scan.bulk,
		func(i int) bool { return scan.ackCritical(i, req.FromSeqs) },
		func(i int) bool {
			_, isRing := ringSeats[req.Slots[i]]
			return isRing && scan.moved[i]
		})
	e.noteRingCatchUp(req.Follower, ringPending)
	if ringPending {
		e.diag.mfetchRingCatchUp.Add(1)
	}

	out := &MFetchResponse{Follower: req.Follower, Items: make([]FetchItem, 0, 64)}
	// Ledger snapshot: the slots this round reports the follower's positions
	// for. prog.apply advances the watermarks and empties the batch, so the
	// slot list is kept here and the post-advance watermarks are read after
	// it — the ledger always carries the FRESHEST bound the leader knows.
	var ledgerSlots []int32
	e.diag.mfetchRounds.Add(1)
	budget := e.fetchBudget
	shared := e.ringCatchUpBusy(req.Follower)
	if shared {
		e.diag.mfetchHalfBudget.Add(1)
	}
	if shared {
		// Sharing the round with a node whose ring seats a layout change is
		// waiting on: this session's redundancy traffic takes half.
		budget /= 2
	}
	starved := false
	served := false
	// Progress piggyback is applied in bulk after phase 1: per-item calls
	// took replMu (twice: note + advanceHW) per slot every round, most of
	// them re-reporting unchanged LEOs.
	prog := e.progressPool.Get().(*progressBatch)
	prog.follower = req.Follower
	prog.stamp = req.Sweep
	prog.now = time.Now()

	for _, i := range order {
		s, from := req.Slots[i], req.FromSeqs[i]
		if s < 0 || int(s)>>6 >= len(own.words) || from == 0 {
			continue
		}
		if own.words[s>>6]&(1<<(s&63)) == 0 {
			continue // not led here: nothing to fetch, no progress to note
		}
		if req.Follower != "" && req.Follower != e.self {
			prog.add(s, from-1)
		}
		// The scan captured the wake handle before it read the position
		// (no-lost-wake discipline): an append landing in between closes
		// that handle, so the parked wait fires at once instead of sleeping
		// to the deadline. Without a long poll there is nothing to park on.
		var h <-chan struct{}
		if waitMS > 0 {
			h = scan.wakes[i]
		}
		if budget <= 0 {
			// Out of budget: no data this round. If the slot HAS pending
			// data, do NOT park a fresh wake handle — the data landed
			// before this point, so a clean handle would only fire on the
			// NEXT append and the slot would idle until the wait deadline
			// (that stall, not the wake path, was the p99=fetchWait tail). The rotating cursor serves it next round; marking
			// starved makes the response return now instead. If the slot
			// is truly empty, park so future appends still wake the poll.
			if waitMS > 0 {
				if scan.moved[i] {
					starved = true
				} else {
					parked = append(parked, parkedEntry{s, from, h, nil})
				}
			}
			continue
		}
		if !scan.moved[i] {
			if h != nil {
				parked = append(parked, parkedEntry{s, from, h, nil})
			}
			continue // empty slot: implied absent in the sparse response
		}
		_, next, payload, err := e.store.ReadSlotBytes(s, from, 0, min(4<<20, budget))
		if err != nil {
			e.scanPool.Put(scan.reset())
			e.orderPool.Put(order[:cap(order)])
			e.parkedPool.Put(parked[:0])
			e.progressPool.Put(prog.reset())
			return nil, err
		}
		if len(payload) == 0 {
			// The scan saw data but the read came back empty (a range
			// trimmed between the two): park like an empty slot rather
			// than answering with an empty item.
			if h != nil {
				parked = append(parked, parkedEntry{s, from, h, nil})
			}
			continue
		}
		budget -= int64(len(payload))
		served = true
		out.Items = append(out.Items, FetchItem{Slot: s, FromSeq: from, NextSeq: next, Payload: payload})
	}
	// Ledger cadence: a sweep round carries EVERY slot this follower reports
	// a position for (its whole followed set — the baseline that seeds a new
	// or restarted replica); an ordinary round carries the slots whose HW
	// computation moved (progressBatch.changed: a LEO that grew or a
	// re-joined replica — the ONLY events that can advance a watermark)
	// PLUS every slot it SERVED data for (the tightened echo: the proof
	// must never trail records already written to the replica's disk — see
	// the merge below). A steady round with no data and no movement still
	// costs zero ledger bytes, and HW never moves silently: advanceHW runs
	// exactly on the changed set.
	sweepLedger := req.Sweep && req.Follower != "" && req.Follower != e.self && len(prog.slots) > 0
	if sweepLedger {
		ledgerSlots = append(ledgerSlots[:0], prog.slots...)
	}
	prog.apply(e)
	if req.Follower != "" && req.Follower != e.self {
		if !sweepLedger {
			// Tightened echo: the ledger carries prog.changed (slots whose
			// HW computation moved on this round's reports) PLUS every slot
			// this round SERVED data for. The served set is what kills the
			// last round of ack-visibility lag: a follower that receives a
			// record in THIS response has already moved up to the acked
			// watermark the moment it writes it, but the leader's HW only
			// folds that in once the NEXT round's progress report lands —
			// until then a synced replica's proof could trail the data
			// sitting on its own disk. Shipping the post-report HW next to
			// the payloads costs a few (slot, hw) pairs against KB-sized
			// items and lets the replica prove-synced in the same round the
			// data arrived. Steady rounds with no data and no movement
			// still carry zero ledger bytes.
			ledgerSlots = prog.changed // post-report: the slots advanceHW touched
			if len(out.Items) > 0 {
				merged := make([]int32, 0, len(ledgerSlots)+len(out.Items))
				merged = append(merged, ledgerSlots...)
				for k := range out.Items {
					s := out.Items[k].Slot
					dup := false
					for _, o := range merged {
						if o == s {
							dup = true
							break
						}
					}
					if !dup {
						merged = append(merged, s)
					}
				}
				ledgerSlots = merged
			}
		}
		// HW ledger: for every slot in this round's ledger, the post-report
		// watermark (0 for slots with no bookkeeping entry yet — brand-new
		// seats, answered-but-not-led windows). The follower seeds its
		// read-admission bound from it (see hwSeen). One replMu hold for the
		// whole sweep: the entries a progress apply just touched are warm in
		// the map and 560 lock round-trips would dominate a quiet round.
		//
		// Vouch discipline: the sample is the leader's PROMISE that every
		// acknowledged record at or below it sits on the REQUESTING replica's
		// disk, and that promise is sound only for a seat the ack actually
		// covered — an in-sync, live, not-down seat (see seatVouchable; the
		// ack gate waits on exactly that set's min). A seat that dropped out
		// of the window cannot be vouched for: echoing the slot's HW to it
		// would admit a local read capped at a watermark that acked PAST its
		// own log — the short read of acknowledged data this fence exists to
		// kill. For such a seat the sample is clamped to the leader's whole
		// durable LEO, the only bound an unvouchable copy satisfies by
		// catching up in full; until then admission redirects (or serves
		// empty) and never answers short.
		e.replMu.Lock()
		for _, s := range ledgerSlots {
			hw := uint64(0)
			if sr := e.repl[s]; sr != nil {
				hw = sr.hw
				if !e.seatVouchable(sr, req.Follower) {
					hw = e.store.LastSeqOf(s)
				}
			}
			out.HwSlots = append(out.HwSlots, s)
			out.HwValues = append(out.HwValues, hw)
		}
		// Arm the parked slots' hw handles INSIDE this hold: the hold just
		// read the watermark they guard, so any advance from here on closes
		// a handle this round holds — the hwPing scan then books the fresh
		// value into this same response and answers early. Slots already
		// booked above need no handle for THIS value's sake; the arm keeps
		// them covered for advances after the hold.
		for k := range parked {
			pe := &parked[k]
			if pe.hwWake == nil {
				pe.hwWake = e.reArmHW(pe.slot)
			}
		}
		e.replMu.Unlock()
	}
	// The scan buffer's tenure ENDS here: everything below works off `parked`
	// (slot, from, wake handle) and re-reads the store by slot. Putting it back a
	// second time from the wait loop let two concurrent rounds hold the same
	// buffer — one of them resets/truncates it while the other is still indexing
	// it, which is an `index out of range [...] with length 0` panic inside
	// HandleMFetchCtx (observed once under heavy churn). One Put, one owner.
	e.scanPool.Put(scan.reset())
	e.orderPool.Put(order[:cap(order)])

	if served {
		e.diag.mfetchServed.Add(1)
	}
	if starved {
		e.diag.mfetchStarved.Add(1)
	}
	// A round that booked ledger values answers NOW even when it has no
	// payload: the sample this round computed is exactly what the follower's
	// read admission waits on, and parking would hold it back until the next
	// data wake or the wait deadline (up to fetchWait — the stall that made
	// an acknowledged watermark visibly trail at a synced replica on the
	// live cluster). Pure-changed rounds are rare and cheap (a report lift,
	// an ISR re-join); data rounds already answer now via `served`.
	if len(parked) > 0 && waitMS > 0 && !starved && !served && len(out.HwSlots) == 0 {
		e.diag.mfetchParked.Add(1)
		e.diag.parkedNow.Add(1)
		defer e.diag.parkedNow.Add(-1)
		wait := time.Duration(waitMS) * time.Millisecond
		if wait > fetchMaxWait {
			wait = fetchMaxWait
		}
		deadline := time.After(wait)
		ping := e.hwPingWait() // arm BEFORE parking (see hwPingWait)
		// The wake scan body, shared by the ping case and the one-shot
		// catch-up scan below (an advance kicked between this round's
		// ledger hold and the ping arm must still end the wait early —
		// its value was already booked, but a LATER advance's value was
		// not, and the handles captured pre-kick are closed).
		scanHwWake := func() bool {
			anyHW := false
			for k := range parked {
				pe := &parked[k]
				if pe.hwWake == nil {
					continue // slot without a gate: the sweep is its backstop
				}
				select {
				case <-pe.hwWake:
				default:
					continue
				}
				anyHW = true
			}
			return anyHW
		}
		if scanHwWake() {
			e.replMu.Lock()
			for k := range parked {
				pe := &parked[k]
				select {
				case <-pe.hwWake:
					pe.hwWake = e.reArmHW(pe.slot)
					hw := uint64(0)
					if sr := e.repl[pe.slot]; sr != nil {
						hw = sr.hw
					}
					out.HwSlots = append(out.HwSlots, pe.slot)
					out.HwValues = append(out.HwValues, hw)
				default:
				}
			}
			e.replMu.Unlock()
			e.diag.mfetchByHW.Add(1)
			e.parkedPool.Put(parked[:0])
			e.progressPool.Put(prog.reset())
			return out, nil
		}
	waitLoop:
		for {
			// One select over THREE cases instead of one case per parked
			// slot: the store bus aggregates every slot's advance signal,
			// and a non-blocking scan of the parked handles finds WHICH
			// slots moved. Rebuilding a 1400-case reflect.Select per round
			// was a top CPU contributor on the fetch hot path.
			select {
			case <-deadline:
				e.diag.mfetchByDeadl.Add(1)
				break waitLoop // budget spent: answer with whatever we have
			case <-ctx.Done():
				e.diag.mfetchByCtx.Add(1)
				e.parkedPool.Put(parked[:0])
				e.progressPool.Put(prog.reset())
				return nil, ctx.Err() // client went away
			case <-ping:
				// A watermark advanced on some slot this node leads. Scan
				// the armed handles to find WHICH parked entries it moved,
				// book their fresh values into this response's ledger, and
				// answer: the early exit is the tight echo. Payload work is
				// untouched — a pure watermark movement carries no data, and
				// the ledger alone is the answer the follower needs.
				anyHW := false
				for k := range parked {
					pe := &parked[k]
					if pe.hwWake == nil {
						continue // slot without a gate: the sweep is its backstop
					}
					select {
					case <-pe.hwWake:
					default:
						continue
					}
					// A slot booked EARLIER in this round may also have
					// moved since that booking (the round's own apply, then
					// a late report): ship the fresh value as a SECOND
					// entry — applyHwLedger applies the ledger in order, so
					// the last value for a slot wins on the follower. The
					// wire cost is one (slot, hw) pair; the alternative —
					// skipping — would answer with the stale booking,
					// which is exactly the lag this wake exists to kill.
					anyHW = true
					// Re-arm BEFORE reading the watermark (no-lost-wake,
					// same order as the data branch's store re-arm): an
					// advance landing between the two closes the fresh
					// handle and the next scan (or the next round's armed
					// copy) sees it.
					e.replMu.Lock()
					pe.hwWake = e.reArmHW(pe.slot)
					hw := uint64(0)
					if sr := e.repl[pe.slot]; sr != nil {
						hw = sr.hw
					}
					e.replMu.Unlock()
					out.HwSlots = append(out.HwSlots, pe.slot)
					out.HwValues = append(out.HwValues, hw)
				}
				if anyHW {
					e.diag.mfetchByHW.Add(1)
					break waitLoop
				}
				// The generation fired but NO parked slot moved (the ping is
				// engine-wide; the advance was another slot's). Hold a fresh
				// generation before looping again: the consumed one stays
				// closed forever.
				ping = e.hwPingWait()
			case <-bus:
				// Burst drain with a coalescing window: one wake answers
				// for EVERY slot that has data, not just the one that
				// fired. A write burst scatters across slots; answering
				// one slot per round would cost the follower one RTT per
				// remaining slot before HW can advance (the write
				// acknowledgement pays this on every record). The short settle lets sibling
				// appends of the same burst land before we scan — without
				// it the scan races the burst and captures only the first
				// slot. 2ms against a multi-second cadence is negligible.
				//
				// fetchServe measures this branch from wake to answer: the
				// settle sleep + the parked-handle re-scan + the payload
				// reads of the burst's slots. It is one of the two terms that
				// carry hw_wait's residual (the other is the follower's
				// replicaApply), and it touches no fsync — the follower's LEO
				// rides its page-cache write, so this is protocol cadence,
				// not disk. Only a wake that actually finds data is booked
				// (a blip that parks again is not a served answer).
				tServe := time.Now()
				if e.fetchSettle > 0 {
					time.Sleep(e.fetchSettle)
				}
				anyData := false
				for k := range parked {
					pe := &parked[k]
					select {
					case <-pe.h:
						// New appends since it parked: re-arm BEFORE
						// re-reading (no-lost-wake discipline).
						pe.h = e.store.WakeChan(pe.slot)
					default:
						continue // this slot did not move
					}
					if budget <= 0 {
						continue // out of budget: ride the next round
					}
					_, next, payload, err := e.store.ReadSlotBytes(pe.slot, pe.from, 0, min(4<<20, budget))
					if err != nil {
						e.parkedPool.Put(parked[:0])
						e.progressPool.Put(prog.reset())
						return nil, err
					}
					budget -= int64(len(payload))
					if len(payload) == 0 {
						continue // woke without fetchable data (e.g. gap-filled replay)
					}
					anyData = true
					out.Items = append(out.Items, FetchItem{Slot: pe.slot, FromSeq: pe.from, NextSeq: next, Payload: payload})
				}
				if anyData {
					e.diag.mfetchByWake.Add(1)
					e.ack.fetchServe.observe(time.Since(tServe))
					break waitLoop // answer with the whole burst
				}
			}
		}
	}
	e.parkedPool.Put(parked[:0])
	e.progressPool.Put(prog.reset())
	return out, nil
}

// ownershipSnapshot returns the bitmap of slots led by this node, rebuilt
// only when the Raft table version advances. Rebuilding walks the whole
// entry map once per table change instead of building a lookup map per
// fetch round (thousands of rounds per second under load).
type ownership struct {
	gen   uint64
	words []uint64
	// ring lists, per follower, the slots whose seat the ring granted it: the
	// fetch round's priority class (see HandleMFetchCtx). Built with the
	// snapshot — a grant lands in the table, so it is fresh — and nil while no
	// layout change is converging, which is the steady state.
	ring map[string]map[int32]struct{}
}

func (e *Engine) ownershipSnapshot() *ownership {
	e.tableMu.RLock()
	gen := e.tableGen
	e.tableMu.RUnlock()
	if o := e.own.Load(); o != nil && o.gen == gen {
		return o
	}
	e.ownMu.Lock()
	defer e.ownMu.Unlock()
	if o := e.own.Load(); o != nil && o.gen == gen {
		return o
	}
	words := make([]uint64, (e.store.SlotCount+63)/64)
	var ring map[string]map[int32]struct{}
	e.tableMu.RLock()
	for s, p := range e.table.Slots {
		if p.Leader == e.self && s >= 0 && int(s>>6) < len(words) {
			words[s>>6] |= 1 << (s & 63)
		}
		for node, kind := range p.SeatGrant {
			if kind != SeatGrantRing || node == e.self {
				continue
			}
			if ring == nil {
				ring = map[string]map[int32]struct{}{}
			}
			if ring[node] == nil {
				ring[node] = map[int32]struct{}{}
			}
			ring[node][s] = struct{}{}
		}
	}
	e.tableMu.RUnlock()
	o := &ownership{gen: gen, words: words, ring: ring}
	e.own.Store(o)
	return o
}

// ReplicateRecord applies a leader-assigned record frame on a follower (or
// import target) at its fixed seq, landing the leader's bytes verbatim.
func (e *Engine) ReplicateRecord(slot int32, seq uint64, payload []byte) error {
	_, consumed, err := data.DecodeRecordMeta(payload)
	if err != nil {
		return err
	}
	if consumed != len(payload) {
		return fmt.Errorf("payload holds %d extra bytes", len(payload)-consumed)
	}
	return e.store.AppendFrameAtSeq(slot, seq, payload)
}

// ---- Multiplexed replication sessions ----------------------------------------
//
// replicaLoop maintains one fetchSession per slot leader this node follows.
// Each session long-polls PeerService.MFetch with the follower position of
// every slot it covers (progress reports ride inside the same request), so a
// node following 2731 slots over 2 leaders holds 2 connections, not 2731.

func (e *Engine) replicaLoop(ctx context.Context) {
	e.syncSessions(ctx)
	for {
		select {
		case <-ctx.Done():
			e.sessMu.Lock()
			for _, s := range e.sessions {
				close(s.stop)
			}
			e.sessions = map[string]*fetchSession{}
			e.sessMu.Unlock()
			return
		case <-e.sessKick:
			e.syncSessions(ctx)
		}
	}
}

// kickSessions asks replicaLoop to reconcile its fetch sessions now. Called
// off the Raft apply path (non-blocking) so a leader move re-establishes the
// follower sessions immediately: the new leader gets its replicas' progress
// reports within one RPC round trip instead of up to a second later, so its
// watermark does not stall.
func (e *Engine) kickSessions() {
	select {
	case e.sessKick <- struct{}{}:
	default:
	}
}

// syncSessions reconciles sessions with the current table: every leader we
// follow as a replica gets a session, departed leaders get stopped, and
// membership changes land on the running session's slot list.
func (e *Engine) syncSessions(ctx context.Context) {
	e.tableMu.RLock()
	groups := map[string][]int32{}
	for s, p := range e.table.Slots {
		if p.Leader == e.self {
			continue
		}
		for _, r := range p.Replicas {
			if r == e.self {
				groups[p.Leader] = append(groups[p.Leader], s)
				break
			}
		}
	}
	e.tableMu.RUnlock()
	for leader := range groups {
		sort.Slice(groups[leader], func(i, j int) bool { return groups[leader][i] < groups[leader][j] })
	}

	e.sessMu.Lock()
	defer e.sessMu.Unlock()
	for leader, sess := range e.sessions {
		if _, ok := groups[leader]; !ok {
			close(sess.stop)
			delete(e.sessions, leader)
		}
	}
	for leader, slots := range groups {
		if sess, ok := e.sessions[leader]; ok {
			sess.slots.Store(slots)
			continue
		}
		sess := &fetchSession{leader: leader, stop: make(chan struct{})}
		sess.slots.Store(slots)
		e.sessions[leader] = sess
		go e.sessionLoop(ctx, sess)
	}
}

// sessionLoop drives one leader until stopped or context end. Round cadence:
// a productive round re-polls after the base interval; an idle round or an
// error doubles the sleep up to maxBackoff. The leader's long-poll absorbs
// quiet periods inside the request itself.
func (e *Engine) sessionLoop(ctx context.Context, sess *fetchSession) {
	sess.lastSweep = time.Now().Add(-fetchSweepInterval) // sweep on the first round
	backoff := time.Duration(0)
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-sess.stop:
			return
		case <-time.After(backoff):
		}
		slots, ok := sess.slots.Load().([]int32)
		if !ok || len(slots) == 0 {
			backoff = fetchBaseInterval
			continue
		}
		// Ordinary rounds report positions and let the leader skip
		// unchanged entries; a sweep round every fetchSweepInterval stamps
		// liveness for everything (<= a few seconds, far inside the 10s
		// staleness window even counting a full long-poll round).
		sweep := time.Since(sess.lastSweep) >= fetchSweepInterval
		if sweep {
			sess.lastSweep = time.Now()
		}
		_, err := e.fetchRound(ctx, sess.leader, slots, sweep)
		if err != nil {
			e.logger.Warn("fetch round failed", "leader", sess.leader, "error", err)
			// Evidence, not a verdict: a transport-level failure repeated twice
			// tells the controller this leader looks dead so it can skip its own
			// probe threshold (see reportLeaderUnreachable). A slow server, a
			// routing answer or a session stopped by the table is NOT evidence.
			if transportUnreachable(err) {
				sess.unreachable++
				if sess.unreachable >= unreachableReportAfter && time.Since(sess.lastReport) >= unreachableWindow {
					sess.lastReport = time.Now()
					e.reportLeaderUnreachable(sess.leader)
				}
			} else {
				sess.unreachable = 0
			}
			// Transport/leader trouble: back off so a dead leader does not
			// spin. An *empty* round is NOT backed off — the long-poll made
			// it zero-CPU, and sleeping between rounds is exactly what
			// stalled records past the wait deadline (p90 = the
			// fetchWait tail): a record landing in the backoff window rode
			// no wake until the next round started.
			if backoff == 0 {
				backoff = fetchBaseInterval
			} else {
				backoff *= 2
			}
			if backoff > fetchMaxBackoff {
				backoff = fetchMaxBackoff
			}
			continue
		}
		sess.unreachable = 0 // a round that answered says nothing is broken
		backoff = 0          // healthy round (data or absorbed-idle): re-park at once
	}
}

// fetchRound performs one multiplexed fetch: the covered slots' positions
// ride as progress reports (FromSeq-1 == durable LEO); the leader holds the
// request until at least one slot advances or the wait budget elapses.
func (e *Engine) fetchRound(ctx context.Context, leader string, slots []int32, sweep bool) (bool, error) {
	addr := e.peerAddr(leader)
	if addr == "" {
		return false, data.ErrNotLeader
	}
	e.diag.fetchRounds.Add(1)
	scr := e.fetchScratchPool.Get().(*fetchScratch)
	defer e.fetchScratchPool.Put(scr.reset())
	froms := sizedU64(scr.froms, len(slots))
	scr.froms = froms
	for i, s := range slots {
		froms[i] = e.store.LastSeqOf(s) + 1
	}
	reqCtx, cancel := context.WithTimeout(ctx, fetchWait+fetchSlack+5*time.Second)
	defer cancel()
	fr, release, err := e.peerMFetch(reqCtx, addr, &MFetchRequest{Follower: e.self, WaitMS: fetchWait.Milliseconds(), Slots: slots, FromSeqs: froms, Sweep: sweep})
	if err != nil {
		return false, err
	}
	e.applyHwLedger(leader, fr.HwSlots, fr.HwValues)
	// The payloads alias the response's receive buffer; releasing the lease
	// returns that buffer to the pool, so it must happen after the round has
	// written everything it took.
	defer release()
	e.diag.lastRoundNS.Store(time.Now().UnixNano())
	if len(fr.Items) > 0 {
		e.diag.fetchData.Add(1)
		for i := range fr.Items {
			e.diag.fetchBytes.Add(uint64(len(fr.Items[i].Payload)))
		}
	}
	var productive bool
	if len(fr.Items) > 0 {
		// The round's landing positions, collected from whichever worker applied
		// each slot. Reporting is what advances the leader's watermark, so the
		// collection must stay safe under the apply pool. A quarantined slot is
		// skipped by applyItem.
		//
		// An ACK-CRITICAL position is reported the moment it lands instead of
		// at the end of the round: the leader served those tails first (a live
		// append is parked on one), and the round's bulk apply can take seconds
		// on a catching-up replica — waiting for it is what a single-in-flight
		// writer pays as its whole round trip (measured on a cluster whose
		// per-slot logs run to tens of megabytes: acknowledged writes fell to
		// ~100 msg/s with clean node-side counters, i.e. the append waited on
		// the bulk apply, not on anything broken). A tail item carries at most
		// fetchTailRecords records, which is what identifies it here.
		//
		// replicaApply times the landing itself (the apply pool's wall time for
		// this round's items): the follower's term of hw_wait's residual, from
		// "the answer arrived" to "every slot's position is ready to report".
		var repMu sync.Mutex
		var changed []int32
		var leos []uint64
		earlyReports := 0
		flush := func() {
			repMu.Lock()
			c, l := changed, leos
			changed, leos = nil, nil
			repMu.Unlock()
			if len(c) > 0 {
				e.reportProgress(addr, c, l, true)
			}
		}
		tApply := time.Now()
		productive, err = e.applyFetchItems(fr.Items, func(it *FetchItem, from uint64) {
			repMu.Lock()
			changed = append(changed, it.Slot)
			leos = append(leos, from)
			early := earlyReports < maxEarlyReportsPerRound && it.ackCritical()
			if early {
				earlyReports++
			}
			repMu.Unlock()
			if early {
				flush()
			}
		})
		e.ack.replicaApply.observe(time.Since(tApply))
		if err == nil {
			flush()
		}
	} else {
		productive, err = e.applyFetchItems(fr.Items, nil)
	}
	if err != nil {
		return productive, err
	}
	return productive, nil
}

// applyFetchItems lands one fetch round's items. Each slot is applied on its
// own: a slot whose local log forked from the leader's (a seq already held with
// different bytes) is QUARANTINED — its diverging records are skipped, the rest
// of that slot's batch still lands, and every other slot of the round is still
// applied. The fork is never returned as a round error, so one slot cannot fail
// the whole multiplexed session (which used to back the session off and leave
// every other slot of that leader without a fetch round — the 10s watermark
// freeze). A genuine failure (a malformed frame) still aborts the round.
func (e *Engine) applyFetchItems(items []FetchItem, report func(it *FetchItem, from uint64)) (bool, error) {
	if len(items) == 0 {
		return false, nil
	}
	if len(items) < applyParallelMin {
		for i := range items {
			if err := e.applyItem(&items[i], report); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	workers := applyWorkers
	if workers > len(items) {
		workers = len(items)
	}
	// A cursor hands each worker the next item, so a slow slot does not pin the
	// others; a genuine failure stops the round early (the slots already applied
	// are not lost — the next round's positions come from the store itself).
	var cursor, failed atomic.Int64
	var errMu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for {
				i := int(cursor.Add(1)) - 1
				if i >= len(items) || failed.Load() != 0 {
					return
				}
				if err := e.applyItem(&items[i], report); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					failed.Store(1)
					return
				}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return false, firstErr
	}
	return true, nil
}

// applyItem lands one slot's share of a round: its records in seq order, then
// its new position to report. A quarantined slot is deliberately not reported,
// so a replica holding different bytes cannot stand in for an in-sync one.
func (e *Engine) applyItem(it *FetchItem, report func(it *FetchItem, from uint64)) error {
	dvg, err := applyFetchPayload(e.store, it.Slot, it.NextSeq, it.Payload)
	if len(dvg) > 0 {
		e.noteDivergence(it.Slot, dvg)
		// The quarantine used to be the END of the story — the slot silently
		// left the ISR forever. It is now the start of a repair: discard and
		// refetch whole (leaderTail = this round's batch end).
		e.rebuildDivergedCopy(it.Slot, it.NextSeq-1)
	} else {
		// A version-chain break is invisible to the byte-comparison above:
		// every frame matches its slot on the source, yet the local WAL
		// holds a frame whose version skipped a number that will never
		// arrive (it was written as a NEW frame during a handover demotion
		// lag — the dg8 slot-23 class, 1968 records stranded unreadable
		// behind one misfit while the home stayed self-consistent). Ask at
		// end-of-seq only: the fetch batch that reached the source's tail
		// left nothing for the strand to heal against.
		if e.store.SlotPresent(it.Slot) && it.NextSeq >= e.store.LastSeqOf(it.Slot) {
			if stalled := e.store.StalledAggs(it.Slot); len(stalled) > 0 {
				e.noteDivergence(it.Slot, []uint64{it.NextSeq - 1})
				e.logger.Warn("replication: slot's version chain is permanently broken on this copy",
					"slot", it.Slot, "stalled_aggs", len(stalled), "op", "chain_break")
				e.rebuildDivergedCopy(it.Slot, it.NextSeq-1)
			} else {
				e.healRebuiltCopy(it.Slot)
			}
		} else {
			e.healRebuiltCopy(it.Slot)
		}
	}
	if err != nil {
		return err
	}
	if report != nil && !e.isDiverged(it.Slot) {
		report(it, e.store.LastSeqOf(it.Slot)+1)
	}
	return nil
}

// noteDivergence latches a slot whose local log forked from its leader's at one
// or more seqs. The set is the fetch loop's quarantine list: the slot stops
// being reported as in-sync (so it cannot gate the watermark with wrong bytes)
// and every OTHER slot of the same multiplexed session keeps replicating. It is
// logged once per slot — the fence around the migration commit window is what
// keeps this from happening at all; reaching here means that invariant broke.
// rebuildDivergedCopy discards a forked replica copy so the fetch loop
// refetches it whole from the leader (the same move the migration gate makes
// on a diverged TARGET — see ensureTargetConsistent — applied to the steady
// replication path). leaderTail is the leader's log end as this round saw it:
// landing that seq is the rebuild's completion. Refusing to discard a slot
// THIS node leads is the safety line (a leader's WAL holds what it
// acknowledged — nobody can refetch a leader from below); a leader-role fork
// stays quarantined-and-silent, and the flip-side repair belongs to the
// migration machinery, not here.
func (e *Engine) rebuildDivergedCopy(slot int32, leaderTail uint64) {
	e.divMu.Lock()
	if _, busy := e.rebuildTarget[slot]; busy {
		e.divMu.Unlock()
		return // a refetch is already running for this fork
	}
	// A chain break copied from a POISONED source re-forms in the rebuilt
	// copy (the replay walks the same foreign frame): cap whole-history
	// refetches so a broken slot cannot livelock the repairer. Past the cap
	// the copy stays quarantined — it cannot gate the watermark, and the
	// log line tells the operator which slot needs a placement intervention.
	e.rebuildAttempts[slot]++
	if e.rebuildAttempts[slot] > 3 {
		e.divMu.Unlock()
		e.logger.Error("divergence repair: rebuild budget exhausted; the copy stays quarantined (the source likely carries the broken chain — move leadership off it or restore the replica)",
			"slot", slot, "attempts", e.rebuildAttempts[slot])
		return
	}
	e.divMu.Unlock()
	if e.Leads(slot) {
		return // guard rail, see the comment
	}
	if err := e.store.DropSlot(slot); err != nil {
		e.logger.Warn("divergence repair: could not discard the forked copy; it stays quarantined",
			"slot", slot, "err", err)
		return
	}
	e.divMu.Lock()
	e.rebuildTarget[slot] = leaderTail
	e.divMu.Unlock()
	e.logger.Warn("divergence repair: forked copy discarded, refetching the slot from seq 1",
		"slot", slot, "leader_tail", leaderTail)
}

// healRebuiltCopy lifts the quarantine once the refetched copy holds the
// leader's tail the discard was taken at. Reporting resumes with the NEXT
// round (applyItem re-checks isDiverged after this), and the copy re-enters
// the leader's ISR view as a byte-identical replica.
func (e *Engine) healRebuiltCopy(slot int32) {
	e.divMu.Lock()
	defer e.divMu.Unlock()
	tgt, ok := e.rebuildTarget[slot]
	if !ok {
		return
	}
	// LEO parity alone is NOT healed: the dg8 class rebuilds a byte-faithful
	// copy of a chain-broken source and reaches the tail while still unable
	// to read thousands of records behind one foreign frame. Lift the latch
	// only when the rebuilt copy is also chain-sound.
	if e.store.LastSeqOf(slot) >= tgt && len(e.store.StalledAggs(slot)) == 0 {
		delete(e.rebuildTarget, slot)
		delete(e.diverged, slot)
		e.logger.Info("divergence repair: rebuilt copy caught the leader's tail, quarantine lifted",
			"slot", slot, "tail", tgt)
	}
}

func (e *Engine) noteDivergence(slot int32, seqs []uint64) {
	e.divMu.Lock()
	_, had := e.diverged[slot]
	e.diverged[slot] = fmt.Sprintf("seqs %v", seqs)
	e.divMu.Unlock()
	if !had {
		e.logger.Warn("replica fetch: local log diverged from the leader at a seq; quarantining this slot and keeping the rest of the session (other slots unaffected)",
			"slot", slot, "seqs", seqs)
	}
}

// isDiverged reports whether a slot has been quarantined for a fork.
func (e *Engine) isDiverged(slot int32) bool {
	e.divMu.Lock()
	_, ok := e.diverged[slot]
	e.divMu.Unlock()
	return ok
}

// applyFetchPayload replays one slot's concatenated records; the batch ended
// at nextSeq, so it starts at nextSeq-recordCount(payload).
//
// A record whose seq the local log already holds with DIFFERENT bytes is a
// divergence: it is reported in the returned slice and SKIPPED, so the rest of
// the batch still lands and the caller can quarantine just this slot. A genuine
// failure (a malformed frame) is returned as an error. Returns (diverged, err).
func applyFetchPayload(store *storage.Store, slot int32, nextSeq uint64, payload []byte) ([]uint64, error) {
	seq := nextSeq - uint64(countRecords(payload))
	var diverged []uint64
	rest := payload
	for len(rest) > 0 {
		_, consumed, err := data.DecodeRecordMeta(rest)
		if err != nil {
			return diverged, err
		}
		if aerr := store.AppendFrameAtSeq(slot, seq, rest[:consumed]); aerr != nil {
			if errors.Is(aerr, storage.ErrSeqDivergence) {
				diverged = append(diverged, seq)
				rest = rest[consumed:]
				seq++
				continue
			}
			return diverged, aerr
		}
		rest = rest[consumed:]
		seq++
	}
	return diverged, nil
}

// countRecords walks a concatenated payload counting length prefixes.
func countRecords(payload []byte) int {
	n, off := 0, 0
	for off+4 <= len(payload) {
		l := int(binary.BigEndian.Uint32(payload[off : off+4]))
		if l < 30 || off+4+l > len(payload) {
			break
		}
		off += 4 + l
		n++
	}
	return n
}

// reportProgress posts follower LEOs in bulk to a leader's peer plane (one
// RPC per productive round, replacing the old per-slot reportLEO).
func (e *Engine) reportProgress(addr string, slots []int32, froms []uint64, stamp bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	e.diag.reportsSent.Add(1)
	e.diag.reportsSlots.Add(uint64(len(slots)))
	e.diag.lastReportNS.Store(time.Now().UnixNano())
	t0 := time.Now()
	_ = e.peerProgress(ctx, addr, e.self, slots, froms, stamp)
	// The follower's half of the acknowledgement hop: how long posting the new
	// positions takes to come back (see ackDiag.reportRTT).
	e.ack.reportRTT.observe(time.Since(t0))
}

// LeaderReplica reports, from this node's placement view and under a single
// table lock, who leads one slot and whether this node holds it: the leader id,
// the leader's client-plane (gRPC) address, and a local flag that is true when
// this node either LEADS or REPLICATES the slot (found is false when the table
// has no entry for it).
//
// It exists because callers repeatedly ask "where does this slot live and do I
// serve it myself?" (read proxying, redirect targets, ownership checks) and each
// of those questions only needs one slot. Going through TableSnapshot to answer
// them cloned every placement in the table — measured at 81.7% of a read node's
// CPU under a read-only load — for a lookup that is O(replicas).
//
// The local flag covers both roles deliberately: a replica serves reads from its
// own log (bounded by its durable LEO), so "do I hold this slot" is the question
// callers actually have, not "am I the leader".
func (e *Engine) LeaderReplica(slot int32) (leader, clientAddr string, local, found bool) {
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	p, ok := e.table.Slots[slot]
	if !ok {
		return "", "", false, false
	}
	leader = p.Leader
	clientAddr = e.table.Peers[leader].ClientAddr
	if leader == e.self {
		return leader, clientAddr, true, true
	}
	for _, r := range p.Replicas {
		if r == e.self {
			return leader, clientAddr, true, true // local replica (read admission is LocalRead's question)
		}
	}
	return leader, clientAddr, false, true
}

func (e *Engine) LocalRead(slot int32) (uptoSeq uint64, serveEmpty bool, redirect string, leader string) {
	bound, leader, _, redirect := e.readAdmission(slot)
	switch {
	case redirect != "":
		return 0, false, redirect, leader // MOVED redirect
	case bound == nilBounded:
		return 0, false, "", leader // leader path / unassigned: unbounded local
	case bound == 0:
		return 0, true, "", leader // synced but nothing confirmed: empty
	default:
		return bound, false, "", leader // synced replica: ceiling = the synced watermark
	}
}

// nilBounded marks the leader's "no ceiling" in readAdmission's bound. A
// synced watermark can itself be 0 (serveEmpty); the leader's 0 means
// unbounded, so the two are distinguished by which node leads.
const nilBounded = ^uint64(0)

// readAdmission resolves one slot's placement and replica-sync proof into
// (bound, leader, leaderClientAddr, redirect) with bound=nilBounded when the
// leader path applies. Unassigned slots answer locally (0 = unbounded) as
// before — there is no leader to redirect to.
func (e *Engine) readAdmission(slot int32) (bound uint64, leader string, leaderAddr string, redirect string) {
	e.tableMu.RLock()
	p, found := e.table.Slots[slot]
	if !found {
		e.tableMu.RUnlock()
		return nilBounded, "", "", "" // unassigned: local empty answer, unchanged contract
	}
	leader = p.Leader
	leaderAddr = e.table.Peers[leader].ClientAddr
	epoch, held := p.Epoch, replicaListHas(p.Replicas, e.self)
	e.tableMu.RUnlock()
	if leader == e.self {
		if e.slotPending(slot) {
			// Fenced leader (owner.go): the local copy answers empty until
			// an operator adopts it or discards it — reading unproven
			// events while their seq space is disputed is half the hazard
			// the fence kills.
			return 0, leader, leaderAddr, ""
		}
		return nilBounded, leader, leaderAddr, "" // the leader owns its log
	}
	if !held {
		return 0, leader, leaderAddr, leaderAddr // a non-holder redirects, as ever
	}
	e.hwMu.Lock()
	s, has := e.hwSeen[slot]
	e.hwMu.Unlock()
	// The PROOF is the sample itself: an unsampled replica (no ledger round
	// has answered for it yet — a fresh boot, a new seat) is unproven and
	// redirects even when its LEO is 0, because "0 records visible" and
	// "nothing vouched yet" look identical in the log but not in safety:
	// a redirect cannot answer short. A zero-HW SAMPLE is a different fact —
	// the leader vouched that nothing is confirmed — and admits a local read
	// with an empty visibility answer (serveEmpty).
	// The ceiling is the SAMPLE, never the replica's own LEO: the records
	// above the sampled watermark have not been acknowledged yet (the ack
	// gate is the in-sync min, and the sample is the leader's vouch for it),
	// and serving the replica's unacked tail would expose records a new
	// leader could drop — the dirty-read class the HW rule exists to kill.
	// The lag between an ack and its echo at the replica (the short-read
	// window found on the live cluster) is closed on the TIMELINESS axis,
	// not the bound axis: the served∪changed ledger plus the hwPing early
	// answer keep the sample within one round-trip of the acked watermark.
	// The ceiling is the SAMPLE, never the replica's own LEO: records above
	// the sampled watermark are not (yet) vouched confirmed; serving them
	// locally would expose a tail a new leader could drop (the dirty-read
	// class the ≤HW rule kills), and capping BELOW the replica's head is
	// sound precisely because of the ledger VOUCH discipline (see the
	// HandleMFetchCtx hold): a seat that does not live in the ack promise —
	// caught-up staleness, a building or re-joining copy — receives the
	// leader's whole durable log as its sample, an equality it cannot reach
	// by partial catch-up, so leo >= hw on such a seat means "fully caught
	// up" and its local reads answer complete. A fresh echo of the acked
	// watermark then lands via the served∪changed ledger and the hwPing
	// early answer (the tight echo); what trails an ack on a vouched seat
	// is only the record set the ack itself never covered.
	if has && s.epoch == epoch && e.store.LastSeqOf(slot) >= s.hw {
		return s.hw, leader, leaderAddr, "" // synced replica: local, ceiling = the synced HW
	}
	return 0, leader, leaderAddr, leaderAddr // not proven in sync: MOVED redirect
}

// NoteHwLedger seeds one slot's read-admission bound directly, at this
// node's current placement epoch for the slot — the follower-side test seam
// for the fetch round's ledger (see NoteReplicaProgress, the leader-side
// twin). Production callers go through applyHwLedger.
func (e *Engine) NoteHwLedger(slot int32, hw uint64) {
	e.tableMu.RLock()
	p, ok := e.table.Slots[slot]
	var epoch int64
	if ok {
		epoch = p.Epoch
	}
	e.tableMu.RUnlock()
	if !ok {
		return
	}
	e.hwMu.Lock()
	if e.hwSeen == nil {
		e.hwSeen = map[int32]epochHW{}
	}
	e.hwSeen[slot] = epochHW{epoch: epoch, hw: hw}
	e.hwMu.Unlock()
}

// applyHwLedger seeds this node's read-admission bound from one HW ledger --
// either a fetch round's (MFetchResponse.HwSlots/HwValues) or the echo of a
// progress report (ReplicaProgressResponse, the zero-lag channel: the report
// that completes an append's ack carries the watermark that append needs).
// Samples are keyed by the placement EPOCH of the slot's current leader as
// this node's table sees it: the round answered by `leader` is only
// authoritative for the placement epoch that leader leads under, and an epoch
// bump (failover, migration commit) invalidates every sample taken at the old
// leader -- the replica re-proves its sync against the new one before serving
// local reads again. A slot the table says this node does not replicate, or
// that a different leader answers for, is skipped (no sample to trust).
func (e *Engine) applyHwLedger(leader string, slots []int32, values []uint64) {
	if len(slots) == 0 {
		return
	}
	type sample struct {
		slot  int32
		epoch int64
		hw    uint64
	}
	buf := make([]sample, 0, len(slots))
	e.tableMu.RLock()
	for i, slot := range slots {
		if i >= len(values) {
			break
		}
		p, ok := e.table.Slots[slot]
		if !ok || p.Leader != leader || !replicaListHas(p.Replicas, e.self) {
			continue
		}
		buf = append(buf, sample{slot, p.Epoch, values[i]})
	}
	e.tableMu.RUnlock()
	e.hwMu.Lock()
	for _, s := range buf {
		e.hwSeen[s.slot] = epochHW{epoch: s.epoch, hw: s.hw}
	}
	e.hwMu.Unlock()
}

// ---- Controller: membership + failover ---------------------------------------

// livenessFailThreshold is the number of consecutive failed health probes
// before the controller fails a peer's slots over (~3s of outage given the
// 1s reconcile tick and 2s probe timeout; well beyond a node's boot time).
const livenessFailThreshold = 3

// RunController reconciles cluster state; only the Raft leader executes it.
// planSettleMaxRounds bounds how long the layout waits for the data-plane pool
// to stop changing. Two deferred rounds is one cold start's worth of
// registration and one straggler's worth of patience; a membership that keeps
// moving gets laid out anyway rather than not at all.
const planSettleMaxRounds = 2

// poolSettled reports whether the layout may be planned under this pool in this
// round, carrying the settle state across rounds.
//
// The data-plane pool is assembled by REGISTRATION, and a cold start registers
// its members over a fraction of a second — so a round that plans the moment
// the FIRST member announces plans for a cluster that has not finished showing
// up. That is what a classic both-shape cold start looked like before this
// gate: the first plan ran under one member (factor 1 — every slot on whichever
// node announced first), the second member's arrival then re-planned all 1680
// replica sets at factor 2, and the cluster spent its first seconds
// single-copy with a full re-seat right after. The plan now waits one round for
// the pool to repeat itself; the cost is one reconcile interval on a cold
// start, the gain is a first layout that is already the intended shape.
//
// It defers only while the pool differs from the one the last plan ran under
// (planned): a steady cluster, or one whose plan already matches its members,
// never waits. planSettleMaxRounds bounds the wait so a pool that keeps moving
// is still planned under.
func (e *Engine) poolSettled(pool, planned []string) bool {
	if sameMembers(pool, planned) {
		e.planPoolSeen, e.planPoolWaited = nil, 0
		return true
	}
	if sameMembers(pool, e.planPoolSeen) {
		// Unchanged since the previous round: this is the pool to lay out.
		e.planPoolSeen, e.planPoolWaited = nil, 0
		return true
	}
	if e.planPoolWaited >= planSettleMaxRounds {
		e.loggerf("layout: the data-plane pool (%v) is still changing after %d deferred rounds; planning under it anyway",
			pool, e.planPoolWaited)
		e.planPoolSeen, e.planPoolWaited = nil, 0
		return true
	}
	e.planPoolSeen = append([]string(nil), pool...)
	e.planPoolWaited++
	e.loggerf("layout: %d data-plane member(s) (%v) so far; waiting one round for the pool to settle before planning under it",
		len(pool), pool)
	return false
}

func (e *Engine) RunController(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if !e.node.IsLeader() {
			continue
		}
		// A drain marked while the rebalancer slept (interval 0) has no
		// loop to run its handover rounds; this leader-gated tick is the
		// wake point that gives one — including after a controller change,
		// where the new leader never saw the DELETE at all.
		e.wakeDrainRunner()
		// 1) sync directory from raft membership: every voter (including
		// ourselves and bootstrapped-but-unregistered peers) gets a join
		// command. PeerAddr is authoritative from the raft configuration;
		// admin/client addresses come from the peer's registration
		// announcement (OpRegister patches them in whenever they land).
		// The inverse direction is reconciled too: a directory entry whose
		// id is no longer a raft voter is a member an operator removed —
		// leave_node deletes it. Liveness NEVER deletes (see the sweep):
		// delete-then-rejoin dropped the announced addresses and re-seeded
		// an empty shell every cycle, which made a failed node flicker in
		// the console and re-planned the whole table every few seconds.
		tbl := e.TableSnapshot()
		raftAddrs := e.node.PeerAddrs()
		for id, peerAddr := range raftAddrs {
			canonical := NormalizeAddr(peerAddr)
			if p, ok := tbl.Peers[id]; ok {
				// heal a moved peer address without waiting for a full rejoin
				if HostPort(p.PeerAddr) != peerAddr {
					p.PeerAddr = canonical
					e.submit(&Command{Op: OpJoinNode, Peer: &p})
				}
				continue
			}
			p := Peer{ID: id, PeerAddr: canonical}
			if id == e.self {
				p.AdminAddr, p.ClientAddr = e.node.Cfg().AdminAddr, e.node.Cfg().ClientAddr
				p.Role = e.node.Cfg().Role
			}
			e.submit(&Command{Op: OpJoinNode, Peer: &p})
		}
		for id := range tbl.Peers {
			if _, ok := raftAddrs[id]; ok {
				continue
			}
			if p, in := tbl.Peers[id]; in && p.Role == RoleLearner {
				// Learners are admitted through replicated commands and
				// live in the directory only — the raft configuration never
				// names them, and their absence from it is not a leave.
				continue
			}
			e.loggerf("peer %s is no longer a raft voter; removing it from the directory", id)
			e.submit(&Command{Op: OpLeaveNode, NodeID: id})
		}
		// 2) plan: first re-derive the table's replica factor from the
		// replicated policy tier and the data-plane pool — ReplicaCountForPolicy
		// reads Table.Policy (the tier the admin endpoint sets, seeded at
		// bootstrap by the flag), so low/medium hold the factor steady against
		// membership and only high follows the pool itself: every data node
		// holds every slot. A grown cluster on high carries one more full copy
		// of the data on every new member. Then full-replan only
		// when nothing is assigned; otherwise converge the layout — fill gaps
		// left by member joins and re-derive replica sets left stale by a
		// factor change (OpReplanSlots' own function, run on a clone: a replan
		// is submitted only when it would actually change the table, so the
		// steady-state table never costs a Raft entry per round).
		tbl = e.TableSnapshot()
		if len(tbl.Peers) == 0 {
			continue
		}
		// The cluster generation goes into the replicated state before
		// anything plans (Table.Topology): the id this node bootstrapped its
		// consensus dir with becomes the value every node — and every data
		// node's owner fence — reads from then on, so a later leadership
		// move cannot re-fence the data plane. First writer wins at apply, so
		// only a table without a generation submits at all.
		if tbl.Topology == "" {
			if id := e.topologyID(); id != "" {
				e.loggerf("recording the cluster generation %s in the replicated state", id)
				if err := e.submit(&Command{Op: OpSetTopology, Topology: id}); err == nil {
					tbl = e.TableSnapshot()
				}
			}
		}
		// Seed first: the -replica-policy flag only touches a brand-new
		// cluster — an empty table has never been planned and no OpSetPolicy
		// can have reached it, so this is the one and only moment the startup
		// value is applied. Once the plan lands the table has slots and this
		// branch never runs again: every later policy change comes from the
		// admin endpoint alone, and a restart of an existing node keeps the
		// policy the replicated table holds.
		if len(tbl.Slots) == 0 && e.bootPolicy != "" && e.bootPolicy != tbl.Policy {
			e.loggerf("seeding the replica policy %q from the startup flag", e.bootPolicy)
			if err := e.submit(&Command{Op: OpSetPolicy, Policy: string(e.bootPolicy)}); err == nil {
				tbl = e.TableSnapshot()
			}
		}
		// The layout waits for the pool to settle: planning under a pool that
		// is still being assembled is what made a cold start lay out its first
		// member alone and then re-seat everything (see poolSettled). The
		// factor is written under the same gate, so the first plan never runs
		// at a factor derived from a half-arrived pool.
		pool := tbl.HostingIDs()
		if e.poolSettled(pool, tbl.PlanMembers) {
			if want := ReplicaCountForPolicy(tbl.Policy, len(pool)); tbl.Replicas != want {
				e.loggerf("replica factor: table has %d, policy %q with %d data-plane members wants %d — changing it",
					tbl.Replicas, tbl.Policy, len(pool), want)
				if err := e.submit(&Command{Op: OpConfig, Replicas: want}); err == nil {
					// Re-read: the probe below plans with the factor this
					// round just wrote, instead of the one it replaced.
					tbl = e.TableSnapshot()
				}
			}
			if len(tbl.Slots) == 0 {
				// No data-plane member yet (a pure control plane waiting for
				// its first learner): submitting the plan would fail apply and
				// re-commit every second — the idle-creep pattern again.
				// Clients get the explicit "no data nodes" answer at the write
				// path; the first hosting registration arms the plan.
				if len(pool) > 0 {
					e.submit(&Command{Op: OpPlanSlots})
				}
			} else if probe := tbl.Clone(); probe.applyReplanSlots() {
				e.submit(&Command{Op: OpReplanSlots})
			}
		}
		// 3) liveness sweep: probe peers over the peer plane; a peer that
		// misses several consecutive probes is MARKED DOWN (leadership
		// moves to live replicas; the directory entry, its addresses and
		// its replica-set membership survive). A booting node is never
		// evicted (threshold), and a node that answers probes again is
		// marked back up — its copy is still a replica, so the ordinary
		// fetch loop catches it up and the rebalancer hands its ring slots
		// back. Commands are submitted only on a REAL state change: the
		// steady-state table of a dead node is one mark_down, not a Raft
		// entry per round.
		for id, p := range tbl.Peers {
			if id == e.self || p.PeerAddr == "" {
				// peers without a peer address are not probed: membership
				// is owned by the raft configuration, not by liveness.
				continue
			}
			e.failMu.Lock()
			if !e.alive(p.PeerAddr) {
				e.failStreak[id]++
				// A probe that fails because NOTHING IS LISTENING is not a slow
				// node: the process is gone, and the deeper question the strike
				// threshold exists for (is it slow or is it dead?) is already
				// answered. Mark it down on this round instead of three — every
				// round it stays in the table as an up member, its frozen LEO
				// pins the watermark of every slot it replicated and its slots
				// stay routed to a dead leader, which a batch workload sees as
				// seconds of 0 msg/s.
				refused := dialRefused(p.PeerAddr)
				markDown := (e.failStreak[id] >= livenessFailThreshold || refused) && !p.Down
				if markDown {
					e.loggerf("peer %s unreachable (%d consecutive probes, refused connection: %t), marking it down",
						id, e.failStreak[id], refused)
				}
				e.failMu.Unlock()
				if markDown {
					// The pick asks the replicas for their LEOs (peer round
					// trips), so it runs WITHOUT the liveness lock: the sweep
					// must not stall behind one failover.
					e.failoverPeer(id)
				}
				continue
			}
			e.failStreak[id] = 0
			if p.Down {
				e.loggerf("peer %s answers probes again, marking it up", id)
				e.submit(&Command{Op: OpMarkUp, NodeID: id})
			}
			e.failMu.Unlock()
		}
		// 4) abandoned migrations: a controller that dies mid-hand-over leaves
		// the slot's replicated state in migrating_out with nobody to clear
		// it. Nothing else revisits a non-stable slot (replan_slots skips it,
		// the rebalancer yields to it), so the layout would stay frozen for
		// good. Return the ones this process is not driving to stable.
		e.reconcileOrphanMigrations()
	}
}

// alive probes a peer over the peer plane (PeerService.Ping).
func (e *Engine) alive(addr string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), peerPingTimeout)
	defer cancel()
	return e.peerPing(ctx, addr) == nil
}

// dialRefusedTimeout bounds the fast "is anything listening" dial. A refused
// connection comes back immediately; only a black-holed address (a firewall
// dropping SYN) waits this long.
const dialRefusedTimeout = 300 * time.Millisecond

// dialRefused reports whether the peer's port actively REFUSES connections —
// ECONNREFUSED, i.e. nothing is listening there. That is the unambiguous "the
// process is gone" signal the liveness sweep can act on within one round: the
// app-level probe (and its strike threshold) exists to tell a dead node from a
// slow one, and a refused connect answers that question outright. A timeout, a
// reset from a half-open socket or any other error is NOT this signal.
func dialRefused(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, dialRefusedTimeout)
	if err == nil {
		c.Close()
		return false
	}
	return errors.Is(err, syscall.ECONNREFUSED)
}

// submit commits a command through Raft. Every command submission funnels
// through here, so this is also the single place that enforces "controller
// commands run on the controller": a follower refuses with a
// *NotControllerError (which names the controller) instead of forwarding.
func (e *Engine) submit(c *Command) error {
	if err := e.controllerGuard(); err != nil {
		return err
	}
	_, err := e.node.Apply(c.Encode())
	return err
}

func (e *Engine) loggerf(format string, args ...any) {
	if e.logger != nil {
		e.logger.Warn(fmt.Sprintf(format, args...))
	}
}

// ---- Start / Stop -------------------------------------------------------------

// Start launches background loops (replica fetch + controller + leader
// rebalance; the rebalancer exits at once when the knob is turned off).
func (e *Engine) Start(ctx context.Context) {
	// The feed hub runs everywhere (on a learner it can never acquire a
	// subscriber, since the serving path redirects anything that is not the
	// controller), and shares the consensus heartbeat interval.
	go e.layoutFeed().run(ctx)
	go e.replicaLoop(ctx)
	go e.RunController(ctx)
	go e.RunRebalancer(ctx)
	go e.runStorageSampler(ctx)
	go e.runOwnerFence(ctx)
}

// ---- cluster storage gauge -----------------------------------------------------

// storageGauge is one sample of the cluster's stored volume and stored count:
// the sum of the on-disk bytes AND the seq ledger of every slot's LEADER copy.
// Counting leaders (not "some copy somewhere") is what keeps both numbers
// meaningful across a replica-set change or a migration: the writes landed on
// the leader, and the other copies are the same data one replication round
// behind — so summing every copy would multiply both numbers by the replica
// factor. Both folds share one fan-out and one completeness verdict.
type storageGauge struct {
	bytes    uint64
	count    uint64 // sum of leader copies' seq ledgers (存储数量)
	complete bool   // every slot leader answered; false makes both sums lower bounds
	at       time.Time
}

// storageSampleInterval is how stale the cluster storage gauge may get before
// a reader of /admin/cluster/status triggers a background refresh. The sum
// needs one peer round trip per other member, so it is never computed inline:
// the status answers from the last sample, and the read that found the sample
// stale schedules the next one. A console polling the page therefore keeps the
// gauge fresh; with no reader at all, an idle node samples nothing.
const storageSampleInterval = 2 * time.Second

// runStorageSampler keeps the gauge fresh ON DEMAND: only the first sample (at
// boot) and reader-triggered kicks refresh it. A status read older than
// storageSampleInterval schedules the next sample (see ClusterStorageStats),
// so the gauge converges to the console's polling cadence instead of adding a
// peer-plane round trip per other member every 2 seconds forever — which is
// what an idle cluster was paying for a number nobody was looking at.
func (e *Engine) runStorageSampler(ctx context.Context) {
	e.refreshStorage() // first sample immediately, so status is not empty for a tick
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.storageKick:
			e.refreshStorage()
		}
	}
}

// kickStorage nudges the sampler to recompute the gauge; the signal coalesces
// (cap 1) because refreshStorage always samples the current table.
func (e *Engine) kickStorage() {
	select {
	case e.storageKick <- struct{}{}:
	default:
	}
}

// refreshStorage samples the on-disk size of every slot's leader copy: the slots
// this node leads are read locally, every other leader is asked over the peer
// plane in parallel under storageQueryTimeout. A leader that does not answer
// leaves the sample incomplete (the sum is then a lower bound) rather than
// failing the refresh — a dead node must not hide the size of everything else.
func (e *Engine) refreshStorage() {
	tbl := e.TableSnapshot()
	byLeader := map[string][]int32{}
	var total, count uint64
	for s, p := range tbl.Slots {
		switch {
		case p.Leader == "":
			continue
		case p.Leader == e.self:
			total += uint64(e.SlotSize(s))
			count += e.HandleLEO(s)
		default:
			byLeader[p.Leader] = append(byLeader[p.Leader], s)
		}
	}
	complete := true
	if len(byLeader) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), storageQueryTimeout)
		defer cancel()
		type answer struct {
			slots []int32
			bytes []uint64
			seqs  []uint64
		}
		answers := make(chan answer, len(byLeader))
		var wg sync.WaitGroup
		for id, slots := range byLeader {
			addr := e.peerAddr(id)
			if addr == "" {
				complete = false
				continue
			}
			wg.Add(1)
			go func(addr string, slots []int32) {
				defer wg.Done()
				gotB, gotS, err := e.peerSlotSizes(ctx, addr, slots)
				if err != nil {
					return
				}
				answers <- answer{slots, gotB, gotS}
			}(addr, slots)
		}
		wg.Wait()
		close(answers)
		n := 0
		for a := range answers {
			n++
			for _, b := range a.bytes {
				total += b
			}
			for _, sq := range a.seqs {
				count += sq
			}
		}
		// Every leader that was asked and did not answer makes both sums partial.
		if n < len(byLeader) {
			complete = false
		}
	}
	e.storageMu.Lock()
	e.gauge = storageGauge{bytes: total, count: count, complete: complete, at: time.Now()}
	e.storageMu.Unlock()
}

// ClusterStorageStats reports the last storage sample: the bytes sum and the
// seq-ledger sum over every slot's leader copy, and whether every leader
// answered. Zero/false until the first sample lands (the sampler runs one
// immediately on start). A read that finds the sample older than the staleness
// window kicks the background sampler, so the gauge converges on whoever is
// reading it and stays untouched while nobody is.
func (e *Engine) ClusterStorageStats() (bytes, count uint64, complete bool) {
	e.storageMu.Lock()
	bytes, count, complete, at := e.gauge.bytes, e.gauge.count, e.gauge.complete, e.gauge.at
	stale := at.IsZero() || time.Since(at) >= storageSampleInterval
	e.storageMu.Unlock()
	if stale {
		e.kickStorage()
	}
	return bytes, count, complete && !at.IsZero()
}

// LeaderSeqTotal is THIS node's 存储数量: the sum of the seq ledgers of the
// copies it currently leads, straight from its own view of the replicated
// table and its own store. The fold is a plain scan (no table clone) the way
// SyncedLeaderSlots reads it — a poll target, so O(slot_count) per read is the
// budget. A slot this node leads always has its copy loaded (the leader lands
// writes on it), so no cold-copy undercount exists here.
func (e *Engine) LeaderSeqTotal() uint64 {
	var total uint64
	e.tableMu.RLock()
	for s, p := range e.table.Slots {
		if p == nil || p.Leader != e.self {
			continue
		}
		total += e.HandleLEO(s)
	}
	e.tableMu.RUnlock()
	return total
}

// LeaderSlotBytes narrows a per-slot byte gauge to the copies this node LEADS:
// a slot it merely replicates answers 0. The array is the byte twin of
// storage_count — summed it is this node's leader-copy volume, the same
// one-leader rule the cluster storage gauge follows, so a per-node total and
// its seq total always talk about the same copies. Same table lock and O(slot
// count) scan as LeaderSeqTotal.
func (e *Engine) LeaderSlotBytes(bytes []int64) []int64 {
	out := make([]int64, len(bytes))
	e.tableMu.RLock()
	for s, p := range e.table.Slots {
		if p == nil || p.Leader != e.self {
			continue
		}
		if s < 0 || int(s) >= len(bytes) {
			continue
		}
		out[s] = bytes[s]
	}
	e.tableMu.RUnlock()
	return out
}

// SlotSize is a slot's local on-disk footprint (see storage.Store.SlotDiskBytes):
// a loaded copy answers from its own accounting, a cold one from its directory.
func (e *Engine) SlotSize(slot int32) int64 {
	if e.store == nil {
		return 0
	}
	return e.store.SlotDiskBytes(slot)
}
