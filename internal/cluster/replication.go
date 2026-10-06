package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"pushupes/internal/data"
	pushupesv1 "pushupes/internal/grpcapi/pushupes/v1"
	"pushupes/internal/storage"

	"github.com/sirupsen/logrus"
)

// Engine is the application-level cluster state: it owns the slot table
// (replicated through Raft via the Applier interface), routes client appends
// to slot leaders, runs the follower fetch loop, and tracks ISR/high
// watermark per slot. Event data never enters Raft — it is replicated by
// a dedicated seq-based fetch protocol.
type Engine struct {
	node    *Node
	store   *storage.Store
	table   *Table
	tableMu sync.RWMutex
	logger  *logrus.Entry
	peers   *peerClient // cached PeerService connections, keyed by peer addr
	self    string

	// replication bookkeeping per slot, only meaningful on slot leaders
	replMu sync.Mutex
	repl   map[int32]*slotRepl

	// rotation cursor for the mfetch payload budget
	fetchRot atomic.Uint64

	// fetch round buffers (sync.Pool): a session round allocates its
	// rotation order and parked set fresh every time; a hot leader
	// serves thousands of rounds per second and these were the
	// dominant malloc traffic in the profile.
	orderPool        sync.Pool // []int, cap >= followed slots
	parkedPool       sync.Pool // []parkedEntry
	progressPool     sync.Pool // *progressBatch
	scanPool         sync.Pool // *fetchScan (per-position "has data" + wake handle)
	fetchScratchPool sync.Pool // *fetchScratch (per-round positions + report)

	// ownership bitmap: which slots this node leads, rebuilt only when
	// the Raft table version advances. Per-item map lookups under the
	// fetch hot loop (~1.4k followed slots per round) were a top CPU
	// contributor on the write path; a word-test per slot is free.
	ownMu sync.Mutex
	own   atomic.Pointer[ownership]
	// tableGen bumps with every applied table change (holds tableMu).
	tableGen uint64

	// migration forwarding: slot -> target addr while migrating_out
	fwdMu sync.RWMutex
	fwd   map[int32]string

	// when this leader first saw a slot in migrating_out, so a target that
	// never catches up cannot keep the best-effort mirror alive forever
	// (see migrationForwardExpired). Cleared as soon as the slot is no
	// longer migrating_out.
	migFwdMu    sync.Mutex
	migFwdSince map[int32]time.Time

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

	// slots this node led as of the previous table walk, so syncMigrationState
	// can tell a leadership GAIN (drop the previous term's stale follower
	// positions; see advanceHW) from a steady state.
	ledMu   sync.Mutex
	ledPrev map[int32]bool

	// one replication session per slot leader (multiplexed long-poll fetch)
	sessMu   sync.Mutex
	sessions map[string]*fetchSession
	// sessKick wakes replicaLoop to reconcile sessions at once when the table
	// changes (a leader move makes a follower follow a new leader; waiting for
	// the 1s ticker left the new leader's watermark without a report for up to
	// a second, which stalled every acknowledged append to it).
	sessKick chan struct{}

	// controller liveness: consecutive health-probe failures per peer.
	// A single failure is not enough — a peer still booting (HTTP not yet
	// listening) would otherwise be failed over instantly on join.
	failMu     sync.Mutex
	failStreak map[string]int

	// unreach is the controller's witness table for the unreachable reports
	// observers send (suspect -> reporter -> when). See recordUnreachable.
	unreachMu sync.Mutex
	unreach   map[string]map[string]time.Time

	// leader rebalance (balance.go): how often the controller re-checks the
	// ring layout and how many leader hand-overs one round may execute.
	// Set from the -rebalance-interval / -rebalance-batch flags.
	rebalanceInterval time.Duration
	rebalanceBatch    int

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
	fetchBaseInterval    = 100 * time.Millisecond
	fetchMaxBackoff      = 2 * time.Second
	fetchWait            = 2 * time.Second      // follower-requested long-poll budget
	fetchSlack           = 1 * time.Second      // client-side timeout margin
	fetchMaxWait         = 5 * time.Second      // leader-side cap on a requested wait
	fetchBurstSettle     = 2 * time.Millisecond // coalescing window after one waiter wakes
	fetchSweepInterval   = 2 * time.Second      // stamp all positions at least this often
	isrStaleAfter        = 10 * time.Second     // must exceed fetchWait+fetchMaxBackoff
	defaultHWWaitTimeout = 10 * time.Second     // the write-ack watermark deadline

	// maxPayloadBytes caps one long-poll response: during a backlog the
	// session streams item by item across rounds instead of building a
	// gigabyte JSON blob of every slot at once.
	maxPayloadBytes = 32 << 20
)

// NewEngine wires the cluster to the local storage. The Raft node may be
// attached later with SetNode (they reference each other).
func NewEngine(node *Node, store *storage.Store, self string, logger *logrus.Entry) *Engine {
	e := &Engine{
		node:  node,
		store: store,
		self:  self,
		// The table starts on the replica count of a single-voter cluster; the
		// controller re-derives it from the member count on its first round
		// (ReplicaCountForMembers), so there is no startup knob to get wrong.
		table:  NewTable(store.SlotCount, ReplicaCountForMembers(1)),
		logger: logger,
		// One HTTP/2 connection per leader carries every multiplexed
		// long-poll fetch round plus progress/replication/leo probes —
		// gRPC multiplexes them over the single cached conn (the old HTTP
		// transport needed a large idle-conn pool to avoid re-dial churn).
		peers:       newPeerClient(),
		repl:        map[int32]*slotRepl{},
		fwd:         map[int32]string{},
		sessions:    map[string]*fetchSession{},
		sessKick:    make(chan struct{}, 1),
		failStreak:  map[string]int{},
		migFwdSince: map[int32]time.Time{},
		ledPrev:     map[int32]bool{},
		fenceSet:    map[int32]bool{},
		diverged:    map[int32]string{},
		migrating:   map[int32]struct{}{},
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
		// and -rebalance-batch (an interval or batch of 0 turns it off).
		rebalanceInterval: DefaultRebalanceInterval,
		rebalanceBatch:    DefaultRebalanceBatch,
	}
	e.orderPool.New = func() any { return make([]int, 0, 64) }
	e.parkedPool.New = func() any { return make([]parkedEntry, 0, 64) }
	e.progressPool.New = func() any { return &progressBatch{} }
	e.scanPool.New = func() any { return &fetchScan{} }
	e.fetchScratchPool.New = func() any { return &fetchScratch{} }
	e.fences = make([]slotFence, store.SlotCount)
	if e.logger == nil {
		l := logrus.New()
		l.SetLevel(logrus.WarnLevel)
		e.logger = l.WithField("component", "cluster")
	}
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

// TableSnapshot returns a copy of the current table (for the admin API).
func (e *Engine) TableSnapshot() *Table {
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	return e.table.Clone()
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

// syncMigrationState refreshes the write-forwarding map from the table and
// drives the post-migration cleanup countdown (localdrop.go).
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
	fwd := make(map[int32]string, 4)
	member := make(map[int32]bool, len(e.table.Slots))
	fenceOK := make(map[int32]bool, 4) // slots a held fence is still valid for
	var surplus []int32                // left the replica set while holding a copy: arm a countdown
	for s, p := range e.table.Slots {
		if fencedSet[s] {
			after[s] = leaderEpoch{p.Leader, p.Epoch}
		}
		if p.State == SlotMigratingOut && p.MigratingTo != "" {
			fwd[s] = p.MigratingTo
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
			e.holdFenceUntilTargetLeader(s, pl.leader, pl.epoch)
			continue
		}
		e.releaseSlotFence(s)
	}

	e.setReplicaMembership(member)
	e.fwdMu.Lock()
	e.fwd = fwd
	e.fwdMu.Unlock()

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
			sr = &slotRepl{}
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

	// Deadline bookkeeping for the best-effort forward: stamp the moment the
	// leader first sees a slot in migrating_out, so a target that never
	// catches up cannot keep the mirror alive forever (see
	// migrationForwardExpired). Anything no longer migrating is dropped.
	now := time.Now()
	e.migFwdMu.Lock()
	for s := range e.migFwdSince {
		if _, ok := fwd[s]; !ok {
			delete(e.migFwdSince, s)
		}
	}
	for s := range fwd {
		if _, ok := e.migFwdSince[s]; !ok {
			e.migFwdSince[s] = now
		}
	}
	e.migFwdMu.Unlock()

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
func (e *Engine) SubmitAppend(ctx context.Context, rec *data.EventRecord) (*data.AppendResponse, error) {
	slot := e.SlotOf(rec.AggregateID)
	leave := e.enterWriteFence(slot)
	defer leave()
	return e.submitAppendLocked(ctx, rec, slot)
}

// submitAppendLocked is the routing/execution half of SubmitAppend, run while
// the slot's write-fence registration is held (so the fence drain accounts for
// this append).
func (e *Engine) submitAppendLocked(ctx context.Context, rec *data.EventRecord, slot int32) (*data.AppendResponse, error) {

	// Copy the routing fields under the lock: the placement is mutated in
	// place by the Raft apply loop (OpSlotState/OpLeaderMove), so holding the
	// *Placement past RUnlock raced a concurrent rollback — it read
	// MigratingTo as "" mid-write and the forward then dialed an empty peer
	// address.
	e.tableMu.RLock()
	pp, ok := e.table.Slots[slot]
	var pState SlotState
	var pMigratingTo, pLeader string
	if ok {
		pState, pMigratingTo, pLeader = pp.State, pp.MigratingTo, pp.Leader
	}
	e.tableMu.RUnlock()
	if !ok {
		// single-node / unassigned: serve locally
		return e.localAppend(slot, rec)
	}

	switch {
	case pState == SlotMigratingOut && pMigratingTo != "" && pLeader == e.self:
		// step 4 of migration: source stays leader and keeps serving writes,
		// and every accepted record is also pushed to the target at the same
		// seq so the two copies converge before the commit step.
		//
		// The push is BEST EFFORT. The target is a replica of this slot, so
		// the ordinary fetch loop already carries the same records to it and
		// the migration only commits after the target has caught up
		// (awaitCaughtUp). A push that fails because the target is not ready
		// yet — no contiguous prefix locally, a snapshot still importing, no
		// reachable peer address — therefore must NOT fail the client's
		// write (it is already durable in the leader's WAL) and must NOT roll
		// the whole migration back. Failing it did both, and turned every
		// in-flight append in the migration window into a 10s
		// watermark timeout plus a duplicated failure on the client's retry.
		resp, err := e.localAppend(slot, rec)
		if err != nil {
			return nil, err
		}
		switch {
		case resp.Status == data.StatusSuccess && resp.Seq > 0:
			e.forwardBestEffort(ctx, slot, pMigratingTo, resp.Seq, rec)
		case resp.Status == data.StatusExists && resp.Seq > 0 && resp.Record != nil:
			// a retried command: make sure the target converged too
			e.forwardBestEffort(ctx, slot, pMigratingTo, resp.Seq, resp.Record)
		}
		return resp, nil
	case pState == SlotMigratingOut:
		// someone else leads a migrating slot: the leader handles forwarding
		addr := e.clientAddr(pLeader)
		return nil, &RedirectError{Kind: data.ErrIDMigrating, Slot: slot, Node: pLeader, Addr: addr}
	case pLeader == e.self:
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
// target MOVED/ASK redirects and cross-node read proxies point at.
func (e *Engine) clientAddr(nodeID string) string {
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	return e.table.Peers[nodeID].ClientAddr
}

// localAppend runs the business rules against the local WAL, then waits for
// the slot's high watermark to cover the record before reporting success:
// an append acknowledged without that wait could vanish if the leader died
// before its replicas pulled the record.
func (e *Engine) localAppend(slot int32, rec *data.EventRecord) (*data.AppendResponse, error) {
	resp, err := e.landLocalAppend(slot, rec)
	if err != nil || resp.Status != data.StatusSuccess {
		return resp, err
	}
	// success: wait for the watermark so the acknowledged record survives a
	// leader crash (see the function comment).
	if _, err := e.waitForHW(context.Background(), slot, resp.Seq, e.hwWait); err != nil {
		resp.Status = data.StatusFail
		resp.ErrID = data.ErrIDNotLeader
		resp.Err = err.Error()
		return resp, nil
	}
	return resp, nil
}

// landLocalAppend applies the business rules to the local WAL WITHOUT the
// watermark wait; the caller decides when and how long to wait (a single
// append waits for its own seq, a batch waits once for the group's max —
// see SubmitBatch). The returned response for a success carries the assigned
// seq; exists carries the stored record; fail carries the wire error.
func (e *Engine) landLocalAppend(slot int32, rec *data.EventRecord) (*data.AppendResponse, error) {
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
		} else {
			resp.Err = "bad request"
		}
		return resp, nil
	case data.StatusExists:
		resp.Record = out.Record // stored record, raw bytes end to end
		return resp, nil
	}
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
func (e *Engine) SubmitBatch(ctx context.Context, slot int32, recs []*data.EventRecord) ([]*data.AppendResponse, error) {
	if len(recs) == 0 {
		return nil, nil
	}
	leave := e.enterWriteFence(slot)
	defer leave()

	// One routing snapshot for the group (same copy-under-lock rule as
	// submitAppendLocked: the placement is mutated in place by the apply loop).
	e.tableMu.RLock()
	pp, ok := e.table.Slots[slot]
	var pState SlotState
	var pMigratingTo, pLeader string
	if ok {
		pState, pMigratingTo, pLeader = pp.State, pp.MigratingTo, pp.Leader
	}
	e.tableMu.RUnlock()

	switch {
	case !ok:
		// single-node / unassigned: serve locally
		return e.batchLocal(ctx, slot, recs, "")
	case pState == SlotMigratingOut && pMigratingTo != "" && pLeader == e.self:
		// step 4 of migration: source keeps serving and mirrors accepted
		// records to the target (see submitAppendLocked for why the push is
		// best effort)
		return e.batchLocal(ctx, slot, recs, pMigratingTo)
	case pState == SlotMigratingOut:
		return nil, &RedirectError{Kind: data.ErrIDMigrating, Slot: slot, Node: pLeader, Addr: e.clientAddr(pLeader)}
	case pLeader == e.self:
		return e.batchLocal(ctx, slot, recs, "")
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
func (e *Engine) batchLocal(ctx context.Context, slot int32, recs []*data.EventRecord, fwdTarget string) ([]*data.AppendResponse, error) {
	out := make([]*data.AppendResponse, len(recs))
	var maxSeq uint64
	for i, rec := range recs {
		resp, err := e.landLocalAppend(slot, rec)
		if err != nil {
			return nil, err
		}
		out[i] = resp
		if resp.Status == data.StatusSuccess && resp.Seq > maxSeq {
			maxSeq = resp.Seq
		}
		if fwdTarget != "" && resp.Seq > 0 {
			switch resp.Status {
			case data.StatusSuccess:
				e.forwardBestEffort(ctx, slot, fwdTarget, resp.Seq, rec)
			case data.StatusExists:
				e.forwardBestEffort(ctx, slot, fwdTarget, resp.Seq, resp.Record)
			}
		}
	}
	if maxSeq == 0 {
		return out, nil // nothing new landed: fail/exists only, no wait
	}
	if hw, err := e.waitForHW(context.Background(), slot, maxSeq, e.hwWait); err != nil {
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
				resp.Err = err.Error()
			}
		}
	}
	return out, nil
}

// forwardTo pushes a just-appended record (with its leader-assigned seq) to
// the migration target's WAL. Idempotent: the target ignores records it
// already has at that seq (e.g. replicated earlier via ordinary fetch).
func (e *Engine) forwardTo(ctx context.Context, toAddr string, slot int32, seq uint64, rec *data.EventRecord) error {
	// peer plane: one Replicate RPC carries the raw encoded record (the
	// old HTTP round-trip base64-wrapped it inside JSON).
	return e.peerReplicate(ctx, toAddr, slot, seq, rec.EncodeBinary(nil))
}

// migrationForwardTimeout bounds how long a slot leader keeps mirroring writes
// to a migration target that is not making progress.
//
// The mirror in forwardBestEffort is only a convergence accelerator: the
// target is a replica and the fetch loop carries the same records to it, so
// stopping the push can never lose data — the migration commit is gated on
// the target having caught up (awaitCaughtUp), never on this push. Past this
// bound the leader stops pushing (writes stay plain leader writes, which is
// what keeps a stuck migration from taxing every append) and the migration
// controller's own catch-up timeout aborts the migration cleanly. This is the
// "a slot must not sit in migrating_out forever" fallback.
const migrationForwardTimeout = 30 * time.Second

// forwardBestEffort mirrors one accepted record to the migration target and
// swallows every failure. It never fails the caller's write and never rolls
// the migration back (see SubmitAppend): a failure means "the target is not
// ready yet", which the replica fetch loop and the migration's own catch-up
// wait handle.
func (e *Engine) forwardBestEffort(ctx context.Context, slot int32, toNode string, seq uint64, rec *data.EventRecord) {
	if toNode == "" || e.migrationForwardExpired(slot) {
		return
	}
	addr := e.peerAddr(toNode)
	if addr == "" {
		return // target not registered (yet); fetch catches up when it is
	}
	if err := e.forwardTo(ctx, addr, slot, seq, rec); err != nil {
		e.logger.WithError(err).WithFields(map[string]any{"slot": slot, "target": toNode}).
			Debug("migration forward skipped; target catches up via fetch")
	}
}

// migrationForwardExpired reports whether a slot has been migrating_out longer
// than migrationForwardTimeout, i.e. its target is not making progress. The
// exact duration is recorded when the leader first sees the slot migrating
// (see syncMigrationState).
func (e *Engine) migrationForwardExpired(slot int32) bool {
	e.migFwdMu.Lock()
	since, ok := e.migFwdSince[slot]
	e.migFwdMu.Unlock()
	return ok && time.Since(since) > migrationForwardTimeout
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
	deadline := time.Now().Add(timeout)
	for {
		e.replMu.Lock()
		sr := e.repl[slot]
		hw := uint64(0)
		inSync := false
		if sr != nil {
			hw = sr.hw
			cutoff := time.Now().Add(-isrStaleAfter)
			for i, node := range sr.node {
				if !sr.lastOK[i].After(cutoff) {
					continue
				}
				// A replica whose connection died (or that the controller
				// marked down) is not being served: count it as out of ISR, so
				// the append falls back to "no in-sync replica to wait for"
				// instead of blocking on a watermark that was pinned by it.
				if e.sessionGone(node, sr.lastOK[i]) || e.peerDown(node) {
					continue
				}
				inSync = true
				break
			}
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
			return hw, nil
		}
		if time.Now().After(deadline) {
			e.logHWStall(slot, seq, hw)
			return hw, fmt.Errorf("timeout waiting for high watermark (hw %d < seq %d)", hw, seq)
		}
		time.Sleep(2 * time.Millisecond)
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
		Node  string `json:"node"`
		LEO   uint64 `json:"leo"`
		AgeS  int64  `json:"age_s"`
		InISR bool   `json:"in_isr"`
	}
	var reps []rep
	if sr != nil {
		cutoff := time.Now().Add(-isrStaleAfter)
		now := time.Now()
		for i, node := range sr.node {
			reps = append(reps, rep{node, sr.leo[i], int64(now.Sub(sr.lastOK[i]).Seconds()), sr.lastOK[i].After(cutoff)})
		}
	}
	e.replMu.Unlock()
	e.logger.WithFields(map[string]any{
		"slot": slot, "seq": seq, "hw": hw,
		"leader_leo": e.store.LastSeqOf(slot),
		"replicas":   e.tableReplicasOf(slot),
		"positions":  reps,
		"target":     e.migrationTargetOf(slot),
	}).Warn("watermark wait deadline hit: high watermark did not cover the append")
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
		e.logger.WithFields(map[string]any{"follower": follower, "slots": len(slots)}).
			Info("follower fetch session lost: its positions stop gating the watermark until it reports again")
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
			e.logger.WithField("suspect", suspect).Debug("unreachable report: controller did not take it (it will probe)")
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
	if leaders := e.freshestReplicas(id); len(leaders) > 0 {
		cmd.NewLeaders = leaders
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
func (e *Engine) freshestReplicas(down string) map[int32]string {
	tbl := e.TableSnapshot()
	slots := make([]int32, 0, 64)
	for s, p := range tbl.Slots {
		if p.Leader == down {
			slots = append(slots, s)
		}
	}
	if len(slots) == 0 {
		return nil
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
			if peer, ok := tbl.Peers[r]; !ok || peer.Offline() || streak[r] > 0 {
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
		return nil
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
		}
	}
	return out
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
// rests on — the migration only commits after it caught up (awaitCaughtUp).
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
	}
}

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
func (e *Engine) SetNode(node *Node) { e.node = node }

// RaftStats exposes consensus counters for the admin API.
func (e *Engine) RaftStats() map[string]any {
	return e.node.Stats()
}

// ISR exposes the in-sync replica set (public form for the admin API).
func (e *Engine) ISR(slot int32) []string { return e.isr(slot) }

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
		sr = &slotRepl{}
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
			sr = &slotRepl{}
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
	froms   []uint64
	changed []int32
	leos    []uint64
}

func (f *fetchScratch) reset() *fetchScratch {
	f.froms, f.changed, f.leos = f.froms[:0], f.changed[:0], f.leos[:0]
	return f
}

func sizedU64(b []uint64, n int) []uint64 {
	if cap(b) < n {
		return make([]uint64, n)
	}
	return b[:n]
}

func sizedI32(b []int32, n int) []int32 {
	if cap(b) < n {
		return make([]int32, n)
	}
	return b[:n]
}

// fetchScan is one round's per-position answers from Store.ScanFetchState:
// whether the slot has data for the follower, and the wake handle to park on.
type fetchScan struct {
	slots []int32
	moved []bool
	wakes []<-chan struct{}
}

func (f *fetchScan) resize(n int) {
	if cap(f.moved) < n {
		f.moved = make([]bool, n*2)
	}
	if cap(f.wakes) < n {
		f.wakes = make([]<-chan struct{}, n*2)
	}
	f.moved = f.moved[:n]
	f.wakes = f.wakes[:n]
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
}

// HandleMFetchCtx serves PeerService.MFetch: one multiplexed replica fetch
// round (all slots a follower takes from this leader, long-polled as a
// unit). ctx cancels the long-poll (client disconnect / deadline).
func (e *Engine) HandleMFetchCtx(ctx context.Context, req MFetchRequest) (*MFetchResponse, error) {
	n := len(req.Slots)
	if n != len(req.FromSeqs) || n > 8192 {
		return nil, fmt.Errorf("mfetch: %d slots / %d positions invalid or over cap", n, len(req.FromSeqs))
	}
	t0 := time.Now()
	defer func() {
		e.logger.WithFields(logrus.Fields{"self": e.self, "follower": req.Follower, "items": n,
			"ms": time.Since(t0).Milliseconds()}).Debug("mfetch round")
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
	order := e.orderPool.Get().([]int)
	if cap(order) < n {
		order = make([]int, n*2)
	}
	order = order[:n]
	rot := int(e.fetchRot.Add(1)) % max(1, n)
	for i := range order {
		order[i] = (i + rot) % n
	}

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
	e.store.ScanFetchState(req.Slots, req.FromSeqs, scan.moved, scan.wakes)

	out := &MFetchResponse{Follower: req.Follower, Items: make([]FetchItem, 0, 64)}
	budget := int64(maxPayloadBytes)
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
					parked = append(parked, parkedEntry{s, from, h})
				}
			}
			continue
		}
		if !scan.moved[i] {
			if h != nil {
				parked = append(parked, parkedEntry{s, from, h})
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
				parked = append(parked, parkedEntry{s, from, h})
			}
			continue
		}
		budget -= int64(len(payload))
		served = true
		out.Items = append(out.Items, FetchItem{Slot: s, FromSeq: from, NextSeq: next, Payload: payload})
	}
	prog.apply(e)
	// The scan buffer's tenure ENDS here: everything below works off `parked`
	// (slot, from, wake handle) and re-reads the store by slot. Putting it back a
	// second time from the wait loop let two concurrent rounds hold the same
	// buffer — one of them resets/truncates it while the other is still indexing
	// it, which is an `index out of range [...] with length 0` panic inside
	// HandleMFetchCtx (observed once under heavy churn). One Put, one owner.
	e.scanPool.Put(scan.reset())
	e.orderPool.Put(order[:cap(order)])

	if len(parked) > 0 && waitMS > 0 && !starved && !served {
		wait := time.Duration(waitMS) * time.Millisecond
		if wait > fetchMaxWait {
			wait = fetchMaxWait
		}
		deadline := time.After(wait)
	waitLoop:
		for {
			// One select over THREE cases instead of one case per parked
			// slot: the store bus aggregates every slot's advance signal,
			// and a non-blocking scan of the parked handles finds WHICH
			// slots moved. Rebuilding a 1400-case reflect.Select per round
			// was a top CPU contributor on the fetch hot path.
			select {
			case <-deadline:
				break waitLoop // budget spent: answer with whatever we have
			case <-ctx.Done():
				e.parkedPool.Put(parked[:0])
				e.progressPool.Put(prog.reset())
				return nil, ctx.Err() // client went away
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
				time.Sleep(fetchBurstSettle)
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
	e.tableMu.RLock()
	for s, p := range e.table.Slots {
		if p.Leader == e.self && s >= 0 && int(s>>6) < len(words) {
			words[s>>6] |= 1 << (s & 63)
		}
	}
	e.tableMu.RUnlock()
	o := &ownership{gen: gen, words: words}
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
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
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
		case <-ticker.C:
			e.syncSessions(ctx)
		}
	}
}

// kickSessions asks replicaLoop to reconcile its fetch sessions now instead of
// on the next tick. Called off the Raft apply path (non-blocking) so a leader
// move re-establishes the follower sessions immediately: the new leader gets
// its replicas' progress reports within one RPC round trip instead of up to a
// second later, so its watermark does not stall.
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
			e.logger.WithField("leader", sess.leader).WithError(err).Warn("fetch round failed")
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
	// The payloads alias the response's receive buffer; releasing the lease
	// returns that buffer to the pool, so it must happen after the round has
	// written everything it took.
	defer release()
	productive, err := e.applyFetchItems(fr.Items)
	if err != nil {
		return productive, err
	}
	if len(fr.Items) > 0 {
		// Positions moved: report only the slots that actually advanced
		// (a productive round on a busy leader touches tens of slots, not
		// the full 1.4k set), so the leader advances HW immediately
		// instead of waiting for the next round's piggyback.
		changed := sizedI32(scr.changed, len(fr.Items))[:0]
		leos := sizedU64(scr.leos, len(fr.Items))[:0]
		for _, it := range fr.Items {
			if e.isDiverged(it.Slot) {
				// A quarantined slot's content is not the leader's: do not
				// report it, so it cannot stand in for an in-sync replica.
				continue
			}
			changed = append(changed, it.Slot)
			leos = append(leos, e.store.LastSeqOf(it.Slot)+1)
		}
		// The report is a synchronous unary call, so the slices can go back to
		// the scratch right after it: the message is already on the wire.
		if len(changed) > 0 {
			e.reportProgress(addr, changed, leos, false)
		}
		scr.changed, scr.leos = changed[:0], leos[:0]
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
func (e *Engine) applyFetchItems(items []FetchItem) (bool, error) {
	productive := false
	for _, it := range items {
		dvg, err := applyFetchPayload(e.store, it.Slot, it.NextSeq, it.Payload)
		if err != nil {
			return productive, err
		}
		if len(dvg) > 0 {
			e.noteDivergence(it.Slot, dvg)
		}
		productive = true
	}
	return productive, nil
}

// noteDivergence latches a slot whose local log forked from its leader's at one
// or more seqs. The set is the fetch loop's quarantine list: the slot stops
// being reported as in-sync (so it cannot gate the watermark with wrong bytes)
// and every OTHER slot of the same multiplexed session keeps replicating. It is
// logged once per slot — the fence around the migration commit window is what
// keeps this from happening at all; reaching here means that invariant broke.
func (e *Engine) noteDivergence(slot int32, seqs []uint64) {
	e.divMu.Lock()
	_, had := e.diverged[slot]
	e.diverged[slot] = fmt.Sprintf("seqs %v", seqs)
	e.divMu.Unlock()
	if !had {
		e.logger.WithFields(map[string]any{"slot": slot, "seqs": seqs}).Warn(
			"replica fetch: local log diverged from the leader at a seq; quarantining this slot and keeping the rest of the session (other slots unaffected)")
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
	_ = e.peerProgress(ctx, addr, e.self, slots, froms, stamp)
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
			return leader, clientAddr, true, true // local replica, <=LEO semantics apply
		}
	}
	return leader, clientAddr, false, true
}

// ReadProxyAddr returns the slot leader's gRPC address when this node
// holds neither the slot nor any of its replicas; it returns "" when reads
// can be served locally. Serving an empty local result instead of
// forwarding would silently mis-report aggregates through single-endpoint
// front-ends (e.g. the console proxy).
//
// This runs on EVERY read RPC (ReadStream, ReadTails, ReadByCommand), so the
// placement lookup behind it is the single-lock LeaderReplica rather than a
// whole-table snapshot (see its doc for the measured cost).
//
// The three "" cases are the contract and stay exactly as they were: an
// unassigned slot, this node is the leader, or this node is a replica (local
// reads keep their own <=LEO bounding). Anything else is forwarded.
func (e *Engine) ReadProxyAddr(slot int32) string {
	_, clientAddr, local, found := e.LeaderReplica(slot)
	if !found || local {
		return ""
	}
	return clientAddr
}

// ---- Controller: membership + failover ---------------------------------------

// livenessFailThreshold is the number of consecutive failed health probes
// before the controller fails a peer's slots over (~3s of outage given the
// 1s reconcile tick and 2s probe timeout; well beyond a node's boot time).
const livenessFailThreshold = 3

// RunController reconciles cluster state; only the Raft leader executes it.
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
				p.AdminAddr, p.ClientAddr = e.node.cfg.AdminAddr, e.node.cfg.ClientAddr
			}
			e.submit(&Command{Op: OpJoinNode, Peer: &p})
		}
		for id := range tbl.Peers {
			if _, ok := raftAddrs[id]; !ok {
				e.loggerf("peer %s is no longer a raft voter; removing it from the directory", id)
				e.submit(&Command{Op: OpLeaveNode, NodeID: id})
			}
		}
		// 2) plan: first re-derive the table's replica factor from the member
		// count (ReplicaCountForMembers — it is not configurable), because a
		// fresh plan takes its replica count from the table and a grown
		// cluster carries more copies of every slot. Then full-replan only
		// when nothing is assigned; otherwise converge the layout — fill gaps
		// left by member joins and re-derive replica sets left stale by a
		// factor change (OpReplanSlots' own function, run on a clone: a replan
		// is submitted only when it would actually change the table, so the
		// steady-state table never costs a Raft entry per round).
		tbl = e.TableSnapshot()
		if len(tbl.Peers) == 0 {
			continue
		}
		if want := ReplicaCountForMembers(len(tbl.Peers)); tbl.Replicas != want {
			e.loggerf("replica factor: table has %d, %d members need %d — changing it",
				tbl.Replicas, len(tbl.Peers), want)
			if err := e.submit(&Command{Op: OpConfig, Replicas: want}); err == nil {
				// Re-read: the probe below plans with the factor this round
				// just wrote, instead of the one it replaced.
				tbl = e.TableSnapshot()
			}
		}
		if len(tbl.Slots) == 0 {
			e.submit(&Command{Op: OpPlanSlots})
		} else if probe := tbl.Clone(); probe.applyReplanSlots() {
			e.submit(&Command{Op: OpReplanSlots})
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
		e.logger.Warnf(format, args...)
	}
}

// ---- Start / Stop -------------------------------------------------------------

// Start launches background loops (replica fetch + controller + leader
// rebalance; the rebalancer exits at once when the knob is turned off).
func (e *Engine) Start(ctx context.Context) {
	go e.replicaLoop(ctx)
	go e.RunController(ctx)
	go e.RunRebalancer(ctx)
}
