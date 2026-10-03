package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"pushupes/internal/data"
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

	// leader rebalance (balance.go): how often the controller re-checks the
	// ring layout and how many leader hand-overs one round may execute.
	// Set from the -rebalance-interval / -rebalance-batch flags.
	rebalanceInterval time.Duration
	rebalanceBatch    int

	// post-migration local cleanup: slots whose local copy this node is
	// scheduled to drop after the retention window (slot -> schedule), plus
	// this node's replica-set membership as of the previous table walk (the
	// transition member -> not-a-member is what arms a countdown). Exposed to
	// the admin plane so the console can show which copy of a slot is on its
	// way out. See localdrop.go.
	dropMu       sync.Mutex
	dropAfter    time.Duration
	dropGen      uint64
	pendingDrops map[int32]pendingDrop
	replicaOf    map[int32]bool
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
}

// Fetch cadence per session: a productive round restarts immediately
// (base gap), an idle one doubles the sleep up to maxBackoff. The leader
// long-poll (wait) absorbs quiet periods inside one request, so the
// whole idle cycle (wait+maxBackoff) must stay inside the ISR staleness
// window for replicas to remain in-sync.
const (
	fetchBaseInterval  = 100 * time.Millisecond
	fetchMaxBackoff    = 2 * time.Second
	fetchWait          = 2 * time.Second      // follower-requested long-poll budget
	fetchSlack         = 1 * time.Second      // client-side timeout margin
	fetchMaxWait       = 5 * time.Second      // leader-side cap on a requested wait
	fetchBurstSettle   = 2 * time.Millisecond // coalescing window after one waiter wakes
	fetchSweepInterval = 2 * time.Second      // stamp all positions at least this often
	isrStaleAfter      = 10 * time.Second     // must exceed fetchWait+fetchMaxBackoff

	// maxPayloadBytes caps one long-poll response: during a backlog the
	// session streams item by item across rounds instead of building a
	// gigabyte JSON blob of every slot at once.
	maxPayloadBytes = 32 << 20
)

// NewEngine wires the cluster to the local storage. The Raft node may be
// attached later with SetNode (they reference each other).
func NewEngine(node *Node, store *storage.Store, self string, logger *logrus.Entry) *Engine {
	e := &Engine{
		node:   node,
		store:  store,
		self:   self,
		table:  NewTable(store.SlotCount, 2),
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
		// post-migration cleanup: the former source keeps its copy for this
		// long once it sees the hand-over committed (-drop-after).
		dropAfter: DefaultDropRetention,
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
	return nil, nil
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
	for s, p := range e.table.Slots {
		if p.Leader == e.self {
			led[s] = true
		}
	}
	e.tableMu.RUnlock()
	e.ledMu.Lock()
	prevLed := e.ledPrev
	e.ledPrev = led
	e.ledMu.Unlock()
	e.replMu.Lock()
	for s := range led {
		if !prevLed[s] {
			delete(e.repl, s)
		}
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
	// success: wait for the watermark so the acknowledged record survives a
	// leader crash (see the function comment).
	if err := e.waitForHW(context.Background(), slot, out.Seq, 10*time.Second); err != nil {
		resp.Status = data.StatusFail
		resp.ErrID = data.ErrIDNotLeader
		resp.Err = err.Error()
		return resp, nil
	}
	// Success carries status/seq only: the caller already holds the record it
	// sent, and echoing a 100KiB body back doubled the bytes on the wire per
	// append (see DESIGN.md §6). EXISTS still returns the stored record, which
	// is the one case where the caller cannot know it.
	return resp, nil
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
func (e *Engine) waitForHW(ctx context.Context, slot int32, seq uint64, timeout time.Duration) error {
	if e.replicaCount(slot) <= 1 {
		return nil // nothing to wait for
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
			for i := range sr.lastOK {
				if sr.lastOK[i].After(cutoff) {
					inSync = true
					break
				}
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
			return nil
		}
		if time.Now().After(deadline) {
			e.logHWStall(slot, seq, hw)
			return fmt.Errorf("timeout waiting for high watermark (hw %d < seq %d)", hw, seq)
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

// isr returns in-sync replicas: followers whose LEO is within the lag window.
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
		if sr.lastOK[i].After(cutoff) {
			out = append(out, node)
		}
	}
	return out
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
				e.scanPool.Put(scan.reset())
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
		backoff = 0 // healthy round (data or absorbed-idle): re-park at once
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
		// 2) plan: full replan only when nothing is assigned; otherwise fill
		// gaps left by member joins without disturbing placed slots.
		tbl = e.TableSnapshot()
		if len(tbl.Peers) == 0 {
			continue
		}
		if len(tbl.Slots) == 0 {
			e.submit(&Command{Op: OpPlanSlots})
		} else if len(tbl.Slots) < int(tbl.SlotCount) || tableReplicaShortfall(tbl) {
			e.submit(&Command{Op: OpReplanSlots})
		}
		// 3) liveness sweep: probe peers over the peer plane; fail a peer
		// over only after several consecutive misses so a booting node is
		// not evicted.
		for id, p := range tbl.Peers {
			if id == e.self || p.PeerAddr == "" {
				// peers without a peer address are not probed: membership
				// is owned by the raft configuration, not by liveness.
				continue
			}
			e.failMu.Lock()
			if !e.alive(p.PeerAddr) {
				e.failStreak[id]++
				if e.failStreak[id] >= livenessFailThreshold {
					e.loggerf("peer %s unreachable (%d consecutive probes), failing slots over", id, e.failStreak[id])
					e.submit(&Command{Op: OpLeaveNode, NodeID: id})
					delete(e.failStreak, id)
				}
			} else {
				e.failStreak[id] = 0
			}
			e.failMu.Unlock()
		}
	}
}

// alive probes a peer over the peer plane (PeerService.Ping).
func (e *Engine) alive(addr string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), peerPingTimeout)
	defer cancel()
	return e.peerPing(ctx, addr) == nil
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
