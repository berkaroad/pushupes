package cluster

import (
	"context"
	"encoding/binary"
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
	// rotation order and parked set fresh every time; under acks=all the
	// leader serves thousands of rounds per second and these were the
	// dominant malloc traffic in the profile.
	orderPool    sync.Pool // []int, cap >= followed slots
	parkedPool   sync.Pool // []parkedEntry
	progressPool sync.Pool // *progressBatch

	// ownership bitmap: which slots this node leads, rebuilt only when
	// the Raft table version advances. Per-item map lookups under the
	// fetch hot loop (~1.4k followed slots per round) were a top CPU
	// contributor under acks=all; a word-test per slot is free.
	ownMu sync.Mutex
	own   atomic.Pointer[ownership]
	// tableGen bumps with every applied table change (holds tableMu).
	tableGen uint64

	// migration forwarding: slot -> target addr while migrating_out
	fwdMu sync.RWMutex
	fwd   map[int32]string

	// one replication session per slot leader (multiplexed long-poll fetch)
	sessMu   sync.Mutex
	sessions map[string]*fetchSession

	// controller liveness: consecutive health-probe failures per peer.
	// A single failure is not enough — a peer still booting (HTTP not yet
	// listening) would otherwise be failed over instantly on join.
	failMu     sync.Mutex
	failStreak map[string]int

	acksDefault string
}

// slotRepl tracks follower LEOs and the high watermark for one slot's
// replication group.
type slotRepl struct {
	// replica LEO per follower node id (leader LEO = store.LastSeqOf(slot))
	leo    map[string]uint64
	lastOK map[string]time.Time
	hw     uint64
}

// fetchSession is one follower's multiplexed fetch loop against one slot
// leader: a single goroutine long-polls PeerService.MFetch carrying the
// follower positions of every slot this node replicates from that leader.
// One session per leader => O(nodes) connections instead of O(followed slots).
type fetchSession struct {
	leader string
	slots  atomic.Value // []int32, refreshed by sessionLoop
	stop   chan struct{}
}

// Fetch cadence per session: a productive round restarts immediately
// (base gap), an idle one doubles the sleep up to maxBackoff. The leader
// long-poll (wait) absorbs quiet periods inside one request, so the
// whole idle cycle (wait+maxBackoff) must stay inside the ISR staleness
// window for replicas to remain in-sync.
const (
	fetchBaseInterval = 100 * time.Millisecond
	fetchMaxBackoff   = 2 * time.Second
	fetchWait         = 2 * time.Second      // follower-requested long-poll budget
	fetchSlack        = 1 * time.Second      // client-side timeout margin
	fetchMaxWait      = 5 * time.Second      // leader-side cap on a requested wait
	fetchBurstSettle  = 2 * time.Millisecond // coalescing window after one waiter wakes
	isrStaleAfter     = 10 * time.Second     // must exceed fetchWait+fetchMaxBackoff

	// maxPayloadBytes caps one long-poll response: during a backlog the
	// session streams item by item across rounds instead of building a
	// gigabyte JSON blob of every slot at once.
	maxPayloadBytes = 32 << 20
)

// NewEngine wires the cluster to the local storage. The Raft node may be
// attached later with SetNode (they reference each other).
func NewEngine(node *Node, store *storage.Store, self string, acksDefault string, logger *logrus.Entry) *Engine {
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
		failStreak:  map[string]int{},
		acksDefault: acksDefault,
	}
	e.orderPool.New = func() any { return make([]int, 0, 64) }
	e.parkedPool.New = func() any { return make([]parkedEntry, 0, 64) }
	e.progressPool.New = func() any { return &progressBatch{} }
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
// for 4096 slots; snapshots ride the control plane).
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

// ---- Routing ---------------------------------------------------------------

// SlotOf routes an aggregate to its slot.
func (e *Engine) SlotOf(aggregateID string) int32 { return e.store.SlotOf(aggregateID) }

// syncMigrationState refreshes the forwarding map from the table.
func (e *Engine) syncMigrationState() {
	e.tableMu.RLock()
	fwd := make(map[int32]string, 4)
	for s, p := range e.table.Slots {
		if p.State == SlotMigratingOut && p.MigratingTo != "" {
			fwd[s] = p.MigratingTo
		}
	}
	e.tableMu.RUnlock()
	e.fwdMu.Lock()
	e.fwd = fwd
	e.fwdMu.Unlock()
}

// SubmitAppend routes a client append to the slot leader (executing locally
// when we are it) and applies acks semantics.
func (e *Engine) SubmitAppend(ctx context.Context, rec *data.EventRecord, acks string) (*data.AppendResponse, error) {
	if acks == "" {
		acks = e.acksDefault
	}
	slot := e.SlotOf(rec.AggregateID)

	e.tableMu.RLock()
	p, ok := e.table.Slots[slot]
	e.tableMu.RUnlock()
	if !ok {
		// single-node / unassigned: serve locally
		return e.localAppend(slot, rec, acks)
	}

	switch {
	case p.State == SlotMigratingOut && p.MigratingTo != "" && p.Leader == e.self:
		// step 4 of migration: source stays leader and keeps serving writes,
		// but every accepted record is also pushed to the target at the same
		// seq so the two copies converge before the commit step.
		resp, err := e.localAppend(slot, rec, acks)
		if err != nil {
			return nil, err
		}
		// fast path: the command already landed during an earlier attempt
		switch {
		case resp.Status == data.StatusSuccess && resp.Seq > 0:
			if ferr := e.forwardTo(ctx, e.peerAddr(p.MigratingTo), slot, resp.Seq, rec); ferr != nil {
				e.logger.WithError(ferr).WithField("slot", slot).Warn("migration forward failed, rollback slot state")
				e.rollbackMigration(slot)
				return nil, ferr
			}
		case resp.Status == data.StatusExists && resp.Seq > 0 && resp.Record != nil:
			// a retried command: make sure the target converged too
			stored := &data.EventRecord{
				AggregateID: resp.Record.AggregateID,
				Version:     resp.Record.Version,
				UnixTime:    resp.Record.UnixTime,
				CommandID:   resp.Record.CommandID,
			}
			for _, ev := range resp.Record.Events {
				stored.Events = append(stored.Events, data.Event{Type: ev.Type, Body: ev.Body})
			}
			if ferr := e.forwardTo(ctx, e.peerAddr(p.MigratingTo), slot, resp.Seq, stored); ferr != nil {
				e.logger.WithError(ferr).WithField("slot", slot).Warn("migration exists-forward failed")
				e.rollbackMigration(slot)
				return nil, ferr
			}
		}
		return resp, nil
	case p.State == SlotMigratingOut:
		// someone else leads a migrating slot: the leader handles forwarding
		addr := e.clientAddr(p.Leader)
		return nil, &RedirectError{Kind: data.ErrIDMigrating, Slot: slot, Node: p.Leader, Addr: addr}
	case p.Leader == e.self:
		return e.localAppend(slot, rec, acks)
	default:
		addr := e.clientAddr(p.Leader)
		if addr == "" {
			return nil, data.ErrSlotNotLocal
		}
		return nil, &RedirectError{Kind: data.ErrIDSlotNotLocal, Slot: slot, Node: p.Leader, Addr: addr}
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

// localAppend runs the business rules against the local WAL and waits for
// the high watermark when acks=all.
func (e *Engine) localAppend(slot int32, rec *data.EventRecord, acks string) (*data.AppendResponse, error) {
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
		resp.Record = out.Record.ToRecordJSON(out.Seq)
		return resp, nil
	}
	// success: optionally wait for replication
	if acks == "all" {
		if err := e.waitForHW(context.Background(), slot, out.Seq, 10*time.Second); err != nil {
			resp.Status = data.StatusFail
			resp.ErrID = data.ErrIDNotLeader
			resp.Err = err.Error()
			return resp, nil
		}
	}
	resp.Record = out.Record.ToRecordJSON(out.Seq)
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

// HandleReplicate serves PeerService.Replicate: a migration forwarding push.
// The payload is a single encoded record to be appended at a fixed seq.
func (e *Engine) HandleReplicate(slot int32, seq uint64, payload []byte) error {
	rec, consumed, err := data.DecodeRecord(payload)
	if err != nil {
		return err
	}
	if consumed != len(payload) {
		return fmt.Errorf("replicate: payload holds %d extra bytes", len(payload)-consumed)
	}
	return e.store.AppendAtSeq(slot, seq, &rec)
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
		if sr != nil {
			hw = sr.hw
		}
		e.replMu.Unlock()
		if hw >= seq {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for high watermark (hw %d < seq %d)", hw, seq)
		}
		time.Sleep(2 * time.Millisecond)
	}
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
	for node, t := range sr.lastOK {
		if t.After(cutoff) {
			out = append(out, node)
		}
	}
	return out
}

// advanceHW recomputes the slot high watermark as the min LEO across the
// in-sync set (leader included). Called on every replica progress.
func (e *Engine) advanceHW(slot int32) {
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
		if !sr.lastOK[node].After(cutoff) {
			continue // out of ISR: excluded from HW computation
		}
		if l := sr.leo[node]; l < minLEO {
			minLEO = l
		}
	}
	if minLEO > sr.hw {
		sr.hw = minLEO
	}
}

func (e *Engine) tableReplicasOf(slot int32) []string {
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	return e.table.ReplicasOf(slot)
}

// HW returns the leader-tracked high watermark for a slot.
func (e *Engine) HW(slot int32) uint64 {
	e.replMu.Lock()
	defer e.replMu.Unlock()
	if sr := e.repl[slot]; sr != nil {
		return sr.hw
	}
	return 0
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
// like OpPlanSlots).
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
		sr = &slotRepl{leo: map[string]uint64{}, lastOK: map[string]time.Time{}}
		e.repl[slot] = sr
	}
	prev, wasFresh := sr.leo[follower], now.Sub(sr.lastOK[follower]) <= isrStaleAfter
	sr.leo[follower] = leo
	sr.lastOK[follower] = now
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
	slots    []int32
	leos     []uint64
	now      time.Time
}

func (b *progressBatch) add(slot int32, leo uint64) {
	b.slots = append(b.slots, slot)
	b.leos = append(b.leos, leo)
}

// reset returns the batch to a clean state for pool reuse. apply already
// truncates the position slices; this also drops the follower/now stamps.
func (b *progressBatch) reset() *progressBatch {
	b.follower = ""
	b.slots = b.slots[:0]
	b.leos = b.leos[:0]
	b.now = time.Time{}
	return b
}

// apply records every sample (lastOK always refreshed, one shared timestamp),
// then advances the HW once per slot whose in-sync min could have moved.
func (b *progressBatch) apply(e *Engine) {
	if len(b.slots) == 0 {
		return
	}
	changed := make([]int32, 0, len(b.slots))
	e.replMu.Lock()
	for i, slot := range b.slots {
		leo := b.leos[i]
		sr := e.repl[slot]
		if sr == nil {
			sr = &slotRepl{leo: map[string]uint64{}, lastOK: map[string]time.Time{}}
			e.repl[slot] = sr
		}
		prev, wasFresh := sr.leo[b.follower], b.now.Sub(sr.lastOK[b.follower]) <= isrStaleAfter
		sr.leo[b.follower] = leo
		sr.lastOK[b.follower] = b.now
		if leo > prev || !wasFresh {
			changed = append(changed, slot)
		}
	}
	e.replMu.Unlock()
	for _, slot := range changed {
		e.advanceHW(slot)
	}
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
// the wire and CPU cost under acks=all.
type MFetchRequest struct {
	Follower string   `json:"follower"`
	WaitMS   int64    `json:"wait_ms,omitempty"`
	Slots    []int32  `json:"slots"`
	FromSeqs []uint64 `json:"from_seqs"`
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

	out := &MFetchResponse{Follower: req.Follower, Items: make([]FetchItem, 0, 64)}
	budget := int64(maxPayloadBytes)
	starved := false
	served := false
	// Progress piggyback is applied in bulk after phase 1: per-item calls
	// took replMu (twice: note + advanceHW) per slot every round, most of
	// them re-reporting unchanged LEOs.
	prog := e.progressPool.Get().(*progressBatch)
	prog.follower = req.Follower
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
		if budget <= 0 {
			// Out of budget: no data this round. If the slot HAS pending
			// data, do NOT park a fresh wake handle — the data landed
			// before this point, so a clean handle would only fire on the
			// NEXT append and the slot would idle until the wait deadline
			// (that stall, not the wake path, was the acks=all p99=fetchWait
			// tail). The rotating cursor serves it next round; marking
			// starved makes the response return now instead. If the slot
			// is truly empty, park so future appends still wake the poll.
			if waitMS > 0 {
				h := e.store.WakeChan(s)
				if e.store.LastSeqOf(s) >= from {
					starved = true
				} else {
					parked = append(parked, parkedEntry{s, from, h})
				}
			}
			continue
		}
		// Take the wake handle BEFORE the read (no-lost-wake discipline):
		// an append landing during the read closes this handle, so the
		// first bus scan finds it fired and re-reads. Acquiring after the
		// read left a window that stalled the round to the full WaitMS.
		var h <-chan struct{}
		if waitMS > 0 {
			h = e.store.WakeChan(s)
		}
		_, next, payload, err := e.store.ReadSlotBytes(s, from, 0, min(4<<20, budget))
		if err != nil {
			e.orderPool.Put(order[:cap(order)])
			e.parkedPool.Put(parked[:0])
			e.progressPool.Put(prog.reset())
			return nil, err
		}
		budget -= int64(len(payload))
		if len(payload) == 0 {
			if h != nil {
				parked = append(parked, parkedEntry{s, from, h})
			}
			continue // empty slot: implied absent in the sparse response
		}
		served = true
		out.Items = append(out.Items, FetchItem{Slot: s, FromSeq: from, NextSeq: next, Payload: payload})
	}
	prog.apply(e)
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
			// was a top CPU contributor under acks=all.
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
				// remaining slot before HW can advance (acks=all pays
				// this on every write). The short settle lets sibling
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
// only when the Raft table version advances. Rebuilding walks the 4096-
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

// ReplicateRecord applies a leader-assigned record on a follower (or import
// target): decode each record and append at its fixed seq.
func (e *Engine) ReplicateRecord(slot int32, seq uint64, payload []byte) error {
	rec, consumed, err := data.DecodeRecord(payload)
	if err != nil {
		return err
	}
	if consumed != len(payload) {
		return fmt.Errorf("payload holds %d extra bytes", len(payload)-consumed)
	}
	return e.store.AppendAtSeq(slot, seq, &rec)
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
		case <-ticker.C:
			e.syncSessions(ctx)
		}
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
		_, err := e.fetchRound(ctx, sess.leader, slots)
		if err != nil {
			e.logger.WithField("leader", sess.leader).WithError(err).Warn("fetch round failed")
			// Transport/leader trouble: back off so a dead leader does not
			// spin. An *empty* round is NOT backed off — the long-poll made
			// it zero-CPU, and sleeping between rounds is exactly what
			// stalled records past the wait deadline (acks=all p90 =
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
func (e *Engine) fetchRound(ctx context.Context, leader string, slots []int32) (bool, error) {
	addr := e.peerAddr(leader)
	if addr == "" {
		return false, data.ErrNotLeader
	}
	froms := make([]uint64, len(slots))
	for i, s := range slots {
		froms[i] = e.store.LastSeqOf(s) + 1
	}
	reqCtx, cancel := context.WithTimeout(ctx, fetchWait+fetchSlack+5*time.Second)
	defer cancel()
	fr, err := e.peerMFetch(reqCtx, addr, &MFetchRequest{Follower: e.self, WaitMS: fetchWait.Milliseconds(), Slots: slots, FromSeqs: froms})
	if err != nil {
		return false, err
	}
	productive := false
	for _, it := range fr.Items {
		// Sparse response: every entry carries data for its slot.
		if err := applyFetchPayload(e.store, it.Slot, it.NextSeq, it.Payload); err != nil {
			return productive, err
		}
		productive = true
	}
	if productive {
		// Positions moved: report the new LEOs immediately so the leader
		// advances HW without waiting for the next round's FromSeq piggyback.
		prog := make([]uint64, len(slots))
		for i, s := range slots {
			prog[i] = e.store.LastSeqOf(s) + 1
		}
		e.reportProgress(addr, slots, prog)
	}
	return productive, nil
}

// applyFetchPayload replays one slot's concatenated records; the batch ended
// at nextSeq, so it starts at nextSeq-recordCount(payload).
func applyFetchPayload(store *storage.Store, slot int32, nextSeq uint64, payload []byte) error {
	seq := nextSeq - uint64(countRecords(payload))
	rest := payload
	for len(rest) > 0 {
		rec, consumed, err := data.DecodeRecord(rest)
		if err != nil {
			return err
		}
		if err := store.AppendAtSeq(slot, seq, &rec); err != nil {
			return err
		}
		rest = rest[consumed:]
		seq++
	}
	return nil
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
func (e *Engine) reportProgress(addr string, slots []int32, froms []uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = e.peerProgress(ctx, addr, e.self, slots, froms)
}

// ReadProxyAddr returns the slot leader's gRPC address when this node
// holds neither the slot nor any of its replicas; it returns "" when reads
// can be served locally. Serving an empty local result instead of
// forwarding would silently mis-report aggregates through single-endpoint
// front-ends (e.g. the console proxy).
func (e *Engine) ReadProxyAddr(slot int32) string {
	t := e.TableSnapshot()
	p, ok := t.Slots[slot]
	if !ok {
		return "" // unassigned table entry: keep local behavior
	}
	if p.Leader == e.self {
		return ""
	}
	for _, r := range p.Replicas {
		if r == e.self {
			return "" // local replica, <=HW semantics still apply
		}
	}
	return e.clientAddr(p.Leader)
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

// submit commits a command through Raft.
func (e *Engine) submit(c *Command) error {
	if !e.node.IsLeader() {
		return ErrNotLeader
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

// Start launches background loops (replica fetch + controller).
func (e *Engine) Start(ctx context.Context) {
	go e.replicaLoop(ctx)
	go e.RunController(ctx)
}
