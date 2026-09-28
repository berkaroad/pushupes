package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
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
// seq-based fetch exactly like DeadliftMQ/Kafka.
type Engine struct {
	node    *Node
	store   *storage.Store
	table   *Table
	tableMu sync.RWMutex
	logger  *logrus.Entry
	httpC   *http.Client
	self    string

	// replication bookkeeping per slot, only meaningful on slot leaders
	replMu sync.Mutex
	repl   map[int32]*slotRepl

	// rotation cursor for the mfetch payload budget
	fetchRot atomic.Uint64

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
// leader: a single goroutine long-polls /internal/mfetch carrying the
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
	fetchWait         = 2 * time.Second  // follower-requested long-poll budget
	fetchSlack        = 1 * time.Second  // client-side timeout margin
	fetchMaxWait      = 5 * time.Second  // leader-side cap on a requested wait
	isrStaleAfter     = 10 * time.Second // must exceed fetchWait+fetchMaxBackoff

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
		httpC: &http.Client{
			Timeout: 15 * time.Second,
			// Long-poll keeps one live fetch per followed slot (~64) against
			// the same leader; the default 2 idle conns per host would close
			// and re-dial the rest every response (load-time churn).
			Transport: &http.Transport{
				MaxIdleConns:        2048,
				MaxIdleConnsPerHost: 1024,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		repl:        map[int32]*slotRepl{},
		fwd:         map[int32]string{},
		sessions:    map[string]*fetchSession{},
		failStreak:  map[string]int{},
		acksDefault: acksDefault,
	}
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
			if ferr := e.forwardTo(ctx, e.adminAddr(p.MigratingTo), slot, resp.Seq, rec); ferr != nil {
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
			if ferr := e.forwardTo(ctx, e.adminAddr(p.MigratingTo), slot, resp.Seq, stored); ferr != nil {
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

// adminAddr is the admin/inter-node HTTP address used for replication,
// migration forwards and liveness probes.
func (e *Engine) adminAddr(nodeID string) string {
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	return e.table.Peers[nodeID].AdminAddr
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
	if toAddr == "" {
		return fmt.Errorf("forward: no address for migration target")
	}
	payload := rec.EncodeBinary(nil)
	body, _ := json.Marshal(map[string]any{
		"slot": slot, "seq": seq, "payload": payload,
	})
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, HTTPURL(toAddr, "/internal/replicate"), bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := e.httpC.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("forward seq %d: status %d: %s", seq, resp.StatusCode, string(raw))
	}
	return nil
}

// HandleReplicate serves /internal/replicate: a migration forwarding push.
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
			continue // out of ISR: excluded from HW like Kafka
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
	e.replMu.Lock()
	sr := e.repl[slot]
	if sr == nil {
		sr = &slotRepl{leo: map[string]uint64{}, lastOK: map[string]time.Time{}}
		e.repl[slot] = sr
	}
	sr.leo[follower] = leo
	sr.lastOK[follower] = time.Now()
	e.replMu.Unlock()
	e.advanceHW(slot)
}

// ---- Replica fetch protocol (data plane) -------------------------------------

// FetchItem is one slot's share of a multiplexed fetch: the follower's next
// wanted seq (FromSeq; FromSeq-1 is its durable LEO and doubles as the
// progress report the leader records), and — on the response — the byte
// range + concatenated payload starting at that seq.
type FetchItem struct {
	Slot      int32  `json:"slot"`
	FromSeq   uint64 `json:"from_seq"`
	NextSeq   uint64 `json:"next_seq,omitempty"` // response: first seq after the batch
	Payload   []byte `json:"payload,omitempty"`  // response: concatenated records
	LeaderLEO uint64 `json:"leader_leo,omitempty"`
	LeaderHW  uint64 `json:"leader_hw,omitempty"`
}

// MFetchRequest is one session round: a follower's positions across every
// slot led by this node, long-polled as a unit.
type MFetchRequest struct {
	Follower string      `json:"follower"`
	WaitMS   int64       `json:"wait_ms,omitempty"`
	Items    []FetchItem `json:"items"`
}

// MFetchResponse answers an MFetchRequest; only slots with data (or
// requested diagnostics) carry payload bytes.
type MFetchResponse struct {
	Follower string      `json:"follower"`
	Items    []FetchItem `json:"items"`
}

// HandleMFetch serves one multiplexed fetch round on a node that leads the
// requested slots.
//
//  1. Progress piggyback: every item's FromSeq-1 is the follower's durable
//     LEO for that slot; the leader records replica progress from the
//     request itself — no separate report round trip on idle rounds.
//  2. Long-poll as a unit: when nothing is ready and WaitMS>0, the leader
//     holds the request and selects across the wake channels of all
//     requested slots (Slot.advanceNotifyLocked closes them on append),
//     answering as soon as ANY slot advances or the budget elapses. A quiet
//     cluster therefore pays ~1 request per leader-session per wait window,
//     not one per slot.
//
// Slots this node does not lead are answered empty (failover is in flight);
// the session retries and the table will reassign them.
func (e *Engine) HandleMFetch(req MFetchRequest) (*MFetchResponse, error) {
	if len(req.Items) > 8192 {
		return nil, fmt.Errorf("mfetch: %d items exceeds cap", len(req.Items))
	}
	// Phase 1: record progress and try to read current data per slot.
	// A global payload budget (with a rotating start cursor so no slot
	// starves under sustained backlog) bounds one response's JSON size.
	type pending struct {
		idx  int
		wake <-chan struct{}
	}
	out := &MFetchResponse{Follower: req.Follower, Items: make([]FetchItem, len(req.Items))}
	var waiters []pending
	budget := int64(maxPayloadBytes)
	rot := int(e.fetchRot.Add(1)) % max(1, len(req.Items))
	order := make([]int, len(req.Items))
	for i := range order {
		order[i] = (i + rot) % len(req.Items)
	}
	for _, i := range order {
		it := req.Items[i]
		out.Items[i] = FetchItem{Slot: it.Slot, FromSeq: it.FromSeq}
		if budget <= 0 {
			// out of budget: no data this round; waiter registered below so
			// the long-poll still picks this slot up when others advance.
			if req.WaitMS > 0 {
				if p, ok := e.tableLeaderSlot(it.Slot); ok && p.Leader == e.self {
					waiters = append(waiters, pending{i, e.store.WakeChan(it.Slot)})
				}
			}
			continue
		}
		out.Items[i] = FetchItem{Slot: it.Slot, FromSeq: it.FromSeq}
		p, ok := e.tableLeaderSlot(it.Slot)
		if !ok || p.Leader != e.self {
			continue
		}
		if req.Follower != "" && req.Follower != e.self && it.FromSeq > 0 {
			e.NoteReplicaProgress(it.Slot, req.Follower, it.FromSeq-1)
		}
		// Take the wake handle BEFORE the read (same re-arm discipline as
		// the wait loop): an append that lands during the read closes this
		// handle — the first select iteration then fires immediately and
		// re-reads. Acquiring after the read left a lost-wake window
		// (append between read and acquire closes the *previous* handle),
		// which stalled the round to the full WaitMS.
		var wake <-chan struct{}
		if req.WaitMS > 0 {
			wake = e.store.WakeChan(it.Slot)
		}
		_, next, payload, err := e.store.ReadSlotBytes(it.Slot, it.FromSeq, 0, min(4<<20, budget))
		if err != nil {
			return nil, err
		}
		budget -= int64(len(payload))
		out.Items[i].NextSeq = next
		out.Items[i].Payload = payload
		out.Items[i].LeaderLEO = e.store.LastSeqOf(it.Slot)
		out.Items[i].LeaderHW = e.HW(it.Slot)
		if len(payload) == 0 && wake != nil {
			waiters = append(waiters, pending{i, wake})
		}
	}
	if len(waiters) > 0 && req.WaitMS > 0 {
		wait := time.Duration(req.WaitMS) * time.Millisecond
		if wait > fetchMaxWait {
			wait = fetchMaxWait
		}
		deadline := time.After(wait)
	waitLoop:
		for {
			cases := make([]reflect.SelectCase, 0, len(waiters)+1)
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(deadline)})
			for _, w := range waiters {
				cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(w.wake)})
			}
			chosen, _, _ := reflect.Select(cases)
			if chosen == 0 {
				break waitLoop // deadline: answer with whatever phase 1 found
			}
			w := &waiters[chosen-1]
			it := req.Items[w.idx]
			// Re-arm BEFORE re-reading: an advance that lands during the
			// read closes the fresh handle, so the next select iteration
			// (or this read) catches it — no lost-wakeup window.
			w.wake = e.store.WakeChan(it.Slot)
			_, next, payload, err := e.store.ReadSlotBytes(it.Slot, it.FromSeq, 0, min(int64(4<<20), max(budget, 1<<10)))
			budget -= int64(len(payload))
			if err != nil {
				return nil, err
			}
			if len(payload) > 0 {
				out.Items[w.idx].NextSeq = next
				out.Items[w.idx].Payload = payload
				out.Items[w.idx].LeaderLEO = e.store.LastSeqOf(it.Slot)
				out.Items[w.idx].LeaderHW = e.HW(it.Slot)
				break waitLoop // answer now; other slots ride the next round
			}
		}
	}
	return out, nil
}

func (e *Engine) tableLeaderSlot(slot int32) (*Placement, bool) {
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	p, ok := e.table.Slots[slot]
	return p, ok
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
// Each session long-polls /internal/mfetch with the follower position of
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
		productive, err := e.fetchRound(ctx, sess.leader, slots)
		if err != nil || !productive {
			if backoff == 0 {
				backoff = fetchBaseInterval
			} else {
				backoff *= 2
			}
			if backoff > fetchMaxBackoff {
				backoff = fetchMaxBackoff
			}
		} else {
			// A productive round means the leader had backlog: keep the
			// pipeline saturated (no sleep). Idle supply is absorbed by
			// the round's long-poll itself, so 0-backoff cannot busy-loop.
			backoff = 0
		}
	}
}

// fetchRound performs one multiplexed fetch: the covered slots' positions
// ride as progress reports (FromSeq-1 == durable LEO); the leader holds the
// request until at least one slot advances or the wait budget elapses.
func (e *Engine) fetchRound(ctx context.Context, leader string, slots []int32) (bool, error) {
	addr := e.adminAddr(leader)
	if addr == "" {
		return false, data.ErrNotLeader
	}
	items := make([]FetchItem, len(slots))
	for i, s := range slots {
		items[i] = FetchItem{Slot: s, FromSeq: e.store.LastSeqOf(s) + 1}
	}
	body := EncodeMRequest(&MFetchRequest{Follower: e.self, WaitMS: fetchWait.Milliseconds(), Items: items})
	reqCtx, cancel := context.WithTimeout(ctx, fetchWait+fetchSlack+5*time.Second)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, HTTPURL(addr, "/internal/mfetch"), bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	httpReq.Header.Set("Content-Type", "application/octet-stream")
	httpResp, err := e.httpC.Do(httpReq)
	if err != nil {
		return false, err
	}
	defer httpResp.Body.Close()
	raw, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return false, err
	}
	if httpResp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("mfetch %d: %s", httpResp.StatusCode, string(raw))
	}
	fr, err := DecodeMResponse(raw)
	if err != nil {
		return false, err
	}
	productive := false
	for _, it := range fr.Items {
		if len(it.Payload) == 0 {
			continue
		}
		if err := applyFetchPayload(e.store, it.Slot, it.NextSeq, it.Payload); err != nil {
			return productive, err
		}
		productive = true
	}
	if productive {
		// Positions moved: report the new LEOs immediately so the leader
		// advances HW without waiting for the next round's FromSeq piggyback.
		prog := make([]FetchItem, 0, len(slots))
		for _, s := range slots {
			prog = append(prog, FetchItem{Slot: s, FromSeq: e.store.LastSeqOf(s) + 1})
		}
		e.reportProgress(addr, prog)
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

// reportProgress posts follower LEOs in bulk to a leader (one request per
// productive round, replacing the old per-slot reportLEO).
func (e *Engine) reportProgress(addr string, items []FetchItem) {
	body, _ := json.Marshal(map[string]any{"follower": e.self, "items": items})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, HTTPURL(addr, "/internal/replica-progress"), bytes.NewReader(body))
	if err != nil {
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if resp, err := e.httpC.Do(httpReq); err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
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
		// 3) liveness sweep: probe peers; fail a peer over only after
		// several consecutive misses so a booting node is not evicted.
		for id, p := range tbl.Peers {
			if id == e.self || p.AdminAddr == "" {
				// unregistered peers are not probed: no admin address is
				// known yet (its announcer is still booting); membership is
				// owned by the raft configuration, not by liveness.
				continue
			}
			e.failMu.Lock()
			if !e.alive(p.AdminAddr) {
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

// alive probes a peer's health endpoint.
func (e *Engine) alive(addr string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, HTTPURL(addr, "/healthz"), nil)
	if err != nil {
		return false
	}
	resp, err := e.httpC.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
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
