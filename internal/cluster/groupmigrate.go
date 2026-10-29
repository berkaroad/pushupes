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
	"fmt"
	"sort"
	"time"

	"github.com/berkaroad/pushupes/internal/config"
	"github.com/berkaroad/pushupes/internal/data"
	pushupesv1 "github.com/berkaroad/pushupes/internal/grpcapi/pushupes/v1"
)

// groupExpandBatch is how many source slots are replayed before the controller
// re-checks: the calibration target retires a segment in batches so the disk
// peak stays near the steady state (DESIGN §4.5.8, 128 slots per batch).
const groupExpandBatch = 128

// GroupExpandResult reports what a generation migration did.
type GroupExpandResult struct {
	From       SlotGroup
	To         SlotGroup
	Slots      int // source slots replayed
	Aggregates int // aggregate streams replayed
	Records    int // records landed in the destination segment
}

// GroupMigrationStatus is the live view of the running (or last) generation
// migration. A real one takes hours — the calibration is ~1 TiB per slot — so it
// cannot be a blocking request: the operator starts it, then watches this.
type GroupMigrationStatus struct {
	State      string    `json:"state"` // idle|staging|replaying|catching|switching|retiring|done|failed
	FromGen    int       `json:"from_gen"`
	FromSize   int32     `json:"from_size"`
	ToGen      int       `json:"to_gen"`
	ToSize     int32     `json:"to_size"`
	Slot       int32     `json:"slot"` // source slot being replayed
	SlotIndex  int       `json:"slot_index"`
	Slots      int       `json:"slots"`
	Aggregates int       `json:"aggregates"`
	GapRecords int64     `json:"gap_records"`
	Records    int       `json:"records"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	Err        string    `json:"error,omitempty"`
}

func (s *GroupMigrationStatus) running() bool {
	switch s.State {
	case "done", "failed":
		return false
	}
	return true
}

// GroupMigrationStatus returns a copy of the current migration view.
func (e *Engine) GroupMigrationStatus() GroupMigrationStatus {
	e.groupMigMu.Lock()
	defer e.groupMigMu.Unlock()
	if e.groupMig == nil {
		return GroupMigrationStatus{State: "idle"}
	}
	return *e.groupMig
}

func (e *Engine) migSet(fn func(*GroupMigrationStatus)) {
	e.groupMigMu.Lock()
	defer e.groupMigMu.Unlock()
	if e.groupMig != nil {
		fn(e.groupMig)
	}
}

// StartGroupExpand launches a generation migration in the background and returns
// the initial status. Only one may run at a time: two replays writing the same
// destination slots would fight over record order.
func (e *Engine) StartGroupExpand(dst string) (GroupMigrationStatus, error) {
	if err := e.controllerGuard(); err != nil {
		return GroupMigrationStatus{}, err
	}
	st := GroupMigrationStatus{State: "staging", StartedAt: time.Now()}
	e.groupMigMu.Lock()
	if e.groupMig != nil && e.groupMig.running() {
		cur := *e.groupMig
		e.groupMigMu.Unlock()
		return cur, fmt.Errorf("a generation migration is already running (%s, %d/%d slots)", cur.State, cur.SlotIndex, cur.Slots)
	}
	e.groupMig = &st
	e.groupMigMu.Unlock()

	go func() {
		// Detached from the caller's request: the replay outlives any HTTP
		// connection. The timeout is a backstop against a wedged peer, not a
		// budget — a segment at the calibration target legitimately takes hours.
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Hour)
		defer cancel()
		res, err := e.groupExpand(ctx, dst)
		e.migSet(func(s *GroupMigrationStatus) {
			s.FinishedAt = time.Now()
			if err != nil {
				s.State = "failed"
				s.Err = err.Error()
				return
			}
			s.State = "done"
			s.FromGen, s.FromSize = res.From.Gen, res.From.Size
			s.ToGen, s.ToSize = res.To.Gen, res.To.Size
			s.Slots, s.Aggregates, s.Records = res.Slots, res.Aggregates, res.Records
		})
	}()
	return st, nil
}

// GroupExpand migrates the served segment into a larger generation: stage the
// destination, replay every aggregate into the slot the new routing names for it,
// switch, and retire the old segment.
//
// The replay is aggregate-by-aggregate on purpose. A generation change is NOT a
// slot-to-slot copy: one source slot's aggregates scatter across several
// destination slots (that is what a bigger segment means), so the unit that
// survives the move is the aggregate, and its identity is the aggregate id plus
// its version — never a slot offset.
func (e *Engine) groupExpand(ctx context.Context, dstName string) (GroupExpandResult, error) {
	if err := e.controllerGuard(); err != nil {
		return GroupExpandResult{}, err
	}
	want, err := ParseSlotGroup(dstName, e.store.SlotCount)
	if err != nil {
		return GroupExpandResult{}, err
	}
	tbl := e.TableSnapshot()
	from := SlotGroup{Gen: tbl.GroupGen, Base: tbl.GroupBase, Size: tbl.GroupSize, Epoch: tbl.LayoutEpoch}
	if want.Size <= from.Size {
		return GroupExpandResult{}, fmt.Errorf("group expand: %s (%d slots) is not larger than the served segment (%d slots)", dstName, want.Size, from.Size)
	}

	if !tbl.segmentStaged(want.Base, want.Size) {
		if err := e.submit(&Command{Op: OpStageGroup, GroupGen: want.Gen, GroupBase: want.Base, GroupSize: want.Size}); err != nil {
			return GroupExpandResult{}, fmt.Errorf("group expand: stage: %w", err)
		}
		if err := e.awaitApplied(ctx, config.HandoverStageTimeout); err != nil {
			return GroupExpandResult{}, err
		}
		tbl = e.TableSnapshot()
	}

	var src []int32
	for s, p := range tbl.Slots {
		if p.State == SlotStable && s >= from.Base && s < from.Base+from.Size {
			src = append(src, s)
		}
	}
	sort.Slice(src, func(i, j int) bool { return src[i] < src[j] })
	res := GroupExpandResult{From: from, To: want, Slots: len(src)}
	e.migSet(func(s *GroupMigrationStatus) {
		s.State = "replaying"
		s.FromGen, s.FromSize = from.Gen, from.Size
		s.ToGen, s.ToSize = want.Gen, want.Size
		s.Slots = len(src)
	})

	for i := 0; i < len(src); i += groupExpandBatch {
		end := i + groupExpandBatch
		if end > len(src) {
			end = len(src)
		}
		if err := e.replaySourceSlots(ctx, tbl, src[i:end], want, &res); err != nil {
			return res, err
		}
	}

	// The replay so far only covers what the source slots held when it ran: while
	// it ran, clients were still writing to the OLD segment (the routing has not
	// switched yet), so the tail of every touched aggregate can be newer than what
	// landed. Freeze the source segment, catch the tails up with a second pass
	// (the replay is idempotent per version, so it only writes what is missing),
	// and only then switch. Without this window the writes that arrived during
	// the replay would exist in the old segment and never in the new one.
	// Approach and seal. The replay runs while the source still serves normally;
	// once the two segments are within sealGap records of each other the source
	// slots go into sealing_out, which answers NEW client writes with ASK pointing
	// at the destination slot. The source log then grows only from writes that were
	// already inside the gate, so the record gap decreases monotonically and the
	// switch is taken at zero — a state, not a sampled LEO that happens to repeat.
	e.migSet(func(s *GroupMigrationStatus) { s.State = "sealing" })
	sealed := false
	quiet := 0
	var frozen map[string][]int32
	for attempt := 0; attempt < 600; attempt++ {
		gap, err := e.segmentGap(ctx, tbl, src, want)
		if err != nil {
			e.thawSourceSlots(ctx, frozen)
			return res, fmt.Errorf("group expand: measure the replay gap: %w", err)
		}
		e.migSet(func(s *GroupMigrationStatus) { s.GapRecords = gap })
		switch {
		case gap > 0:
			quiet = 0
			if !sealed && gap < sealGap {
				e.sealSourceSlots(ctx, src)
				sealed = true
			}
			if err := e.replaySourceSlots(ctx, tbl, src, want, &res); err != nil {
				e.thawSourceSlots(ctx, frozen)
				return res, fmt.Errorf("group expand: replay: %w", err)
			}
		case !sealed:
			// The destination caught up before the sealing threshold (a small or
			// quiet segment): seal anyway, the switch requires it.
			e.sealSourceSlots(ctx, src)
			sealed = true
			quiet++
		default:
			quiet++
		}
		if quiet >= 2 {
			break
		}
		select {
		case <-ctx.Done():
			e.thawSourceSlots(ctx, frozen)
			return res, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}

	e.migSet(func(s *GroupMigrationStatus) { s.State = "switching" })
	if err := e.submit(&Command{Op: OpSetGroup, GroupGen: want.Gen, GroupBase: want.Base, GroupSize: want.Size}); err != nil {
		return res, fmt.Errorf("group expand: switch: %w", err)
	}
	if err := e.awaitApplied(ctx, config.HandoverStageTimeout); err != nil {
		return res, err
	}
	// The switch removed the source slots from the table, so the freeze has nothing
	// left to protect: release it so anything parked wakes and re-routes (the
	// destination answers 1010 to the writes that were waiting there).
	e.thawSourceSlots(ctx, frozen)
	e.migSet(func(s *GroupMigrationStatus) { s.State = "retiring" })
	// Retire the segment being left: its slots are out of the table now, so the
	// only remaining hold on their bytes is the copies on disk. Every node that
	// held a seat gets told to drop it — leaving them would keep a whole segment's
	// worth of storage alive forever and would resurface as phantom copies if the
	// slot id were ever reused.
	// The data plane learns about the switch from the layout feed, not from the
	// Raft apply this call just waited on, so a node still believes it leads its
	// old slots for a few hundred milliseconds — and DropSlot refuses to drop a
	// slot a node leads (a guard that exists to protect live data). Retry, with
	// the drop being idempotent, rather than skip the retirement.
	after := e.TableSnapshot()
	type retireTarget struct {
		node  string
		addr  string
		slots []int32
	}
	pending := map[string]*retireTarget{}
	for _, s := range src {
		for _, node := range tbl.Slots[s].Replicas {
			pe, ok := after.Peers[node]
			if !ok || pe.PeerAddr == "" {
				continue
			}
			t, ok := pending[node]
			if !ok {
				t = &retireTarget{node: node, addr: pe.PeerAddr}
				pending[node] = t
			}
			t.slots = append(t.slots, s)
		}
	}
	for attempt := 0; attempt < 20 && len(pending) > 0; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return res, ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
		}
		for node, t := range pending {
			var still []int32
			for _, s := range t.slots {
				if err := e.peerDropSlot(ctx, t.addr, s, t.node); err != nil {
					still = append(still, s)
				}
			}
			if len(still) == 0 {
				delete(pending, node)
			} else {
				t.slots = still
			}
		}
	}
	for _, t := range pending {
		e.logger.Warn("group expand: some source slots could not be retired", "node", t.node, "slots", len(t.slots))
	}
	res.To.Epoch = after.LayoutEpoch
	return res, nil
}

// replaySourceSlots walks the aggregates of the given source slots and has each
// destination leader pull them in.
func (e *Engine) replaySourceSlots(ctx context.Context, tbl *Table, slots []int32, want SlotGroup, res *GroupExpandResult) error {
	for _, src := range slots {
		e.migSet(func(s *GroupMigrationStatus) { s.Slot = src })
		pl := tbl.Slots[src]
		if pl == nil || pl.Leader == "" {
			return fmt.Errorf("group expand: source slot %d has no leader", src)
		}
		srcAddr := e.peerAddrOf(tbl, pl.Leader)
		if srcAddr == "" {
			return fmt.Errorf("group expand: source slot %d leader %s has no peer address", src, pl.Leader)
		}
		after := ""
		for {
			cli, err := e.peerData(srcAddr)
			if err != nil {
				return err
			}
			page, err := cli.SlotStreams(ctx, &pushupesv1.SlotStreamsRequest{Slot: src, After: after, Limit: 512})
			if err != nil {
				return fmt.Errorf("group expand: list streams of slot %d: %w", src, err)
			}
			for _, agg := range page.GetAggregates() {
				dst := want.Base + data.LogicalSlotOf(agg, int(want.Size))
				dpl, ok := tbl.Slots[dst]
				if !ok || dpl.Leader == "" {
					return fmt.Errorf("group expand: destination slot %d (%s) is not staged with a leader", dst, agg)
				}
				dstAddr := e.peerAddrOf(tbl, dpl.Leader)
				if dstAddr == "" {
					return fmt.Errorf("group expand: destination slot %d leader %s has no peer address", dst, dpl.Leader)
				}
				dcli, err := e.peerData(dstAddr)
				if err != nil {
					return err
				}
				out, err := dcli.ReplayInto(ctx, &pushupesv1.ReplayIntoRequest{
					SrcSlot: src, SrcAddr: srcAddr, DstSlot: dst, AggregateId: agg,
				})
				if err != nil {
					return fmt.Errorf("group expand: replay %s (%d -> %d): %w", agg, src, dst, err)
				}
				res.Aggregates++
				res.Records += int(out.GetLanded())
				e.migSet(func(s *GroupMigrationStatus) {
					s.Aggregates = res.Aggregates
					s.Records = res.Records
					s.Slot = src
				})
			}
			if page.GetNextAfter() == "" {
				break
			}
			after = page.GetNextAfter()
		}
	}
	return nil
}

// peerAddrOf resolves a member's peer-plane address, treating the local node
// like any other: the replay path dials whoever leads the slot, and for the local
// leader that is this process's own peer listener.
func (e *Engine) peerAddrOf(tbl *Table, node string) string {
	if pe, ok := tbl.Peers[node]; ok {
		return pe.PeerAddr
	}
	return ""
}

// replayChunkInto appends one replayed chunk into a LOCAL slot (DESIGN §4.5.8,
// the generation migration's landing half). The source half is
// PeerService.ReplicateAggregate; this is where those bytes become records again
// on the destination.
//
// Version and command id travel with the record, so the replayed history is the
// same history: a client that retries an old command still hits the same
// idempotency key, and a reader sees versions 1..N with no gaps. The append is
// idempotent per (aggregate, version) — a replay that is interrupted and resumed
// from an earlier cursor must not duplicate records, and the migration WILL be
// interrupted (a batch is 128 slots of real data).
//
// A version gap is an error, not something to skip: the replay is ordered, so a
// gap means the caller lost a chunk, and continuing would silently truncate the
// aggregate's history on the far side.
func (e *Engine) replayChunkInto(slot int32, blobs [][]byte) (int, error) {
	applied := 0
	for _, blob := range blobs {
		rec, _, err := data.DecodeRecord(blob)
		if err != nil {
			return applied, fmt.Errorf("replay into slot %d: decode: %w", slot, err)
		}
		tail, err := e.store.TailVersionOf(slot, rec.AggregateID, 0)
		if err != nil {
			return applied, fmt.Errorf("replay into slot %d: tail of %s: %w", slot, rec.AggregateID, err)
		}
		if rec.Version <= tail {
			continue // already landed (a resumed replay)
		}
		if rec.Version != tail+1 {
			return applied, fmt.Errorf("replay into slot %d: aggregate %s jumps from version %d to %d",
				slot, rec.AggregateID, tail, rec.Version)
		}
		seq := e.store.LastSeqOf(slot) + 1
		if err := e.store.AppendAtSeq(slot, seq, &rec); err != nil {
			return applied, fmt.Errorf("replay into slot %d: append %s v%d: %w", slot, rec.AggregateID, rec.Version, err)
		}
		applied++
	}
	return applied, nil
}

// segmentOf names the generation a slot id belongs to: g owns [2^g, 2^(g+1)), so
// the segment is a pure function of the id (DESIGN §4.5, base == size == 2^g).
func segmentOf(slot int32) (base, size int32, ok bool) {
	if slot <= 0 {
		return 0, 0, false
	}
	g := int32(0)
	for (int32(1) << (g + 1)) <= slot {
		g++
	}
	base = int32(1) << g
	return base, base, true
}

// StagingSlotOf answers "where does this aggregate belong in the segment a
// migration is filling?" — the destination slot, when one is staged right now.
//
// It exists because routing only ever names the SERVED segment: a client that is
// redirected to the destination during the sealing window would, by routing, be
// sent straight back to the source. This is the one place that knows the pending
// layout, and it is deliberately narrow: only slots that are importing_in.
func (e *Engine) StagingSlotOf(aggID string) (int32, bool) {
	e.tableMu.RLock()
	defer e.tableMu.RUnlock()
	return e.table.stagingSlotOf(aggID)
}

func (t *Table) stagingSlotOf(aggID string) (int32, bool) {
	var base, size int32
	for s, p := range t.Slots {
		if p.State != SlotImportingIn {
			continue
		}
		b, sz, ok := segmentOf(s)
		if !ok {
			continue
		}
		base, size = b, sz
		break
	}
	if size == 0 {
		return 0, false
	}
	slot := base + data.LogicalSlotOf(aggID, int(size))
	p, ok := t.Slots[slot]
	if !ok || p.State != SlotImportingIn {
		return 0, false
	}
	return slot, true
}

// AwaitTableChange parks until the next layout command is applied (or ctx ends)
// and reports whether one arrived. Callers re-read the table afterwards — the
// condition they were waiting on may or may not have changed.
func (e *Engine) AwaitTableChange(ctx context.Context) bool {
	e.tableMu.RLock()
	ch := e.tableChanged
	e.tableMu.RUnlock()
	if ch == nil {
		return false
	}
	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	}
}

// AwaitImportSettled blocks while a slot is importing_in and returns once it is
// not (the switch landed) or the context ends. A write that arrives during the
// sealing window waits here: it is neither refused nor written into the old
// segment, and the server answers 1010 when it wakes so the client re-routes
// through the new layout.
func (e *Engine) AwaitImportSettled(ctx context.Context, slot int32) bool {
	for {
		e.tableMu.RLock()
		p, ok := e.table.Slots[slot]
		importing := ok && p.State == SlotImportingIn
		e.tableMu.RUnlock()
		if !importing {
			return ok
		}
		if !e.AwaitTableChange(ctx) {
			return false
		}
	}
}

// sealGap is how close the destination has to be before the source slots are
// sealed. Sealing is what stops new client writes, so it must not be entered too
// early (the destination would then be the only writer for a long stretch, and
// every record it takes would be a redirect for a client) nor too late (the gap
// would still be large when the source goes quiet).
const sealGap = 32

// sealSourceSlots puts the source slots into sealing_out: new client writes get
// ASK pointing at the destination, replays continue, and the log stops growing
// once the in-gate writes land. Idempotent — the same state is applied again on a
// retried migration.
func (e *Engine) sealSourceSlots(ctx context.Context, src []int32) {
	if err := e.submit(&Command{Op: OpSlotState, Slots: src, State: SlotSealingOut}); err != nil {
		e.logger.Warn("group expand: sealing the source slots failed", "slots", len(src), "err", err)
		return
	}
	if err := e.awaitApplied(ctx, config.HandoverStageTimeout); err != nil {
		e.logger.Warn("group expand: sealing did not apply in time", "err", err)
	}
}

// segmentGap is the number of source records the destination has not landed yet:
// it compares record COUNTS, because the destination's seq space is its own
// (the replay re-assigns seq) — both sides' LEO is a count of records, so the
// difference is the backlog.
func (e *Engine) segmentGap(ctx context.Context, tbl *Table, src []int32, want SlotGroup) (int64, error) {
	leoOf := func(slot int32) (uint64, error) {
		p := tbl.Slots[slot]
		if p == nil || p.Leader == "" {
			return 0, nil
		}
		addr := e.peerAddrOf(tbl, p.Leader)
		if addr == "" {
			return 0, nil
		}
		cli, err := e.peerData(addr)
		if err != nil {
			return 0, err
		}
		out, err := cli.SlotLeo(ctx, &pushupesv1.SlotLeoRequest{Slot: slot})
		if err != nil {
			return 0, err
		}
		return out.GetLeo(), nil
	}
	var srcTotal, dstTotal int64
	for _, s := range src {
		v, err := leoOf(s)
		if err != nil {
			return 0, err
		}
		srcTotal += int64(v)
	}
	for s := want.Base; s < want.Base+want.Size; s++ {
		v, err := leoOf(s)
		if err != nil {
			return 0, err
		}
		dstTotal += int64(v)
	}
	return srcTotal - dstTotal, nil
}

// freezeSourceSlots closes the write gate on every node that leads a source slot
// and returns what it froze (address → slots) for the matching thaw.
func (e *Engine) freezeSourceSlots(ctx context.Context, tbl *Table, src []int32) map[string][]int32 {
	frozen := map[string][]int32{}
	for _, s := range src {
		p := tbl.Slots[s]
		if p == nil || p.Leader == "" {
			continue
		}
		addr := e.peerAddrOf(tbl, p.Leader)
		if addr == "" {
			continue
		}
		cli, err := e.peerData(addr)
		if err != nil {
			continue
		}
		if _, err := cli.FreezeSlot(ctx, &pushupesv1.FreezeSlotRequest{Slot: s}); err != nil {
			e.logger.Warn("group expand: freezing a source slot failed", "slot", s, "peer", addr, "err", err)
			continue
		}
		frozen[addr] = append(frozen[addr], s)
	}
	return frozen
}

// thawSourceSlots releases whatever freezeSourceSlots closed. Safe with nil and
// safe to call twice (ThawSlot is a no-op on an open gate).
func (e *Engine) thawSourceSlots(ctx context.Context, frozen map[string][]int32) {
	for addr, slots := range frozen {
		cli, err := e.peerData(addr)
		if err != nil {
			continue
		}
		for _, s := range slots {
			if _, err := cli.ThawSlot(ctx, &pushupesv1.ThawSlotRequest{Slot: s}); err != nil {
				// The node may not have applied the switch yet; the thaw is retried
				// by the next migration round and the slot is gone either way.
				e.logger.Warn("group expand: thawing a source slot failed", "slot", s, "peer", addr, "err", err)
			}
		}
	}
}
