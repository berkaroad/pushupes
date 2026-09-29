package cluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"pushupes/internal/storage"
)

// ---- Migration wire types ---------------------------------------------------

// SegmentFile is one sealed WAL segment streamed to the migration target.
type SegmentFile struct {
	Name    string `json:"name"`
	Payload []byte `json:"payload"`
}

// PushSegmentsRequest carries sealed segments from source to target.
type PushSegmentsRequest struct {
	Slot     int32         `json:"slot"`
	Segments []SegmentFile `json:"segments"`
}

// ---- Engine-side migration steps ---------------------------------------------

// StartMigration drives a slot migration from the controller node. The six
// steps from the design doc:
//
//	1 stage:    mark the slot migrating_out -> toNode (Raft; source forwards writes)
//	2 snapshot: stream sealed segments to the target
//	3 catch-up: target pulls the remainder over the replica fetch protocol
//	4 forward:  state-driven in SubmitAppend (migrating_out makes writes land on both)
//	5 commit:   leader_move to target via Raft (epoch+1)
//	6 cleanup:  old source copy dropped after a retention delay
//
// Only the controller runs this; data transfer is direct source<->target.
func (e *Engine) StartMigration(ctx context.Context, slot int32, toNode string) error {
	if !e.node.IsLeader() {
		return ErrNotLeader
	}
	e.tableMu.RLock()
	p, ok := e.table.Slots[slot]
	e.tableMu.RUnlock()
	if !ok {
		return fmt.Errorf("slot %d unassigned", slot)
	}
	from := p.Leader
	if from == toNode {
		return fmt.Errorf("slot %d already led by %s", slot, toNode)
	}
	if _, ok := e.TableSnapshot().Peers[toNode]; !ok {
		return fmt.Errorf("unknown target node %q", toNode)
	}
	// target must already hold a replica copy (else this is a move, not a
	// balance); for replica_count>=2 the placement guarantees that.
	inSet := false
	for _, r := range p.Replicas {
		if r == toNode {
			inSet = true
		}
	}
	if !inSet {
		return fmt.Errorf("target %s is not a replica of slot %d", toNode, slot)
	}

	// step 1
	if err := e.submit(&Command{Op: OpSlotState, Slots: []int32{slot}, State: SlotMigratingOut, MigratingTo: toNode}); err != nil {
		return fmt.Errorf("stage: %w", err)
	}
	if err := e.awaitApplied(ctx, 5*time.Second); err != nil {
		return err
	}

	// step 2: the source streams its sealed segments (peer plane).
	srcAddr := e.peerAddr(from)
	if srcAddr == "" {
		e.rollbackMigration(slot)
		return fmt.Errorf("no address for source %s", from)
	}
	if err := e.snapshotTo(ctx, srcAddr, slot, toNode); err != nil {
		e.rollbackMigration(slot)
		return fmt.Errorf("snapshot: %w", err)
	}

	// step 3+4: wait until the target has caught up to the source LEO.
	if err := e.awaitCaughtUp(ctx, from, toNode, slot); err != nil {
		e.rollbackMigration(slot)
		return fmt.Errorf("catch-up: %w", err)
	}

	// step 5
	if err := e.submit(&Command{Op: OpLeaderMove, Slots: []int32{slot}, NewLeader: toNode}); err != nil {
		e.rollbackMigration(slot)
		return fmt.Errorf("commit: %w", err)
	}
	if err := e.awaitApplied(ctx, 5*time.Second); err != nil {
		return err
	}

	// step 6
	go e.scheduleDropAfter(ctx, slot, from, 10*time.Minute)
	e.logger.WithFields(map[string]any{"slot": slot, "from": from, "to": toNode}).Info("Slot migration committed")
	return nil
}

// snapshotTo asks the source to push sealed segments over the peer plane;
// when we ARE the source it pushes directly to the target.
func (e *Engine) snapshotTo(ctx context.Context, srcAddr string, slot int32, toNode string) error {
	if srcAddr == e.node.cfg.PeerAddr {
		return e.pushSealedSegments(ctx, slot, toNode)
	}
	sctx, cancel := context.WithTimeout(ctx, peerRPCTimeout)
	defer cancel()
	return e.peerTriggerSnapshot(sctx, srcAddr, slot, toNode)
}

// pushSealedSegments streams all sealed segments of a slot to the target.
func (e *Engine) pushSealedSegments(ctx context.Context, slot int32, toNode string) error {
	sl, err := e.store.Slot(slot)
	if err != nil {
		return err
	}
	segs := sl.SealedSegments()
	if len(segs) == 0 {
		return nil // catch-up via fetch covers everything
	}
	var req PushSegmentsRequest
	req.Slot = slot
	for _, seg := range segs {
		b, err := os.ReadFile(seg.Path)
		if err != nil {
			return err
		}
		req.Segments = append(req.Segments, SegmentFile{Name: filepath.Base(seg.Path), Payload: b})
	}
	sctx, cancel := context.WithTimeout(ctx, peerRPCTimeout)
	defer cancel()
	return e.peerPushSegments(sctx, e.peerAddr(toNode), req)
}

// awaitCaughtUp polls the target's LEO until it reaches the source's LEO.
// The replica fetch loop on the target keeps pulling; migration just waits.
func (e *Engine) awaitCaughtUp(ctx context.Context, from, toNode string, slot int32) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		srcLEO, err := e.remoteLEO(ctx, e.peerAddr(from), slot)
		if err != nil {
			return err
		}
		tgtLEO, err := e.remoteLEO(ctx, e.peerAddr(toNode), slot)
		if err != nil {
			return err
		}
		if tgtLEO >= srcLEO {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("target LEO %d < source LEO %d (timeout)", tgtLEO, srcLEO)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// awaitApplied waits until the local FSM applied everything committed.
func (e *Engine) awaitApplied(ctx context.Context, timeout time.Duration) error {
	num := func(key string) uint64 {
		st := e.node.raft.Stats()
		v, _ := strconv.ParseUint(st[key], 10, 64)
		return v
	}
	target := num("commit-index")
	deadline := time.Now().Add(timeout)
	for num("applied-index") < target {
		if time.Now().After(deadline) {
			return fmt.Errorf("raft apply timeout")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	return nil
}

// rollbackMigration returns a failed migration to stable.
func (e *Engine) rollbackMigration(slot int32) {
	_ = e.submit(&Command{Op: OpSlotState, Slots: []int32{slot}, State: SlotStable})
}

// remoteLEO asks a peer for its LEO of a slot over the peer plane.
func (e *Engine) remoteLEO(ctx context.Context, addr string, slot int32) (uint64, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return e.peerLeo(cctx, addr, slot)
}

// scheduleDropAfter cleans up the old source copy after retention (step 6).
// Only the former source node performs the drop, and only if the committed
// table no longer assigns the slot to it.
func (e *Engine) scheduleDropAfter(ctx context.Context, slot int32, from string, delay time.Duration) {
	if from != e.self {
		return
	}
	select {
	case <-ctx.Done():
		return
	case <-time.After(delay):
	}
	e.tableMu.RLock()
	p, ok := e.table.Slots[slot]
	e.tableMu.RUnlock()
	if ok && p.Leader == e.self {
		return // we own it again (e.g. later failover); keep the data
	}
	if err := e.store.DropSlot(slot); err != nil {
		e.logger.WithError(err).WithField("slot", slot).Warn("post-migration drop failed")
	} else {
		e.logger.WithField("slot", slot).Info("Post-migration source copy dropped")
	}
}

// ---- Target-side handlers (routes wired in the api layer) ---------------------

// HandlePushSegments writes streamed sealed segments into the importing slot
// directory, validating each file's WAL header (magic, slot id) before
// touching disk. Existing identical copies are skipped (retry-friendly).
func (e *Engine) HandlePushSegments(req PushSegmentsRequest) error {
	dir := e.store.SlotDir(req.Slot)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	segs := append([]SegmentFile(nil), req.Segments...)
	sort.Slice(segs, func(i, j int) bool { return segs[i].Name < segs[j].Name })

	// validate every payload before writing any
	for _, s := range segs {
		if len(s.Payload) < storage.WALHeaderLen {
			return fmt.Errorf("segment %s: too short", s.Name)
		}
		slotID, err := storage.HeaderSlotID(s.Payload)
		if err != nil {
			return fmt.Errorf("segment %s: %w", s.Name, err)
		}
		if slotID != req.Slot {
			return fmt.Errorf("segment %s: header slot %d != %d", s.Name, slotID, req.Slot)
		}
	}
	for _, s := range segs {
		path := filepath.Join(dir, s.Name)
		if old, err := os.ReadFile(path); err == nil && len(old) == len(s.Payload) {
			continue // already replicated
		}
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, s.Payload, 0o644); err != nil {
			return err
		}
		if err := os.Rename(tmp, path); err != nil {
			return err
		}
	}
	// reload the slot so catch-up (replica fetch) continues from disk state
	return e.store.ReloadSlot(req.Slot)
}

// HandleSnapshotCommand triggers this node (as source) to push its sealed
// segments of a slot to the target.
func (e *Engine) HandleSnapshotCommand(ctx context.Context, slot int32, toNode string) error {
	return e.pushSealedSegments(ctx, slot, toNode)
}

// HandleLEO answers a slot-LEO probe.
func (e *Engine) HandleLEO(slot int32) uint64 {
	return e.store.LastSeqOf(slot)
}
