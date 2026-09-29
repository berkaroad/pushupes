package cluster

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	pushupesv1 "pushupes/internal/grpcapi/pushupes/v1"
	"pushupes/internal/storage"
)

// ---- Migration stream bounds ---------------------------------------------------

// snapshotChunkBytes bounds one streamed segment chunk: the source reads
// sealed segment files through one reusable window instead of holding whole
// segments (up to 256MiB each) in memory, and the target assembles into a
// .tmp file. It matches the replica fetch per-item payload cap.
const snapshotChunkBytes = 4 << 20

// snapshotStreamTimeout bounds a whole snapshot transfer: a slot may carry
// hundreds of MiB, so this is far above the unary peerRPCTimeout.
const snapshotStreamTimeout = 10 * time.Minute

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

// pushSealedSegments streams all sealed segments of a slot to the target in
// bounded chunks, reading each WAL file through one reusable buffer — the
// source's memory stays O(chunk), not O(slot).
func (e *Engine) pushSealedSegments(ctx context.Context, slot int32, toNode string) error {
	sl, err := e.store.Slot(slot)
	if err != nil {
		return err
	}
	segs := sl.SealedSegments()
	if len(segs) == 0 {
		return nil // catch-up via fetch covers everything
	}
	sctx, cancel := context.WithTimeout(ctx, snapshotStreamTimeout)
	defer cancel()
	stream, err := e.peerOpenPush(sctx, e.peerAddr(toNode))
	if err != nil {
		return err
	}
	fail := func(err error) error {
		_, _ = stream.CloseAndRecv()
		return err
	}
	buf := make([]byte, snapshotChunkBytes)
	for _, seg := range segs {
		f, err := os.Open(seg.Path)
		if err != nil {
			return fail(err)
		}
		name := filepath.Base(seg.Path)
		st, err := f.Stat()
		if err != nil {
			f.Close()
			return fail(err)
		}
		if err := stream.Send(&pushupesv1.PushSegmentsRequest{Slot: slot, Name: name, Size: uint64(st.Size())}); err != nil {
			f.Close()
			return err // stream already dead; CloseAndRecv would mask the cause
		}
		var sent int64
		for sent < st.Size() {
			n, err := f.Read(buf)
			if n > 0 {
				if serr := stream.Send(&pushupesv1.PushSegmentsRequest{Slot: slot, Data: buf[:n]}); serr != nil {
					f.Close()
					return serr
				}
				sent += int64(n)
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				return fail(fmt.Errorf("segment %s: %w", name, err))
			}
		}
		f.Close()
	}
	_, err = stream.CloseAndRecv()
	return err
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

// ---- Target-side handlers (invoked from the peer gRPC server) -----------------

// chunkSource is the receive half of a PushSegments stream, extracted as an
// interface so the importer can be unit-tested without a live gRPC server.
type chunkSource interface {
	Recv() (*pushupesv1.PushSegmentsRequest, error)
}

// writeSegments consumes a PushSegments chunk stream and lands the sealed
// segment files into the importing slot directory. Each file streams into a
// "<name>.tmp" — memory stays O(chunk), never O(segment). The WAL header
// (magic/version + slot id) is assembled from the first WALHeaderLen bytes
// (chunks may split anywhere), validated before anything is written to the
// file, and a file whose existing copy already matches the announced size is
// drained, not rewritten (retry-friendly). Incomplete totals are rejected
// and the .tmp removed.
func (e *Engine) writeSegments(recv chunkSource) error {
	var slot int32
	seenSlot := false
	var dir string

	var cur *os.File // open .tmp; nil while draining a duplicate file
	var path, tmpPath string
	var want, got uint64
	var hdr [storage.WALHeaderLen]byte
	var hdrGot int
	var hdrOK bool

	abort := func() {
		if cur != nil {
			cur.Close()
			_ = os.Remove(tmpPath)
			cur = nil
		}
	}
	finalize := func() error {
		if cur == nil {
			return nil
		}
		if got != want {
			abort()
			return fmt.Errorf("segment %s: got %d of %d bytes", filepath.Base(path), got, want)
		}
		if err := cur.Close(); err != nil {
			cur = nil
			_ = os.Remove(tmpPath)
			return err
		}
		cur = nil
		return os.Rename(tmpPath, path)
	}

	for {
		chunk, err := recv.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			abort()
			return err
		}
		if !seenSlot {
			slot, seenSlot = chunk.Slot, true
			dir = e.store.SlotDir(slot)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				abort()
				return err
			}
		} else if chunk.Slot != slot {
			abort()
			return fmt.Errorf("snapshot stream: slot %d != %d", chunk.Slot, slot)
		}

		if chunk.Name != "" { // descriptor: a new file begins
			if err := finalize(); err != nil {
				abort()
				return err
			}
			path = filepath.Join(dir, filepath.Base(chunk.Name))
			tmpPath = path + ".tmp"
			want, got, hdrGot, hdrOK = chunk.Size, 0, 0, false
			if st, err := os.Stat(path); err == nil && st.Size() == int64(chunk.Size) {
				cur = nil // duplicate: drain this file's chunks untouched
				continue
			}
			cur, err = os.Create(tmpPath)
			if err != nil {
				abort()
				return err
			}
		}
		if cur == nil || len(chunk.Data) == 0 {
			continue // draining a duplicate
		}
		data := chunk.Data

		// assemble + validate the WAL header before the first write
		if !hdrOK {
			n := copy(hdr[hdrGot:], data)
			hdrGot += n
			data = data[n:]
			if hdrGot < storage.WALHeaderLen {
				continue // header split across chunks: accumulate
			}
			slotID, err := storage.HeaderSlotID(hdr[:])
			if err != nil {
				abort()
				return fmt.Errorf("segment %s: %w", filepath.Base(path), err)
			}
			if slotID != slot {
				abort()
				return fmt.Errorf("segment %s: header slot %d != %d", filepath.Base(path), slotID, slot)
			}
			hdrOK = true
			if _, err := cur.Write(hdr[:]); err != nil {
				abort()
				return err
			}
			got += storage.WALHeaderLen
		}
		if len(data) == 0 {
			continue
		}
		if _, err := cur.Write(data); err != nil {
			abort()
			return err
		}
		got += uint64(len(data))
	}
	if err := finalize(); err != nil {
		abort()
		return err
	}
	if !seenSlot {
		return nil // empty stream: nothing imported
	}
	// reload the slot so catch-up (replica fetch) continues from disk state
	return e.store.ReloadSlot(slot)
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
