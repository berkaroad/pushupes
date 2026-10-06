package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pushupes/internal/data"
	pushupesv1 "pushupes/internal/grpcapi/pushupes/v1"
	"pushupes/internal/lease"
	"sync"
	"pushupes/internal/payloadcodec"
	"pushupes/internal/storage"
)

// aggInSlot finds an aggregate id that routes to the given slot.
func aggInSlot(t *testing.T, e *Engine, slot int32) string {
	t.Helper()
	for i := 0; i < 100000; i++ {
		cand := fmt.Sprintf("agg-%d", i)
		if e.SlotOf(cand) == slot {
			return cand
		}
	}
	t.Fatalf("no aggregate lands on slot %d", slot)
	return ""
}

func makeRecord(agg string, version uint32, cmd string) *data.EventRecord {
	return &data.EventRecord{
		AggregateID: agg,
		Version:     version,
		UnixTime:    time.Now().Unix(),
		CommandID:   cmd,
		Events:      []data.Event{{Type: "T", Body: []byte("{}")}},
	}
}

func TestHandleFetchLeaderGuard(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// slot 1 is led by node-2 in a 2-node ring; a fetch round must answer it
	// empty (failover in flight), while our own slot 0 is served normally.
	resp, err := e.HandleMFetch(MFetchRequest{Slots: []int32{1, 0}, FromSeqs: []uint64{1, 1}})
	if err != nil {
		t.Fatalf("mfetch: %v", err)
	}
	// Sparse response: neither slot has data (1 is not led here, 0 is
	// empty), so neither appears — and no error either way.
	if _, ok := resp.Item(1); ok {
		t.Fatalf("non-led slot must be absent: %+v", resp.Items)
	}
	if it, ok := resp.Item(0); ok && (it.NextSeq != 0 || len(it.Payload) != 0) {
		t.Fatalf("empty slot must not carry data: %+v", it)
	}
}

func TestFetchPayloadRoundTrip(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// write 3 records into slot 0 (single-node plan: node-1 leads all)
	aggX := aggInSlot(t, e, 0)
	for v := uint64(1); v <= 3; v++ {
		out, err := st.Append(makeRecord(aggX, uint32(v), fmt.Sprintf("c-%d", v)))
		if err != nil || out.Status != data.StatusSuccess {
			t.Fatalf("append v%d: %+v %v", v, out, err)
		}
	}
	resp, err := e.HandleMFetch(MFetchRequest{Slots: []int32{0}, FromSeqs: []uint64{1}})
	if err != nil {
		t.Fatal(err)
	}
	it, ok := resp.Item(0)
	if !ok || it.NextSeq != 4 {
		t.Fatalf("next_seq want 4, got %+v", resp.Items)
	}
	// decode the payload back and replicate into a second store
	e2, st2 := newTestEngine(t, "node-2")
	join(t, e2, "node-1", "127.0.0.1:1")
	applyCmd(t, e2, &Command{Op: OpPlanSlots})
	// make node-2 the replica holder in its own table view
	applyCmd(t, e2, &Command{Op: OpSlotState, Slots: []int32{0}, State: SlotStable})

	rest := it.Payload
	seq := uint64(1)
	for len(rest) > 0 {
		rec, consumed, err := data.DecodeRecord(rest)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if err := st2.AppendAtSeq(0, seq, &rec); err != nil {
			t.Fatalf("appendatseq %d: %v", seq, err)
		}
		rest = rest[consumed:]
		seq++
	}
	if got := st2.LastSeqOf(0); got != 3 {
		t.Fatalf("replica LEO %d want 3", got)
	}
	recs, _, err := st2.ReadAggregate(aggX, 1, 10, 0)
	if err != nil || len(recs) != 3 || recs[0].CommandID != "c-1" {
		t.Fatalf("replica read: %v %+v", err, recs)
	}
}

func TestNoteReplicaProgressAdvancesHW(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	// The watermark is tracked from replica progress, so the fixture needs a
	// follower: two members derive ONE copy, hence the explicit two here.
	pinReplicas(t, e, 2)
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// 2 records on the leader, in the slot this node leads (0)
	aggH := aggInSlot(t, e, 0)
	for v := uint64(1); v <= 2; v++ {
		if _, err := st.Append(makeRecord(aggH, uint32(v), fmt.Sprintf("h-%d", v))); err != nil {
			t.Fatal(err)
		}
	}
	if hw := e.HW(0); hw != 0 {
		t.Fatalf("initial hw %d want 0", hw)
	}
	e.NoteReplicaProgress(0, "node-2", 1)
	if hw := e.HW(0); hw != 1 {
		t.Fatalf("hw after lagging replica %d want 1", hw)
	}
	e.NoteReplicaProgress(0, "node-2", 2)
	if hw := e.HW(0); hw != 2 {
		t.Fatalf("hw after catch-up %d want 2", hw)
	}
	if got := e.ISR(0); len(got) != 1 || got[0] != "node-2" {
		t.Fatalf("ISR %v want [node-2]", got)
	}
	// stale replica leaves ISR; with ISR={leader} only, the HW follows the
	// leader's LEO (acknowledgement falls back to a leader-only
	// guarantee until the replica re-syncs, never a silent stall).
	e.replMu.Lock()
	e.repl[0].setLastOK("node-2", time.Now().Add(-time.Minute))
	e.replMu.Unlock()
	if got := e.ISR(0); len(got) != 0 {
		t.Fatalf("ISR after staleness %v want empty", got)
	}
	if _, err := st.Append(makeRecord(aggH, 3, "h-3")); err != nil {
		t.Fatal(err)
	}
	e.advanceHW(0)
	if hw := e.HW(0); hw != 3 {
		t.Fatalf("hw with ISR shrunk to leader should follow LEO 3, got %d", hw)
	}
}

func TestReplicateRecordIdempotent(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	rec := makeRecord("agg-f", 1, "f-1")
	buf := make([]byte, rec.EncodedSize())
	buf = rec.EncodeBinary(buf[:0])

	if err := e.HandleReplicate(0, 1, buf); err != nil {
		t.Fatalf("first replicate: %v", err)
	}
	// replay at the same seq must be a no-op (migration forward + fetch race)
	if err := e.HandleReplicate(0, 1, buf); err != nil {
		t.Fatalf("replay replicate: %v", err)
	}
	if leo := st.LastSeqOf(0); leo != 1 {
		t.Fatalf("LEO after replay %d want 1", leo)
	}
	// divergent payload at an old seq must be refused
	other := makeRecord("agg-f", 7, "f-7")
	ob := make([]byte, other.EncodedSize())
	ob = other.EncodeBinary(ob[:0])
	if err := e.HandleReplicate(0, 1, ob); err == nil {
		t.Fatal("expected divergence error")
	}
}

func TestSubmitAppendRedirects(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// find an aggregate landing on a slot led by node-2 (odd slots)
	agg := ""
	for i := 0; i < 1000 && agg == ""; i++ {
		cand := fmt.Sprintf("agg-%d", i)
		if slot := e.SlotOf(cand); slot%2 == 1 {
			agg = cand
		}
	}
	if agg == "" {
		t.Fatal("no odd-slot aggregate found")
	}
	_, err := e.SubmitAppend(context.Background(), makeRecord(agg, 1, "r-1"))
	rd, ok := err.(*RedirectError)
	if !ok {
		t.Fatalf("expected RedirectError, got %v", err)
	}
	if rd.Kind != data.ErrIDSlotNotLocal || rd.Node != "node-2" || rd.Addr != "127.0.0.1:2" {
		t.Fatalf("redirect: %+v", rd)
	}
	if want := e.SlotOf(agg); rd.Slot != want {
		t.Fatalf("redirect slot %d want %d", rd.Slot, want)
	}
}

func TestSubmitAppendMigratingRedirectsToSource(t *testing.T) {
	e, _ := newTestEngine(t, "node-2") // we follow but do not lead slot 0
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	applyCmd(t, e, &Command{Op: OpPlanSlots})
	// slot 0: leader=node-1. Mark it migrating to node-2.
	applyCmd(t, e, &Command{Op: OpSlotState, Slots: []int32{0}, State: SlotMigratingOut, MigratingTo: "node-2"})

	// An aggregate in slot 0 — that is the slot the state above was set on, so
	// search for one that routes *there*, not for one that merely shares its
	// parity with it (a coincidence that used to hold and then stopped).
	agg := ""
	for i := 0; i < 20000 && agg == ""; i++ {
		cand := fmt.Sprintf("agg-%d", i)
		if e.SlotOf(cand) == 0 {
			agg = cand
		}
	}
	_, err := e.SubmitAppend(context.Background(), makeRecord(agg, 1, "m-1"))
	rd, ok := err.(*RedirectError)
	if !ok {
		t.Fatalf("expected redirect, got %v", err)
	}
	if rd.Kind != data.ErrIDMigrating || rd.Addr != "127.0.0.1:1" {
		t.Fatalf("migrating redirect: %+v", rd)
	}
}

// ---- migration abort path ---------------------------------------------------

// fakeChunks plays back a chunk list through the chunkSource interface.
type fakeChunks struct {
	chunks []*pushupesv1.PushSegmentsRequest
	i      int
}

func (f *fakeChunks) Recv() (*pushupesv1.PushSegmentsRequest, error) {
	if f.i >= len(f.chunks) {
		return nil, io.EOF
	}
	c := f.chunks[f.i]
	f.i++
	return c, nil
}

func TestWriteSegmentsRejectsCorrupt(t *testing.T) {
	e, _ := newTestEngine(t, "node-2")
	// header claims slot 5 but the stream says slot 0
	seg, err := storage.CreateSegment(t.TempDir(), 5, 1)
	if err != nil {
		t.Fatal(err)
	}
	segPath := seg.Path
	seg.Close()
	raw, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatal(err)
	}
	err = e.writeSegments(&fakeChunks{chunks: []*pushupesv1.PushSegmentsRequest{
		{Slot: 0, Name: "x.wal", Size: uint64(len(raw))},
		{Slot: 0, Data: raw},
	}})
	if err == nil || !strings.Contains(err.Error(), "header slot") {
		t.Fatalf("expected slot mismatch refusal, got %v", err)
	}
	// first chunk shorter than the WAL header and the stream then ends:
	// the header never assembles, so the file comes up short
	err = e.writeSegments(&fakeChunks{chunks: []*pushupesv1.PushSegmentsRequest{
		{Slot: 0, Name: "y.wal", Size: 8},
		{Slot: 0, Data: raw[:8]},
	}})
	if err == nil || !strings.Contains(err.Error(), "got 0 of 8") {
		t.Fatalf("expected incomplete-stream refusal, got %v", err)
	}
	// announced size never fully arrives (valid slot-0 header, one byte short)
	seg0, err := storage.CreateSegment(t.TempDir(), 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := seg0.Append(1, makeRecord("agg-1", 1, "c-1")); err != nil {
		t.Fatal(err)
	}
	seg0.Close()
	raw0, err := os.ReadFile(seg0.Path)
	if err != nil {
		t.Fatal(err)
	}
	err = e.writeSegments(&fakeChunks{chunks: []*pushupesv1.PushSegmentsRequest{
		{Slot: 0, Name: "z.wal", Size: uint64(len(raw0))},
		{Slot: 0, Data: raw0[:len(raw0)-1]},
	}})
	if err == nil || !strings.Contains(err.Error(), "bytes") {
		t.Fatalf("expected incomplete-stream refusal, got %v", err)
	}
	// nothing landed, no .tmp debris
	entries, _ := os.ReadDir(e.store.SlotDir(0))
	for _, en := range entries {
		t.Fatalf("unexpected file after refused imports: %s", en.Name())
	}
}

func TestWriteSegmentsAcceptsGoodStream(t *testing.T) {
	e, _ := newTestEngine(t, "node-2")
	dir := t.TempDir()
	seg, err := storage.CreateSegment(dir, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := seg.Append(1, makeRecord("agg-1", 1, "c-1")); err != nil {
		t.Fatal(err)
	}
	segPath := seg.Path
	seg.Close()
	raw, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(segPath)
	// split into 3-byte chunks: header must survive streaming regardless
	var chunks []*pushupesv1.PushSegmentsRequest
	chunks = append(chunks, &pushupesv1.PushSegmentsRequest{Slot: 0, Name: name, Size: uint64(len(raw))})
	for off := 0; off < len(raw); off += 3 {
		end := min(off+3, len(raw))
		chunks = append(chunks, &pushupesv1.PushSegmentsRequest{Slot: 0, Data: raw[off:end]})
	}
	if err := e.writeSegments(&fakeChunks{chunks: chunks}); err != nil {
		t.Fatalf("good stream refused: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(e.store.SlotDir(0), name))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(raw) {
		t.Fatalf("imported %d bytes want %d", len(got), len(raw))
	}
}

// TestMigratingForwardFailureKeepsWriteAndSlot replaces the old
// "a failed write forward rolls the slot back" expectation. A dead migration
// target (nothing listening on its peer address) must not fail the client's
// write — it is already durable in the source WAL — and must not roll the
// whole migration back either. The source stays leader, keeps serving, and
// leaves the abort decision to the migration controller's own catch-up
// timeout (awaitCaughtUp), which is the only place with the whole picture.
func TestMigratingForwardFailureKeepsWriteAndSlot(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	e.node = newTestRaftNode(t, e)

	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:9") // nothing listening there
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// 2 records so there is content, in slot 0 which node-1 leads.
	aggAB := aggInSlot(t, e, 0)
	for v := uint64(1); v <= 2; v++ {
		if _, err := st.Append(makeRecord(aggAB, uint32(v), fmt.Sprintf("ab-%d", v))); err != nil {
			t.Fatal(err)
		}
	}
	applyCmd(t, e, &Command{Op: OpSlotState, Slots: []int32{0}, State: SlotMigratingOut, MigratingTo: "node-2"})
	e.syncMigrationState()
	e.fwdMu.RLock()
	_, forwarding := e.fwd[0]
	e.fwdMu.RUnlock()
	if !forwarding {
		t.Fatal("expected forwarding map populated during migrating_out")
	}

	// The write during the window is served by the source and its mirror push
	// to the dead target fails fast: the write must still be acknowledged.
	out, err := e.SubmitAppend(context.Background(), makeRecord(aggAB, 3, "ab-3"))
	if err != nil {
		t.Fatalf("a failed migration push must not fail the client write: %v", err)
	}
	if out == nil || out.Status != data.StatusSuccess || out.Seq != 3 {
		t.Fatalf("write not acknowledged off the local copy: %+v", out)
	}
	if leo := st.LastSeqOf(0); leo != 3 {
		t.Fatalf("source LEO %d want 3", leo)
	}
	// the slot keeps migrating: only a per-write push failed, not the migration
	if p, _ := e.TableSnapshot().Slots[0]; p.State != SlotMigratingOut || p.MigratingTo != "node-2" {
		t.Fatalf("a failed push rolled the migration back: %+v", p)
	}
	// a replayed command after the failed push is still idempotent
	out, err = e.SubmitAppend(context.Background(), makeRecord(aggAB, 3, "ab-3"))
	if err != nil || out.Status != data.StatusExists {
		t.Fatalf("post-push replay: %+v %v", out, err)
	}
}

// TestMigratingForwardNotContiguousKeepsWriteAndMigration is the regression
// test for the migration-forward stall. The target is a LIVE peer that does
// not hold the slot yet (its seq counter is 0 — a snapshot/catch-up still in
// flight, or a replica the post-migration cleanup recycled), so the push of
// seq 3 is refused with "seq 3 not contiguous (counter 0)". That refusal is an
// internal catch-up fact, not a client failure: before the fix the engine
// returned it to the client AND rolled the migration back, which turned every
// in-flight append in the migration window into a failure — and the client's
// retry (which takes the EXISTS path) into another one. Now the write is
// acknowledged off its durable local copy, the slot keeps migrating, and the
// target is left to catch up over the ordinary fetch loop.
func TestMigratingForwardNotContiguousKeepsWriteAndMigration(t *testing.T) {
	payloadcodec.InstallCodec()
	leader, st := newTestEngine(t, "node-1")
	leader.node = newTestRaftNode(t, leader)

	// a live target peer whose local copy of the slot does not exist yet
	tgt, tgtStore := newTestEngine(t, "node-2")
	tgtAddr := newPeerHarness(t, tgt)

	join(t, leader, "node-1", "127.0.0.1:1")
	join(t, leader, "node-2", tgtAddr)
	applyCmd(t, leader, &Command{Op: OpPlanSlots})

	agg := aggInSlot(t, leader, 0)
	for v := uint64(1); v <= 2; v++ {
		if _, err := st.Append(makeRecord(agg, uint32(v), fmt.Sprintf("nc-%d", v))); err != nil {
			t.Fatal(err)
		}
	}
	applyCmd(t, leader, &Command{Op: OpSlotState, Slots: []int32{0}, State: SlotMigratingOut, MigratingTo: "node-2"})
	leader.syncMigrationState()

	resp, err := leader.SubmitAppend(context.Background(), makeRecord(agg, 3, "nc-3"))
	if err != nil {
		t.Fatalf("a forward the target refuses as non-contiguous must not fail the write: %v", err)
	}
	if resp == nil || resp.Status != data.StatusSuccess || resp.Seq != 3 {
		t.Fatalf("write not acknowledged off the local copy: %+v", resp)
	}
	if leo := st.LastSeqOf(0); leo != 3 {
		t.Fatalf("leader LEO %d want 3", leo)
	}
	// the migration was NOT rolled back and the slot is still writable here
	if p, _ := leader.TableSnapshot().Slots[0]; p.State != SlotMigratingOut || p.MigratingTo != "node-2" {
		t.Fatalf("migration must stay staged, got %+v", p)
	}
	// the target genuinely had no prefix: the push really was refused
	if leo := tgtStore.LastSeqOf(0); leo != 0 {
		t.Fatalf("target LEO %d want 0 (the push must really have failed)", leo)
	}
}

// TestLeaderGainDropsStaleFollowerHW pins the leadership-change reset. A node
// that starts leading a slot used to keep the previous term's follower
// positions in its replication bookkeeping; their LEOs are as of the old
// leader's last sample (often far below this node's own LEO) and stay "fresh"
// for the staleness window, so the watermark froze below the leader
// LEO until each follower happened to report again — the seconds-long pause
// right after a migration's leader move. Gaining leadership must drop them.
func TestLeaderGainDropsStaleFollowerHW(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// hand slot 0 to node-2, then take it back: the take-back is a leadership
	// GAIN for node-1.
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{0}, NewLeader: "node-2"})
	if p, _ := e.TableSnapshot().Slots[0]; p.Leader != "node-2" {
		t.Fatalf("setup: slot 0 leader %q", p.Leader)
	}
	// the previous term's follower bookkeeping, exactly as advanceHW would
	// have left it: fresh stamp, far-behind LEO.
	e.replMu.Lock()
	e.repl[0] = &slotRepl{node: []string{"node-2"}, leo: []uint64{1}, lastOK: []time.Time{time.Now()}, hw: 1}
	e.replMu.Unlock()

	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{0}, NewLeader: "node-1"})
	if p, _ := e.TableSnapshot().Slots[0]; p.Leader != "node-1" {
		t.Fatalf("setup: slot 0 not back on node-1: %q", p.Leader)
	}
	e.replMu.Lock()
	_, kept := e.repl[0]
	e.replMu.Unlock()
	if kept {
		t.Fatal("a newly gained leader must drop the previous term's stale follower positions")
	}
}

// TestMigratingForwardExpires pins the migrating_out timeout fallback: a slot
// that has been migrating_out past migrationForwardTimeout stops mirroring,
// so a target that never catches up cannot keep every write paying for a
// doomed push (the persistent-collapse path). Returning to stable clears the
// deadline so a later migration starts with a fresh window.
func TestMigrationForwardExpires(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	e.node = newTestRaftNode(t, e)
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:9")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	applyCmd(t, e, &Command{Op: OpSlotState, Slots: []int32{0}, State: SlotMigratingOut, MigratingTo: "node-2"})
	e.syncMigrationState()
	if e.migrationForwardExpired(0) {
		t.Fatal("a freshly staged migration must still mirror")
	}
	e.migFwdMu.Lock()
	if _, ok := e.migFwdSince[0]; !ok {
		e.migFwdMu.Unlock()
		t.Fatal("staging must record the forward deadline")
	}
	e.migFwdSince[0] = time.Now().Add(-migrationForwardTimeout - time.Second)
	e.migFwdMu.Unlock()
	if !e.migrationForwardExpired(0) {
		t.Fatal("a migration older than the timeout must stop mirroring")
	}

	applyCmd(t, e, &Command{Op: OpSlotState, Slots: []int32{0}, State: SlotStable})
	e.syncMigrationState()
	e.migFwdMu.Lock()
	_, still := e.migFwdSince[0]
	e.migFwdMu.Unlock()
	if still {
		t.Fatal("a slot back to stable must not keep a forward deadline")
	}
}

// TestMigrationTargetDoesNotGateHW pins the stall fix. A migration target that
// is still catching up is an EXTRA copy the migration is building, not one of
// the replicas the slot's durability promise rests on, so it must not freeze
// the high watermark at its low LEO. Counting it made the watermark lag the
// leader's LEO and every append block to the 10s wait deadline — the
// seconds-long pause around a migration. An ordinary (non-target) lagging
// replica still gates, so the acknowledged-write promise is not weakened.
func TestMigrationTargetDoesNotGateHW(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// slot 0 (led by node-1): add node-3 as the migration target.
	applyCmd(t, e, &Command{Op: OpSlotAddReplica, Slots: []int32{0}, NodeID: "node-3"})
	agg := aggInSlot(t, e, 0)
	for v := uint64(1); v <= 5; v++ {
		if _, err := st.Append(makeRecord(agg, uint32(v), fmt.Sprintf("hw-%d", v))); err != nil {
			t.Fatal(err)
		}
	}
	applyCmd(t, e, &Command{Op: OpSlotState, Slots: []int32{0}, State: SlotMigratingOut, MigratingTo: "node-3"})

	// The target reports a far-behind LEO and nothing else has reported: the
	// watermark must not be pinned at 1 (pre-fix) but follow the leader's LEO.
	e.NoteReplicaProgress(0, "node-3", 1)
	if hw := e.HW(0); hw != 5 {
		t.Fatalf("a catching-up migration target pinned HW at %d, want the leader LEO 5", hw)
	}

	// counter-case, on a non-migrating slot led by this node (slot 3): an
	// ordinary lagging replica still holds the watermark back.
	agg3 := aggInSlot(t, e, 3)
	for v := uint64(1); v <= 5; v++ {
		if _, err := st.Append(makeRecord(agg3, uint32(v), fmt.Sprintf("hw3-%d", v))); err != nil {
			t.Fatal(err)
		}
	}
	e.NoteReplicaProgress(3, "node-2", 2)
	if hw := e.HW(3); hw != 2 {
		t.Fatalf("an ordinary lagging replica must still gate HW, got %d want 2", hw)
	}
}

func TestHandleFetchAndReplicateOverPeerPlane(t *testing.T) {
	// The payload codec is installed here too, so this exercises the production
	// decode path — including the receive-buffer leases the follower takes on
	// fetched payloads and releases once they are written.
	payloadcodec.InstallCodec()
	leasesBefore := lease.Outstanding()
	// Wire two engines through the real PeerService gRPC plane (over the
	// peer-port mux demux) to prove replication end to end: fetch round,
	// WAL replay on the follower, LEO report advancing leader HW.
	leader, ldrStore := newTestEngine(t, "node-1")
	join(t, leader, "node-1", "127.0.0.1:1")
	join(t, leader, "node-2", "127.0.0.1:2")
	applyCmd(t, leader, &Command{Op: OpPlanSlots})
	addr := newPeerHarness(t, leader)

	follower, fdrStore := newTestEngine(t, "node-2")
	join(t, follower, "node-1", addr)
	join(t, follower, "node-2", "127.0.0.1:2")
	applyCmd(t, follower, &Command{Op: OpPlanSlots})

	// seed leader WAL (slot 0 is led by node-1)
	aggE := aggInSlot(t, leader, 0)
	for v := uint64(1); v <= 3; v++ {
		out, _ := ldrStore.Append(makeRecord(aggE, uint32(v), fmt.Sprintf("e-%d", v)))
		if out.Status != data.StatusSuccess {
			t.Fatalf("seed %d: %+v", v, out)
		}
	}
	// drive one replica fetch round (session loop's unit of work)
	if _, err := follower.fetchRound(context.Background(), "node-1", []int32{0}, true); err != nil {
		t.Fatalf("fetchRound: %v", err)
	}
	if leo := follower.store.LastSeqOf(0); leo != 3 {
		t.Fatalf("follower LEO %d want 3", leo)
	}
	// the fetch also reported LEO -> leader HW advanced
	deadline := time.Now().Add(2 * time.Second)
	for leader.HW(0) < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if hw := leader.HW(0); hw != 3 {
		t.Fatalf("leader HW %d want 3", hw)
	}

	// The follower must have landed the leader's bytes verbatim: replication
	// writes frames through, it does not decode and re-encode them.
	_, _, want, err := ldrStore.ReadSlotBytes(0, 1, 4, 1<<20)
	if err != nil {
		t.Fatalf("leader ReadSlotBytes: %v", err)
	}
	_, _, got, err := fdrStore.ReadSlotBytes(0, 1, 4, 1<<20)
	if err != nil {
		t.Fatalf("follower ReadSlotBytes: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("follower WAL differs from leader WAL: %d vs %d bytes", len(got), len(want))
	}
	if _, n, err := data.DecodeRecordMeta(got); err != nil || n <= 0 {
		t.Fatalf("follower payload does not start with a valid frame: n=%d err=%v", n, err)
	}
	if got := lease.Outstanding(); got != leasesBefore {
		t.Fatalf("%d receive-buffer leases left outstanding after the fetch round", got-leasesBefore)
	}
	_ = storage.WALHeaderLen
}
func TestConcurrentMFetchRoundsDoNotShareTheScanBuffer(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	pinReplicas(t, e, 2)
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// Each round covers a different number of slots, so a buffer shared
			// by two rounds is resized to different lengths while in use.
			n := 8 + w*40
			slots := make([]int32, 0, n)
			froms := make([]uint64, 0, n)
			for i := 0; i < n; i++ {
				slots = append(slots, int32(i%8)) // slots 0,2,4,6 are led here
				froms = append(froms, 1<<40)      // far ahead of every LEO: empty → parks
			}
			for iter := 0; iter < 150; iter++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
				if _, err := e.HandleMFetchCtx(ctx, MFetchRequest{
					Follower: "node-2", WaitMS: 50, Slots: slots, FromSeqs: froms,
				}); err != nil && ctx.Err() == nil {
					t.Errorf("unexpected mfetch error: %v", err)
				}
				cancel()
			}
		}(w)
	}
	wg.Wait()
}

// TestFailoverPrefersTheControllersPick pins the failover rule's two halves: the
// controller's pick (the freshest live replica, computed from real LEOs it alone
// can see) is honoured, and anything that does not hold up as a placement falls
// back to the deterministic table-only rule (the first live replica). The pick
// travels in the replicated command, so every node must reach the same verdict
// from the same table.
