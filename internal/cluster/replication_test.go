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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/berkaroad/pushupes/internal/data"
	pushupesv1 "github.com/berkaroad/pushupes/internal/grpcapi/pushupes/v1"
	"github.com/berkaroad/pushupes/internal/lease"
	"github.com/berkaroad/pushupes/internal/payloadcodec"
	"github.com/berkaroad/pushupes/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"net"
	"sync"
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
	// A seat is a copy being built until it reaches the leader's LEO once; from
	// then on it is an ordinary ISR member and its LEO gates the watermark.
	e.NoteReplicaProgress(0, "node-2", 2)
	if hw := e.HW(0); hw != 2 {
		t.Fatalf("hw after catch-up %d want 2", hw)
	}
	// A third record, and the follower reports it late: that lag holds the
	// watermark back — the promise, not a stall to remove.
	if _, err := st.Append(makeRecord(aggH, 3, "h-3")); err != nil {
		t.Fatal(err)
	}
	e.NoteReplicaProgress(0, "node-2", 2)
	if hw := e.HW(0); hw != 2 {
		t.Fatalf("hw after a lagging ordinary replica %d want 2 (leader LEO 3)", hw)
	}
	e.NoteReplicaProgress(0, "node-2", 3)
	if hw := e.HW(0); hw != 3 {
		t.Fatalf("hw after catch-up %d want 3", hw)
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
	sr := e.repl[0]
	positions, hw := 0, uint64(0)
	if sr != nil {
		positions, hw = len(sr.node), sr.hw
	}
	e.replMu.Unlock()
	if positions != 0 || hw != 0 {
		t.Fatalf("a newly gained leader must drop the previous term's stale follower positions: %d position(s), hw %d", positions, hw)
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
	// It has to catch up ONCE first: until then it is a copy being built and is
	// excluded by design (see TestReLayoutSeatDoesNotGateHW). Afterwards a lag
	// does hold the watermark back.
	e.NoteReplicaProgress(3, "node-2", 5)
	if _, err := st.Append(makeRecord(agg3, 6, "hw3-6")); err != nil {
		t.Fatal(err)
	}
	e.NoteReplicaProgress(3, "node-2", 5)
	if hw := e.HW(3); hw != 5 {
		t.Fatalf("an ordinary lagging replica must still gate HW, got %d want 5 (leader LEO 6)", hw)
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
func TestFailoverPicksTheFreshestReplica(t *testing.T) {
	ctrl, _ := newTestEngine(t, "node-1")
	ctrl.node = newTestRaftNode(t, ctrl)

	// Three replicas for slot 0, which node-2 leads: node-3 holds records,
	// node-4 holds none, node-5 is unreachable (no peer harness at all).
	var addrs = map[string]string{}
	for _, id := range []string{"node-3", "node-4"} {
		e, st := newTestEngine(t, id)
		agg := aggInSlot(t, e, 0)
		n := 5
		if id == "node-4" {
			n = 1
		}
		for v := 1; v <= n; v++ {
			if _, err := st.Append(makeRecord(agg, uint32(v), fmt.Sprintf("%s-%d", id, v))); err != nil {
				t.Fatal(err)
			}
		}
		addrs[id] = newPeerHarness(t, e)
	}
	for _, id := range []string{"node-2", "node-3", "node-4", "node-5"} {
		p := Peer{ID: id, PeerAddr: addrs[id], ClientAddr: addrs[id]}
		if _, ok := addrs[id]; !ok {
			// node-5 announced addresses nobody serves: it is a candidate that
			// cannot be reached, so the pick must skip it.
			p = Peer{ID: id, PeerAddr: "127.0.0.1:1", ClientAddr: "http://127.0.0.1:1"}
		}
		applyCmd(t, ctrl, &Command{Op: OpJoinNode, Peer: &p})
	}
	applyCmd(t, ctrl, &Command{Op: OpConfig, Replicas: 3})
	applyCmd(t, ctrl, &Command{Op: OpPlanSlots})
	// One slot of our own, led by node-2 with the three others as replicas.
	applyCmd(t, ctrl, &Command{Op: OpLeaderMove, Slots: []int32{0}, NewLeader: "node-2"})
	if p := ctrl.TableSnapshot().Slots[0]; p.Leader != "node-2" {
		t.Fatalf("setup: slot 0 leader %q", p.Leader)
	}
	ctrl.tableMu.Lock()
	ctrl.table.Slots[0].Leader = "node-2"
	ctrl.table.Slots[0].Replicas = []string{"node-2", "node-3", "node-4", "node-5"}
	ctrl.tableMu.Unlock()

	picks := ctrl.freshestReplicas("node-2")
	if got := picks[0]; got != "node-3" {
		t.Fatalf("freshest replica for slot 0 = %q, want node-3 (node-4 has less, node-5 is unreachable)", got)
	}
	ctrl.failoverPeer("node-2")
	tbl := ctrl.TableSnapshot()
	if !tbl.Peers["node-2"].Down {
		t.Fatal("node-2 must be marked down")
	}
	if got := tbl.Slots[0].Leader; got != "node-3" {
		t.Fatalf("slot 0 moved to %q, want the freshest replica node-3", got)
	}
}
func TestFailoverPrefersTheControllersPick(t *testing.T) {
	tbl := NewTable(8, 3)
	for _, id := range []string{"node-1", "node-2", "node-3", "node-4", "node-5"} {
		tbl.Peers[id] = Peer{ID: id, ClientAddr: "http://" + id + ":8591"}
	}
	tbl.Slots[0] = &Placement{Leader: "node-2", Replicas: []string{"node-2", "node-3", "node-4"}, Epoch: 1, State: SlotStable}
	tbl.Slots[1] = &Placement{Leader: "node-2", Replicas: []string{"node-2", "node-3", "node-4"}, Epoch: 1, State: SlotStable}

	// A valid pick wins over the replica-set order: node-4 holds what node-2
	// acknowledged, node-3 does not.
	if err := tbl.Apply(&Command{Op: OpMarkDown, NodeID: "node-2", NewLeaders: map[int32]string{0: "node-4"}}); err != nil {
		t.Fatal(err)
	}
	if got := tbl.Slots[0].Leader; got != "node-4" {
		t.Fatalf("pick ignored: slot 0 leader %q, want node-4", got)
	}
	if e := tbl.Slots[0].Epoch; e != 2 {
		t.Fatalf("epoch %d want 2 (a leader change bumps it)", e)
	}
	// No pick for slot 1: the table rule decides (first live replica after the
	// failed node).
	if got := tbl.Slots[1].Leader; got != "node-3" {
		t.Fatalf("slot 1 leader %q, want the table rule's node-3", got)
	}

	// Invalid picks fall back: an unknown member, a member that is not a replica,
	// the failed node itself, and an offline replica.
	tbl.Peers["node-4"] = Peer{ID: "node-4"} // drops its announced client addr -> offline
	for i, pick := range []string{"node-9", "node-5", "node-2", "node-4"} {
		s := int32(10 + i)
		tbl.Slots[s] = &Placement{Leader: "node-2", Replicas: []string{"node-2", "node-3", "node-4"}, Epoch: 1, State: SlotStable}
		if err := tbl.Apply(&Command{Op: OpMarkDown, NodeID: "node-2", NewLeaders: map[int32]string{s: pick}}); err != nil {
			t.Fatal(err)
		}
		if got := tbl.Slots[s].Leader; got != "node-3" {
			t.Fatalf("pick %q accepted for slot %d (leader %q): it must fall back to the table rule node-3", pick, s, got)
		}
	}
}

// TestFailoverPicksTheFreshestReplica drives the whole path on real engines: the
// controller asks the candidates for their LEOs over the peer plane, picks the
// one that holds the most, and the mark_down command carries that pick — so the
// slot does not land on a replica that is behind (which would lose the records
// its dead leader had already acknowledged).
func TestFollowerReportReachesController(t *testing.T) {
	ctrl, _ := newTestEngine(t, "node-1")
	ctrl.node = newTestRaftNode(t, ctrl)
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		join(t, ctrl, id, "127.0.0.1:1")
	}
	ctrlAddr := newPeerHarness(t, ctrl)

	// A node that is a FOLLOWER in its own consensus group: it must report to
	// the controller rather than record the evidence itself.
	foll, _ := newTestEngine(t, "node-9")
	follNode, leaderID := newFollowerRaftNode(t)
	foll.node = follNode
	// Its directory maps the controller id it knows to the controller's peer
	// plane; the rest is irrelevant to this hop.
	applyCmd(t, foll, &Command{Op: OpJoinNode, Peer: &Peer{ID: leaderID, PeerAddr: ctrlAddr}})
	applyCmd(t, foll, &Command{Op: OpJoinNode, Peer: &Peer{ID: "node-9", PeerAddr: "127.0.0.1:1"}})

	foll.reportLeaderUnreachable("node-3")

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctrl.unreachMu.Lock()
		witnesses := len(ctrl.unreach["node-3"])
		ctrl.unreachMu.Unlock()
		if witnesses > 0 {
			return // the controller holds the witness
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the follower's unreachable report never reached the controller")
}

// TestConcurrentMFetchRoundsDoNotShareTheScanBuffer is the regression net for
// the fetch round's pooled scan buffer: returning it to the pool while a round
// still owns it lets two rounds hold the same buffer, and the one that resizes
// or resets it truncates it under the other's feet — an
// "index out of range [N] with length 0" panic inside HandleMFetchCtx (seen once
// under real churn, which is what this test reproduces: concurrent PARKED
// rounds of different sizes, cancelled at different moments). It panics when the
// ownership is broken rather than failing an assertion, so it is run as a plain
// test — the fetch path's contract is "one buffer, one owner".
func TestUnreachableQuorumMarksPeerDown(t *testing.T) {
	ctrl, _ := newTestEngine(t, "node-1")
	ctrl.node = newTestRaftNode(t, ctrl) // the controller acts on its own Raft log
	for _, id := range []string{"node-1", "node-2", "node-3", "node-4"} {
		join(t, ctrl, id, "127.0.0.1:1")
	}
	addr := newPeerHarness(t, ctrl)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cli := pushupesv1.NewPeerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	report := func(reporter, suspect string) {
		t.Helper()
		if _, err := cli.ReportUnreachable(ctx, &pushupesv1.ReportUnreachableRequest{Reporter: reporter, Suspect: suspect}); err != nil {
			t.Fatalf("report %s -> %s: %v", reporter, suspect, err)
		}
	}
	down := func(id string) bool { return ctrl.TableSnapshot().Peers[id].Down }

	// One witness: not enough, the node is left alone.
	report("node-2", "node-3")
	if down("node-3") {
		t.Fatal("a single witness must not mark a node down")
	}

	// A second, different observer: quorum. The verdict is submitted off the
	// RPC path (the controller asks the candidates for their LEOs first, so the
	// pick can travel with the command), hence the wait.
	report("node-4", "node-3")
	waitFor(t, 3*time.Second, func() bool { return down("node-3") },
		"two distinct witnesses must mark the node down")

	// The suspect itself, an unknown id, and an already-down node are all
	// ignored (no report storms, no phantom members).
	report("node-3", "node-3")
	report("node-2", "node-99")
	report("node-2", "node-3")
	if !down("node-3") {
		t.Fatal("node-3 must stay down")
	}

	// A witness older than the window must not count towards a quorum.
	ctrl.unreachMu.Lock()
	ctrl.unreach["node-4"] = map[string]time.Time{"node-2": time.Now().Add(-2 * unreachableWindow)}
	ctrl.unreachMu.Unlock()
	report("node-4", "node-4") // self-report: ignored, and must not refresh the table
	if down("node-4") {
		t.Fatal("a stale witness plus an ignored report must not mark node-4 down")
	}
	report("node-2", "node-4")
	if down("node-4") {
		t.Fatalf("one fresh witness must not be a quorum even with a stale one present (window is %s)", unreachableWindow)
	}
	report("node-3", "node-4")
	// (Only the verdict is handed off the RPC path; the quorum decision itself is
	// synchronous, which is why the "not yet down" checks above can read directly.)
	waitFor(t, 3*time.Second, func() bool { return down("node-4") },
		"two fresh witnesses within the window must mark node-4 down")
}

// TestFollowerReportReachesController closes the loop on the sender side: a
// follower that saw a leader's connection die sends its evidence to the
// CONTROLLER (resolved through its own table), and the controller records that
// witness. Everything before the quorum is reached depends on this hop.
func TestTransportUnreachableOnlyCountsTransport(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"connection gone", status.Error(codes.Unavailable, "connection error: connection refused"), true},
		{"wrapped connection gone", fmt.Errorf("fetch round: %w", status.Error(codes.Unavailable, "x")), true},
		{"slow server", status.Error(codes.DeadlineExceeded, "context deadline exceeded"), false},
		{"session stopped here", status.Error(codes.Canceled, "context canceled"), false},
		{"application error", status.Error(codes.Internal, "mfetch: invalid"), false},
		{"routing answer", data.ErrNotLeader, false},
		{"no error", nil, false},
	}
	for _, c := range cases {
		if got := transportUnreachable(c.err); got != c.want {
			t.Errorf("%s: transportUnreachable = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestUnreachableQuorumMarksPeerDown drives the witness path through the real
// peer plane: one observer is not evidence, two distinct observers within the
// window are, and the controller submits the same mark_down the probe sweep
// would have submitted — just seconds earlier.
func TestDialRefusedDistinguishesDeadFromSlow(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()

	if dialRefused(ln.Addr().String()) {
		t.Fatalf("a listening port must not read as refused (%s)", ln.Addr())
	}
	dead := ln.Addr().String()
	ln.Close() // nothing listens there any more
	if !dialRefused(dead) {
		t.Fatalf("a closed port must read as refused (%s): the sweep is what turns that into an immediate mark_down", dead)
	}
}

// TestTransportUnreachableOnlyCountsTransport pins which fetch-round failures
// count as evidence that a node is unreachable: the connection itself failing.
// A slow server (deadline), a session this node stopped (cancellation), a
// routing answer or an application error are not — reporting those would mark a
// healthy leader down, which is exactly what the probe threshold guards against.
func TestTakeoverKeepsBuildingSeatsOutOfTheHW(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// slot 0 (led by node-1) gains a third seat: a copy being built.
	applyCmd(t, e, &Command{Op: OpSlotAddReplica, Slots: []int32{0}, NodeID: "node-3"})
	agg := aggInSlot(t, e, 0)
	for v := uint32(1); v <= 5; v++ {
		if _, err := st.Append(makeRecord(agg, v, fmt.Sprintf("take-%d", v))); err != nil {
			t.Fatal(err)
		}
	}
	e.NoteReplicaProgress(0, "node-2", 5)
	e.NoteReplicaProgress(0, "node-3", 0) // still fetching
	if hw := e.HW(0); hw != 5 {
		t.Fatalf("setup: a seat being built must not gate, HW %d want the leader LEO 5", hw)
	}

	// The slot changes leaders and comes back: that is a takeover for node-1.
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{0}, NewLeader: "node-2"})
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{0}, NewLeader: "node-1"})

	// The new term's positions arrive — the still-fetching seat reports first,
	// as a real fetch round does — and the next write follows it.
	e.NoteReplicaProgress(0, "node-3", 0)
	e.NoteReplicaProgress(0, "node-2", 5)
	if _, err := st.Append(makeRecord(agg, 6, "take-6")); err != nil {
		t.Fatal(err)
	}
	e.NoteReplicaProgress(0, "node-2", 6)
	if hw := e.HW(0); hw != 6 {
		t.Fatalf("after a takeover a still-fetching seat pinned HW at %d, want the leader LEO 6", hw)
	}
}

// TestDialRefusedDistinguishesDeadFromSlow pins the fast liveness signal the
// controller's sweep acts on: a port with nothing listening REFUSES the
// connection (that node's process is gone, act now), while a port that is
// accepting is not a failure at all. Anything else — a timeout, a drop — must
// NOT read as "dead": those are what the strike threshold is for.
func TestMarkedDownPeerReleasesHW(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	pinReplicas(t, e, 2) // a leader plus a follower needs two copies (see pinReplicas)
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	agg := aggInSlot(t, e, 0)
	for v := uint32(1); v <= 5; v++ {
		if _, err := st.Append(makeRecord(agg, v, fmt.Sprintf("down-%d", v))); err != nil {
			t.Fatal(err)
		}
	}
	// Catch it up once (a copy being built never gates), then let it fall behind:
	// an ordinary ISR member gates.
	e.NoteReplicaProgress(0, "node-2", 5)
	if _, err := st.Append(makeRecord(agg, 6, "down-6")); err != nil {
		t.Fatal(err)
	}
	e.NoteReplicaProgress(0, "node-2", 5)
	if hw := e.HW(0); hw != 5 {
		t.Fatalf("setup: HW %d want 5 (leader LEO 6)", hw)
	}
	if p, _ := e.TableSnapshot().Slots[0]; !replicaListHas(p.Replicas, "node-2") {
		t.Fatalf("setup: node-2 must still hold its seat: %v", p.Replicas)
	}

	applyCmd(t, e, &Command{Op: OpMarkDown, NodeID: "node-2"})
	if hw := e.HW(0); hw != 6 {
		t.Fatalf("a marked-down peer pinned HW at %d, want the leader LEO 6", hw)
	}
	if p, _ := e.TableSnapshot().Slots[0]; !replicaListHas(p.Replicas, "node-2") {
		t.Fatalf("mark_down must keep the seat (only its vote on the watermark goes away): %v", p.Replicas)
	}
}

// TestTakeoverKeepsBuildingSeatsOutOfTheHW pins the takeover half of the
// building-seat rule. A slot can change leaders while one of its seats is still
// fetching — the rebalancer's hand-over gate checks the TARGET's copy, not the
// other seats' — and the new leader inherits those positions with no leadership
// history of its own. If it counts a still-fetching seat as an ordinary in-sync
// replica, the very stall the mark removes comes back at every hand-over.
func TestFollowerSessionLossReleasesHW(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	pinReplicas(t, e, 2) // a leader plus a follower needs two copies (see pinReplicas)
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// slot 0 is led by node-1 with node-2 as its in-sync replica.
	agg := aggInSlot(t, e, 0)
	for v := uint32(1); v <= 5; v++ {
		if _, err := st.Append(makeRecord(agg, v, fmt.Sprintf("loss-%d", v))); err != nil {
			t.Fatal(err)
		}
	}
	// node-2 catches up once (a copy still being built never gates), then falls
	// one record behind: now it is an ordinary ISR member and it gates.
	e.NoteReplicaProgress(0, "node-2", 5)
	if _, err := st.Append(makeRecord(agg, 6, "loss-6")); err != nil {
		t.Fatal(err)
	}
	e.NoteReplicaProgress(0, "node-2", 5)
	if hw := e.HW(0); hw != 5 {
		t.Fatalf("setup: a live lagging replica must gate, HW %d want 5 (leader LEO 6)", hw)
	}

	// node-2 is killed: its session ends without an answer.
	e.noteFollowerSessionLost("node-2", []int32{0})
	if hw := e.HW(0); hw != 6 {
		t.Fatalf("a dead follower's position pinned HW at %d, want the leader LEO 6", hw)
	}
	if isr := e.ISR(0); seatListHas(isr, "node-2") {
		t.Fatalf("a follower whose session died must not read as in-sync: %v", isr)
	}

	// It comes back and reports: it is in-sync again and gates again.
	e.NoteReplicaProgress(0, "node-2", 5)
	if isr := e.ISR(0); !seatListHas(isr, "node-2") {
		t.Fatalf("a reconnected replica must read as in-sync again: %v", isr)
	}
	if _, err := st.Append(makeRecord(agg, 7, "loss-7")); err != nil {
		t.Fatal(err)
	}
	e.NoteReplicaProgress(0, "node-2", 6)
	if hw := e.HW(0); hw != 6 {
		t.Fatalf("a live lagging replica must gate again, HW %d want 6 (leader LEO 7)", hw)
	}
}

// TestMarkedDownPeerReleasesHW: the replicated liveness verdict is the slow half
// (it takes consecutive probe rounds), and when it lands the marked-down peer's
// seats are still in the replica sets — it must stop holding the watermark back
// from that moment, without waiting for its seats to be removed or for the
// staleness window to close.
func TestReLayoutSeatDoesNotGateHW(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// slot 0 is led by node-1 with node-2 as its in-sync replica.
	agg := aggInSlot(t, e, 0)
	for v := uint32(1); v <= 5; v++ {
		if _, err := st.Append(makeRecord(agg, v, fmt.Sprintf("relayout-%d", v))); err != nil {
			t.Fatal(err)
		}
	}

	// counter-case FIRST, before any re-layout: a seat that was already in the
	// set is an ordinary ISR member — behind, it gates (the acknowledged-write
	// promise). slot 3 is on the same terms.
	agg3 := aggInSlot(t, e, 3)
	for v := uint32(1); v <= 5; v++ {
		if _, err := st.Append(makeRecord(agg3, v, fmt.Sprintf("relayout3-%d", v))); err != nil {
			t.Fatal(err)
		}
	}
	// It catches up once (until then it is a copy being built, by design), and
	// from then on it is an ordinary ISR member: a lag holds the watermark back.
	e.NoteReplicaProgress(3, "node-2", 5)
	if _, err := st.Append(makeRecord(agg3, 6, "relayout3-6")); err != nil {
		t.Fatal(err)
	}
	e.NoteReplicaProgress(3, "node-2", 5)
	if hw := e.HW(3); hw != 5 {
		t.Fatalf("a seat that was in the set from the start must gate while behind, got HW %d want 5 (leader LEO 6)", hw)
	}

	// The re-layout adds node-3 to slot 0 (OpReplanSlots adds the ring's missing
	// seats; the command is the same one it submits).
	applyCmd(t, e, &Command{Op: OpSlotAddReplica, Slots: []int32{0}, NodeID: "node-3"})

	// The fresh seat reports an empty log: the watermark must follow the
	// leader's LEO instead of waiting for the copy to be built.
	e.NoteReplicaProgress(0, "node-3", 0)
	if hw := e.HW(0); hw != 5 {
		t.Fatalf("a seat being built pinned HW at %d, want the leader LEO 5", hw)
	}
	if isr := e.ISR(0); seatListHas(isr, "node-3") {
		t.Fatalf("a seat being built must not read as in-sync: %v", isr)
	}

	// It catches up: the mark clears, it reads as in-sync again...
	e.NoteReplicaProgress(0, "node-3", 5)
	if hw := e.HW(0); hw != 5 {
		t.Fatalf("HW after the catch-up %d, want 5", hw)
	}
	if isr := e.ISR(0); !seatListHas(isr, "node-3") {
		t.Fatalf("a caught-up seat must read as in-sync: %v", isr)
	}

	// ...and from now on it gates like any other in-sync replica.
	if _, err := st.Append(makeRecord(agg, 6, "relayout-6")); err != nil {
		t.Fatal(err)
	}
	e.NoteReplicaProgress(0, "node-3", 5)
	if hw := e.HW(0); hw != 5 {
		t.Fatalf("a replica that caught up and fell behind again must gate, got HW %d want 5", hw)
	}
}

// TestFollowerSessionLossReleasesHW pins the fast half of "a replica that is no
// longer being served must not pin the writes": a killed node's fetch long poll
// ends without an answer, and from that moment its frozen LEO must stop gating
// the slot's watermark — otherwise every slot it replicated stays pinned until
// its seats leave the table (a batch workload shows that as seconds of 0 msg/s,
// because one pinned slot stalls a whole batch).

// TestSlotReplSeatBookkeeping pins the marker itself: which seats a re-layout
// marks as "being built" and when the mark goes away.
func TestSlotReplSeatBookkeeping(t *testing.T) {
	sr := &slotRepl{}

	// Taking the slot over records the seats present and marks the ones still
	// young (seated within the grace: plausibly still fetching) as copies being
	// built — this leader has no history to tell them from established members.
	sr.takeover([]string{"node-1", "node-2"}, []string{"node-2"})
	if !sr.isBuilding("node-2") {
		t.Fatalf("a young seat at takeover is a copy being built: seats=%v building=%v", sr.seats, sr.building)
	}
	if sr.isBuilding("node-1") {
		t.Fatalf("only the young seats are marked: %v", sr.building)
	}
	sr.dropBuilding("node-2") // it caught up: an ordinary member from now on

	// A seat that appears LATER (a re-layout adding the ring's seats) is a copy
	// being built.
	sr.syncSeats([]string{"node-1", "node-2", "node-3"})
	if !sr.isBuilding("node-3") {
		t.Fatalf("a seat added while this node leads must be marked as building: seats=%v building=%v", sr.seats, sr.building)
	}
	if sr.isBuilding("node-2") {
		t.Fatalf("an inherited seat must stay unmarked: %v", sr.building)
	}

	// Caught up: the mark clears and a LATER lag gates like any other replica.
	sr.dropBuilding("node-3")
	if sr.isBuilding("node-3") {
		t.Fatalf("a caught-up seat must lose its mark: %v", sr.building)
	}

	// A seat that leaves the set loses its mark; the same node coming back is a
	// fresh transition and is marked again (its data may be stale by then).
	sr.syncSeats([]string{"node-1", "node-3"})
	if sr.isBuilding("node-3") {
		t.Fatalf("a seat that stays in the set must not be re-marked: %v", sr.building)
	}
	sr.syncSeats([]string{"node-1", "node-3", "node-2"})
	if !sr.isBuilding("node-2") {
		t.Fatalf("a seat that re-joined the set is a copy being built again: %v", sr.building)
	}
	if len(sr.seats) != 3 {
		t.Fatalf("the recorded set must mirror the table's: %v", sr.seats)
	}
}

// TestReLayoutSeatDoesNotGateHW pins the behaviour the marker exists for: a
// seat a re-layout added must not hold the slot's watermark back while it
// fetches, and must gate again once it has caught up.
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
