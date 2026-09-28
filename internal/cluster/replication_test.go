package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"pushupes/internal/data"
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

func makeRecord(agg string, version uint64, cmd string) *data.EventRecord {
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
	resp, err := e.HandleMFetch(MFetchRequest{Items: []FetchItem{{Slot: 1, FromSeq: 1}, {Slot: 0, FromSeq: 1}}})
	if err != nil {
		t.Fatalf("mfetch: %v", err)
	}
	if resp.Items[0].NextSeq != 0 || len(resp.Items[0].Payload) != 0 {
		t.Fatalf("non-led slot must be empty: %+v", resp.Items[0])
	}
	if resp.Items[1].LeaderLEO != 0 || resp.Items[1].NextSeq != 1 {
		t.Fatalf("empty slot resp: %+v", resp.Items[1])
	}
}

func TestFetchPayloadRoundTrip(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// write 3 records into slot 0 (single-node plan: node-1 leads all)
	aggX := aggInSlot(t, e, 0)
	for v := uint64(1); v <= 3; v++ {
		out, err := st.Append(makeRecord(aggX, v, fmt.Sprintf("c-%d", v)))
		if err != nil || out.Status != data.StatusSuccess {
			t.Fatalf("append v%d: %+v %v", v, out, err)
		}
	}
	resp, err := e.HandleMFetch(MFetchRequest{Items: []FetchItem{{Slot: 0, FromSeq: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Items[0].NextSeq != 4 {
		t.Fatalf("next_seq %d want 4", resp.Items[0].NextSeq)
	}
	// decode the payload back and replicate into a second store
	e2, st2 := newTestEngine(t, "node-2")
	join(t, e2, "node-1", "127.0.0.1:1")
	applyCmd(t, e2, &Command{Op: OpPlanSlots})
	// make node-2 the replica holder in its own table view
	applyCmd(t, e2, &Command{Op: OpSlotState, Slots: []int32{0}, State: SlotStable})

	rest := resp.Items[0].Payload
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
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// 2 records on the leader, in the slot this node leads (0)
	aggH := aggInSlot(t, e, 0)
	for v := uint64(1); v <= 2; v++ {
		if _, err := st.Append(makeRecord(aggH, v, fmt.Sprintf("h-%d", v))); err != nil {
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
	// leader's LEO — Kafka semantics (acks=all degrades to a leader-only
	// guarantee until the replica re-syncs, never a silent stall).
	e.replMu.Lock()
	e.repl[0].lastOK["node-2"] = time.Now().Add(-time.Minute)
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
	_, err := e.SubmitAppend(context.Background(), makeRecord(agg, 1, "r-1"), "leader")
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

	// aggregate in slot 0 (even) — must be redirected to the source leader with ASK/MIGRATING
	agg := ""
	for i := 0; i < 1000 && agg == ""; i++ {
		cand := fmt.Sprintf("agg-%d", i)
		if e.SlotOf(cand)%2 == 0 {
			agg = cand
		}
	}
	_, err := e.SubmitAppend(context.Background(), makeRecord(agg, 1, "m-1"), "leader")
	rd, ok := err.(*RedirectError)
	if !ok {
		t.Fatalf("expected redirect, got %v", err)
	}
	if rd.Kind != data.ErrIDMigrating || rd.Addr != "127.0.0.1:1" {
		t.Fatalf("migrating redirect: %+v", rd)
	}
}

// ---- migration abort path ---------------------------------------------------

func TestHandlePushSegmentsRejectsCorrupt(t *testing.T) {
	e, _ := newTestEngine(t, "node-2")
	// header claims slot 5 but request says slot 0
	hdr := make([]byte, storage.WALHeaderLen)
	copy(hdr, []byte("ESWL"))
	slotIn := int32(5)
	// rebuild a valid header via CreateSegment on a scratch dir instead of
	// hand-rolling bytes: simpler to let storage produce it.
	seg, err := storage.CreateSegment(t.TempDir(), slotIn, 1)
	if err != nil {
		t.Fatal(err)
	}
	segPath := seg.Path
	seg.Close()
	raw, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatal(err)
	}
	err = e.HandlePushSegments(PushSegmentsRequest{Slot: 0, Segments: []SegmentFile{{Name: "x.wal", Payload: raw}}})
	if err == nil || !strings.Contains(err.Error(), "header slot") {
		t.Fatalf("expected slot mismatch refusal, got %v", err)
	}
	// truncated payload
	err = e.HandlePushSegments(PushSegmentsRequest{Slot: 0, Segments: []SegmentFile{{Name: "y.wal", Payload: raw[:8]}}})
	if err == nil || !strings.Contains(err.Error(), "too short") {
		t.Fatalf("expected too-short refusal, got %v", err)
	}
}

func TestSnapshotFailureRollsBackSlotState(t *testing.T) {
	// Source (node-1) leads slot 0; migration target node-2 is dead ->
	// the write forward during migrating_out fails -> the engine must roll
	// the table back to stable so the source keeps serving untouched.
	e, st := newTestEngine(t, "node-1")
	e.node = newTestRaftNode(t, e)

	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:9") // nothing listening there
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// 2 records so there is content, in slot 0 which node-1 leads.
	aggAB := aggInSlot(t, e, 0)
	for v := uint64(1); v <= 2; v++ {
		if _, err := st.Append(makeRecord(aggAB, v, fmt.Sprintf("ab-%d", v))); err != nil {
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

	// A write during the window is served by the source and forwarded; the
	// dead target makes the forward fail and the engine must abort the slot.
	e.httpC = &http.Client{Timeout: 2 * time.Second}
	_, err := e.SubmitAppend(context.Background(), makeRecord(aggAB, 3, "ab-3"), "leader")
	if err == nil {
		t.Fatal("expected forward failure")
	}
	p, _ := e.TableSnapshot().Slots[0]
	if p.State != SlotStable || p.MigratingTo != "" {
		t.Fatalf("slot must be rolled back to stable, got %+v", p)
	}
	// the source WAL still holds all three records — no data was lost
	if leo := st.LastSeqOf(0); leo != 3 {
		t.Fatalf("source LEO %d want 3", leo)
	}
	// replayed command after abort: exists again via idempotency
	out, err := e.SubmitAppend(context.Background(), makeRecord(aggAB, 3, "ab-3"), "leader")
	if err != nil || out.Status != data.StatusExists {
		t.Fatalf("post-abort replay: %+v %v", out, err)
	}
}

func TestHandleFetchAndReplicateOverHTTP(t *testing.T) {
	// Wire two engines through httptest to prove the internal endpoints
	// actually drive replication end to end (fetch + LEO report).
	leader, ldrStore := newTestEngine(t, "node-1")
	join(t, leader, "node-1", "127.0.0.1:1")
	join(t, leader, "node-2", "127.0.0.1:2")
	applyCmd(t, leader, &Command{Op: OpPlanSlots})
	ldrMux := http.NewServeMux()
	ldrMux.HandleFunc("POST /internal/mfetch", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		req, err := DecodeMRequest(raw)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := leader.HandleMFetch(*req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		w.Write(EncodeMResponse(resp))
	})
	ldrMux.HandleFunc("POST /internal/replica-progress", func(w http.ResponseWriter, r *http.Request) {
		var p struct {
			Follower string      `json:"follower"`
			Slot     int32       `json:"slot"`
			LEO      uint64      `json:"leo"`
			Items    []FetchItem `json:"items"`
		}
		json.NewDecoder(r.Body).Decode(&p)
		if len(p.Items) > 0 {
			for _, it := range p.Items {
				if it.FromSeq > 0 {
					leader.NoteReplicaProgress(it.Slot, p.Follower, it.FromSeq-1)
				}
			}
		} else {
			leader.NoteReplicaProgress(p.Slot, p.Follower, p.LEO)
		}
		w.WriteHeader(http.StatusOK)
	})
	ts := httptest.NewServer(ldrMux)
	defer ts.Close()
	// rewrite node-1 peer addr to the test server
	applyCmd(t, leader, &Command{Op: OpJoinNode, Peer: &Peer{ID: "node-1", PeerAddr: "x", AdminAddr: strings.TrimPrefix(ts.URL, "http://")}})

	follower, _ := newTestEngine(t, "node-2")
	join(t, follower, "node-1", strings.TrimPrefix(ts.URL, "http://"))
	join(t, follower, "node-2", "127.0.0.1:2")
	applyCmd(t, follower, &Command{Op: OpPlanSlots})

	// seed leader WAL (slot 0 is led by node-1)
	aggE := aggInSlot(t, leader, 0)
	for v := uint64(1); v <= 3; v++ {
		out, _ := ldrStore.Append(makeRecord(aggE, v, fmt.Sprintf("e-%d", v)))
		if out.Status != data.StatusSuccess {
			t.Fatalf("seed %d: %+v", v, out)
		}
	}
	// drive one replica fetch round (session loop's unit of work)
	if _, err := follower.fetchRound(context.Background(), "node-1", []int32{0}); err != nil {
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
	_ = storage.WALHeaderLen
}
