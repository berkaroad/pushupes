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

package grpcapi

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/berkaroad/pushupes/internal/cluster"
	"github.com/berkaroad/pushupes/internal/data"
	"github.com/berkaroad/pushupes/internal/storage"
	pushupesv1 "github.com/berkaroad/pushupes/pkg/grpcapi/pushupes/v1"
)

// A leader must be able to read its own durable log. Capping a LEADER's read at
// the replication high watermark turns a complete stream into a SILENT short
// read whenever a replica lags behind — which is exactly how "acked writes come
// back missing from the new leader" was observed. The watermark bound belongs
// to a follower's view, not to the node that owns the log.
func TestLeaderReadIsNotCappedByHighWatermark(t *testing.T) {
	store, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	eng := cluster.NewEngine(nil, store, "node-1", nil)

	for _, id := range []string{"node-1", "node-2", "node-3"} {
		c := &cluster.Command{Op: cluster.OpJoinNode, Peer: &cluster.Peer{Role: cluster.RoleLearner,
			ID: id, PeerAddr: "127.0.0.1:1", AdminAddr: "127.0.0.1:1", ClientAddr: "127.0.0.1:1",
		}}
		if _, err := eng.ApplyCommand(c.Encode()); err != nil {
			t.Fatal(err)
		}
	}
	// Read-path fixtures: the copy count is pinned (production derives it) so
	// the reads under test see the layout they were written against.
	pinTwoReplicas(t, eng)
	if _, err := eng.ApplyCommand((&cluster.Command{Op: cluster.OpPlanSlots}).Encode()); err != nil {
		t.Fatal(err)
	}

	// Find an aggregate whose slot node-1 leads and node-2 replicates (so the
	// watermark can be pinned by a lagging replica).
	tbl := eng.TableSnapshot()
	agg, slot := "", int32(-1)
	follower := ""
	for i := 0; i < 4096 && agg == ""; i++ {
		id := "hw-agg-" + itoa(i)
		s := data.SlotOf(id, 8)
		p, ok := tbl.Slots[s]
		if !ok || p.Leader != "node-1" {
			continue
		}
		// the watermark is the min over the table's replicas including the
		// leader; any other replica will do
		for _, r := range p.Replicas {
			if r != "node-1" {
				follower = r
				break
			}
		}
		if follower == "" {
			continue
		}
		agg, slot = id, s
	}
	if agg == "" {
		t.Skip("no slot led by node-1 with a replica in this table")
	}
	if !eng.Leads(slot) {
		t.Fatalf("test setup: node-1 should lead slot %d", slot)
	}

	// Three durable records, and the replica catches up to them (a seat that
	// has not caught up once is still a copy being built and never gates). Then
	// a fourth record it reports late pins the watermark at 3. Before the fix a
	// leader read stopped at the watermark.
	appendV := func(v int) {
		if _, err := store.Append(&data.EventRecord{
			AggregateID: agg, Version: uint32(v),
			CommandID: "hw-cmd-" + itoa(v),
			Events:    []data.Event{{Type: "E", Body: []byte("x")}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	for v := 1; v <= 3; v++ {
		appendV(v)
	}
	eng.NoteReplicaProgress(slot, follower, 3)
	appendV(4)
	eng.NoteReplicaProgress(slot, follower, 3)
	if hw := eng.HW(slot); hw != 3 {
		t.Fatalf("test setup: want the watermark pinned at 3 by the lagging replica, got %d", hw)
	}

	cli := serveEngine(t, eng, store)
	cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.ReadStream(cctx, &pushupesv1.ReadStreamRequest{AggregateId: agg, FromVersion: 1, Limit: 100})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(resp.Records) != 4 {
		t.Fatalf("leader read returned %d records, want 4 (the durable log must not be capped by HW=%d)",
			len(resp.Records), eng.HW(slot))
	}
}

// A replica's local read is admitted only by PROVEN sync with the slot's
// current leader: a ledger sample taken at the slot's current placement
// epoch whose watermark the copy's durable LEO has reached. This replaces the
// old "a replica reads its whole local log" rule, which let a NOT-YET-SYNCED
// copy (a new seat mid-catch-up) serve acknowledged records it did not hold —
// a short read of acked data.
//
// The same rule closes the historical step-down hole: a node that once led a
// slot keeps leader-side bookkeeping (repl[slot], its frozen watermark) that
// nothing clears when leadership moves. It was never part of admission — the
// read path consults the follower-side ledger only — so a frozen former-leader
// watermark cannot silently truncate anything.
func TestSteppedDownReplicaForwardsUntilReProven(t *testing.T) {
	store, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	eng := cluster.NewEngine(nil, store, "node-1", nil)

	for _, id := range []string{"node-1", "node-2", "node-3"} {
		c := &cluster.Command{Op: cluster.OpJoinNode, Peer: &cluster.Peer{Role: cluster.RoleLearner,
			ID: id, PeerAddr: "127.0.0.1:1", AdminAddr: "127.0.0.1:1", ClientAddr: "127.0.0.1:1",
		}}
		if _, err := eng.ApplyCommand(c.Encode()); err != nil {
			t.Fatal(err)
		}
	}
	// Read-path fixtures: the copy count is pinned (production derives it) so
	// the reads under test see the layout they were written against.
	pinTwoReplicas(t, eng)
	if _, err := eng.ApplyCommand((&cluster.Command{Op: cluster.OpPlanSlots}).Encode()); err != nil {
		t.Fatal(err)
	}

	// An aggregate whose slot node-1 leads and another node replicates, so a
	// watermark can be pinned by a lagging replica in the first place.
	tbl := eng.TableSnapshot()
	agg, slot, follower := "", int32(-1), ""
	for i := 0; i < 4096 && agg == ""; i++ {
		id := "stepdown-agg-" + itoa(i)
		s := eng.SlotOf(id)
		p, ok := tbl.Slots[s]
		if !ok || p.Leader != "node-1" {
			continue
		}
		for _, r := range p.Replicas {
			if r != "node-1" {
				follower = r
				break
			}
		}
		if follower == "" {
			continue
		}
		agg, slot = id, s
	}
	if agg == "" {
		t.Skip("no slot led by node-1 with a replica in this table")
	}

	appendV := func(v int) {
		t.Helper()
		if _, err := store.Append(&data.EventRecord{
			AggregateID: agg, Version: uint32(v),
			CommandID: "sd-cmd-" + itoa(v),
			Events:    []data.Event{{Type: "E", Body: []byte("x")}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Three versions while node-1 leads, and a ledger sample node-1 would have
	// as a replica: proof at the CURRENT epoch (the fixture seeds it directly;
	// production grows it from fetch rounds).
	for v := 1; v <= 3; v++ {
		appendV(v)
	}
	eng.NoteHwLedger(slot, 3)
	if _, _, redirect, _ := eng.LocalRead(slot); redirect != "" {
		t.Fatalf("test setup: synced replica at epoch %d should read locally, redirect=%q", tbl.Slots[slot].Epoch, redirect)
	}

	// node-1 steps down: the table hands the slot to the follower and node-1
	// is left as an ordinary replica (still in the replica set). The epoch
	// bumps; the proof was taken under the OLD leader.
	if _, err := eng.ApplyCommand((&cluster.Command{
		Op: cluster.OpLeaderMove, Slots: []int32{slot}, NewLeader: follower,
	}).Encode()); err != nil {
		t.Fatal(err)
	}
	if eng.Leads(slot) {
		t.Fatalf("test setup: node-1 should no longer lead slot %d", slot)
	}
	// Root cause the OLD rule could not see: the stepped-down node still
	// reports the leader-side watermark frozen at its step-down point. It
	// must report 0 — and the read path never consults it either way.
	if hw := eng.HW(slot); hw != 0 {
		t.Fatalf("stepped-down replica reports frozen watermark %d, want 0: the "+
			"watermark is leader-side bookkeeping and is not valid on a non-leader", hw)
	}
	// The stale proof must NOT admit: forwarding beats serving a log the new
	// leader cannot vouch for.
	if _, _, redirect, _ := eng.LocalRead(slot); redirect == "" {
		t.Fatal("stale-epoch proof admitted a local read after the step-down")
	}

	// One more version lands AFTER the step-down - what the new leader
	// accepts; this replica's log already holds v4 in the fixture (the store
	// is shared with the old leader's writes).
	appendV(4)
	if leo := store.LastSeqOf(slot); leo != 4 {
		t.Fatalf("test setup: local LEO %d want 4", leo)
	}

	// Re-prove sync at the NEW epoch — at a watermark of 2, i.e. the cluster
	// has only confirmed through seq 2. The replica reads LOCALLY but shows
	// exactly the confirmed prefix: v4 in its log is not visible, because it
	// is not acked. The read itself must not fail or forward.
	eng.NoteHwLedger(slot, 2)
	cli := serveEngine(t, eng, store)
	cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.ReadStream(cctx, &pushupesv1.ReadStreamRequest{AggregateId: agg, FromVersion: 1, Limit: 100})
	if err != nil {
		t.Fatalf("read on a re-proven replica: %v", err)
	}
	if len(resp.Records) != 2 {
		t.Fatalf("replica at watermark 2 returned %d records (last version %d): must show exactly the confirmed prefix",
			len(resp.Records), resp.LastVersion)
	}
	for i, r := range resp.Records {
		if r.Version != uint32(i+1) {
			t.Fatalf("record %d has version %d, want %d: silent gap in the stream", i, r.Version, i+1)
		}
	}

	// Advance the ledger to the LEO: the stream is whole again.
	eng.NoteHwLedger(slot, 4)
	resp, err = cli.ReadStream(cctx, &pushupesv1.ReadStreamRequest{AggregateId: agg, FromVersion: 1, Limit: 100})
	if err != nil {
		t.Fatalf("re-read after the ledger advanced: %v", err)
	}
	if len(resp.Records) != 4 || resp.NextVersion != 5 || resp.LastVersion != 4 {
		t.Fatalf("synced-to-LEO replica read returned %d records next/last %d/%d, want 4 5/4",
			len(resp.Records), resp.NextVersion, resp.LastVersion)
	}
}

// A replica whose ledger sample is zero — every record it fetched is still
// un-acknowledged (the ack path parks them above the watermark) — admits the
// read but shows NOTHING. Serving its durable tail would hand the caller
// records the cluster has not committed; the store's 0-means-unbounded
// convention makes a zero ceiling unrepresentable, so the read path answers
// the empty shape itself (serveEmpty).
func TestZeroWatermarkReplicaShowsNothing(t *testing.T) {
	store, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	eng := cluster.NewEngine(nil, store, "node-1", nil)
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		c := &cluster.Command{Op: cluster.OpJoinNode, Peer: &cluster.Peer{Role: cluster.RoleLearner,
			ID: id, PeerAddr: "127.0.0.1:1", AdminAddr: "127.0.0.1:1", ClientAddr: "127.0.0.1:1",
		}}
		if _, err := eng.ApplyCommand(c.Encode()); err != nil {
			t.Fatal(err)
		}
	}
	pinTwoReplicas(t, eng)
	if _, err := eng.ApplyCommand((&cluster.Command{Op: cluster.OpPlanSlots}).Encode()); err != nil {
		t.Fatal(err)
	}

	// A slot node-1 REPLICATES (it does not lead it), with one record for an
	// aggregate on that slot in node-1's log: the copy is real, confirmed is
	// nothing.
	var repSlot int32 = -1
	var repAgg string
	for i := 0; i < 8192 && repSlot < 0; i++ {
		id := "zwhw-agg-" + itoa(i)
		s := eng.SlotOf(id)
		p, ok := eng.TableSnapshot().Slots[s]
		if !ok || p.Leader == "node-1" {
			continue
		}
		for _, r := range p.Replicas {
			if r == "node-1" {
				repSlot, repAgg = s, id
				break
			}
		}
	}
	if repSlot < 0 {
		t.Fatal("test setup: node-1 replicates no slot")
	}
	if _, err := store.Append(&data.EventRecord{
		AggregateID: repAgg, Version: 1, CommandID: "zwhw-cmd",
		Events: []data.Event{{Type: "E", Body: []byte("x")}},
	}); err != nil {
		t.Fatal(err)
	}
	if store.LastSeqOf(repSlot) == 0 {
		t.Fatalf("test setup: record did not land on slot %d", repSlot)
	}

	eng.NoteHwLedger(repSlot, 0)
	upto, serveEmpty, redirect, _ := eng.LocalRead(repSlot)
	if redirect != "" || !serveEmpty || upto != 0 {
		t.Fatalf("zero-watermark replica: upto=%d serveEmpty=%v redirect=%q, want 0/true/local", upto, serveEmpty, redirect)
	}
	// The gRPC answer is the empty stream shape, not an error and not the log.
	cli := serveEngine(t, eng, store)
	cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.ReadStream(cctx, &pushupesv1.ReadStreamRequest{AggregateId: repAgg, FromVersion: 1, Limit: 100})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(resp.Records) != 0 {
		t.Fatalf("zero-watermark replica served %d records from an unconfirmed log", len(resp.Records))
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [12]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}

// pinTwoReplicas pins the table's per-slot copy count for these read-path
// tests: production derives it from the member count, and a three-member
// fixture derives two anyway — stating it keeps the tests independent of the
// policy (they are about ReadTails/reads, not about how many copies a cluster
// of this size should carry).
func pinTwoReplicas(t *testing.T, eng *cluster.Engine) {
	t.Helper()
	c := &cluster.Command{Op: cluster.OpConfig, Replicas: 2}
	if _, err := eng.ApplyCommand(c.Encode()); err != nil {
		t.Fatal(err)
	}
}

// serveEngine wires a gRPC server onto an engine over an in-memory listener.
func serveEngine(t *testing.T, eng *cluster.Engine, store *storage.Store) pushupesv1.EventServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	NewServer(eng, store, nil, "127.0.0.1:8591").Register(gs)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pushupesv1.NewEventServiceClient(conn)
}
