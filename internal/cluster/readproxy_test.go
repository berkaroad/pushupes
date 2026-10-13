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
	"strconv"
	"testing"

	"github.com/berkaroad/pushupes/internal/data"
	"github.com/berkaroad/pushupes/internal/storage"
)

// LocalRead decides, on every read RPC, whether this node serves a slot
// locally or forwards it to the slot leader — and with what visibility
// ceiling when it serves. The contract:
//
//   - the slot LEADER serves locally, unbounded (own durable LEO): a leader
//     read capped at a watermark would hide records it already acknowledged;
//   - a REPLICA serves locally only once its sync is PROVEN: a ledger sample
//     (the fetch round's HW echo) taken at the slot's CURRENT placement
//     epoch, with this copy's durable LEO at or above the sampled watermark.
//     Ceiling = that watermark: exactly the confirmed prefix, no more.
//   - a sample of zero admits the replica but shows NOTHING (nothing has
//     been confirmed yet — serving the log tail would leak unacked records
//     the store's 0-means-unbounded convention cannot express as a ceiling);
//   - an unproven copy (new seat, lagging, or one whose proof predates a
//     failover: the epoch key invalidates it) and any NON-HOLDER forward.
//
// These tests pin the answer table itself; the forwarding it prevents is a
// silently short read of acknowledged data.
func TestLocalReadMatchesTableSemantics(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	tbl := e.TableSnapshot()
	if len(tbl.Slots) == 0 {
		t.Fatal("test setup: no placements")
	}

	locals, forwards := 0, 0
	for slot, p := range tbl.Slots {
		_, _, proxy, _ := e.LocalRead(slot)
		switch {
		case p.Leader == e.self:
			if proxy != "" {
				t.Fatalf("slot %d: leader must serve locally, got proxy %q", slot, proxy)
			}
			locals++
		case replicaListHas(p.Replicas, e.self):
			// No ledger sample yet: unproven sync — forward.
			if proxy != tbl.Peers[p.Leader].ClientAddr {
				t.Fatalf("slot %d: unsynced replica must forward to the leader client addr %q, got %q",
					slot, tbl.Peers[p.Leader].ClientAddr, proxy)
			}
			forwards++
		default:
			if proxy != tbl.Peers[p.Leader].ClientAddr {
				t.Fatalf("slot %d: non-holder must forward to %q, got %q", slot, tbl.Peers[p.Leader].ClientAddr, proxy)
			}
			forwards++
		}
	}
	if locals == 0 || forwards == 0 {
		t.Fatalf("fixture must exercise both branches (locals=%d forwards=%d)", locals, forwards)
	}

	// An unassigned slot answers locally (empty), never a panic or a bogus
	// forward.
	for s := int32(0); s < 8192; s++ {
		if _, ok := tbl.Slots[s]; !ok {
			if _, _, proxy, _ := e.LocalRead(s); proxy != "" {
				t.Errorf("unassigned slot %d: got proxy %q, want local empty", s, proxy)
			}
			break
		}
	}
}

// The three local-serve shapes on one replicated slot, isolated so a change
// in one fails as that case rather than as an unexplained mismatch.
func TestLocalReadReplicaShapes(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})
	tbl := e.TableSnapshot()

	var replicated int32 = -1
	for s, p := range tbl.Slots {
		if p.Leader != "node-1" && replicaListHas(p.Replicas, "node-1") {
			replicated = s
			break
		}
	}
	if replicated < 0 {
		t.Fatal("test setup: node-1 replicates no slot")
	}
	wantLeaderAddr := tbl.Peers[tbl.Slots[replicated].Leader].ClientAddr

	// Shape 1: unsynced (no sample) — forward.
	if _, _, proxy, _ := e.LocalRead(replicated); proxy != wantLeaderAddr {
		t.Fatalf("unsynced replica: proxy=%q, want %q", proxy, wantLeaderAddr)
	}

	// Shape 2: sampled at zero — admitted but empty (NOT unbounded: the
	// store reads a 0 ceiling as "whole log", which would leak).
	e.NoteHwLedger(replicated, 0)
	upto, serveEmpty, proxy, _ := e.LocalRead(replicated)
	if proxy != "" || !serveEmpty || upto != 0 {
		t.Fatalf("zero-watermark replica: upto=%d empty=%v proxy=%q, want 0/true/\"\"", upto, serveEmpty, proxy)
	}

	// Shape 3: synced at LEO with records on disk — local, ceiling = the
	// sampled watermark.
	appendOne := func(agg string) {
		if _, err := st.Append(&data.EventRecord{
			AggregateID: agg, Version: 1, CommandID: "cmd-" + agg,
			Events: []data.Event{{Type: "E", Body: []byte("x")}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	// one record ON this slot, then seed the watermark at its LEO. The
	// aggregate must hash to the chosen slot or the LEO never moves and the
	// shape under test degenerates (pick by the slot function, like the table
	// itself does).
	agg := ""
	for i := 0; i < 4096 && agg == ""; i++ {
		id := "lr-synced-" + strconv.Itoa(i)
		if e.SlotOf(id) == replicated {
			agg = id
		}
	}
	if agg == "" {
		t.Fatal("test setup: no aggregate hashes to the replicated slot")
	}
	appendOne(agg)
	leo := st.LastSeqOf(replicated)
	if leo == 0 {
		t.Fatalf("test setup: append did not land on slot %d (leo=%d)", replicated, leo)
	}
	e.NoteHwLedger(replicated, leo)
	upto, serveEmpty, proxy, _ = e.LocalRead(replicated)
	if proxy != "" || serveEmpty || upto != leo {
		t.Fatalf("synced replica: upto=%d empty=%v proxy=%q, want %d/false/local", upto, serveEmpty, proxy, leo)
	}

	// An LEO that has not reached the sample yet stays unproven: a sample of
	// leo+5 cannot exist before those records land, so drive it directly.
	e.NoteHwLedger(replicated, leo+5)
	if _, _, proxy, _ := e.LocalRead(replicated); proxy != wantLeaderAddr {
		t.Fatalf("LEO below the sampled watermark must forward, got proxy %q", proxy)
	}

	// Epoch invalidation: the sample was taken under the OLD leader. A
	// leader move bumps the epoch; the stale proof must not admit the read.
	// The new leader is another MEMBER of the slot's replica set (the table
	// only accepts moves within the set).
	var other string
	for _, r := range tbl.Slots[replicated].Replicas {
		if r != "node-1" && r != tbl.Slots[replicated].Leader {
			other = r
			break
		}
	}
	if other == "" {
		t.Skip("test setup: no third seat in the slot's replica set")
	}
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{replicated}, NewLeader: other})
	if _, _, proxy, _ := e.LocalRead(replicated); proxy == "" {
		t.Fatal("stale-epoch sample admitted a local read after a failover: epoch key is broken")
	}
	// The round under the NEW leader re-proves it: reads land locally again.
	e.NoteHwLedger(replicated, st.LastSeqOf(replicated))
	if _, _, proxy, _ := e.LocalRead(replicated); proxy != "" {
		t.Fatalf("post-failover sample must admit the local read, proxy=%q", proxy)
	}
}

// The ledger is the fetch round's post-report view: HandleMFetchCtx must
// carry, for the slots it reports, the watermark as it stands AFTER applying
// this round's progress (so a replica's ceiling tracks its own landing), and
// applyHwLedger must seed a follower's admission from it — only for slots
// this node replicates at the leader that answered.
func TestHwLedgerRoundTrip(t *testing.T) {
	st, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	lead := NewEngine(nil, st, "node-1", nil)
	join(t, lead, "node-1", "127.0.0.1:1")
	join(t, lead, "node-2", "127.0.0.1:2")
	applyCmd(t, lead, &Command{Op: OpConfig, Replicas: 2})
	applyCmd(t, lead, &Command{Op: OpPlanSlots})
	tbl := lead.TableSnapshot()
	var slot int32 = -1
	for s, p := range tbl.Slots {
		if p.Leader == "node-1" && replicaListHas(p.Replicas, "node-2") {
			slot = s
			break
		}
	}
	if slot < 0 {
		t.Fatal("test setup: no slot led by node-1 with node-2 replicating")
	}
	if _, err := st.Append(&data.EventRecord{
		AggregateID: "ledger-agg", Version: 1, CommandID: "ledger-cmd",
		Events: []data.Event{{Type: "E", Body: []byte("x")}},
	}); err != nil {
		t.Fatal(err)
	}
	leo := st.LastSeqOf(slot)

	// A SWEEP round from the follower reports its position: the ledger must
	// cover every reported slot, and the watermark must be the POST-report
	// value (node-2 at LEO => HW == LEO for a two-seat set).
	resp, err := lead.HandleMFetchCtx(context.Background(), MFetchRequest{
		Follower: "node-2", Slots: []int32{slot}, FromSeqs: []uint64{leo + 1}, Sweep: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	hw, found := 0, false
	for i, s := range resp.HwSlots {
		if s == slot {
			hw, found = int(resp.HwValues[i]), true
		}
	}
	if !found {
		t.Fatalf("sweep ledger missing slot %d: %+v", slot, resp.HwSlots)
	}
	if uint64(hw) != leo {
		t.Fatalf("ledger hw=%d, want post-report watermark %d", hw, leo)
	}

	// An ORDINARY round with no movement carries an EMPTY ledger (no changed
	// slots): steady sessions cost zero ledger bytes.
	resp2, err := lead.HandleMFetchCtx(context.Background(), MFetchRequest{
		Follower: "node-2", Slots: []int32{slot}, FromSeqs: []uint64{leo + 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp2.HwSlots) != 0 {
		t.Fatalf("idle round carried a %d-entry ledger, want 0", len(resp2.HwSlots))
	}

	// applyHwLedger on the follower side seeds admission.
	fs, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	fe := NewEngine(nil, fs, "node-2", nil)
	join(t, fe, "node-1", "127.0.0.1:1")
	join(t, fe, "node-2", "127.0.0.1:2")
	applyCmd(t, fe, &Command{Op: OpConfig, Replicas: 2})
	applyCmd(t, fe, &Command{Op: OpPlanSlots})
	fe.applyHwLedger("node-1", []int32{slot}, []uint64{uint64(hw)})
	upto, _, proxy, _ := fe.LocalRead(slot)
	if proxy != "" || upto != uint64(hw) {
		t.Fatalf("follower after ledger seed: upto=%d proxy=%q, want %d/local", upto, proxy, hw)
	}
	// A ledger answered by a node the follower's table does NOT see as the
	// slot's leader is ignored (a split-brain or stale session must not
	// admit reads on someone else's word).
	fresh, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	fe2 := NewEngine(nil, fresh, "node-2", nil)
	join(t, fe2, "node-1", "127.0.0.1:1")
	join(t, fe2, "node-2", "127.0.0.1:2")
	applyCmd(t, fe2, &Command{Op: OpConfig, Replicas: 2})
	applyCmd(t, fe2, &Command{Op: OpPlanSlots})
	leader := tbl.Slots[slot].Leader
	if leader != "node-1" {
		t.Fatalf("fixture: slot %d leader is %q", slot, leader)
	}
	fe2.applyHwLedger("node-ghost", []int32{slot}, []uint64{uint64(hw)})
	if _, _, proxy, _ := fe2.LocalRead(slot); proxy == "" {
		t.Fatal("a ledger from a non-leader admitted the local read")
	}
}

// LeaderReplica is the primitive the OTHER (write/redirect) paths share: one
// table-lock read answering "who leads this slot, where is that leader's
// gRPC plane, and do I hold the slot (as leader or replica)?". LocalRead is
// the read-path policy built alongside it; anything that needs the same
// facts must not go back to cloning the table.
func TestLeaderReplicaReportsPlacement(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	tbl := e.TableSnapshot()
	if len(tbl.Slots) == 0 {
		t.Fatal("test setup: no placements")
	}

	for slot, p := range tbl.Slots {
		leader, addr, local, found := e.LeaderReplica(slot)
		if !found {
			t.Fatalf("slot %d: found=false for a placed slot", slot)
		}
		if leader != p.Leader {
			t.Fatalf("slot %d: leader %q, want %q", slot, leader, p.Leader)
		}
		if want := tbl.Peers[p.Leader].ClientAddr; addr != want {
			t.Fatalf("slot %d: addr %q, want the leader's client addr %q", slot, addr, want)
		}
		// local must mean "leads OR replicates" — ownership, not admission:
		// a replica may hold the slot and still forward reads until its
		// sync is proven (see TestLocalReadReplicaShapes).
		wantLocal := p.Leader == e.self || replicaListHas(p.Replicas, e.self)
		if local != wantLocal {
			t.Fatalf("slot %d: local=%v, want %v (leader=%s replicas=%v self=%s)",
				slot, local, wantLocal, p.Leader, p.Replicas, e.self)
		}
	}

	// An unassigned slot: found=false, and no address to forward to.
	for s := int32(0); s < 8192; s++ {
		if _, ok := tbl.Slots[s]; !ok {
			leader, addr, local, found := e.LeaderReplica(s)
			if found || leader != "" || addr != "" || local {
				t.Errorf("unassigned slot %d: leader=%q addr=%q local=%v found=%v, want all empty/false",
					s, leader, addr, local, found)
			}
			break
		}
	}
}

// A slot led by a peer this node does not replicate: local=false and the
// returned address is the leader's, which is what a forward needs.
func TestLeaderReplicaRemoteSlot(t *testing.T) {
	st, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	e := NewEngine(nil, st, "node-1", nil)

	applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{Role: RoleLearner,
		ID: "node-1", PeerAddr: "127.0.0.1:8391", AdminAddr: "127.0.0.1:8091", ClientAddr: "127.0.0.1:8591",
	}})
	applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{Role: RoleLearner,
		ID: "node-2", PeerAddr: "127.0.0.1:8392", AdminAddr: "127.0.0.1:8092", ClientAddr: "127.0.0.1:8592",
	}})
	// This test needs a slot led by node-2 with node-1 as a removable replica;
	// two members derive a single copy, so the fixture pins two.
	pinReplicas(t, e, 2)
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	target := int32(-1)
	for s, p := range e.TableSnapshot().Slots {
		if p.Leader != "node-1" && replicaListHas(p.Replicas, "node-1") && len(p.Replicas) > 1 {
			target = s
			break
		}
	}
	if target < 0 {
		t.Fatal("test setup: no slot led elsewhere with node-1 as a removable replica")
	}
	applyCmd(t, e, &Command{Op: OpSlotRemoveReplica, Slots: []int32{target}, NodeID: "node-1"})

	leader, addr, local, found := e.LeaderReplica(target)
	if !found || local {
		t.Fatalf("slot %d: found=%v local=%v, want found and not local", target, found, local)
	}
	if leader != "node-2" {
		t.Fatalf("slot %d: leader %q, want node-2", target, leader)
	}
	if addr != "127.0.0.1:8592" {
		t.Errorf("slot %d: addr %q, want the client-plane address 127.0.0.1:8592 (peer addr is 127.0.0.1:8392)",
			target, addr)
	}
}

// The forward address must be the CLIENT-plane one: a reader dials it as a
// gRPC event client, so handing back the peer (raft) address would aim reads
// at the raft listener. The read path now answers it through LocalRead.
func TestLocalReadForwardsToClientPlane(t *testing.T) {
	st, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	e := NewEngine(nil, st, "node-1", nil)

	applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{Role: RoleLearner,
		ID: "node-1", PeerAddr: "127.0.0.1:8391", AdminAddr: "127.0.0.1:8091", ClientAddr: "127.0.0.1:8591",
	}})
	applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{Role: RoleLearner,
		ID: "node-2", PeerAddr: "127.0.0.1:8392", AdminAddr: "127.0.0.1:8092", ClientAddr: "127.0.0.1:8592",
	}})
	// This test needs a slot led by node-2 with node-1 as a removable replica;
	// two members derive a single copy, so the fixture pins two.
	pinReplicas(t, e, 2)
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// A slot led by node-2 with node-1 removed from its replica set, so this
	// node holds no part of it. With RF=2 over 2 nodes the planning gives
	// node-1 a copy of every slot, so the set has to be trimmed explicitly.
	tbl := e.TableSnapshot()
	target := int32(-1)
	for s, p := range tbl.Slots {
		if p.Leader != "node-1" && replicaListHas(p.Replicas, "node-1") && len(p.Replicas) > 1 {
			target = s
			break
		}
	}
	if target < 0 {
		t.Fatal("test setup: no slot led elsewhere with node-1 as a removable replica")
	}
	applyCmd(t, e, &Command{Op: OpSlotRemoveReplica, Slots: []int32{target}, NodeID: "node-1"})

	tbl = e.TableSnapshot()
	leader := tbl.Slots[target].Leader
	if replicaListHas(tbl.Slots[target].Replicas, "node-1") {
		t.Fatalf("test setup: node-1 still a replica of slot %d", target)
	}
	want := tbl.Peers[leader].ClientAddr
	if want == "" || want == tbl.Peers[leader].PeerAddr {
		t.Fatalf("test setup: leader %s has no distinct client addr (%q)", leader, want)
	}
	if _, _, proxy, _ := e.LocalRead(target); proxy != want {
		t.Errorf("LocalRead proxy addr = %q, want the client-plane address %q (peer addr is %q)",
			proxy, want, tbl.Peers[leader].PeerAddr)
	}
}
