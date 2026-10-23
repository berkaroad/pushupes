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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/berkaroad/pushupes/internal/data"
)

// ownerCase drives decideOwner with one placement / record shape.
type ownerCase struct {
	name     string
	leader   string // placement leader
	replicas []string
	epoch    int64
	owner    *slotOwner // nil = no record on disk
	hasWAL   bool       // the slot dir holds segments (fence input)
	stranger bool       // the dir's admission stamp names another generation
	want     ownerAction
	wantWhy  ownerReason
}

func TestDecideOwnerMatrix(t *testing.T) {
	const topo, self = "gen-A", "node-1"
	mk := func(leader string, replicas []string, epoch int64) *Placement {
		return &Placement{Leader: leader, Replicas: replicas, Epoch: epoch, State: SlotStable}
	}
	stampOK := slotOwner{Topology: topo, Leader: self, Epoch: 4, State: "stable"}
	cases := []ownerCase{
		// The table is the proof: every member slot without a record gets
		// one — leader or seat, WAL or no WAL yet (an empty placement is
		// stamped as the copy's birth certificate).
		{name: "empty-leader-placement-stamps", leader: self, replicas: []string{self}, epoch: 1,
			owner: nil, want: ownerStamp},
		{name: "born-copy-no-record-leader", leader: self, replicas: []string{self}, epoch: 1,
			owner: nil, hasWAL: true, want: ownerStamp},
		{name: "fresh-copy-no-record-leader", leader: self, replicas: []string{self}, epoch: 1,
			owner: nil, want: ownerStamp},
		{name: "matching-record-refreshes", leader: self, replicas: []string{self}, epoch: 4,
			owner: &stampOK, want: ownerNone},
		{name: "leader-continues-across-epoch-bump", leader: self, replicas: []string{self}, epoch: 5,
			owner: &stampOK, want: ownerStamp}, // stale record: re-stamp, not fence
		{name: "topology-rotation-fences", leader: self, replicas: []string{self}, epoch: 1,
			owner: &slotOwner{Topology: "gen-B", Leader: self, Epoch: 1},
			want:  ownerPending, wantWhy: ownerTopoMismatch},
		{name: "epoch-ahead-of-table-fences", leader: self, replicas: []string{self}, epoch: 2,
			owner: &slotOwner{Topology: topo, Leader: self, Epoch: 7},
			want:  ownerPending, wantWhy: ownerEpochBackward},
		{name: "same-epoch-with-a-different-leader-fences", leader: self, replicas: []string{self}, epoch: 3,
			owner: &slotOwner{Topology: topo, Leader: "node-2", Epoch: 3},
			want:  ownerPending, wantWhy: ownerLineageUnknown},
		{name: "promoted-seat-is-proven", leader: self, replicas: []string{self, "node-2"}, epoch: 6,
			owner: &slotOwner{Topology: topo, Leader: "node-2", Epoch: 5},
			want:  ownerStamp}, // proven: refresh the record to name the new leader
		{name: "surplus-copy-not-fenced-here", leader: "node-2", replicas: []string{"node-2"}, epoch: 4,
			owner: nil, want: ownerNone},
		{name: "seat-without-record-stamps", leader: "node-2", replicas: []string{"node-2", self}, epoch: 4,
			owner: nil, want: ownerStamp},
		// The stranger dir is the one shape the table cannot vouch for:
		// its record-less leader copy waits for the operator's adopt, and
		// a record-less seat stays unstamped so a later promotion judges
		// it under the same leader rule.
		{name: "stranger-dir-leader-without-record-fences", leader: self, replicas: []string{self, "node-2"}, epoch: 1,
			owner: nil, hasWAL: true, stranger: true, want: ownerPending, wantWhy: ownerForeignDir},
		{name: "stranger-dir-empty-placement-just-seats", leader: self, replicas: []string{self}, epoch: 1,
			owner: nil, hasWAL: false, stranger: true, want: ownerNone},
		{name: "stranger-dir-seat-without-record-unstamped", leader: "node-2", replicas: []string{"node-2", self}, epoch: 4,
			owner: nil, stranger: true, want: ownerNone},
		{name: "seat-refreshes-a-stale-but-matching-record", leader: "node-2", replicas: []string{"node-2", self}, epoch: 4,
			owner: &slotOwner{Topology: topo, Leader: "node-2", Epoch: 3}, want: ownerStamp},
		// A seat whose record names an older leader is proven (older epoch ⇒
		// the demotion path) and refreshed, not ignored.
		{name: "seat-refreshes-a-record-from-the-previous-leader", leader: "node-2", replicas: []string{"node-2", self}, epoch: 4,
			owner: &slotOwner{Topology: topo, Leader: self, Epoch: 3}, want: ownerStamp},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var o slotOwner
			has := false
			if tc.owner != nil {
				o, has = *tc.owner, true
			}
			p := mk(tc.leader, tc.replicas, tc.epoch)
			act, why := decideOwner(topo, self, p, o, has, tc.hasWAL, tc.stranger)
			if act != tc.want {
				t.Fatalf("action = %v, want %v", act, tc.want)
			}
			if tc.want == ownerPending && why != tc.wantWhy {
				t.Fatalf("reason = %q, want %q", why, tc.wantWhy)
			}
			// Every pending verdict fences the LEADER only — a fenced seat
			// that is not ours to lead is none of this fence's business.
			if act == ownerPending && p.Leader != self {
				t.Fatal("pending verdict against a slot this node does not lead")
			}
		})
	}
}

func TestTopologyIdPersistence(t *testing.T) {
	dir := t.TempDir()
	first, err := loadOrInitTopology(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 32 {
		t.Fatalf("topology id shape %q", first)
	}
	again, err := loadOrInitTopology(dir)
	if err != nil {
		t.Fatal(err)
	}
	if again != first {
		t.Fatalf("restart changed the generation id: %q -> %q", first, again)
	}
	// A rebuilt consensus dir (the fence's trigger scenario) gets a NEW id,
	// which is what mismatches every surviving owner.json.
	if err := os.RemoveAll(filepath.Join(dir, "topology")); err != nil {
		t.Fatal(err)
	}
	wiped, err := loadOrInitTopology(dir)
	if err != nil {
		t.Fatal(err)
	}
	if wiped == first {
		t.Fatal("topology id survived deleting the consensus dir")
	}
}

// aggForSlot returns an aggregate id the store routes to the slot, so the
// AppendAtSeq seeding below and the engine appends above address the SAME
// slot directory (store.Append routes by hash; it must land where the fence
// was planted).
func aggForSlot(t *testing.T, e *Engine, slot int32) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		agg := fmt.Sprintf("of-%d-%d", slot, i)
		if e.SlotOf(agg) == slot {
			return agg
		}
	}
	t.Fatalf("no aggregate routes to slot %d", slot)
	return ""
}

func ownerRecord(agg string, version uint32, cmd string) *data.EventRecord {
	return &data.EventRecord{
		AggregateID: agg, Version: version, CommandID: cmd,
		Events: []data.Event{{Type: "T", Body: []byte("owner-fence")}},
	}
}

func TestOwnerFenceEndToEnd(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	// Slot 3, 4 and 5: this node leads at epoch 1.
	e.tableMu.Lock()
	e.table.Slots[3] = &Placement{Leader: "node-1", Replicas: []string{"node-1"}, Epoch: 1, State: SlotStable}
	e.table.Slots[4] = &Placement{Leader: "node-1", Replicas: []string{"node-1"}, Epoch: 1, State: SlotStable}
	e.table.Slots[5] = &Placement{Leader: "node-1", Replicas: []string{"node-1"}, Epoch: 1, State: SlotStable}
	e.tableMu.Unlock()
	agg3, agg4, agg5 := aggForSlot(t, e, 3), aggForSlot(t, e, 4), aggForSlot(t, e, 5)

	// (a) A copy written under the live table is stamped, never fenced.
	if err := st.AppendAtSeq(3, 1, ownerRecord(agg3, 1, "cmd-3")); err != nil {
		t.Fatal(err)
	}
	e.refreshOwners()
	if e.slotPending(3) {
		t.Fatal("a copy written under the live table was fenced")
	}
	owner, has, err := readOwner(st.SlotDir(3))
	if err != nil || !has || owner.Leader != "node-1" || owner.Topology != e.topologyID() {
		t.Fatalf("fresh copy not stamped: %+v has=%v err=%v", owner, has, err)
	}

	// (b) The table is the proof: a record-less copy in a dir admitted to
	// THIS generation — the shape that used to fence every slot of a
	// scale-out's promoted seats — is stamped by the placement itself and
	// stays in service.
	if err := st.AppendAtSeq(5, 1, ownerRecord(agg5, 1, "cmd-5")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(ownerPath(st.SlotDir(5))); err != nil {
		t.Fatal(err)
	}
	e.refreshOwners()
	if e.slotPending(5) {
		t.Fatal("a record-less copy inside the admitted dir was fenced: the table placement is the proof")
	}
	if _, has, _ := readOwner(st.SlotDir(5)); !has {
		t.Fatal("the placement did not re-stamp its copy")
	}
	resp, err := e.landLocalAppend(5, ownerRecord(agg5, 2, "cmd-5b"))
	if err != nil || resp.Status == data.StatusFail {
		t.Fatalf("append after the placement stamp: %+v/%v", resp, err)
	}

	// (c) The stranger dir is the one shape the table cannot vouch for: the
	// admission stamp names another generation, so a record-less leader
	// copy is unproven — refused on writes AND local reads, with the
	// reason surfaced, and nothing deleted while the operator decides.
	e.writeAdmissionStamp("gen-B")
	if err := os.Remove(ownerPath(st.SlotDir(5))); err != nil {
		t.Fatal(err)
	}
	e.refreshOwners()
	if !e.slotPending(5) {
		t.Fatal("a record-less copy in a dir stamped for another generation passed the fence")
	}
	resp, err = e.landLocalAppend(5, ownerRecord(agg5, 3, "cmd-5c"))
	if err != nil || resp.Status != data.StatusFail || resp.ErrID != data.ErrIDSlotUnproven {
		t.Fatalf("fenced append = %+v/%v, want fail/1007", resp, err)
	}
	if bound, _, _, _ := e.readAdmission(5); bound != 0 {
		t.Fatalf("fenced leader read admission bound = %d, want 0 (empty answer)", bound)
	}
	pend := e.PendingSlots()
	if len(pend) != 1 || pend[0].Slot != 5 || !strings.Contains(pend[0].Reason, "admission") {
		t.Fatalf("pending view = %+v, want slot 5 with the foreign-dir reason", pend)
	}
	if !st.HasWAL(5) {
		t.Fatal("the fence removed the local WAL it was judging")
	}

	// (d) Adopt: the operator's declaration puts the copy in service and
	// admits the dir — without the stamp refresh every later record-less
	// slot would fence all over again.
	if n, sk := e.AdoptPending([]int32{5}); n != 1 || len(sk) != 0 {
		t.Fatalf("adopt(5) = (%d, %v), want one adopted none skipped", n, sk)
	}
	if e.slotPending(5) {
		t.Fatal("adopt left the slot fenced")
	}
	e.refreshOwners()
	if e.slotPending(5) {
		t.Fatal("adopt did not refresh the admission stamp: the next round fenced it again")
	}
	resp, err = e.landLocalAppend(5, ownerRecord(agg5, 2, "cmd-5d"))
	if err != nil {
		t.Fatal(err)
	}
	// Any verdict INSIDE the seq space proves the slot is back in service:
	// 1001 here would be a version conflict, which the fence (1007) could
	// never produce.
	if resp.ErrID == data.ErrIDSlotUnproven || resp.Status == data.StatusFail && resp.ErrID == 0 {
		t.Fatalf("adopted append still fenced: %+v", resp)
	}

	// (e) Discard: WAL and record go together, the seat comes back empty.
	e.writeAdmissionStamp("gen-C")
	if err := os.Remove(ownerPath(st.SlotDir(4))); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendAtSeq(4, 1, ownerRecord(agg4, 1, "cmd-4")); err != nil {
		t.Fatal(err)
	}
	e.refreshOwners()
	if !e.slotPending(4) {
		t.Fatal("setup: slot 4 should be fenced before discard")
	}
	if err := st.DropSlot(4); err != nil {
		t.Fatal(err)
	}
	e.refreshOwners()
	if st.HasWAL(4) || e.slotPending(4) {
		t.Fatalf("after discard: wal=%v pending=%v", st.HasWAL(4), e.slotPending(4))
	}

	// (f) A seat's copy is stamped too (the record documents the placement,
	// it is not a service grant), and a promotion from that seat serves
	// immediately — the shape that froze a scale-out's rebalanced fleet.
	e.writeAdmissionStamp(e.topologyID())
	e.tableMu.Lock()
	e.table.Slots[4] = &Placement{Leader: "node-2", Replicas: []string{"node-2", "node-1"}, Epoch: 3, State: SlotStable}
	e.tableMu.Unlock()
	if err := st.AppendAtSeq(4, 1, ownerRecord(agg4, 1, "cmd-4b")); err != nil {
		t.Fatal(err)
	}
	e.refreshOwners()
	if _, has, _ := readOwner(st.SlotDir(4)); !has {
		t.Fatal("a member seat without a record was not stamped by its placement")
	}
	e.tableMu.Lock()
	e.table.Slots[4] = &Placement{Leader: "node-1", Replicas: []string{"node-1", "node-2"}, Epoch: 4, State: SlotStable}
	e.tableMu.Unlock()
	e.refreshOwners()
	if e.slotPending(4) {
		t.Fatal("promoting a stamped seat to leader fenced it")
	}
}

// TestApplyStampsNewLeaderRecordSynchronously pins the apply-path promise:
// a commit that hands leadership here installs owner.json WITH the apply —
// the new leader never serves, waits, or reads unproven while a fence tick
// catches up.
func TestApplyStampsNewLeaderRecordSynchronously(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	e.tableMu.Lock()
	e.table.Slots[6] = &Placement{Leader: "node-2", Replicas: []string{"node-2", "node-1"}, Epoch: 1, State: SlotStable}
	e.tableMu.Unlock()
	agg6 := aggForSlot(t, e, 6)
	if err := st.AppendAtSeq(6, 1, ownerRecord(agg6, 1, "cmd-6")); err != nil {
		t.Fatal(err)
	}
	if _, has, _ := readOwner(st.SlotDir(6)); has {
		t.Fatal("setup: slot 6 should arrive without a record")
	}

	// The commit that moves leadership to node-1 applies...
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{6}, NewLeader: "node-1"})
	// ...and the record is on disk already — before any refreshOwners run.
	owner, has, err := readOwner(st.SlotDir(6))
	if err != nil || !has {
		t.Fatalf("the apply did not stamp the new leader: has=%v err=%v", has, err)
	}
	if owner.Leader != "node-1" || owner.Topology != e.topologyID() {
		t.Fatalf("stamped record = %+v, wants the live placement", owner)
	}
}

// TestCorruptOwnerRecordIsRestampedFromTheTable pins the repair rule: with
// the placement as the proof, bytes that no longer parse are evidence of
// nothing — a member slot in an admitted dir is stamped over from the table
// instead of being fenced forever. (This is the shape the old concurrent
// writeOwner race left behind: two JSON objects concatenated into one
// owner.json by writers sharing a fixed temp name.)
func TestCorruptOwnerRecordIsRestampedFromTheTable(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	e.tableMu.Lock()
	e.table.Slots[2] = &Placement{Leader: "node-1", Replicas: []string{"node-1"}, Epoch: 1, State: SlotStable}
	e.tableMu.Unlock()
	agg2 := aggForSlot(t, e, 2)
	if err := st.AppendAtSeq(2, 1, ownerRecord(agg2, 1, "cmd-2")); err != nil {
		t.Fatal(err)
	}
	e.refreshOwners()
	if e.slotPending(2) {
		t.Fatal("setup: fresh copy fenced")
	}
	// Corrupt the record the way interleaved writers used to: one whole
	// JSON object plus the tail of a second.
	bad := []byte(`{"topology":"x","leader":"node-1","epoch":1}{"topology":"y"`)
	if err := os.WriteFile(ownerPath(st.SlotDir(2)), bad, 0o644); err != nil {
		t.Fatal(err)
	}
	e.refreshOwners()
	if e.slotPending(2) {
		t.Fatal("a corrupt record fenced a slot the placement proves")
	}
	if o, has, err := readOwner(st.SlotDir(2)); !has || err != nil || o.Topology != e.topologyID() {
		t.Fatalf("the corrupt bytes were not replaced by the table's record: %+v has=%v err=%v", o, has, err)
	}
}

// TestWriteOwnerConcurrentNoGarbage drives the two writers that share a
// slot during a hand-over wave (the apply-path stamp and the fence loop)
// and demands the record parse on EVERY round — the old fixed temp name
// produced interleaved half-and-half files here.
func TestWriteOwnerConcurrentNoGarbage(t *testing.T) {
	dir := t.TempDir()
	done := make(chan struct{}, 64)
	for k := 0; k < 32; k++ {
		go func(k int) {
			defer func() { done <- struct{}{} }()
			for r := 0; r < 20; r++ {
				o := slotOwner{Topology: fmt.Sprintf("gen-%d-%d", k, r), Leader: "node-1", Epoch: int64(r)}
				if err := writeOwner(dir, o); err != nil {
					t.Errorf("writeOwner: %v", err)
					return
				}
				if o2, has, err := readOwner(dir); err != nil || !has {
					t.Errorf("record unreadable mid-race: has=%v err=%v", has, err)
					return
				} else if !strings.HasPrefix(o2.Topology, "gen-") {
					t.Errorf("garbage record: %+v", o2)
					return
				}
			}
		}(k)
	}
	for k := 0; k < 32; k++ {
		<-done
	}
}

// TestOwnerRecordIsIdenticalAcrossMembers pins the reason the record no longer
// carries a per-node fact: two members of one slot must end up with the same
// bytes, so an operator can diff the files to spot a copy that belongs to a
// different lineage instead of reading per-node noise.
func TestOwnerRecordIsIdenticalAcrossMembers(t *testing.T) {
	one, st1 := newTestEngine(t, "node-1")
	two, st2 := newTestEngine(t, "node-2")
	for _, e := range []*Engine{one, two} {
		// Same replicated generation on both sides (a fresh temp dir mints its
		// own id, and the generation is part of the record).
		applyCmd(t, e, &Command{Op: OpSetTopology, Topology: "gen-shared"})
		e.tableMu.Lock()
		e.table.Slots[9] = &Placement{Leader: "node-1", Replicas: []string{"node-1", "node-2"}, Epoch: 2, State: SlotStable}
		e.tableMu.Unlock()
		e.refreshOwners()
	}
	got1, has1, err1 := readOwner(st1.SlotDir(9))
	got2, has2, err2 := readOwner(st2.SlotDir(9))
	if err1 != nil || err2 != nil || !has1 || !has2 {
		t.Fatalf("both members must hold a record: has=(%v,%v) err=(%v,%v)", has1, has2, err1, err2)
	}
	if got1 != got2 {
		t.Fatalf("members disagree about one slot:\n  leader copy: %+v\n  seat copy:   %+v", got1, got2)
	}
	b1, _ := os.ReadFile(ownerPath(st1.SlotDir(9)))
	b2, _ := os.ReadFile(ownerPath(st2.SlotDir(9)))
	if string(b1) != string(b2) {
		t.Fatalf("record bytes differ across members:\n%s\n%s", b1, b2)
	}
}

// TestNonMemberSlotIsNeverStamped pins the other half of "the file names a
// member": a copy this node holds without a seat (a leftover the retention
// drop has not collected yet) is not a member slot, so the fence leaves it
// alone — its record, if any, stays as the stale evidence it is.
func TestNonMemberSlotIsNeverStamped(t *testing.T) {
	e, st := newTestEngine(t, "node-1")
	e.tableMu.Lock()
	e.table.Slots[7] = &Placement{Leader: "node-2", Replicas: []string{"node-2"}, Epoch: 3, State: SlotStable}
	e.tableMu.Unlock()
	// A real leftover copy, not just an empty dir.
	if err := st.AppendAtSeq(7, 1, ownerRecord(aggForSlot(t, e, 7), 1, "cmd-7")); err != nil {
		t.Fatal(err)
	}
	e.refreshOwners()
	if rec, has, err := readOwner(st.SlotDir(7)); has || err != nil {
		t.Fatalf("a non-member slot must stay unstamped: %+v has=%v err=%v", rec, has, err)
	}
	if e.slotPending(7) {
		t.Fatal("a copy this node does not seat must not fence its own slot")
	}
}
