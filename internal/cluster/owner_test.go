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
	hasWAL   bool
	bornWAL  bool
	want     ownerAction
	wantWhy  ownerReason
}

func TestDecideOwnerMatrix(t *testing.T) {
	const topo, self = "gen-A", "node-1"
	mk := func(leader string, replicas []string, epoch int64) *Placement {
		return &Placement{Leader: leader, Replicas: replicas, Epoch: epoch, State: SlotStable}
	}
	stampOK := slotOwner{Topology: topo, Leader: self, Epoch: 4, Replica: true, State: "stable"}
	cases := []ownerCase{
		{name: "no-wal", leader: self, replicas: []string{self}, epoch: 1, hasWAL: false, want: ownerNone},
		{name: "born-copy-no-record-leader", leader: self, replicas: []string{self}, epoch: 1,
			owner: nil, hasWAL: true, bornWAL: true, want: ownerPending, wantWhy: ownerMissing},
		{name: "fresh-copy-no-record-leader", leader: self, replicas: []string{self}, epoch: 1,
			owner: nil, hasWAL: true, bornWAL: false, want: ownerStamp},
		{name: "matching-record-refreshes", leader: self, replicas: []string{self}, epoch: 4,
			owner: &stampOK, hasWAL: true, want: ownerNone},
		{name: "leader-continues-across-epoch-bump", leader: self, replicas: []string{self}, epoch: 5,
			owner: &stampOK, hasWAL: true, want: ownerStamp}, // stale record: re-stamp, not fence
		{name: "topology-rotation-fences", leader: self, replicas: []string{self}, epoch: 1,
			owner: &slotOwner{Topology: "gen-B", Leader: self, Epoch: 1}, hasWAL: true,
			want: ownerPending, wantWhy: ownerTopoMismatch},
		{name: "epoch-ahead-of-table-fences", leader: self, replicas: []string{self}, epoch: 2,
			owner: &slotOwner{Topology: topo, Leader: self, Epoch: 7}, hasWAL: true,
			want: ownerPending, wantWhy: ownerEpochBackward},
		{name: "foreign-leader-record-without-seat-fences", leader: self, replicas: []string{self}, epoch: 3,
			owner: &slotOwner{Topology: topo, Leader: "node-2", Epoch: 3}, hasWAL: true,
			want: ownerPending, wantWhy: ownerLineageUnknown},
		{name: "promoted-seat-is-proven", leader: self, replicas: []string{self, "node-2"}, epoch: 6,
			owner: &slotOwner{Topology: topo, Leader: "node-2", Epoch: 5, Replica: true}, hasWAL: true,
			want: ownerStamp}, // proven: refresh the record to name the new leader
		{name: "surplus-copy-not-fenced-here", leader: "node-2", replicas: []string{"node-2"}, epoch: 4,
			owner: nil, hasWAL: true, bornWAL: true, want: ownerNone},
		{name: "born-seat-without-record-stays-unstamped", leader: "node-2", replicas: []string{"node-2", self}, epoch: 4,
			owner: nil, hasWAL: true, bornWAL: true, want: ownerNone},
		{name: "midlife-seat-without-record-stamps", leader: "node-2", replicas: []string{"node-2", self}, epoch: 4,
			owner: nil, hasWAL: true, bornWAL: false, want: ownerStamp},
		{name: "seat-refreshes-a-stale-but-matching-record", leader: "node-2", replicas: []string{"node-2", self}, epoch: 4,
			owner: &slotOwner{Topology: topo, Leader: "node-2", Epoch: 3, Replica: true}, hasWAL: true, want: ownerStamp},
		{name: "seat-foreign-record-not-proven-no-fence", leader: "node-2", replicas: []string{"node-2", self}, epoch: 4,
			owner: &slotOwner{Topology: topo, Leader: self, Epoch: 3}, hasWAL: true, want: ownerNone},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var o slotOwner
			has := false
			if tc.owner != nil {
				o, has = *tc.owner, true
			}
			p := mk(tc.leader, tc.replicas, tc.epoch)
			act, why := decideOwner(topo, self, p, o, has, tc.hasWAL, tc.bornWAL)
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
	// Slot 3 and 5: this node leads at epoch 1; slot 4 stays clean.
	e.tableMu.Lock()
	e.table.Slots[3] = &Placement{Leader: "node-1", Replicas: []string{"node-1"}, Epoch: 1, State: SlotStable}
	e.table.Slots[4] = &Placement{Leader: "node-1", Replicas: []string{"node-1"}, Epoch: 1, State: SlotStable}
	e.table.Slots[5] = &Placement{Leader: "node-1", Replicas: []string{"node-1"}, Epoch: 1, State: SlotStable}
	e.tableMu.Unlock()
	agg3, agg4, agg5 := aggForSlot(t, e, 3), aggForSlot(t, e, 4), aggForSlot(t, e, 5)

	// (a) A copy that appeared mid-life (migration push / first writes) is
	// this generation's own bytes: stamped, never fenced.
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

	// (b) A copy that predates the process with no record is unproven:
	// fenced out of writes AND local reads, with the reason surfaced.
	if err := st.AppendAtSeq(5, 1, ownerRecord(agg5, 1, "cmd-5")); err != nil {
		t.Fatal(err)
	}
	e.ownerMu.Lock()
	e.bornSet[5] = true
	e.ownerMu.Unlock()
	e.refreshOwners()
	if !e.slotPending(5) {
		t.Fatal("a born copy without an owner record passed the fence")
	}
	resp, err := e.landLocalAppend(5, ownerRecord(agg5, 2, "cmd-5b"))
	if err != nil || resp.Status != data.StatusFail || resp.ErrID != data.ErrIDSlotUnproven {
		t.Fatalf("fenced append = %+v/%v, want fail/1007", resp, err)
	}
	if bound, _, _, _ := e.readAdmission(5); bound != 0 {
		t.Fatalf("fenced leader read admission bound = %d, want 0 (empty answer)", bound)
	}
	pend := e.PendingSlots()
	if len(pend) != 1 || pend[0].Slot != 5 || !strings.Contains(pend[0].Reason, "absent") {
		t.Fatalf("pending view = %+v, want slot 5 with the missing-record reason", pend)
	}
	// The fence holds the slot out of service but never deletes: the WAL —
	// the evidence the operator decides over — must still be on disk.
	if !st.HasWAL(5) {
		t.Fatal("the fence removed the local WAL it was judging")
	}

	// (c) Adopt: the operator's declaration puts the copy in service again.
	if n, sk := e.AdoptPending([]int32{5}); n != 1 || len(sk) != 0 {
		t.Fatalf("adopt(5) = (%d, %v), want one adopted none skipped", n, sk)
	}
	if e.slotPending(5) {
		t.Fatal("adopt left the slot fenced")
	}
	resp, err = e.landLocalAppend(5, ownerRecord(agg5, 2, "cmd-5c"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != data.StatusSuccess && resp.Status != data.StatusExists {
		t.Fatalf("adopted append = %+v, want success", resp)
	}

	// (d) Discard: WAL and record go together, the seat comes back empty.
	if err := st.AppendAtSeq(4, 1, ownerRecord(agg4, 1, "cmd-4")); err != nil {
		t.Fatal(err)
	}
	e.ownerMu.Lock()
	e.bornSet[4] = true
	e.ownerMu.Unlock()
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

	// (e2) A born seat copy cannot buy provenance by holding the seat: it
	// stays unstamped while it is a seat, and the promotion into leader
	// fences it under the leader rules.
	e.tableMu.Lock()
	e.table.Slots[4] = &Placement{Leader: "node-2", Replicas: []string{"node-2", "node-1"}, Epoch: 3, State: SlotStable}
	e.tableMu.Unlock()
	if err := st.AppendAtSeq(4, 1, ownerRecord(agg4, 1, "cmd-4b")); err != nil {
		t.Fatal(err)
	}
	e.ownerMu.Lock()
	e.bornSet[4] = true
	e.ownerMu.Unlock()
	e.refreshOwners()
	if _, has, _ := readOwner(st.SlotDir(4)); has {
		t.Fatal("a born seat stamped itself: unproven copies must not buy provenance through a seat")
	}
	e.tableMu.Lock()
	e.table.Slots[4] = &Placement{Leader: "node-1", Replicas: []string{"node-1"}, Epoch: 4, State: SlotStable}
	e.tableMu.Unlock()
	e.refreshOwners()
	if !e.slotPending(4) {
		t.Fatal("promoting a born unproven seat to leader was not fenced")
	}
	if err := st.DropSlot(4); err != nil {
		t.Fatal(err)
	}

	// (f) Adopt-all is the one-time upgrade action.
	if err := os.Remove(ownerPath(st.SlotDir(5))); err != nil {
		t.Fatal(err)
	}
	e.refreshOwners()
	// Slot 3 is proven (not pending): asking for it by name is skipped,
	// never adopted behind the fence's back. Empty want = every pending slot.
	if n, sk := e.AdoptPending([]int32{3}); n != 0 || len(sk) != 1 || sk[0] != 3 {
		t.Fatalf("adopt(3) on a proven slot = (%d, %v), want (0, [3])", n, sk)
	}
	if n, sk := e.AdoptPending(nil); n != 1 || len(sk) != 0 {
		t.Fatalf("adopt-all = (%d, %v), want one slot adopted", n, sk)
	}
	if e.slotPending(5) {
		t.Fatal("adopt-all left the slot fenced")
	}
}
