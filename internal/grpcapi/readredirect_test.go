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
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/berkaroad/pushupes/internal/cluster"
	"github.com/berkaroad/pushupes/internal/data"
	"github.com/berkaroad/pushupes/internal/storage"
	pushupesv1 "github.com/berkaroad/pushupes/pkg/grpcapi/pushupes/v1"
)

// Reads never hop server to server. A node that can serve a slot answers;
// every other node returns the MOVED in-band (err_id 1003 + the
// authoritative slot + the leader's client-plane node) and the CLIENT
// retries there — the pkg/client tests pin the following half.
//
// The fixture is deliberate about the relay being gone: the table's peer
// addresses reach no live server, so if any handler still dialed the
// leader the call would fail or hang instead of answering the redirect
// instantly.
func TestReadsRedirectNotRelay(t *testing.T) {
	store, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	eng := cluster.NewEngine(nil, store, "node-1", nil)

	for i, id := range []string{"node-1", "node-2", "node-3"} {
		addr := fmt.Sprintf("127.0.0.1:%d", 10+i)
		c := &cluster.Command{Op: cluster.OpJoinNode, Peer: &cluster.Peer{Role: cluster.RoleLearner,
			ID: id, PeerAddr: addr, AdminAddr: addr, ClientAddr: fmt.Sprintf("127.0.0.1:%d", 20+i),
		}}
		if _, err := eng.ApplyCommand(c.Encode()); err != nil {
			t.Fatal(err)
		}
	}
	pinTwoReplicas(t, eng)
	if _, err := eng.ApplyCommand((&cluster.Command{Op: cluster.OpPlanSlots}).Encode()); err != nil {
		t.Fatal(err)
	}
	tbl := eng.TableSnapshot()

	// One slot node-1 leads, one it holds as a proven-synced replica, and
	// one it holds NOTHING of — the redirect subject.
	var leadSlot, heldSlot, freeSlot int32 = -1, -1, -1
	// Slot 0 is reserved and therefore never planned: start at 1 and tolerate holes.
	for s := int32(1); s < 8; s++ {
		p := tbl.Slots[s]
		if p == nil {
			continue
		}
		switch {
		case p.Leader == "node-1":
			if leadSlot < 0 {
				leadSlot = s
			}
		case slices.Contains(p.Replicas, "node-1"):
			if heldSlot < 0 {
				heldSlot = s
			}
		default:
			if freeSlot < 0 {
				freeSlot = s
			}
		}
	}
	if leadSlot < 0 || heldSlot < 0 || freeSlot < 0 {
		t.Fatalf("fixture: need a led, a held and an unheld slot, got lead=%d held=%d free=%d",
			leadSlot, heldSlot, freeSlot)
	}

	findAgg := func(want int32) string {
		for i := 0; i < 1000000; i++ {
			name := fmt.Sprintf("rdoc-%d-%d", want, i)
			if store.SlotOf(name) == want {
				return name
			}
		}
		t.Fatalf("no aggregate hashes to slot %d", want)
		return ""
	}
	freeAgg := findAgg(freeSlot)
	leadAgg := findAgg(leadSlot)
	heldAgg := findAgg(heldSlot)

	// Write something to the led slot so a local answer is not vacuous.
	if _, err := store.AppendRouted(&data.EventRecord{
		AggregateID: leadAgg, Version: 1, CommandID: leadAgg + "-cmd-1",
		Events: []data.Event{{Type: "E", Body: []byte("x")}},
	}); err != nil {
		t.Fatal(err)
	}
	// The held replica answers locally only when proven in sync.
	eng.NoteHwLedger(heldSlot, store.LastSeqOf(heldSlot))

	wantNode := tbl.Peers[tbl.Slots[freeSlot].Leader].ClientAddr
	cli := serveEngine(t, eng, store)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	check := func(name string, errID uint32, slot int32, node string) {
		t.Helper()
		if errID != data.ErrIDSlotNotLocal || slot != freeSlot || node != wantNode {
			t.Fatalf("%s: got err_id=%d slot=%d node=%q, want 1003/%d/%q",
				name, errID, slot, node, freeSlot, wantNode)
		}
	}

	rs, err := cli.ReadStream(ctx, &pushupesv1.ReadStreamRequest{AggregateId: freeAgg})
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	check("ReadStream", rs.ErrId, rs.Slot, rs.Node)
	if len(rs.Records) != 0 {
		t.Fatalf("a redirect answers with no records, got %d", len(rs.Records))
	}

	rbc, err := cli.ReadByCommand(ctx, &pushupesv1.ReadByCommandRequest{AggregateId: freeAgg, CommandId: "anything"})
	if err != nil {
		t.Fatalf("ReadByCommand: %v", err)
	}
	check("ReadByCommand", rbc.ErrId, rbc.Slot, rbc.Node)

	rvt, err := cli.ReadVersionByTime(ctx, &pushupesv1.ReadVersionByTimeRequest{AggregateId: freeAgg})
	if err != nil {
		t.Fatalf("ReadVersionByTime: %v", err)
	}
	check("ReadVersionByTime", rvt.ErrId, rvt.Slot, rvt.Node)

	// ReadTails is per entry: the unheld aggregate redirects while the
	// led and held ones answer versions in the SAME call.
	rt, err := cli.ReadTails(ctx, &pushupesv1.ReadTailsRequest{AggregateIds: []string{freeAgg, leadAgg, heldAgg}})
	if err != nil {
		t.Fatalf("ReadTails: %v", err)
	}
	if len(rt.Results) != 3 {
		t.Fatalf("ReadTails results = %d, want 3", len(rt.Results))
	}
	r0 := rt.Results[0]
	check("ReadTails[free]", r0.ErrId, r0.Slot, r0.Node)
	if r0.AggregateId != freeAgg || rt.Results[1].AggregateId != leadAgg || rt.Results[2].AggregateId != heldAgg {
		t.Fatalf("ReadTails echo mismatch: %+v", rt.Results)
	}
	if r1 := rt.Results[1]; r1.ErrId != 0 || r1.Version != 1 {
		t.Fatalf("leader-served entry: %+v, want version 1 with no redirect", r1)
	}
	if r2 := rt.Results[2]; r2.ErrId != 0 {
		t.Fatalf("proven replica entry must answer locally, got redirect %+v", r2)
	}
}
