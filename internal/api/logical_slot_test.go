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

package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/berkaroad/pushupes/internal/cluster"
	"github.com/berkaroad/pushupes/internal/data"
	"github.com/berkaroad/pushupes/internal/storage"
)

// newLogicalFixture serves g2 — physical slots 4..7, logical slots 0..3 — on an
// 8-slot store, which is the smallest layout where the two id spaces differ.
func newLogicalFixture(t *testing.T) (*Server, *cluster.Engine, *storage.Store) {
	t.Helper()
	st, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	eng := cluster.NewEngine(nil, st, "node-1", nil)
	self := &cluster.Peer{Role: cluster.RoleLearner, ID: "node-1",
		PeerAddr: "127.0.0.1:1", AdminAddr: "127.0.0.1:2", ClientAddr: "127.0.0.1:3"}
	for _, c := range []*cluster.Command{
		{Op: cluster.OpJoinNode, Peer: self},
		{Op: cluster.OpRegister, Peer: self},
		{Op: cluster.OpPlanSlots, GroupGen: 2, GroupBase: 4, GroupSize: 4},
	} {
		if _, err := eng.ApplyCommand(c.Encode()); err != nil {
			t.Fatalf("%s: %v", c.Op, err)
		}
	}
	return New(eng, st), eng, st
}

// TestAdminStatusIsDenseOverLogicalSlots pins the console-facing shape: every
// per-slot array has one entry per SERVED slot, indexed by logical id, and
// slot_count counts those slots (the table's physical capacity rides apart).
// A record written to physical 6 must land in index 2, not 6.
func TestAdminStatusIsDenseOverLogicalSlots(t *testing.T) {
	srv, _, st := newLogicalFixture(t)

	rec := &data.EventRecord{AggregateID: "agg-logical", Version: 1, UnixTime: 1700000000,
		CommandID: "c1", Events: []data.Event{{Type: "E", Body: []byte("{}")}}}
	if _, err := st.AppendToSlot(6, rec); err != nil { // logical 2
		t.Fatalf("append: %v", err)
	}

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/admin/status", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d (%s)", rr.Code, rr.Body.String())
	}
	body := decodeBody(t, rr)
	if got := body["slot_count"]; got != float64(4) {
		t.Fatalf("slot_count = %v, want 4 (the served generation's size)", got)
	}
	if got := body["physical_slot_count"]; got != float64(8) {
		t.Fatalf("physical_slot_count = %v, want 8", got)
	}
	for _, key := range []string{"total_writes", "total_write_bytes", "client_writes", "client_write_bytes",
		"storage_bytes", "streams", "flow_control_hits"} {
		arr, ok := body[key].([]any)
		if !ok {
			t.Fatalf("%s is not an array: %T", key, body[key])
		}
		if len(arr) != 4 {
			t.Fatalf("%s has %d entries, want 4 (one per logical slot)", key, len(arr))
		}
	}
	tw := body["total_writes"].([]any)
	if tw[2] != float64(1) {
		t.Fatalf("total_writes[2] = %v, want 1 (physical slot 6 is logical 2)", tw[2])
	}
	if tw[0] != float64(0) {
		t.Fatalf("total_writes[0] = %v, want 0", tw[0])
	}
}

// TestAdminSlotDescribeSpeaksLogicalSlots pins the path parameter: 0 is the
// first served slot (physical 4 here), and anything past the generation is a
// 400 rather than a lookup in a segment this cluster does not serve.
func TestAdminSlotDescribeSpeaksLogicalSlots(t *testing.T) {
	srv, _, _ := newLogicalFixture(t)

	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/admin/slots/0/describe", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("describe logical 0: %d (%s)", rr.Code, rr.Body.String())
	}
	body := decodeBody(t, rr)
	if body["slot"] != float64(0) {
		t.Fatalf("slot = %v, want logical 0", body["slot"])
	}
	if body["physical_slot"] != float64(4) {
		t.Fatalf("physical_slot = %v, want 4", body["physical_slot"])
	}

	for _, raw := range []string{"4", "-1", "99"} {
		rr := httptest.NewRecorder()
		srv.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/admin/slots/"+raw+"/describe", nil))
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("describe logical %s: %d, want 400 (%s)", raw, rr.Code, rr.Body.String())
		}
	}
}

// TestAdminPlacementMapKeyedByLogical pins the projection behind
// /admin/cluster/status's "slots" map (the handler cannot be driven here: the
// cluster-status view also reads Raft stats, and this fixture has no Raft). Its
// keys are the logical ids the console and the path parameters use, so a row can
// be followed straight through from the map to /admin/slots/{slot}/describe.
func TestAdminPlacementMapKeyedByLogical(t *testing.T) {
	_, eng, _ := newLogicalFixture(t)
	tbl := eng.TableSnapshot()
	spec := cluster.SlotGroup{Base: tbl.GroupBase, Size: tbl.GroupSize}
	if spec.Base != 4 || spec.Size != 4 {
		t.Fatalf("fixture generation = %d/%d, want 4/4", spec.Base, spec.Size)
	}
	got := projectMap(tbl.Slots, spec)
	if len(got) != 4 {
		t.Fatalf("projected %d slots, want 4: %v", len(got), got)
	}
	for l := int32(0); l < 4; l++ {
		if _, ok := got[l]; !ok {
			t.Fatalf("missing logical id %d: %v", l, got)
		}
	}
	if _, ok := got[4]; ok {
		t.Fatalf("leaked a physical id into the logical map: %v", got)
	}
}
