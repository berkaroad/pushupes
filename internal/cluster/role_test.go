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
	"errors"
	"testing"
	"time"

	"github.com/berkaroad/pushupes/internal/data"
)

// The role axis (DESIGN §12): voters are control-plane-only — they vote, host
// no slots, and the layout pool (Table.HostingIDs) is the single place that
// decides it. These tests pin the pool, the admission commands, and the
// write-path guard that keeps a pure control node off slot data.

func roleTable(members map[string]string) *Table {
	t := NewTable(8, 2)
	for id, addr := range members {
		t.Peers[id] = Peer{ID: id, PeerAddr: addr, AdminAddr: addr, ClientAddr: addr, Role: ""}
	}
	return t
}

func TestPlanSlotsSkipsVoters(t *testing.T) {
	tbl := roleTable(map[string]string{
		"ctrl":  "127.0.0.1:9001",
		"data1": "127.0.0.1:9002",
		"data2": "127.0.0.1:9003",
	})
	if err := tbl.Apply(&Command{Op: OpRegister, Peer: &Peer{ID: "ctrl", AdminAddr: "127.0.0.1:9001", ClientAddr: "127.0.0.1:9001", Role: RoleVoter}}); err != nil {
		t.Fatal(err)
	}
	if err := tbl.Apply(&Command{Op: OpPlanSlots}); err != nil {
		t.Fatal(err)
	}
	for s, p := range tbl.Slots {
		if p.Leader == "ctrl" || replicaListHas(p.Replicas, "ctrl") {
			t.Fatalf("slot %d places the pure voter: %+v", s, p)
		}
	}
	if len(tbl.HostingIDs()) != 2 {
		t.Fatalf("hosting pool = %v, want the two data members", tbl.HostingIDs())
	}
	// The failover/rebalance target set excludes voters too, online or not.
	for _, id := range tbl.OnlinePeerIDs() {
		if id == "ctrl" {
			t.Fatal("the online set carries a pure voter")
		}
	}
}

func TestPlanSlotsWithoutDataPlane(t *testing.T) {
	tbl := roleTable(map[string]string{"ctrl": "127.0.0.1:9001"})
	if err := tbl.Apply(&Command{Op: OpRegister, Peer: &Peer{ID: "ctrl", AdminAddr: "127.0.0.1:9001", ClientAddr: "127.0.0.1:9001", Role: RoleVoter}}); err != nil {
		t.Fatal(err)
	}
	err := tbl.Apply(&Command{Op: OpPlanSlots})
	if err == nil {
		t.Fatal("planning with an empty hosting pool must be an explicit refusal")
	}
}

func TestRegisterAnnouncesRole(t *testing.T) {
	tbl := roleTable(map[string]string{"n1": "127.0.0.1:9001"})
	// An announcement carrying no role never erases a recorded one...
	if err := tbl.Apply(&Command{Op: OpRegister, Peer: &Peer{ID: "n1", AdminAddr: "127.0.0.1:9001", ClientAddr: "127.0.0.1:9001", Role: RoleVoter}}); err != nil {
		t.Fatal(err)
	}
	if err := tbl.Apply(&Command{Op: OpRegister, Peer: &Peer{ID: "n1", AdminAddr: "127.0.0.1:9001", ClientAddr: "127.0.0.1:9001"}}); err != nil {
		t.Fatal(err)
	}
	if got := tbl.Peers["n1"].Role; got != RoleVoter {
		t.Fatalf("role-less announcement rewrote the recorded role to %q", got)
	}
	// ...and one that carries a role updates it (the node's own flag wins).
	if err := tbl.Apply(&Command{Op: OpRegister, Peer: &Peer{ID: "n1", AdminAddr: "127.0.0.1:9001", ClientAddr: "127.0.0.1:9001", Role: RoleLearner}}); err != nil {
		t.Fatal(err)
	}
	if got := tbl.Peers["n1"].Role; got != RoleLearner {
		t.Fatalf("announcement role not applied: %q", got)
	}
}

func TestTableBinaryCarriesRoles(t *testing.T) {
	tbl := roleTable(map[string]string{"ctrl": "127.0.0.1:9001", "d1": "127.0.0.1:9002"})
	if err := tbl.Apply(&Command{Op: OpRegister, Peer: &Peer{ID: "ctrl", AdminAddr: "127.0.0.1:9001", ClientAddr: "127.0.0.1:9001", Role: RoleVoter}}); err != nil {
		t.Fatal(err)
	}
	tbl.PlanMembers = tbl.HostingIDs()
	if err := tbl.Apply(&Command{Op: OpPlanSlots}); err != nil {
		t.Fatal(err)
	}
	back, err := DecodeTableBinary(tbl.EncodeTableBinary())
	if err != nil {
		t.Fatal(err)
	}
	if back.Peers["ctrl"].Role != RoleVoter {
		t.Fatalf("role lost through the snapshot encoding: %+v", back.Peers["ctrl"])
	}
	if back.Peers["d1"].Role != "" {
		t.Fatalf("unannounced role materialized: %+v", back.Peers["d1"])
	}
	// The restored table plans without the voter: the pool survives a
	// snapshot, so a restarted cluster never re-seats the control plane.
	if err := back.Apply(&Command{Op: OpPlanSlots}); err != nil {
		t.Fatal(err)
	}
	for _, p := range back.Slots {
		if p.Leader == "ctrl" {
			t.Fatal("restored table placed a slot on the control node")
		}
	}
}

// settledCommit waits until the leader's commit index stops moving, then
// returns it. A freshly elected leader appends its own no-op asynchronously:
// it can land between a test's baseline read and its assertion, which turns a
// "exactly one command was submitted" check into a flake under load (observed
// once in the full parallel suite, green in isolation).
func settledCommit(t *testing.T, n consensus) uint64 {
	t.Helper()
	prev := commitIndexOf(t, n)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		cur := commitIndexOf(t, n)
		if cur == prev {
			return cur
		}
		prev = cur
	}
	return prev
}

func TestAddMemberLearnerAdmission(t *testing.T) {
	e := newLeaderEngine(t)
	ctx := context.Background()
	before := settledCommit(t, e.node)
	if err := e.AddMember(ctx, "node-9", "127.0.0.1:9909", RoleLearner); err != nil {
		t.Fatal(err)
	}
	p := e.TableSnapshot().Peers["node-9"]
	if p.Role != RoleLearner || p.PeerAddr == "" {
		t.Fatalf("admission did not land in the directory: %+v", p)
	}
	if e.node.IsMember("node-9") {
		t.Fatal("a learner admission joined the raft configuration — it must not")
	}
	// Idempotent: re-admitting the same live learner commits nothing new.
	if err := e.AddMember(ctx, "node-9", "127.0.0.1:9909", RoleLearner); err != nil {
		t.Fatal(err)
	}
	if after := commitIndexOf(t, e.node); after != before+1 {
		t.Fatalf("commit index %d -> %d, want exactly the one admission entry", before, after)
	}
	// A name that already votes cannot also be admitted as a learner.
	if err := e.AddMember(ctx, "node-1", "127.0.0.1:9901", RoleLearner); !errors.Is(err, ErrBadMember) {
		t.Fatalf("voter-name collision admitted as learner: %v", err)
	}
}

func TestRemoveLearnerGuardsAndRevokes(t *testing.T) {
	e := newLeaderEngine(t)
	ctx := context.Background()
	if err := e.AddMember(ctx, "node-9", "127.0.0.1:9909", RoleLearner); err != nil {
		t.Fatal(err)
	}
	// A learner that announced its client address is ONLINE: revoking it
	// while the cluster uses its seats is refused, exactly like a voter.
	if err := e.acceptRegistration(&Registration{ID: "node-9", AdminAddr: "127.0.0.1:9909", ClientAddr: "127.0.0.1:9909", Role: RoleLearner}); err != nil {
		t.Fatal(err)
	}
	if err := e.RemoveMember(ctx, "node-9"); !errors.Is(err, ErrOnlineMember) {
		t.Fatalf("online learner revoked: %v", err)
	}
	// Marked down, the same revocation goes through as a directory leave.
	applyCmd(t, e, &Command{Op: OpMarkDown, NodeID: "node-9"})
	if err := e.RemoveMember(ctx, "node-9"); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.TableSnapshot().Peers["node-9"]; ok {
		t.Fatal("revoked learner still in the directory")
	}
}

func TestMembersViewListsBothPlanes(t *testing.T) {
	e := newLeaderEngine(t)
	if err := e.AddMember(context.Background(), "node-9", "127.0.0.1:9909", RoleLearner); err != nil {
		t.Fatal(err)
	}
	views := e.Members()
	var sawVoter, sawLearner bool
	for _, v := range views {
		switch v.ID {
		case "node-1":
			sawVoter = true
			if v.Role != RoleVoter {
				t.Fatalf("voter view role = %q, want voter", v.Role)
			}
		case "node-9":
			sawLearner = true
			if v.Role != RoleLearner {
				t.Fatalf("learner view role = %q", v.Role)
			}
		}
	}
	if !sawVoter || !sawLearner {
		t.Fatalf("membership view = %+v, want the voter AND the learner", views)
	}
}

// TestVoterRefusesUnassignedWrites pins the write-path half of the split:
// the classic cluster answers an unassigned slot locally (single-node shape),
// but a node the directory knows as a pure control node must refuse — its
// WAL growing an unknown seq space is exactly the contamination the layout
// rule exists to keep off the control plane.
func TestVoterRefusesUnassignedWrites(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	e.tableMu.Lock()
	e.table.Peers["node-1"] = Peer{ID: "node-1", PeerAddr: "127.0.0.1:9001", AdminAddr: "127.0.0.1:9001", ClientAddr: "127.0.0.1:9001", Role: RoleVoter}
	e.tableMu.Unlock()
	rec := func(id string) *data.EventRecord {
		return &data.EventRecord{
			AggregateID: id, Version: 1, CommandID: "cmd-" + id,
			Events: []data.Event{{Type: "T", Body: []byte("x")}},
		}
	}
	resp, err := e.SubmitAppend(context.Background(), rec("agg-x"))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != data.StatusFail || resp.ErrID != data.ErrIDNotLeader {
		t.Fatalf("voter append = %+v, want fail/1005 from the control-plane guard", resp)
	}

	// The SAME refusal on the batch path: the client plane (BatchAppend)
	// routes through SubmitBatch, which had its own unassigned fast path —
	// guarding only the single-record path left the real client entry point
	// writing onto the control plane (found live, then pinned here).
	out, err := e.SubmitBatch(context.Background(), e.SlotOf("agg-y"), []*data.EventRecord{rec("agg-y"), rec("agg-z")})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("batch answered %d results, want one per record", len(out))
	}
	for i, r := range out {
		if r.Status != data.StatusFail || r.ErrID != data.ErrIDNotLeader {
			t.Fatalf("batch record %d = %+v, want fail/1005 from the control-plane guard", i, r)
		}
	}
	if out[0] == out[1] {
		t.Fatal("batch records share one response object; each wire answer must be its own")
	}
}
