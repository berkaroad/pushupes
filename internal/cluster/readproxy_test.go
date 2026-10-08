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
	"testing"

	"pushupes/internal/storage"
)

// ReadProxyAddr decides, on every read RPC, whether this node serves a slot
// locally or forwards it to the slot leader. It used to answer that by cloning
// the whole slot table (a pointer, a slice and a map write per slot) — measured
// at 94% of ReadStream's CPU and ~25% of a live node's, for a question about one
// slot.
//
// The rewrite reads the placement and the leader's client address under one
// table lock instead. These tests pin the answer itself, which is the contract
// that must not move: a wrong answer serves a read locally where it should be
// forwarded (returning nothing, silently) or forwards a read the node could
// serve from its own log.
func TestReadProxyAddrMatchesTableSemantics(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	tbl := e.TableSnapshot()
	if len(tbl.Slots) == 0 {
		t.Fatal("test setup: no placements")
	}

	// The reference semantics, spelled out from the table for every slot.
	want := func(slot int32) string {
		p, ok := tbl.Slots[slot]
		if !ok {
			return ""
		}
		if p.Leader == e.self {
			return ""
		}
		for _, r := range p.Replicas {
			if r == e.self {
				return ""
			}
		}
		return tbl.Peers[p.Leader].ClientAddr
	}

	locals, forwards := 0, 0
	for slot := range tbl.Slots {
		got, exp := e.ReadProxyAddr(slot), want(slot)
		if got != exp {
			t.Fatalf("slot %d: ReadProxyAddr = %q, want %q (leader=%s replicas=%v)",
				slot, got, exp, tbl.Slots[slot].Leader, tbl.Slots[slot].Replicas)
		}
		if got == "" {
			locals++
		} else {
			forwards++
		}
	}
	// Both directions must be exercised or this test proves nothing about the
	// branch it skipped.
	if locals == 0 {
		t.Error("no slot resolved locally: a node must serve the slots it leads")
	}
	if forwards == 0 {
		t.Error("no slot forwarded: with present replicas this node should not hold every slot")
	}

	// An unassigned slot answers "" (local behavior), never a panic or a bogus
	// forward.
	for s := int32(0); s < 8192; s++ {
		if _, ok := tbl.Slots[s]; !ok {
			if got := e.ReadProxyAddr(s); got != "" {
				t.Errorf("unassigned slot %d: got %q, want \"\"", s, got)
			}
			break
		}
	}
}

// The three "" cases, isolated so a change in one fails as that case rather than
// as an unexplained mismatch. Each is built from a slot this node does not hold
// by default, then made local one way.
func TestReadProxyAddrLocalCases(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	tbl := e.TableSnapshot()

	// Find, from the real planning, a slot this node LEADS and one it only
	// REPLICATES, plus one it does not hold at all.
	//
	// "Not held" must be checked against BOTH roles: with RF=2 over 3 nodes this
	// node replicates most slots it does not lead, so taking the first slot led
	// elsewhere would pick one it actually holds and make the forward assertion
	// fail (map iteration order made that flaky, not just wrong).
	var led, replicated, remote int32 = -1, -1, -1
	for s, p := range tbl.Slots {
		if p.Leader == "node-1" {
			led = s
			continue
		}
		if replicaListHas(p.Replicas, "node-1") {
			if replicated < 0 {
				replicated = s
			}
			continue
		}
		if remote < 0 {
			remote = s
		}
	}
	if led < 0 {
		t.Fatalf("test setup: node-1 leads no slot (led=%d)", led)
	}
	if remote < 0 {
		// The planning hands this node a copy of every slot when the replica
		// factor covers the cluster; trim one so the forward case is real
		// rather than skipped.
		for s, p := range tbl.Slots {
			if p.Leader != "node-1" && replicaListHas(p.Replicas, "node-1") && len(p.Replicas) > 1 {
				applyCmd(t, e, &Command{Op: OpSlotRemoveReplica, Slots: []int32{s}, NodeID: "node-1"})
				remote = s
				break
			}
		}
	}
	if remote < 0 {
		t.Fatal("test setup: could not place a slot this node does not hold")
	}
	tbl = e.TableSnapshot()
	if replicaListHas(tbl.Slots[remote].Replicas, "node-1") || tbl.Slots[remote].Leader == "node-1" {
		t.Fatalf("test setup: slot %d is still held by node-1 (leader=%s replicas=%v)",
			remote, tbl.Slots[remote].Leader, tbl.Slots[remote].Replicas)
	}

	if got := e.ReadProxyAddr(led); got != "" {
		t.Errorf("leader slot %d: got %q, want \"\" (a leader reads its own log)", led, got)
	}
	if replicated >= 0 {
		if got := e.ReadProxyAddr(replicated); got != "" {
			t.Errorf("replica slot %d: got %q, want \"\" (a replica reads its own log, bounded by its LEO)",
				replicated, got)
		}
	}

	// The remote case forwards to the leader's CLIENT address.
	got := e.ReadProxyAddr(remote)
	want := tbl.Peers[tbl.Slots[remote].Leader].ClientAddr
	if got != want {
		t.Errorf("remote slot %d: got %q, want the leader's client addr %q", remote, got, want)
	}
	if got == "" {
		t.Error("remote slot resolved locally: the read would silently return nothing")
	}
}

// LeaderReplica is the primitive the read paths share: one table-lock read
// answering "who leads this slot, where is that leader's gRPC plane, and do I
// hold the slot (as leader or replica)?". ReadProxyAddr is one policy on top of
// it; anything else that needs the same facts must not go back to cloning the
// table.
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
		// local must mean "leads OR replicates", which is what makes a read
		// serviceable here.
		wantLocal := p.Leader == e.self || replicaListHas(p.Replicas, e.self)
		if local != wantLocal {
			t.Fatalf("slot %d: local=%v, want %v (leader=%s replicas=%v self=%s)",
				slot, local, wantLocal, p.Leader, p.Replicas, e.self)
		}
		// Consistency with the policy built on top of it.
		wantProxy := ""
		if !local {
			wantProxy = addr
		}
		if got := e.ReadProxyAddr(slot); got != wantProxy {
			t.Fatalf("slot %d: ReadProxyAddr=%q, want %q from LeaderReplica", slot, got, wantProxy)
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

	applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{
		ID: "node-1", PeerAddr: "127.0.0.1:8391", AdminAddr: "127.0.0.1:8091", ClientAddr: "127.0.0.1:8591",
	}})
	applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{
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

// The address must be the CLIENT-plane one: a reader dials it as a gRPC event
// client, so handing back the peer (raft) address would aim reads at the raft
// listener.
func TestReadProxyAddrUsesClientPlane(t *testing.T) {
	st, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	e := NewEngine(nil, st, "node-1", nil)

	applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{
		ID: "node-1", PeerAddr: "127.0.0.1:8391", AdminAddr: "127.0.0.1:8091", ClientAddr: "127.0.0.1:8591",
	}})
	applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{
		ID: "node-2", PeerAddr: "127.0.0.1:8392", AdminAddr: "127.0.0.1:8092", ClientAddr: "127.0.0.1:8592",
	}})
	// This test needs a slot led by node-2 with node-1 as a removable replica;
	// two members derive a single copy, so the fixture pins two.
	pinReplicas(t, e, 2)
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	// A slot led by node-2 with node-1 removed from its replica set, so this
	// node holds no part of it. With RF=2 over 3 nodes the planning gives
	// node-1 a copy of every slot, so the set has to be trimmed explicitly —
	// otherwise this test skips and proves nothing about the client plane.
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
	if got := e.ReadProxyAddr(target); got != want {
		t.Errorf("ReadProxyAddr = %q, want the client-plane address %q (peer addr is %q)",
			got, want, tbl.Peers[leader].PeerAddr)
	}
}
