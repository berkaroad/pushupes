package cluster

import (
	"testing"
)

// markdownTable builds a 3-node ring table with all members registered
// (active): 8 slots, RF 2 — node-1 leads 0,3,6.
func markdownTable() *Table {
	tbl := NewTable(8, 2)
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		tbl.Peers[id] = Peer{ID: id, PeerAddr: "http://" + id + ":8391",
			AdminAddr: "http://" + id + ":8091", ClientAddr: "http://" + id + ":8591"}
	}
	tbl.Slots = PlanSlots([]string{"node-1", "node-2", "node-3"}, 8, 2)
	return tbl
}

// TestMarkDownKeepsTheEntry pins the anti-flicker fix: a failed peer is
// MARKED down — leadership moves to live replicas, but the directory entry,
// its announced addresses and its replica-set membership all SURVIVE. The
// old leave_node-on-liveness deleted the entry, the raft-config sync
// re-seeded it without addresses, and the console watched the node flicker
// between "gone" and "address-less shell" every couple of seconds.
func TestMarkDownKeepsTheEntry(t *testing.T) {
	tbl := markdownTable()
	if err := tbl.Apply(&Command{Op: OpMarkDown, NodeID: "node-1"}); err != nil {
		t.Fatal(err)
	}
	p, ok := tbl.Peers["node-1"]
	if !ok {
		t.Fatal("mark_down must NOT delete the directory entry")
	}
	if !p.Down || p.ClientAddr == "" || p.AdminAddr == "" {
		t.Fatalf("entry must keep its addresses and carry the down flag: %+v", p)
	}
	// every slot node-1 led has a live leader now, and node-1 is still a
	// replica (its copy comes back via fetch when it returns)
	for _, s := range []int32{0, 3, 6} {
		pl := tbl.Slots[s]
		if pl.Leader == "node-1" || pl.Leader == "" {
			t.Fatalf("slot %d still led by the down node: %+v", s, pl)
		}
		if !replicaListHas(pl.Replicas, "node-1") {
			t.Fatalf("slot %d: replica set must survive the down-mark: %+v", s, pl)
		}
		if pl.Epoch < 2 {
			t.Fatalf("slot %d: leadership change must bump the epoch: %+v", s, pl)
		}
	}
	// the offline rule reads the flag
	if !p.Offline() {
		t.Fatal("sanity: a marked-down peer must read as offline")
	}
}

// TestMarkUpClearsTheFlag: the node answering probes again returns to the
// active set — the ring rebalancer will hand its slots back on its own.
func TestMarkUpClearsTheFlag(t *testing.T) {
	tbl := markdownTable()
	if err := tbl.Apply(&Command{Op: OpMarkDown, NodeID: "node-1"}); err != nil {
		t.Fatal(err)
	}
	if err := tbl.Apply(&Command{Op: OpMarkUp, NodeID: "node-1"}); err != nil {
		t.Fatal(err)
	}
	if tbl.Peers["node-1"].Down {
		t.Fatal("mark_up must clear the down flag")
	}
	// with all three active again, the ring IS node-1's original layout:
	// the rebalancer plans exactly its slots back
	moves := PlanLeaderRebalance(tbl)
	if len(moves) != 3 {
		t.Fatalf("want node-1's 3 ring slots planned back, got %v", moves)
	}
	for _, m := range moves {
		if m.To != "node-1" {
			t.Fatalf("unexpected move %+v", m)
		}
	}
}

// TestOfflinePeerLeavesTheRebalanceRing: the rebalancer ring and the
// failover leader pick only know ACTIVE members — a down node never gets
// leadership back until it answers probes.
func TestOfflinePeerLeavesTheRebalanceRing(t *testing.T) {
	tbl := markdownTable()
	if err := tbl.Apply(&Command{Op: OpMarkDown, NodeID: "node-2"}); err != nil {
		t.Fatal(err)
	}
	active := tbl.OnlinePeerIDs()
	if len(active) != 2 || active[0] != "node-1" || active[1] != "node-3" {
		t.Fatalf("OnlinePeerIDs must exclude the down member: %v", active)
	}
	// the leader of every slot must not sit on the down member
	for s, p := range tbl.Slots {
		if p.Leader == "node-2" {
			t.Fatalf("slot %d: a down member must not lead: %+v", s, p)
		}
	}
	// and the rebalancer must never plan a move onto it
	for _, m := range PlanLeaderRebalance(tbl) {
		if m.To == "node-2" {
			t.Fatalf("rebalance target is offline: %+v", m)
		}
	}
	// its replica seats survive (the copy is still home for the return)
	kept := 0
	for _, p := range tbl.Slots {
		if replicaListHas(p.Replicas, "node-2") {
			kept++
		}
	}
	if kept == 0 {
		t.Fatal("a down member must keep its replica-set membership")
	}
}

// TestBackupLeaderPrefersActive: a slot led by the down node whose first
// replica is ALSO offline (no announced addr) must still find a leader the
// clients can reach when one exists in the set.
func TestBackupLeaderPrefersActive(t *testing.T) {
	tbl := NewTable(2, 2)
	tbl.Peers["node-1"] = Peer{ID: "node-1", ClientAddr: "http://node-1:8591"}
	tbl.Peers["node-2"] = Peer{ID: "node-2"} // joined, not registered: offline shell
	tbl.Peers["node-3"] = Peer{ID: "node-3", ClientAddr: "http://node-3:8591"}
	tbl.Slots[0] = &Placement{Leader: "node-1", Replicas: []string{"node-2", "node-1", "node-3"}, Epoch: 1}
	if err := tbl.Apply(&Command{Op: OpMarkDown, NodeID: "node-1"}); err != nil {
		t.Fatal(err)
	}
	if got := tbl.Slots[0].Leader; got != "node-3" {
		t.Fatalf("backup leader pick must skip the unregistered node-2 and land on active node-3, got %q", got)
	}
}

// TestTableBinaryRoundTripDownFlag pins that the controller's verdict
// survives Raft snapshots (a bootstrapped follower must know a peer is down
// without waiting for the next sweep).
func TestTableBinaryRoundTripDownFlag(t *testing.T) {
	tbl := markdownTable()
	if err := tbl.Apply(&Command{Op: OpMarkDown, NodeID: "node-3"}); err != nil {
		t.Fatal(err)
	}
	raw := tbl.EncodeTableBinary()
	out, err := DecodeTableBinary(raw)
	if err != nil {
		t.Fatal(err)
	}
	for id, p := range out.Peers {
		want := tbl.Peers[id]
		if p.Down != want.Down || p.ClientAddr != want.ClientAddr || p.PeerAddr != want.PeerAddr {
			t.Fatalf("peer %s changed across the round trip: got %+v want %+v", id, p, want)
		}
	}
	for s, p := range out.Slots {
		want := tbl.Slots[s]
		if p.Leader != want.Leader || p.Epoch != want.Epoch {
			t.Fatalf("slot %d changed across the round trip: %+v vs %+v", s, p, want)
		}
	}
}
