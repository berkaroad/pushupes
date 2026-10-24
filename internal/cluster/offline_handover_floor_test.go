package cluster

// The offline-source commit branch of the handover is the other door through
// which an empty-shell leader is born: the source process is gone, nobody can
// push a frozen tail, and the branch used to commit the leader_move BLIND —
// no floor, and no check of whether the target is current. A re-granted empty
// seat (or any behind copy) could then be seated as leader of records only
// its peers hold: the clients meet the empty directory's version 0, the fetch
// direction is inverted (the leader never pulls from its own replica), and the
// slot rots (live cluster, Oct 24: 82 such DIVERGED slots, writes stuck).
//
// The fix is the failover probe's own discipline moved to this branch: the
// live replicas — the target included — are asked for their durable LEO, the
// best report is the floor, and a target BELOW that bound does not get the
// slot at all (the stage rolls back, the caller retries; if the source
// returns, the fenced path supplies the real frozen tail). When the target is
// current, the commit carries the best live report as CommitLEO.

import (
	"context"
	"fmt"
	"testing"
)

func TestOfflineSourceHandoverRejectsBehindTarget(t *testing.T) {
	ctrl, _ := newTestEngine(t, "node-1")
	ctrl.node = newTestRaftNode(t, ctrl)

	addrs := map[string]string{}
	{
		e3, st3 := newTestEngine(t, "node-3")
		agg := aggInSlot(t, e3, 0)
		for v := 1; v <= 5; v++ {
			if _, err := st3.Append(makeRecord(agg, uint32(v), fmt.Sprintf("n3-%d", v))); err != nil {
				t.Fatal(err)
			}
		}
		addrs["node-3"] = newPeerHarness(t, e3)
	}
	{
		// node-4: a re-granted empty seat — reachable, holds nothing.
		e4, _ := newTestEngine(t, "node-4")
		addrs["node-4"] = newPeerHarness(t, e4)
	}
	// node-2: the dead leader.
	for _, id := range []string{"node-2", "node-3", "node-4"} {
		p := Peer{Role: RoleLearner, ID: id, PeerAddr: addrs[id], ClientAddr: addrs[id]}
		if id == "node-2" {
			p = Peer{Role: RoleLearner, ID: id, PeerAddr: "127.0.0.1:1", ClientAddr: "http://127.0.0.1:1"}
		}
		applyCmd(t, ctrl, &Command{Op: OpJoinNode, Peer: &p})
	}
	applyCmd(t, ctrl, &Command{Op: OpConfig, Replicas: 3})
	applyCmd(t, ctrl, &Command{Op: OpPlanSlots})
	ctrl.tableMu.Lock()
	ctrl.table.Slots[0] = &Placement{
		Leader: "node-2", Replicas: []string{"node-2", "node-3", "node-4"},
		Epoch: 1, State: SlotStable,
	}
	pe := ctrl.table.Peers["node-2"]
	pe.Down = true
	ctrl.table.Peers["node-2"] = pe
	ctrl.tableMu.Unlock()

	ctx := context.Background()

	// 1) Behind target: node-4 holds 0 while node-3 reports 5. The move must
	//    be refused, the stage rolled back, and leadership stay put.
	if err := ctrl.commitMigration(ctx, 0, "node-4"); err == nil {
		t.Fatal("an offline-source commit seated leadership on a copy behind the best live report")
	}
	if p := ctrl.TableSnapshot().Slots[0]; p.Leader != "node-2" || p.State != SlotStable {
		t.Fatalf("deferred handover left the move or the stage armed: %+v", p)
	}

	// 2) Current target: node-3 sits at the best report — the commit goes
	//    through and carries the live evidence as the slot's floor.
	if err := ctrl.commitMigration(ctx, 0, "node-3"); err != nil {
		t.Fatalf("handover to the current target: %v", err)
	}
	tbl := ctrl.TableSnapshot()
	if p := tbl.Slots[0]; p.Leader != "node-3" || p.State != SlotStable {
		t.Fatalf("handover did not complete: %+v", p)
	}
	if got := tbl.CommitLEO[0]; got != 5 {
		t.Fatalf("offline-source commit carried floor %d, want 5 (best live report bounds later promotions)", got)
	}
}
