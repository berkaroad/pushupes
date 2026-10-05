package cluster

import (
	"testing"
)

// TestAdminLeaderRedirect pins what the console's leader-following relies on:
// a FOLLOWER's status answer must name the elected leader AND carry that
// leader's admin address in the peer directory, so a client that started from
// any pool member can resolve where the controller lives and pin its traffic
// there. It also covers the mid-election gap: while no leader is known the
// redirect fields stay empty and the client keeps asking.
func TestAdminLeaderRedirect(t *testing.T) {
	follower, leaderID := newFollowerRaftNodeWithID(t)
	e, _ := newTestEngine(t, follower.cfg.NodeID)
	e.node = follower

	// Registration is what fills admin addresses into the replicated table;
	// drive the FSM directly (a follower cannot submit commands).
	admins := map[string]string{
		"node-1": "http://127.0.0.1:8091",
		"node-2": "http://127.0.0.1:8092",
		"node-3": "http://127.0.0.1:8093",
	}
	for id, addr := range admins {
		applyCmd(t, e, &Command{Op: OpJoinNode, Peer: &Peer{ID: id, PeerAddr: "peer-" + id, AdminAddr: addr, ClientAddr: addr}})
	}

	stats := follower.Stats()
	gotLeader, _ := stats["leader"].(string)
	if gotLeader != leaderID {
		t.Fatalf("stats leader = %q, want %q", gotLeader, leaderID)
	}
	tbl := e.TableSnapshot()
	p, ok := tbl.Peers[leaderID]
	if !ok || p.AdminAddr == "" {
		t.Fatalf("peer directory lacks the leader's admin address: %+v", tbl.Peers)
	}
	if p.AdminAddr != admins[leaderID] {
		t.Fatalf("leader admin addr = %q, want %q", p.AdminAddr, admins[leaderID])
	}

	// Controller() is the same lookup the server-side refusal uses: id +
	// admin address resolved from the table.
	cid, caddr := e.Controller()
	if cid != leaderID || caddr != admins[leaderID] {
		t.Fatalf("Controller() = (%q,%q), want (%q,%q)", cid, caddr, leaderID, admins[leaderID])
	}
}
