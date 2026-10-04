package cluster

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// newFollowerRaftNodeWithID stands up a three-voter in-process Raft cluster,
// waits for a settled election, and returns a Node wrapping one of the two
// FOLLOWERS — with cfg.NodeID / cfg.AdminAddr filled in so it behaves like the
// production Node. Returns the follower node and the elected leader's id.
func newFollowerRaftNodeWithID(t *testing.T) (*Node, string) {
	t.Helper()
	ids := []string{"node-1", "node-2", "node-3"}

	type member struct {
		r     *raft.Raft
		trans *raft.InmemTransport
	}
	members := map[string]*member{}
	servers := make([]raft.Server, 0, len(ids))
	addrs := map[string]raft.ServerAddress{}
	for _, id := range ids {
		addr := raft.ServerAddress("peer-" + id)
		_, trans := raft.NewInmemTransport(addr)
		addrs[id] = addr
		members[id] = &member{trans: trans}
		servers = append(servers, raft.Server{ID: raft.ServerID(id), Address: addr})
	}
	for _, a := range ids {
		for _, b := range ids {
			if a != b {
				members[a].trans.Connect(addrs[b], members[b].trans)
			}
		}
	}
	start := func(id string, bootstrap bool) {
		rc := raft.DefaultConfig()
		rc.LocalID = raft.ServerID(id)
		rc.ElectionTimeout = 50 * time.Millisecond
		rc.LeaderLeaseTimeout = 50 * time.Millisecond
		rc.HeartbeatTimeout = 50 * time.Millisecond
		rc.LogLevel = "ERROR"
		store := raft.NewInmemStore()
		r, err := raft.NewRaft(rc, &FSM{applier: &Engine{}}, store, store, raft.NewDiscardSnapshotStore(), members[id].trans)
		if err != nil {
			t.Fatalf("new raft %s: %v", id, err)
		}
		members[id].r = r
		if bootstrap {
			if err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
				t.Fatalf("bootstrap: %v", err)
			}
		}
	}
	start("node-1", true)
	start("node-2", false)
	start("node-3", false)
	t.Cleanup(func() {
		for _, id := range ids {
			if members[id].r != nil {
				members[id].r.Shutdown().Error()
			}
		}
	})

	deadline := time.Now().Add(15 * time.Second)
	leaderID := ""
	for time.Now().Before(deadline) {
		leaderID = ""
		agree := true
		for _, id := range ids {
			_, lid := members[id].r.LeaderWithID()
			if lid == "" {
				agree = false
				break
			}
			if leaderID == "" {
				leaderID = string(lid)
			} else if string(lid) != leaderID {
				agree = false
				break
			}
		}
		if agree && leaderID != "" {
			break
		}
		leaderID = ""
		time.Sleep(10 * time.Millisecond)
	}
	if leaderID == "" {
		t.Fatal("in-process raft never settled on a leader")
	}
	for _, id := range ids {
		if id == leaderID {
			continue
		}
		n := &Node{
			cfg:  Config{NodeID: id, AdminAddr: "http://127.0.0.1:809" + id[len(id)-1:], ApplyTimeout: 2 * time.Second},
			raft: members[id].r,
		}
		return n, leaderID
	}
	t.Fatal("unreachable: every node claims to be the leader")
	return nil, ""
}

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
