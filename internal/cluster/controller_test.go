package cluster

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// newFollowerRaftNode stands up a three-voter in-process Raft cluster and
// returns a Node that is a FOLLOWER, plus the leader's node id. The
// "controller-only command lands on a follower" path is guarded by
// Node.IsLeader(), so it can only be exercised against a real election.
func newFollowerRaftNode(t *testing.T) (*Node, string) {
	t.Helper()
	ids := []string{"node-1", "node-2", "node-3"}

	type member struct {
		r     *raft.Raft
		trans *raft.InmemTransport
	}
	members := map[string]*member{}
	servers := make([]raft.Server, 0, len(ids))
	// The transport address is deliberately NOT the node id: LeaderWithID
	// returns (address, ServerID), and ids are what the slot table and peer
	// directory are keyed by, so the two must be distinguishable here.
	addrs := map[string]raft.ServerAddress{}
	for _, id := range ids {
		addr := raft.ServerAddress("peer-" + id)
		_, trans := raft.NewInmemTransport(addr)
		addrs[id] = addr
		members[id] = &member{trans: trans}
		servers = append(servers, raft.Server{ID: raft.ServerID(id), Address: addr})
	}
	// Every transport must know every other one before the first election.
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

	// Wait for a settled election: exactly one leader and every voter
	// agreeing on who it is.
	deadline := time.Now().Add(15 * time.Second)
	leaderID := ""
	for time.Now().Before(deadline) {
		leaderID = ""
		agree := true
		for _, id := range ids {
			// (address, ServerID): the id is the second value.
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
		n := &Node{cfg: Config{NodeID: id, ApplyTimeout: 2 * time.Second}, raft: members[id].r}
		return n, leaderID
	}
	t.Fatal("unreachable: every node claims to be the leader")
	return nil, ""
}

// TestFollowerRefusesControllerCommandsWithControllerInfo pins the rule that
// every command mutating replicated (Raft) state is issued on the controller
// (the Raft leader) itself: a follower must refuse — never forward — and the
// refusal must be actionable, i.e. name the controller's node id and admin
// address so the caller can retry against the right node.
func TestFollowerRefusesControllerCommandsWithControllerInfo(t *testing.T) {
	follower, leaderID := newFollowerRaftNode(t)
	e, _ := newTestEngine(t, follower.cfg.NodeID)
	e.node = follower

	// The peer directory holds every node's admin address; at runtime
	// registration is what fills it in, which is exactly why the refusal can
	// only promise the address once the leader has announced itself.
	joins := map[string]string{
		"node-1": "http://127.0.0.1:8091",
		"node-2": "http://127.0.0.1:8092",
		"node-3": "http://127.0.0.1:8093",
	}
	for id, addr := range joins {
		join(t, e, id, addr)
	}
	// A follower's table is populated by Raft application; drive the FSM
	// directly so the slots we try to mutate actually exist.
	applyCmd(t, e, &Command{Op: OpPlanSlots})
	wantAddr := joins[leaderID]
	if wantAddr == "" {
		t.Fatalf("test setup: no admin address for leader %q", leaderID)
	}

	if id, addr := e.Controller(); id != leaderID || addr != wantAddr {
		t.Fatalf("Controller() = (%q, %q), want (%q, %q)", id, addr, leaderID, wantAddr)
	}

	before := e.TableSnapshot()
	cases := []struct {
		name string
		call func() error
	}{
		{"migrate (StartMigration)", func() error {
			return e.StartMigration(context.Background(), 0, "node-3")
		}},
		{"remove-replica (RemoveReplica)", func() error {
			return e.RemoveReplica(context.Background(), 0, "node-3")
		}},
		{"plan (SubmitCommand)", func() error {
			return e.SubmitCommand(&Command{Op: OpPlanSlots})
		}},
	}
	for _, tc := range cases {
		err := tc.call()
		if err == nil {
			t.Fatalf("%s: a follower must refuse, got nil", tc.name)
		}
		var nc *NotControllerError
		if !errors.As(err, &nc) {
			t.Fatalf("%s: got %T (%v), want *NotControllerError", tc.name, err, err)
		}
		if nc.LeaderID != leaderID {
			t.Fatalf("%s: controller id = %q, want %q", tc.name, nc.LeaderID, leaderID)
		}
		if nc.AdminAddr != wantAddr {
			t.Fatalf("%s: controller admin addr = %q, want %q", tc.name, nc.AdminAddr, wantAddr)
		}
		// The refusal must be readable on its own: both facts in the prose.
		if msg := err.Error(); !strings.Contains(msg, leaderID) || !strings.Contains(msg, wantAddr) {
			t.Fatalf("%s: error %q must name the controller id and admin address", tc.name, msg)
		}
		// Existing callers matching the sentinel keep working.
		if !errors.Is(err, ErrNotLeader) {
			t.Fatalf("%s: %v must unwrap to ErrNotLeader", tc.name, err)
		}
	}

	if after := e.TableSnapshot(); !reflect.DeepEqual(before, after) {
		t.Fatalf("a refusal must not touch the slot table:\nbefore %+v\nafter  %+v", before, after)
	}
}

// TestNotControllerErrorWithoutAddress keeps the "leader elected but not yet
// registered" case honest: the refusal still names the controller and says the
// address is not known yet, rather than pretending it can be retried.
func TestNotControllerErrorWithoutAddress(t *testing.T) {
	err := &NotControllerError{LeaderID: "node-2"}
	msg := err.Error()
	if !strings.Contains(msg, "node-2") || !strings.Contains(msg, "not known yet") {
		t.Fatalf("unregistered controller refusal is not actionable: %q", msg)
	}
	if !errors.Is(err, ErrNotLeader) {
		t.Fatal("must unwrap to ErrNotLeader")
	}
	if err := (&NotControllerError{}).Error(); !strings.Contains(err, "no leader elected yet") {
		t.Fatalf("leaderless refusal should say so: %q", err)
	}
}
