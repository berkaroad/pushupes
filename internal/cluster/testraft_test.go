package cluster

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// newTestRaftNode stands up an in-process single-voter Raft so tests can
// exercise the real submit -> FSM -> table path without networking.
func newTestRaftNode(t *testing.T, applier Applier) *Node {
	t.Helper()
	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID("node-1")
	rc.ElectionTimeout = 50 * time.Millisecond
	rc.LeaderLeaseTimeout = 50 * time.Millisecond
	rc.HeartbeatTimeout = 50 * time.Millisecond

	store := raft.NewInmemStore()
	snap := raft.NewDiscardSnapshotStore()
	addr, trans := raft.NewInmemTransport(raft.ServerAddress("node-1"))
	trans.Connect(addr, trans)

	r, err := raft.NewRaft(rc, &FSM{applier: applier}, store, store, snap, trans)
	if err != nil {
		t.Fatalf("new raft: %v", err)
	}
	f := r.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{{ID: raft.ServerID("node-1"), Address: raft.ServerAddress("node-1")}},
	})
	if err := f.Error(); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	n := &Node{
		cfg:  Config{NodeID: "node-1", AdminAddr: "127.0.0.1:1", ApplyTimeout: 5 * time.Second},
		raft: r,
	}
	deadline := time.Now().Add(3 * time.Second)
	for !n.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !n.IsLeader() {
		t.Fatal("test raft never became leader")
	}
	t.Cleanup(func() { r.Shutdown().Error() })
	return n
}
