package grpcapi

import (
	"context"
	"testing"

	"github.com/berkaroad/pushupes/internal/cluster"
	pushupesv1 "github.com/berkaroad/pushupes/pkg/grpcapi/pushupes/v1"
)

// TestPrefetchRoutesIsDenseOverLogicalSlots pins the client plane's id space: the
// routing table answers one row per LOGICAL slot (0 .. group_size-1), and row l
// is the leader of the physical slot group_base + l. A client never sees 4..7.
func TestPrefetchRoutesIsDenseOverLogicalSlots(t *testing.T) {
	cli, eng, _ := newPrefetchClient(t)
	apply := func(c *cluster.Command) {
		if _, err := eng.ApplyCommand(c.Encode()); err != nil {
			t.Fatalf("apply %s: %v", c.Op, err)
		}
	}
	apply(&cluster.Command{Op: cluster.OpJoinNode, Peer: &cluster.Peer{Role: cluster.RoleLearner,
		ID: "node-1", PeerAddr: "p1", AdminAddr: "http://127.0.0.1:18091", ClientAddr: "http://127.0.0.1:18591"}})
	apply(&cluster.Command{Op: cluster.OpJoinNode, Peer: &cluster.Peer{Role: cluster.RoleLearner,
		ID: "node-2", PeerAddr: "p2", AdminAddr: "http://127.0.0.1:18092", ClientAddr: "http://127.0.0.1:18592"}})
	// g2 = physical 4..7 on an 8-slot table.
	apply(&cluster.Command{Op: cluster.OpPlanSlots, GroupGen: 2, GroupBase: 4, GroupSize: 4})

	resp, err := cli.PrefetchRoutes(context.Background(), &pushupesv1.PrefetchRoutesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(resp.SlotNodeIndex), 4; got != want {
		t.Fatalf("logical rows: got %d, want %d (the generation's size)", got, want)
	}
	if resp.GetGroupBase() != 4 || resp.GetGroupSize() != 4 {
		t.Fatalf("generation echoed as base=%d size=%d", resp.GetGroupBase(), resp.GetGroupSize())
	}
	phys := eng.SlotRouteTable()
	for logical := 0; logical < 4; logical++ {
		want := cluster.HostPort(phys[int32(4+logical)])
		idx := resp.SlotNodeIndex[logical]
		if int(idx) >= len(resp.NodeClientAddrs) {
			t.Fatalf("logical %d: row %d out of range", logical, idx)
		}
		if got := resp.NodeClientAddrs[idx]; got != want {
			t.Fatalf("logical %d -> %q, want %q (physical %d)", logical, got, want, 4+logical)
		}
	}
}
