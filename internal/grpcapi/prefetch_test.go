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

package grpcapi

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/berkaroad/pushupes/internal/cluster"
	"github.com/berkaroad/pushupes/internal/storage"
	pushupesv1 "github.com/berkaroad/pushupes/pkg/grpcapi/pushupes/v1"
)

// newPrefetchClient builds the gRPC facade over an in-memory listener with
// its own engine+store (8 slots), and hands back all three so tests can
// mutate the routing table through the real command path before asking.
func newPrefetchClient(t *testing.T) (pushupesv1.EventServiceClient, *cluster.Engine, *storage.Store) {
	t.Helper()
	st, err := storage.OpenStore(t.TempDir(), 8, storage.DefaultSegmentBytes, storage.FlushPolicy{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	eng := cluster.NewEngine(nil, st, "node-1", nil)

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	NewServer(eng, st, nil, 0, "http://127.0.0.1:18591").Register(gs)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pushupesv1.NewEventServiceClient(conn), eng, st
}

// Empty table: every slot falls back to the answering node — the array is
// dense (its length IS the cluster slot count) and has no holes.
func TestPrefetchRoutesFallbackSelf(t *testing.T) {
	cli, _, st := newPrefetchClient(t)
	resp, err := cli.PrefetchRoutes(context.Background(), &pushupesv1.PrefetchRoutesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.SlotNodeIndex) != int(st.SlotCount) {
		t.Fatalf("slot count: got %d rows, want %d", len(resp.SlotNodeIndex), st.SlotCount)
	}
	if len(resp.NodeClientAddrs) != 1 {
		t.Fatalf("fallback must name exactly one addr, got %v", resp.NodeClientAddrs)
	}
	if resp.NodeClientAddrs[0] != "127.0.0.1:18591" {
		t.Fatalf("self addr: %q (scheme must be stripped for gRPC dialing)", resp.NodeClientAddrs[0])
	}
	for s, idx := range resp.SlotNodeIndex {
		if idx != 0 {
			t.Fatalf("slot %d row %d, want 0 (all self)", s, idx)
		}
	}
}

// Planned two-peer table: rows point at the slot leaders' announced addrs,
// de-duplicated in first-appearance order, and the answer matches the
// engine's own table slot for slot.
func TestPrefetchRoutesPlannedTable(t *testing.T) {
	cli, eng, _ := newPrefetchClient(t)
	apply := func(c *cluster.Command) {
		if _, err := eng.ApplyCommand(c.Encode()); err != nil {
			t.Fatalf("apply %s: %v", c.Op, err)
		}
	}
	apply(&cluster.Command{Op: cluster.OpJoinNode, Peer: &cluster.Peer{
		ID: "node-1", PeerAddr: "p1", AdminAddr: "http://127.0.0.1:18091", ClientAddr: "http://127.0.0.1:18591"}})
	apply(&cluster.Command{Op: cluster.OpJoinNode, Peer: &cluster.Peer{
		ID: "node-2", PeerAddr: "p2", AdminAddr: "http://127.0.0.1:18092", ClientAddr: "http://127.0.0.1:18592"}})
	apply(&cluster.Command{Op: cluster.OpPlanSlots})

	resp, err := cli.PrefetchRoutes(context.Background(), &pushupesv1.PrefetchRoutesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.SlotNodeIndex) != 8 {
		t.Fatalf("rows: %d", len(resp.SlotNodeIndex))
	}
	// de-dup: at most one entry per distinct leader addr
	uniq := map[string]bool{}
	for _, a := range resp.NodeClientAddrs {
		if uniq[a] {
			t.Fatalf("addr %q appears twice — rows must de-duplicate", a)
		}
		uniq[a] = true
	}
	tbl := eng.TableSnapshot()
	sawPeer := 0
	for slot, idx := range resp.SlotNodeIndex {
		if idx < 0 || int(idx) >= len(resp.NodeClientAddrs) {
			t.Fatalf("slot %d: row index %d outside %d addrs", slot, idx, len(resp.NodeClientAddrs))
		}
		p, ok := tbl.Slots[int32(slot)]
		if !ok || p.Leader == "" {
			continue // unassigned: the fallback rule is the other test's subject
		}
		want := stripScheme(tbl.Peers[p.Leader].ClientAddr)
		got := resp.NodeClientAddrs[idx]
		if got != want {
			t.Fatalf("slot %d: routed to %q, table leader %s says %q", slot, got, p.Leader, want)
		}
		if want != "127.0.0.1:18591" {
			sawPeer++
		}
	}
	if sawPeer == 0 {
		t.Fatal("planned table routed every slot to self — plan did not spread")
	}
}

func stripScheme(addr string) string {
	for i := 0; i+3 <= len(addr); i++ {
		if addr[i:i+3] == "://" {
			return addr[i+3:]
		}
	}
	return addr
}
