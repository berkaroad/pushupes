package cluster

// Handover ack continuity (fence commit window): a client write that the
// cluster ANSWERED OK must be readable from the slot's leader afterwards —
// including the leader the handover moves to, and including records that land
// in the narrow window while the commit fence is releasing. The dg4/dg5 drain
// storms reproduced exactly this shape failing under load: an aggregate acked
// at version V answers tail V-2 (or worse) after a handover commits, because
// the record was acked on a copy that no one ships the new leader's log from.
// This test pins the property end-to-end with a live fence, a live forward
// path, and concurrent batch writers — no mocks on the write side.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/berkaroad/pushupes/internal/data"
)

func lastVersionOf(rs []*data.EventRecord) uint32 {
	if len(rs) == 0 {
		return 0
	}
	return rs[len(rs)-1].Version
}

func TestHandoverKeepsEveryAckedRecordVisible(t *testing.T) {
	src, sst := newTestEngine(t, "node-1")
	peer, pst := newTestEngine(t, "node-2")
	third, _ := newTestEngine(t, "node-3")
	cl := newTestRaftCluster(t, []string{"node-1", "node-2", "node-3"},
		map[string]Applier{"node-1": src, "node-2": peer, "node-3": third})
	src.node = cl.nodes["node-1"]
	peer.node = cl.nodes["node-2"]
	third.node = cl.nodes["node-3"]
	leaderID := cl.waitLeader(15 * time.Second)
	ctrl := src
	switch leaderID {
	case "node-2":
		ctrl = peer
	case "node-3":
		ctrl = third
	}
	srcAddr := newPeerHarness(t, src)
	peerAddr := newPeerHarness(t, peer)
	thirdAddr := newPeerHarness(t, third)

	// The three tables must agree — the commit goes through real Raft and
	// the followers apply the same commands the controller did. Drive the
	// membership + plan through the controller's raft submit path so every
	// node applies the identical sequence.
	for _, c := range []*Command{
		{Op: OpJoinNode, Peer: &Peer{Role: RoleLearner, ID: "node-1", PeerAddr: srcAddr, AdminAddr: srcAddr, ClientAddr: srcAddr}},
		{Op: OpJoinNode, Peer: &Peer{Role: RoleLearner, ID: "node-2", PeerAddr: peerAddr, AdminAddr: peerAddr, ClientAddr: peerAddr}},
		{Op: OpJoinNode, Peer: &Peer{Role: RoleLearner, ID: "node-3", PeerAddr: thirdAddr, AdminAddr: thirdAddr, ClientAddr: thirdAddr}},
		{Op: OpConfig, Replicas: fixtureFactor(3)},
		{Op: OpPlanSlots},
	} {
		if err := ctrl.submit(c); err != nil {
			t.Fatalf("controller join %s: %v", c.Op, err)
		}
	}
	if err := ctrl.awaitApplied(context.Background(), 15*time.Second); err != nil {
		t.Fatalf("setup apply: %v", err)
	}
	// Every engine applies the same raft entries — and the controller is
	// whoever won the election, so SRC may be a follower here whose apply
	// trails the commit. The planned table must be visible on all three
	// before the ledger starts; check src too (and hold a real deadline,
	// not one recomputed every turn).
	settleDeadline := time.Now().Add(15 * time.Second)
	for {
		if len(src.TableSnapshot().Slots) == 8 && len(peer.TableSnapshot().Slots) == 8 && len(third.TableSnapshot().Slots) == 8 {
			break
		}
		if time.Now().After(settleDeadline) {
			t.Fatalf("tables did not settle: src=%d peer=%d third=%d",
				len(src.TableSnapshot().Slots), len(peer.TableSnapshot().Slots), len(third.TableSnapshot().Slots))
		}
		time.Sleep(20 * time.Millisecond)
	}
	// slot 0 must replicate node-1 -> node-2 so a real handover exists.
	if p, _ := src.TableSnapshot().Slots[0]; p.Leader != "node-1" || !replicaSetHas(p.Replicas, "node-2") {
		t.Fatalf("test setup: slot 0 placement %+v", p)
	}

	// Eight aggregates inside slot 0, written through SubmitBatch on the
	// leader ingress the whole time the handover commits. Acked (agg, version)
	// pairs are the ledger: everything after the handover must resolve.
	const nAggs, nWriters = 8, 4
	aggs := make([]string, nAggs)
	for i := range aggs {
		aggs[i] = aggInSlotOf(t, src, 0, fmt.Sprintf("hc-%d-", i))
	}
	type ledger struct {
		mu sync.Mutex
		v  map[string]uint32
	}
	led := &ledger{v: map[string]uint32{}}

	ctx := context.Background()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var wrote atomic.Int64
	nextVer := make([]atomic.Uint32, nAggs)
	for i := range nextVer {
		nextVer[i].Store(1)
	}
	for w := 0; w < nWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for round := 0; ; round++ {
				select {
				case <-stop:
					return
				default:
				}
				// One record per agg per round through SubmitAppend (the same
				// ingress the batch path routes per slot), version from the
				// writer-owned counter.
				for i := 0; i < nAggs; i++ {
					v := nextVer[i].Add(1) - 1
					rec := makeRecord(aggs[i], v, fmt.Sprintf("hc-w%d-a%d-v%d", w, i, v))
					resp, err := src.SubmitAppend(ctx, rec)
					if err != nil {
						// transport/refused: retract the version claim (the
						// record was NOT acked).
						nextVer[i].Store(v)
						continue
					}
					if resp != nil && resp.Status == data.StatusSuccess {
						led.mu.Lock()
						if led.v[aggs[i]] < v {
							led.v[aggs[i]] = v
						}
						led.mu.Unlock()
						wrote.Add(1)
					} else {
						nextVer[i].Store(v) // business fail (redirect semantics
						// are transparent retries at the client layer; the
						// engine's SubmitAppend only answers Fail for CAS
						// conflicts, which mean this writer is racing itself —
						// back off one version and keep going)
					}
				}
				if round%50 == 49 {
					time.Sleep(time.Millisecond)
				}
			}
		}(w)
	}

	// Let the writers settle, then run a REAL handover WHILE they write:
	// node-1 -> node-2 through the controller commit path. The writers keep
	// hitting src.SubmitAppend (which begins forwarding once the move lands)
	// and the fence window is exactly where stranding would happen.
	time.Sleep(50 * time.Millisecond)
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	if err := ctrl.commitMigration(cctx, 0, "node-2"); err != nil {
		cancel()
		t.Fatalf("handover node-1->node-2 under load: %v", err)
	}
	cancel()
	// Keep writing through the moved leader (src forwards to peer) for a
	// while AFTER the commit, so a late-stranded write would also surface.
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	// Both engines must have applied the move before judging.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !peer.Leads(0) {
		time.Sleep(20 * time.Millisecond)
	}
	if !peer.Leads(0) {
		t.Fatal("node-2 never applied the leader move")
	}
	// drain any in-flight forwards through the new leader path
	time.Sleep(200 * time.Millisecond)

	t.Logf("acked %d records through three handovers", wrote.Load())

	// The ledger must resolve on the FINAL leader (node-2).
	for _, agg := range aggs {
		led.mu.Lock()
		want := led.v[agg]
		led.mu.Unlock()
		if want == 0 {
			continue
		}
		records, _, err := pst.ReadAggregate(agg, 1, uint64(want)+10, 0)
		if err != nil {
			t.Fatalf("agg %s unreadable on the final leader: %v", agg, err)
		}
		if len(records) == 0 || records[len(records)-1].Version < want {
			gotN := 0
			var gotV uint32
			if rs, _, err2 := sst.ReadAggregate(agg, 1, uint64(want)+10, 0); err2 == nil && len(rs) > 0 {
				gotN = len(rs)
				gotV = rs[len(rs)-1].Version
			}
			leo1, leo2 := sst.LastSeqOf(0), pst.LastSeqOf(0)
			t.Fatalf("acked records stranded by the handover: agg %s acked through v%d; final leader resolves %d records (max %d); OLD leader holds %d records (max %d); slot0 LEO old=%d new=%d; src leads=%v peer leads=%v",
				agg, want, len(records), lastVersionOf(records), gotN, gotV, leo1, leo2, src.Leads(0), peer.Leads(0))
		}
	}
}
