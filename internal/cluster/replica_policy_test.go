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

import "testing"

// TestReplicaCountForPolicy pins the three tiers. low and medium are fixed
// factors, clamped to whatever members exist (a cluster cannot hold more
// copies of a slot than it has nodes); high is the fault-tolerance derivation
// — one more copy than the failures the Raft group survives.
func TestReplicaCountForPolicy(t *testing.T) {
	cases := []struct {
		policy  ReplicaPolicy
		members int
		want    int
	}{
		{ReplicaPolicyLow, 1, 1},
		{ReplicaPolicyLow, 3, 1},
		{ReplicaPolicyLow, 7, 1},
		{ReplicaPolicyMedium, 1, 1}, // clamped: one node, one copy
		{ReplicaPolicyMedium, 2, 2},
		{ReplicaPolicyMedium, 5, 2},
		{ReplicaPolicyMedium, 7, 2},
		{ReplicaPolicyHigh, 0, 1}, // no members enumerated yet: never below one copy
		{ReplicaPolicyHigh, 1, 1},
		{ReplicaPolicyHigh, 2, 1},
		{ReplicaPolicyHigh, 3, 2},
		{ReplicaPolicyHigh, 4, 2},
		{ReplicaPolicyHigh, 5, 3},
		{ReplicaPolicyHigh, 6, 3},
		{ReplicaPolicyHigh, 7, 4},
		{ReplicaPolicyHigh, 9, 5},
	}
	for _, c := range cases {
		got := ReplicaCountForPolicy(c.policy, c.members)
		if got != c.want {
			t.Errorf("ReplicaCountForPolicy(%q, %d) = %d, want %d", c.policy, c.members, got, c.want)
		}
		if c.members > 0 && got > c.members {
			t.Errorf("%d members must never plan more replicas (%d) than members", c.members, got)
		}
	}
}

// TestSetPolicyCommandAppliesTierAndFactor pins the replicated command: one
// OpSetPolicy writes the tier into the table AND re-derives the factor from
// the member count it holds, so every node applying the log lands on the same
// (policy, factor) pair. An unknown tier is rejected and changes nothing.
func TestSetPolicyCommandAppliesTierAndFactor(t *testing.T) {
	members := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	tbl := NewTableWithPolicy(16, ReplicaPolicyMedium)
	populatePeers(t, tbl, members)
	if tbl.Replicas != 1 {
		t.Fatalf("setup: a fresh table starts at the single-member factor, got %d", tbl.Replicas)
	}

	for _, c := range []struct {
		policy     string
		wantFactor int
		wantErr    bool
	}{
		{"low", 1, false},
		{"medium", 2, false},
		{"high", ReplicaCountForMembers(len(members)), false},
		{"bogus", 0, true},
	} {
		err := tbl.Apply(&Command{Op: OpSetPolicy, Policy: c.policy})
		if c.wantErr {
			if err == nil {
				t.Fatalf("apply policy %q: want rejection, got none", c.policy)
			}
			continue
		}
		if err != nil {
			t.Fatalf("apply policy %q: %v", c.policy, err)
		}
		if tbl.Policy != ReplicaPolicy(c.policy) {
			t.Errorf("policy = %q, want %q", tbl.Policy, c.policy)
		}
		if tbl.Replicas != c.wantFactor {
			t.Errorf("policy %q: factor = %d, want %d", c.policy, tbl.Replicas, c.wantFactor)
		}
	}
}

// TestPolicyChangeReLaysReplicaSets is the regression for the operator's knob:
// a tier change moves the factor, and the controller's existing factor-change
// path — OpConfig + replan to fill a raised factor, then seat reclaim for a
// lowered one — converges the layout. Medium -> high (2 -> 3 copies over five
// members) must add the ring's seats to every slot; high -> low (3 -> 1) must
// leave surplus seats for reclaimRound to take off, never drop one in the
// replan itself.
func TestPolicyChangeReLaysReplicaSets(t *testing.T) {
	members := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	tbl := NewTable(16, 2) // the medium layout over five members
	populatePeers(t, tbl, members)
	tbl.Slots = PlanSlots(members, tbl.SlotCount, tbl.Replicas)

	// medium -> high: the factor rises 2 -> 3, and the replan adds the third
	// ring seat to every stable slot.
	if err := tbl.Apply(&Command{Op: OpSetPolicy, Policy: "high"}); err != nil {
		t.Fatalf("set policy: %v", err)
	}
	if tbl.Replicas != 3 {
		t.Fatalf("five members on high must want three copies, got %d", tbl.Replicas)
	}
	if !tbl.layoutStale() {
		t.Fatal("a factor change must leave the layout stale until the replan")
	}
	if err := tbl.Apply(&Command{Op: OpReplanSlots}); err != nil {
		t.Fatalf("replan: %v", err)
	}
	for s, p := range tbl.Slots {
		if len(p.Replicas) < 3 {
			t.Fatalf("slot %d has %d replicas after the replan, want 3: %v", s, len(p.Replicas), p.Replicas)
		}
		for i := 0; i < 3; i++ {
			if on := members[(int(s)+i)%len(members)]; !replicaListHas(p.Replicas, on) {
				t.Fatalf("slot %d does not hold the ring's seat %s: %v", s, on, p.Replicas)
			}
		}
	}

	// high -> medium: the factor drops 3 -> 2. The replan never DROPS a seat
	// (the copies a stale set holds are what has been replicating the slot;
	// the surplus comes off later via reclaimRound), so a settled-to-3 set
	// stays where it is and the round must not burn a Raft entry on it.
	if err := tbl.Apply(&Command{Op: OpSetPolicy, Policy: "medium"}); err != nil {
		t.Fatalf("set policy medium: %v", err)
	}
	if tbl.Replicas != 2 {
		t.Fatalf("medium wants two copies, got %d", tbl.Replicas)
	}
	seatsBefore := map[int32]int{}
	for s, p := range tbl.Slots {
		seatsBefore[s] = len(p.Replicas)
	}
	// A shrink IS a stale layout (sets outgrew the factor) — that is what
	// arms the seat reclaim. The surplus each slot carries is exactly the
	// ring window beyond the factor, and surplusSeatForReplan names it.
	if !tbl.layoutStale() {
		t.Fatal("a factor drop must leave surplus seats for reclaimRound")
	}
	planned := PlanSlots(tbl.PeerIDs(), tbl.SlotCount, tbl.Replicas)
	for s, p := range tbl.Slots {
		drop := surplusSeatForReplan(p, tbl.Replicas, planned[s].Replicas)
		if len(p.Replicas) <= tbl.Replicas {
			if drop != "" {
				t.Fatalf("slot %d at the factor must name no surplus seat", s)
			}
			continue
		}
		if drop == "" {
			t.Fatalf("slot %d outgrew the factor (%d > %d) but names no reclaimable seat: %v",
				s, len(p.Replicas), tbl.Replicas, p.Replicas)
		}
		if drop == p.Leader {
			t.Fatalf("the leader is never reclaimable: slot %d", s)
		}
		if len(p.Replicas) != seatsBefore[s] {
			t.Fatalf("the reclaim candidate must not already have dropped the seat")
		}
	}
}

// TestBootPolicySeedIsTableScoped pins that the flag's tier reaches the table
// ONLY through the controller's seed (an unplanned table), and an existing
// cluster keeps whatever the admin endpoint last set: a RestoreState of a v4
// snapshot carrying "low" survives with low even when bootPolicy says high,
// because the seed branch never runs on a planned table.
func TestBootPolicySeedIsTableScoped(t *testing.T) {
	tbl := NewTableWithPolicy(16, ReplicaPolicyHigh)
	populatePeers(t, tbl, []string{"node-1", "node-2", "node-3"})
	tbl.Replicas = 2
	tbl.Slots = PlanSlots([]string{"node-1", "node-2", "node-3"}, 16, 2)
	b := tbl.EncodeTableBinary()
	out, err := DecodeTableBinary(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Policy != ReplicaPolicyHigh || out.Replicas != 2 {
		t.Fatalf("a v4 snapshot must round-trip its tier, got %q/%d", out.Policy, out.Replicas)
	}
	// The seed condition the controller checks: a planned table never re-seeds.
	if len(out.Slots) == 0 {
		t.Fatal("fixture: the table must be planned")
	}
}
