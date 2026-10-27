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

import (
	"fmt"
	"testing"

	"github.com/berkaroad/pushupes/internal/config"
)

// The settle gate is what keeps a cold start from laying out a cluster that has
// not finished showing up: the data-plane pool fills by registration, so the
// first plan of a classic both cluster used to run under whichever single node
// announced first (factor 1, every slot on it), and the rest of the members
// then re-planned all of it at factor 2. See Engine.poolSettled.
func TestPlanWaitsForThePoolToSettle(t *testing.T) {
	e := &Engine{}

	// First sighting of a one-member pool: the other members are still booting.
	if e.poolSettled([]string{"node-1"}, nil) {
		t.Fatal("the first plan must not run under a pool seen for the first time")
	}
	// A second member lands inside the same window: still assembling.
	if e.poolSettled([]string{"node-1", "node-2"}, nil) {
		t.Fatal("a pool that grew again must not be planned under yet")
	}
	// Nobody else shows up: the pool repeated itself, so this is the shape to
	// lay out — the whole point of waiting one round.
	if !e.poolSettled([]string{"node-1", "node-2"}, nil) {
		t.Fatal("a pool that repeated itself must be planned under")
	}
	// Once the plan has run under it (PlanMembers == pool), later rounds plan
	// immediately: the gate must not cost anything in steady state.
	if !e.poolSettled([]string{"node-1", "node-2"}, []string{"node-1", "node-2"}) {
		t.Fatal("a planned pool must not be waited on again")
	}
	if e.planPoolWaited != 0 || e.planPoolSeen != nil {
		t.Fatalf("settle state = (%v, %d), want it cleared after the plan ran", e.planPoolSeen, e.planPoolWaited)
	}
}

// An existing cluster restarting sees its members register one by one, so its
// pool differs from the planned one for a round or two. That must not defer
// forever (the deferral is triggered by CHANGE, not by difference).
func TestPlanSettleDoesNotStallARestartingCluster(t *testing.T) {
	e := &Engine{}
	planned := []string{"node-1", "node-2", "node-3"}
	if e.poolSettled([]string{"node-1"}, planned) {
		t.Fatal("a pool smaller than the planned one is still assembling")
	}
	if !e.poolSettled(planned, planned) {
		t.Fatal("once every member has registered the plan may run")
	}
}

// A pool that never repeats itself must still be laid out: the cap is what
// keeps the gate from turning membership churn into no layout at all.
func TestPlanSettleIsBoundedUnderChurn(t *testing.T) {
	e := &Engine{}
	planned := false
	for i := 0; i < config.PlanSettleMaxRounds+2; i++ {
		pool := []string{"node-1", fmt.Sprintf("node-%d", i+2)}
		planned = e.poolSettled(pool, nil)
		if planned {
			break
		}
	}
	if !planned {
		t.Fatalf("a churning pool was never planned under within %d rounds", config.PlanSettleMaxRounds+2)
	}
	if e.planPoolWaited != 0 {
		t.Fatalf("deferral counter = %d, want it cleared once the plan ran", e.planPoolWaited)
	}
}
