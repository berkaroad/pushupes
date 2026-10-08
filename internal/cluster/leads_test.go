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

// Leads must track the table's placement: true exactly for the slot's leader,
// false for a replica and for a slot this node does not hold. Read handlers
// consult it to decide whether a read may be capped by the replication high
// watermark.
func TestLeadsTracksOwnership(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	join(t, e, "node-1", "127.0.0.1:1")
	join(t, e, "node-2", "127.0.0.1:2")
	join(t, e, "node-3", "127.0.0.1:3")
	applyCmd(t, e, &Command{Op: OpPlanSlots})

	led, replica, foreign := -1, -1, -1
	for s, p := range e.TableSnapshot().Slots {
		switch {
		case p.Leader == "node-1" && led < 0:
			led = int(s)
		case p.Leader != "node-1" && replicaSetHas(p.Replicas, "node-1") && replica < 0:
			replica = int(s)
		case p.Leader != "node-1" && !replicaSetHas(p.Replicas, "node-1") && foreign < 0:
			foreign = int(s)
		}
	}
	if led < 0 || replica < 0 {
		t.Fatalf("test setup: led=%d replica=%d", led, replica)
	}
	if !e.Leads(int32(led)) {
		t.Fatalf("slot %d is led by node-1 but Leads says otherwise", led)
	}
	if e.Leads(int32(replica)) {
		t.Fatalf("slot %d is only replicated by node-1; Leads must be false", replica)
	}
	if foreign >= 0 && e.Leads(int32(foreign)) {
		t.Fatalf("slot %d is not held by node-1; Leads must be false", foreign)
	}

	// A leader move flips it.
	applyCmd(t, e, &Command{Op: OpLeaderMove, Slots: []int32{int32(led)}, NewLeader: "node-2"})
	if e.Leads(int32(led)) {
		t.Fatalf("slot %d was handed to node-2; Leads must be false", led)
	}
}
