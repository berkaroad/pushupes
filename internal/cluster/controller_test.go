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
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

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
