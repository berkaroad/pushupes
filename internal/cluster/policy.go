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

import "fmt"

// ReplicaPolicy is the cluster-wide strategy behind the per-slot replica
// factor. The value lives in the replicated slot table (Table.Policy): the
// whole cluster runs one policy, and changing it is a Raft command — the
// startup flag only seeds a brand-new cluster, and the admin endpoint is the
// entry point for every later change.
//
// ReplicaPolicyLow     — factor 1: one copy per slot.
// ReplicaPolicyMedium  — factor 2: a leader plus one replica (the default).
// ReplicaPolicyHigh    — fault tolerance + 1: the Raft group survives
//
//	                       floor((N-1)/2) failures and every slot keeps exactly
//	                       one more copy than that, so the data outlives the
//	                       same failures the consensus group outlives:
//
//		1 voter  -> 0 + 1 = 1 replica
//		3 voters -> 1 + 1 = 2 replicas
//		5 voters -> 2 + 1 = 3
//		7 voters -> 3 + 1 = 4
//
// Even member counts land on the same count as the odd count below them
// (4 voters -> 2, 6 -> 3) — the same floor((N-1)/2) arithmetic.
type ReplicaPolicy string

const (
	ReplicaPolicyLow    ReplicaPolicy = "low"
	ReplicaPolicyMedium ReplicaPolicy = "medium"
	ReplicaPolicyHigh   ReplicaPolicy = "high"

	// DefaultReplicaPolicy is the policy a brand-new cluster boots with when
	// the operator does not choose one with -replica-policy.
	DefaultReplicaPolicy = ReplicaPolicyMedium
)

// ValidReplicaPolicy reports whether s names one of the three tiers.
func ValidReplicaPolicy(s string) bool {
	switch ReplicaPolicy(s) {
	case ReplicaPolicyLow, ReplicaPolicyMedium, ReplicaPolicyHigh:
		return true
	}
	return false
}

// NormalizeReplicaPolicy maps an empty policy onto the default and validates
// the rest, so a hand-built table and a decoded command share one rule.
func NormalizeReplicaPolicy(s string) (ReplicaPolicy, error) {
	if s == "" {
		return DefaultReplicaPolicy, nil
	}
	if !ValidReplicaPolicy(s) {
		return "", fmt.Errorf("unknown replica policy %q (want low, medium or high)", s)
	}
	return ReplicaPolicy(s), nil
}

// ReplicaCountForMembers is the high tier: the per-slot replica count derived
// from the Raft cluster's own fault tolerance — a group of N voters survives
// floor((N-1)/2) failures, and every slot keeps one more copy than that.
// It is what ReplicaCountForPolicy computes for ReplicaPolicyHigh; the even
// member counts reuse the odd count's arithmetic (2->1, 4->2, 6->3).
func ReplicaCountForMembers(members int) int {
	if members < 1 {
		return 1
	}
	return (members-1)/2 + 1
}

// ReplicaCountForPolicy is the policy's per-slot replica factor for a cluster
// of `members` members: low is always one copy, medium is always two, high
// follows the cluster's fault tolerance (ReplicaCountForMembers). The result
// is clamped to the member count — a factor larger than the cluster has nodes
// would ask slots for replicas that cannot exist (a 1-member cluster on the
// medium default plans one copy, not two). An unrecognised policy value falls
// back to the medium derivation, same as the default.
func ReplicaCountForPolicy(policy ReplicaPolicy, members int) int {
	if members < 1 {
		members = 1
	}
	var want int
	switch policy {
	case ReplicaPolicyLow:
		want = 1
	case ReplicaPolicyHigh:
		want = ReplicaCountForMembers(members)
	default: // medium (and any unknown value, which the table cannot hold anyway)
		want = 2
	}
	if want > members {
		want = members
	}
	return want
}
