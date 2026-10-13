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
// The member count a tier derives against is the DATA-PLANE pool — the
// directory members that host slots (Table.HostingIDs) — not the whole
// directory: a control-only member holds no seat and no copy, so counting it
// into the factor would ask every slot for replicas that cannot exist.
//
//	ReplicaPolicyLow     — factor 1: one copy per slot.
//	ReplicaPolicyMedium  — factor 2: a leader plus one replica (the default).
//	ReplicaPolicyHigh    — one copy on EVERY data-plane node: the factor is
//	                       the pool's own size, so every slot keeps a full
//	                       copy on every member that can hold data — the
//	                       strongest durability tier, at the price of every
//	                       data node storing every byte.
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

// ReplicaCountForPolicy is the policy's per-slot replica factor for a data
// plane of `members` hosting members (Table.HostingIDs): low is always one
// copy, medium is always two, high is the pool itself — every data node holds
// every slot. The result is clamped to the member count — a factor larger
// than the pool would ask slots for replicas that cannot exist (a 1-member
// data plane on the medium tier plans one copy, not two). An unrecognised
// policy value falls back to the medium derivation, same as the seed path.
func ReplicaCountForPolicy(policy ReplicaPolicy, members int) int {
	if members < 1 {
		members = 1
	}
	var want int
	switch policy {
	case ReplicaPolicyLow:
		want = 1
	case ReplicaPolicyHigh:
		want = members // every data-plane node holds every slot
	default: // medium (and any unknown value, which the table cannot hold anyway)
		want = 2
	}
	if want > members {
		want = members
	}
	return want
}
