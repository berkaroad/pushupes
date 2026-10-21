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
// The per-slot replica factor is MIN(data-node count, copies the policy
// asks for), the data-node count being the directory members that host
// slots (Table.HostingIDs).
//
//	ReplicaPolicyLow     — factor 1: one copy per slot.
//	ReplicaPolicyMedium  — factor 2: a leader plus one replica (the default).
//	ReplicaPolicyHigh    — factor 3: a leader plus two replicas.
//
// A policy may also be a plain positive integer — `-replica-policy 5` seeds
// (and the admin endpoint carries) a direct factor of five copies — stored
// verbatim in the table. The tier names are the readable aliases for the
// factors 1, 2 and 3.
type ReplicaPolicy string

const (
	ReplicaPolicyLow    ReplicaPolicy = "low"
	ReplicaPolicyMedium ReplicaPolicy = "medium"
	ReplicaPolicyHigh   ReplicaPolicy = "high"

	// DefaultReplicaPolicy is the policy a brand-new cluster boots with when
	// the operator does not choose one with -replica-policy.
	DefaultReplicaPolicy = ReplicaPolicyMedium
)

// ValidReplicaPolicy reports whether s names a tier or a positive integer
// factor.
func ValidReplicaPolicy(s string) bool {
	switch ReplicaPolicy(s) {
	case ReplicaPolicyLow, ReplicaPolicyMedium, ReplicaPolicyHigh:
		return true
	}
	return replicaFactorDigits(s) > 0
}

// replicaFactorDigits parses s as a positive decimal factor, returning 0
// when s is not a plain number.
func replicaFactorDigits(s string) int {
	if s == "" {
		return 0
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0
		}
	}
	n := 0
	for _, c := range []byte(s) {
		n = n*10 + int(c-'0')
		if n > 1<<20 { // a factor past a million is a typo, not a plan
			return 0
		}
	}
	return n
}

// NormalizeReplicaPolicy maps an empty policy onto the default, rewrites a
// numeric factor onto its canonical digit form, and validates the rest, so
// a hand-built table, a seed flag and a decoded command share one rule.
func NormalizeReplicaPolicy(s string) (ReplicaPolicy, error) {
	if s == "" {
		return DefaultReplicaPolicy, nil
	}
	if n := replicaFactorDigits(s); n > 0 {
		return ReplicaPolicy(fmt.Sprintf("%d", n)), nil
	}
	switch ReplicaPolicy(s) {
	case ReplicaPolicyLow, ReplicaPolicyMedium, ReplicaPolicyHigh:
		return ReplicaPolicy(s), nil
	}
	return "", fmt.Errorf("unknown replica policy %q (want low, medium, high, or a positive number)", s)
}

// ReplicaCountForPolicy is the policy's per-slot replica factor for a data
// plane of `members` hosting members (Table.HostingIDs): the factor is
// MIN(members, copies the policy asks for) — low one copy, medium two, high
// three, or the number itself for a numeric policy. An unrecognised policy
// value falls back to the medium derivation, same as the seed path.
func ReplicaCountForPolicy(policy ReplicaPolicy, members int) int {
	if members < 1 {
		members = 1
	}
	var want int
	switch policy {
	case ReplicaPolicyLow:
		want = 1
	case ReplicaPolicyHigh:
		want = 3
	default:
		if n := replicaFactorDigits(string(policy)); n > 0 {
			want = n // numeric policy: the factor is the number
		} else {
			want = 2 // medium, and any value the table cannot hold anyway
		}
	}
	if want > members {
		want = members
	}
	return want
}
