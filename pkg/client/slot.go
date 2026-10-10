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

// Package client is the pushupes client contract: the pieces of the wire
// protocol a client must understand on its own — the slot routing algorithm
// and the error-id table. Generated gRPC stubs live in pkg/grpcapi.
//
// The routing ALGORITHM is shared with the server (internal/data delegates to
// it), so a client computing a slot and the server routing by it run the same
// code. The slot COUNT is deliberately absent: it is cluster state that the
// server may change, and clients learn the current value from the length of
// EventService/PrefetchRoutes' slot_node_index array.
package client

import "hash/fnv"

// splitmix64 finalizer constants: a bijection with full avalanche, applied to
// the 64-bit id hash before it is narrowed to a slot index.
const (
	smMixA = 0xbf58476d1ce4e5b9
	smMixB = 0x94d049bb133111eb
)

// SlotOf routes an aggregate ID to its slot. slotCount is the cluster's slot
// count, which the client takes from PrefetchRoutes (len(slot_node_index)) —
// the value is cluster state, not client knowledge.
//
// The ID is hashed with FNV-1a and avalanched with the splitmix64 finalizer
// before the modulo, rather than taken straight from a 16-bit digest. CRC16
// is measurably non-uniform over structured ids — the 16-bit histogram of
// zero-padded sequential ids scores chi2/df 1.61 against 1.00 for random
// UUIDs — and that bias lands directly on the slot split once ids are
// structured. Whitening a 16-bit digest afterwards cannot fix it: a bijection
// keeps the bucket counts, so the mix has to happen before the value is
// narrowed.
//
// Modulo is over uint64, so the slot count is not limited to 65535.
// Deterministic across processes and restarts on purpose — a routing hash
// that moved would move data between slots.
func SlotOf(aggregateID string, slotCount int) int32 {
	if slotCount <= 0 {
		panic("slotCount must be positive")
	}
	h := fnv.New64a()
	h.Write([]byte(aggregateID))
	return int32(mix64(h.Sum64()) % uint64(slotCount))
}

func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= smMixA
	x ^= x >> 27
	x *= smMixB
	return x ^ (x >> 31)
}
