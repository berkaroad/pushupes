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

package client

import (
	"fmt"
	"testing"
)

// TestSlotOfGolden vectors pin the routing algorithm: FNV-1a over the raw
// UTF-8 bytes, avalanched with the splitmix64 finalizer, narrowed by modulo
// over uint64. The expected values were computed independently (a reference
// implementation in another language), so a change here either breaks these
// deliberately (bumping them then needs the reasoning written down) or is a
// real drift that would silently move every aggregate's routing while old
// data stays where it was. A routing hash that moved would move data between
// slots: these vectors are the tripwire.
func TestSlotOfGolden(t *testing.T) {
	cases := []struct {
		id   string
		cnt  int
		want int32
	}{
		{"", 4096, 2203},
		{"a", 4096, 248},
		{"agg-1", 4096, 607},
		{"bench-0", 4096, 3594},
		{"bench-99", 4096, 3898},
		{"seed-7-3", 4096, 2118},
		{"grpc-smoke-x", 4096, 176},
		{"世界", 4096, 2865},
		// the fixed cluster default (1024 = 2^10), values recomputed by the
		// independent reference implementation in the same session that
		// reproduced every vector above
		{"", 1024, 155},
		{"a", 1024, 248},
		{"agg-1", 1024, 607},
		{"bench-0", 1024, 522},
		{"bench-99", 1024, 826},
		{"seed-7-3", 1024, 70},
		{"grpc-smoke-x", 1024, 176},
		{"世界", 1024, 817},
		// the largest table: 2048 ids, the segment mask is the whole id space
		{"agg-1", 2048, 607},
		{"bench-0", 2048, 1546},
		{"agg-1", 8, 7},
		{"bench-0", 8, 2},
		{"", 8, 3},
		{"a", 8, 0},
	}
	for _, c := range cases {
		if got := LogicalSlotOf(c.id, c.cnt); got != c.want {
			t.Errorf("LogicalSlotOf(%q, %d) = %d, want %d", c.id, c.cnt, got, c.want)
		}
	}
}

func TestSlotOfRange(t *testing.T) {
	for _, cnt := range []int{1, 2, 4, 8, 16, 32, 64} {
		for _, id := range []string{"", "x", "agg-1", "bench-42", "世界世界"} {
			if s := LogicalSlotOf(id, cnt); s < 0 || int(s) >= cnt {
				t.Fatalf("LogicalSlotOf(%q, %d) = %d outside 0..%d", id, cnt, s, cnt-1)
			}
		}
	}
}

// TestErrIDs pins the wire table: these numbers are what servers answer and
// what clients branch on (1003/1004 drive the redirect self-heal); renaming
// one or sliding one off its value breaks every deployed client.
func TestErrIDs(t *testing.T) {
	cases := map[string]struct{ got, want int }{
		"ErrIDVersionConflict":   {ErrIDVersionConflict, 1001},
		"ErrIDBadRequest":        {ErrIDBadRequest, 1002},
		"ErrIDSlotNotLocal":      {ErrIDSlotNotLocal, 1003},
		"ErrIDMigrating":         {ErrIDMigrating, 1004},
		"ErrIDFlowControl":       {ErrIDFlowControl, 1005},
		"ErrIDSlotUnproven":      {ErrIDSlotUnproven, 1006},
		"ErrIDAckTimeout":        {ErrIDAckTimeout, 1007},
		"ErrIDReplicaCatchingUp": {ErrIDReplicaCatchingUp, 1008},
		"ErrIDNoDataPlane":       {ErrIDNoDataPlane, 1009},
	}
	for name, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", name, c.got, c.want)
		}
	}
}

// TestLogicalSlotOf pins the generation router: a generation of size 2^g owns
// [2^g, 2^(g+1)), so the base is the size and the slot is size+hash&(size-1).
// The vectors come from the same independent reference implementation behind
// TestLogicalSlotOf pins the client-plane router: ids land on LOGICAL slots
// (0 .. size-1). The physical table slot adds the generation base, which is the
// server's business — the client library has no base to pass.
func TestLogicalSlotOf(t *testing.T) {
	cases := []struct {
		id   string
		size int
		want int32
	}{
		{"", 1, 0},
		{"a", 1, 0},
		{"agg-1", 1, 0},
		{"", 2, 1},
		{"世界", 2, 1},
		{"a", 4, 0},
		{"grpc-smoke-x", 4, 0},
		{"agg-1", 8, 7},
		{"bench-0", 8, 2},
		{"agg-1", 16, 15},
		{"bench-99", 16, 10},
		{"", 1024, 155},
		{"a", 1024, 248},
		{"agg-1", 1024, 607},
		{"bench-0", 1024, 522},
		{"世界", 1024, 817},
		{"grpc-smoke-x", 1024, 176},
	}
	for _, c := range cases {
		if got := LogicalSlotOf(c.id, c.size); got != c.want {
			t.Fatalf("LogicalSlotOf(%q, %d) = %d, want %d", c.id, c.size, got, c.want)
		}
	}
}

// TestLogicalSlotOfRanges pins what the layout relies on: every generation
// stays inside its own range, none produces slot-000, g0 collapses to
// slot-001, and a size that is not a power of two is rejected (a mask would
// silently misroute).
func TestLogicalSlotOfRanges(t *testing.T) {
	for _, size := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024} {
		for i := 0; i < 2000; i++ {
			id := fmt.Sprintf("prop-%d-%d", size, i)
			got := LogicalSlotOf(id, size)
			if got < 0 || got >= int32(size) {
				t.Fatalf("LogicalSlotOf(%q, %d) = %d, outside [0, %d)", id, size, got, size)
			}
		}
	}
	for _, id := range []string{"", "a", "agg-1", "世界"} {
		if got := LogicalSlotOf(id, 1); got != 0 {
			t.Fatalf("g0 must route %q to logical slot 0 (physical slot-001), got %d", id, got)
		}
	}
	for _, size := range []int{0, -1, 3, 1000, 1536} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("LogicalSlotOf accepted non-power-of-two size %d", size)
				}
			}()
			LogicalSlotOf("x", size)
		}()
	}
}

// TestLogicalSlotOfStaysInsideTheGeneration pins the client-side id space: a
// client routes onto 0 .. size-1 and hands the server a logical id; the physical
// table slot (group_base + logical) is the server's business.
func TestLogicalSlotOfStaysInsideTheGeneration(t *testing.T) {
	for _, size := range []int{1, 2, 4, 8, 16, 1024} {
		for i := 0; i < 200; i++ {
			agg := fmt.Sprintf("agg-%d", i)
			got := LogicalSlotOf(agg, size)
			if got < 0 || int(got) >= size {
				t.Fatalf("LogicalSlotOf(%q, %d) = %d, out of [0, %d)", agg, size, got, size)
			}
			if want := LogicalSlotOf(agg, size); got != want {
				t.Fatalf("logical %d must be the zero-based segment slot %d", got, want)
			}
		}
	}
}
