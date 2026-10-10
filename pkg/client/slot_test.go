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

import "testing"

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
		{"", 1680, 1083},
		{"a", 1680, 136},
		{"agg-1", 1680, 223},
		{"bench-0", 1680, 1322},
		{"bench-99", 1680, 522},
		{"seed-7-3", 1680, 614},
		{"grpc-smoke-x", 1680, 1232},
		{"世界", 1680, 625},
		// a non-power-of-two count exercises the uint64 modulo
		{"agg-1", 8, 7},
		{"bench-0", 8, 2},
		{"", 8, 3},
		{"a", 8, 0},
	}
	for _, c := range cases {
		if got := SlotOf(c.id, c.cnt); got != c.want {
			t.Errorf("SlotOf(%q, %d) = %d, want %d", c.id, c.cnt, got, c.want)
		}
	}
}

func TestSlotOfRange(t *testing.T) {
	for cnt := 1; cnt <= 64; cnt++ {
		for _, id := range []string{"", "x", "agg-1", "bench-42", "世界世界"} {
			if s := SlotOf(id, cnt); s < 0 || int(s) >= cnt {
				t.Fatalf("SlotOf(%q, %d) = %d outside 0..%d", id, cnt, s, cnt-1)
			}
		}
	}
}

// TestErrIDs pins the wire table: these numbers are what servers answer and
// what clients branch on (1003/1004 drive the redirect self-heal); renaming
// one or sliding one off its value breaks every deployed client.
func TestErrIDs(t *testing.T) {
	cases := map[string]struct{ got, want int }{
		"ErrIDVersionConflict": {ErrIDVersionConflict, 1001},
		"ErrIDBadRequest":      {ErrIDBadRequest, 1002},
		"ErrIDSlotNotLocal":    {ErrIDSlotNotLocal, 1003},
		"ErrIDMigrating":       {ErrIDMigrating, 1004},
		"ErrIDNotLeader":       {ErrIDNotLeader, 1005},
		"ErrIDFlowControl":     {ErrIDFlowControl, 1006},
	}
	for name, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %d, want %d", name, c.got, c.want)
		}
	}
}
