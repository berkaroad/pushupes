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

package storage

import (
	"fmt"
	"testing"

	"pushupes/internal/data"
)

// What encode writes must decode back — and it must answer for the commands
// that went in. The in-memory chain is what the slot uses, so a mismatch here
// only shows up as "the file is there but its filter does not decode", which
// makes a start rebuild every segment's index instead of reusing it.
func TestBloomSetEncodeRoundTrip(t *testing.T) {
	set := &bloomSet{}
	ids := make([]string, 0, 5000)
	for i := 0; i < 5000; i++ {
		id := fmt.Sprintf("cmd-roundtrip-%d", i)
		ids = append(ids, id)
		set.insert(data.HashCommandID(id))
	}
	buf := set.encode()
	got := decodeBloomSet(buf)
	if got == nil {
		t.Fatal("decodeBloomSet rejected what encodeBloomSet wrote")
	}
	if len(got.filters) != len(set.filters) {
		t.Fatalf("filters: %d != %d", len(got.filters), len(set.filters))
	}
	for i := range set.filters {
		if len(got.filters[i].bits) != len(set.filters[i].bits) {
			t.Fatalf("filter %d words: %d != %d", i, len(got.filters[i].bits), len(set.filters[i].bits))
		}
	}
	for _, id := range ids {
		if !got.maybe(data.HashCommandID(id)) {
			t.Fatalf("inserted command %q reported absent", id)
		}
	}
	// A truncated or damaged buffer must be rejected, not trusted.
	if decodeBloomSet(buf[:len(buf)-1]) != nil {
		t.Fatal("truncated buffer decoded")
	}
	bad := append([]byte(nil), buf...)
	bad[6] ^= 0xFF
	if decodeBloomSet(bad) != nil {
		t.Fatal("damaged buffer decoded")
	}
}
