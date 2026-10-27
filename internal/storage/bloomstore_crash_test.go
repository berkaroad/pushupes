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

	"github.com/berkaroad/pushupes/internal/data"
)

// A process that dies without closing the slot leaves the newest generation's
// bits unwritten past its header's coverage. The next start must replay those
// records and place them again — a stored command that the filter reports
// absent is a duplicate write, so coverage is the one thing that may lag.
func TestBloomReplaysUnpersistedTail(t *testing.T) {
	dir := t.TempDir()
	const n = 300

	sl, err := OpenSlotRepairing(dir, 7, 1<<40, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		mustAppend(t, sl, mkRec(fmt.Sprintf("a%04d", i), 1, fmt.Sprintf("c%04d", i)))
	}
	// No Close: this is the crash shape — nothing of the newest generation was
	// committed, so its header still claims no coverage.

	sl2, err := OpenSlotRepairing(dir, 7, 1<<40, FlushPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sl2.Close(); err != nil {
			t.Fatal(err)
		}
		if err := sl.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("c%04d", i)
		rec, seq, err := sl2.RecordByCommand(id)
		if err != nil {
			t.Fatal(err)
		}
		if rec == nil || seq != uint64(i+1) {
			t.Fatalf("command %q after an unclean restart: found=%v seq=%d, want seq %d",
				id, rec != nil, seq, i+1)
		}
	}
	// Idempotency face of the same thing: the retry answers exists.
	out, err := sl2.Append(mkRec("a0000", 1, "c0000"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != data.StatusExists {
		t.Fatalf("retry after an unclean restart: status %s (err %d), want exists",
			out.Status, out.ErrID)
	}
}
