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

package data

import (
	"fmt"
	"testing"

	"github.com/berkaroad/pushupes/pkg/client"
)

// TestLogicalSlotOfDelegatesToClient pins the shared-algorithm contract: the
// server routes by exactly the function clients compute with (pkg/client), over
// the same generation size. If this ever disagrees, a client's PrefetchRoutes row
// and the server's placement name different nodes — every write pays a redirect.
func TestLogicalSlotOfDelegatesToClient(t *testing.T) {
	for _, cnt := range []int{1, 2, 4, 8, 16, 32} {
		for i := 0; i < 200; i++ {
			id := fmt.Sprintf("agg-%d", i)
			if got, want := LogicalSlotOf(id, cnt), client.LogicalSlotOf(id, cnt); got != want {
				t.Fatalf("LogicalSlotOf(%q, %d): server %d, client %d", id, cnt, got, want)
			}
		}
	}
}
