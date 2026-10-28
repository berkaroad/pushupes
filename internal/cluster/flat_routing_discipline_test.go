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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestEngineNeverUsesFlatRouting guards the one rule the generation rollout
// learned the hard way: the storage layer must not derive a slot from an
// aggregate id. The engine computes the slot (from the served generation), fences
// it, waits for its watermark and then writes THAT slot; a store-side flat route
// writes somewhere else, so the engine waits forever on a slot nobody wrote.
//
// The flat helpers still exist for tests and tooling (they are convenient and the
// tooling genuinely has no layout), which is exactly why the rule needs a test
// instead of trust: a new engine call site would compile and look right.
func TestEngineNeverUsesFlatRouting(t *testing.T) {
	forbidden := []string{
		".AppendRouted(",
		".ReadAggregateRouted(",
		".RecordByCommandRouted(",
		".TailVersionOfRouted(",
		".VersionAtOrBeforeTimeRouted(",
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, bad := range forbidden {
			if strings.Contains(string(body), bad) {
				t.Fatalf("%s calls %s: the engine decides the slot (the served generation), the store must be told it", name, bad)
			}
		}
	}
}
