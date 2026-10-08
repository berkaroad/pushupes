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

package raft

import (
	"fmt"
	"testing"
	"time"
)

// TestSnapshotUnderLowThresholdKeepsProgress is the regression for the bug
// where a snapshot trimmed the log from the wrong end: the baseline advanced
// but the in-memory entries below it stayed, so the applied cursor ran past the
// log and every later Apply waited out its timeout. A tight threshold keeps a
// snapshot firing in the middle of a continuous write stream.
func TestSnapshotUnderLowThresholdKeepsProgress(t *testing.T) {
	c := startClusterCfg(t, 1, func(cfg *Config) {
		cfg.SnapshotThreshold = 4
		cfg.SnapshotInterval = 20 * time.Millisecond
	})
	defer c.stopAll()
	id := c.leader(3 * time.Second)
	for i := 0; i < 80; i++ {
		if _, err := c.apply(id, fmt.Sprintf("set k%d v%d", i, i)); err != nil {
			snapIdx, _, _ := c.nodes[id].node.log.Snapshot()
			v := c.nodes[id].node.readView()
			t.Fatalf("apply %d stalled: %v (state=%s commit=%d applied=%d last=%d snap=%d first=%d)",
				i, err, v.state, v.commit, v.applied, v.lastIndex, snapIdx, c.nodes[id].node.log.FirstIndex())
		}
	}
	if snapIdx, _, _ := c.nodes[id].node.log.Snapshot(); snapIdx == 0 {
		t.Fatalf("no snapshot was taken at threshold 4")
	}
	if v, ok := c.nodes[id].fsm.get("k79"); !ok || v != "v79" {
		t.Fatalf("last command missing from the fsm: %q %v", v, ok)
	}
}
