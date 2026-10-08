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
	"sort"
	"sync"
	"testing"
	"time"
)

// applyCmd is a 256-byte command body — representative of a slot-table
// command, the same shape every benchmark in this package applies.
var benchCmd = []byte("x" + string(make([]byte, 255)))

type latencySink struct {
	mu   sync.Mutex
	data []time.Duration
}

func (s *latencySink) add(d time.Duration) {
	s.mu.Lock()
	s.data = append(s.data, d)
	s.mu.Unlock()
}

func (s *latencySink) report(b *testing.B) {
	s.mu.Lock()
	d := append([]time.Duration(nil), s.data...)
	s.data = nil
	s.mu.Unlock()
	if len(d) == 0 {
		return
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	p := func(q float64) time.Duration {
		i := int(q * float64(len(d)-1))
		return d[i]
	}
	b.ReportMetric(float64(p(0.50).Microseconds()), "p50-us")
	b.ReportMetric(float64(p(0.99).Microseconds()), "p99-us")
}

func benchLeader(b *testing.B, size int) (*testCluster, string) {
	c := startCluster(b, size)
	leader := c.leader(10 * time.Second)
	return c, leader
}

func BenchmarkApplySingleNodeSequential(b *testing.B) {
	c, id := benchLeader(b, 1)
	defer c.stopAll()
	n := c.nodes[id].node
	b.ResetTimer()
	sink := &latencySink{}
	for i := 0; i < b.N; i++ {
		t0 := time.Now()
		if _, err := n.Apply(benchCmd, 5*time.Second); err != nil {
			b.Fatalf("apply: %v", err)
		}
		sink.add(time.Since(t0))
	}
	b.StopTimer()
	sink.report(b)
}

func BenchmarkApplySingleNodeParallel16(b *testing.B) {
	c, id := benchLeader(b, 1)
	defer c.stopAll()
	n := c.nodes[id].node
	sink := &latencySink{}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			t0 := time.Now()
			if _, err := n.Apply(benchCmd, 5*time.Second); err != nil {
				b.Fatalf("apply: %v", err)
			}
			sink.add(time.Since(t0))
		}
	})
	b.StopTimer()
	sink.report(b)
}

func BenchmarkApplyThreeNodeSequential(b *testing.B) {
	c, id := benchLeader(b, 3)
	defer c.stopAll()
	n := c.nodes[id].node
	b.ResetTimer()
	sink := &latencySink{}
	for i := 0; i < b.N; i++ {
		t0 := time.Now()
		if _, err := n.Apply(benchCmd, 10*time.Second); err != nil {
			b.Fatalf("apply: %v", err)
		}
		sink.add(time.Since(t0))
	}
	b.StopTimer()
	sink.report(b)
}

// ---- fsync accounting ----

// TestGroupCommitCoalescesFsyncs shows the property the design leans on: many
// appends arriving inside one flush window share a single fsync. It measures
// the ratio on the writer side (flushes / records) rather than asserting wall
// time, so it is not timing-sensitive.
func TestGroupCommitCoalescesFsyncs(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, WALOptions{FlushInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	const n = 200
	lsns := make([]uint64, n)
	for i := 0; i < n; i++ {
		lsn, err := w.Append(RecordEntry, encodeEntry(Entry{Index: uint64(i + 1), Term: 1, Data: benchCmd}))
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		lsns[i] = lsn
	}
	if err := w.Wait(lsns[n-1]); err != nil {
		t.Fatalf("wait: %v", err)
	}
	flushes := w.flushCount()
	if flushes >= n {
		t.Fatalf("expected group commit to coalesce, got %d flushes for %d records", flushes, n)
	}
	t.Logf("%d records committed with %d fsync(s) (%.2f records/fsync)", n, flushes, float64(n)/float64(flushes))
}
