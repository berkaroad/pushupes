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
	"os"
	"testing"

	"github.com/berkaroad/pushupes/internal/data"
)

// benchFixture builds one sealed segment with records of the shape the
// idempotency lookup faces, and returns its dir plus the writable tail's
// open .idx written by the normal path.
type benchFixture struct {
	dir       string
	cmdHashes []uint64
	idxPath   string
	idxFile   *os.File
	baseSeq   uint64
}

func newBenchFixture(b *testing.B, records int) *benchFixture {
	b.Helper()
	dir := b.TempDir()
	sl, err := OpenSlotRepairing(dir, 7, 1<<40, FlushPolicy{})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < records; i++ {
		out, err := sl.Append(mkRec(fmt.Sprintf("a%07d", i), 1, fmt.Sprintf("c%07d", i)))
		if err != nil || out.Status != "success" {
			b.Fatalf("append %d: %v %v", i, out, err)
		}
	}
	sl.mu.Lock()
	tail := sl.segments[len(sl.segments)-1]
	sl.segmentBytes = tail.SizeBytes() // roll: seal the records
	sl.mu.Unlock()
	if _, err := sl.Append(mkRec("zz", 1, "zz-cmd")); err != nil {
		b.Fatal(err)
	}
	sealed := sl.segments[0]
	hashes := make([]uint64, records)
	for i := range hashes {
		hashes[i] = hashOf(fmt.Sprintf("c%07d", i))
	}
	// Keep an independent open of the sealed segment's .idx for the scan
	// benches (the slot owns the writer fd and closes with it).
	idxPath := segIdxPath(dir, sealed.BaseSeq)
	f, err := os.Open(idxPath)
	if err != nil {
		b.Fatal(err)
	}
	base := sealed.BaseSeq
	if err := sl.Close(); err != nil {
		b.Fatal(err)
	}
	f.Close() // re-open per phase below; path is what benches need
	return &benchFixture{dir: dir, cmdHashes: hashes, idxPath: idxPath, baseSeq: base}
}

// A: the sealed .cidx open of a lookup vs the startup one. The decode the
// lookup never reads costs the bloom section's popcount over the segment.
func BenchmarkCidxOpenForLookup(b *testing.B) {
	fx := newBenchFixture(b, 60000) // 60k records: the .cidx bloom is ~80KB
	b.ReportAllocs()
	b.Run("with-bloom(startup)", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ix, err := openSegCmdIndex(fx.dir, 7, fx.baseSeq, true)
			if err != nil || ix == nil {
				b.Fatal(err)
			}
			ix.Close()
		}
	})
	b.Run("no-bloom(lookup)", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ix, err := openSegCmdIndex(fx.dir, 7, fx.baseSeq, false)
			if err != nil || ix == nil {
				b.Fatal(err)
			}
			ix.Close()
		}
	})
	// The search itself answers either way — spot-check one command.
	ix, _ := openSegCmdIndex(fx.dir, 7, fx.baseSeq, false)
	target := uint32(fx.cmdHashes[42] >> 32)
	if _, err := ix.LookupCmd(target, func(e segCmdEntry) bool {
		return e.seq == 43
	}); err != nil {
		b.Fatal(err)
	}
	ix.Close()
}

func hashOf(cmd string) uint64 { return data.HashCommandID(cmd) }
