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
	"encoding/binary"
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
			ix, err := openSegCmdIndex(fx.dir, 7, fx.baseSeq)
			if err != nil || ix == nil {
				b.Fatal(err)
			}
			ix.Close()
		}
	})
	b.Run("no-bloom(lookup)", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ix, err := openSegCmdIndex(fx.dir, 7, fx.baseSeq)
			if err != nil || ix == nil {
				b.Fatal(err)
			}
			ix.Close()
		}
	})
	// The search itself answers either way — spot-check one command.
	ix, _ := openSegCmdIndex(fx.dir, 7, fx.baseSeq)
	target := uint32(fx.cmdHashes[42] >> 32)
	if _, err := ix.LookupCmd(target, func(e segCmdEntry) bool {
		return e.seq == 43
	}); err != nil {
		b.Fatal(err)
	}
	ix.Close()
}

// B: the tail walk with the hash comparison inline vs per-entry callbacks.
// A miss (the false-positive case) walks the whole file and pays every
// entry either way; the specialised form pays an integer compare instead
// of a callback and a slice header.
func BenchmarkTailScanWalk(b *testing.B) {
	fx := newBenchFixture(b, 60000)
	f, err := os.Open(fx.idxPath)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	miss := ^uint64(0) // no entry holds it: the full-file walk
	b.ReportAllocs()
	b.Run("per-entry-callback", func(b *testing.B) {
		// The shape before the specialised walk: one callback per entry
		// with a slice header, the comparison inside the callback.
		for i := 0; i < b.N; i++ {
			if _, err := scanIdxEntriesFile(f, segIdxMagic, 7, fx.baseSeq, segIdxEntryBytes, func(raw []byte) bool {
				return binary.BigEndian.Uint64(raw[0:8]) != miss
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("specialised-hash-walk", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, _, err := scanIdxEntriesHashFile(f, segIdxMagic, 7, fx.baseSeq, segIdxEntryBytes, miss, func([]byte) bool {
				return true
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func hashOf(cmd string) uint64 { return data.HashCommandID(cmd) }
