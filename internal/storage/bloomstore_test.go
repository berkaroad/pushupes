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
	"math/rand"
	"os"
	"testing"

	"github.com/berkaroad/pushupes/internal/data"
)

// Test shapes: small generations so a handful of inserts rolls over, and a bit
// array small enough to keep the test's mapped pages trivial (the production
// shape is 2^31 bits per generation).
const (
	testGenRecords = 1000
	testGenBits    = 1 << 14
)

func openTestBloom(t *testing.T, dir string) *bloomStore {
	t.Helper()
	s, err := openBloomStoreShape(dir, 0, testGenRecords, testGenBits)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// The filter answers for what it was given and stays quiet about what it was
// not: "absent" is the only answer a lookup acts on, so a hash that was placed
// must never come back absent.
func TestBloomStorePlacesItsHashes(t *testing.T) {
	dir := t.TempDir()
	s := openTestBloom(t, dir)
	defer s.close()

	const n = 3000 // three generations at testGenRecords
	for i := 0; i < n; i++ {
		s.insert(uint64(i+1), data.HashCommandID(fmt.Sprintf("placed-%d", i)))
	}
	if got := s.gensCount(); got != 3 {
		t.Fatalf("generations: %d, want 3 (genRecords=%d)", got, testGenRecords)
	}
	if got := s.records(); got != n {
		t.Fatalf("records: %d, want %d", got, n)
	}
	for i := 0; i < n; i++ {
		if !s.maybe(data.HashCommandID(fmt.Sprintf("placed-%d", i))) {
			t.Fatalf("placed command %d reports absent", i)
		}
	}
	// False positives are allowed, but the rate is the whole point of the
	// shape: 16 bits per record and bloomK probes land well under 1%.
	fp := 0
	const probes = 20000
	for i := 0; i < probes; i++ {
		if s.maybe(data.HashCommandID(fmt.Sprintf("absent-%d", i))) {
			fp++
		}
	}
	if rate := float64(fp) / probes; rate > 0.01 {
		t.Fatalf("false-positive rate %.4f over %d probes, want < 0.01", rate, probes)
	}
}

// An empty or unopenable store says maybe: a filter that cannot answer must
// not make a lookup skip the disk.
func TestBloomStoreEmptySaysMaybe(t *testing.T) {
	s := openTestBloom(t, t.TempDir())
	defer s.close()
	if !s.maybe(0xdeadbeef) {
		t.Fatal("empty store reported absent")
	}
	s.unsafe = true
	if !s.maybe(0xdeadbeef) {
		t.Fatal("unsafe store reported absent")
	}
}

// A restart must resume from the header's coverage instead of re-placing every
// record: the watermark is what a replay compares against, and re-placing an
// already-placed hash spends a generation slot for nothing.
func TestBloomStoreWatermarkSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s := openTestBloom(t, dir)
	const n = 2500
	for i := 0; i < n; i++ {
		s.insert(uint64(i+1), data.HashCommandID(fmt.Sprintf("wm-%d", i)))
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	re := openTestBloom(t, dir)
	defer re.close()
	if re.watermark != n {
		t.Fatalf("watermark after reopen: %d, want %d", re.watermark, n)
	}
	if got := re.records(); got != n {
		t.Fatalf("records after reopen: %d, want %d", got, n)
	}
	for i := 0; i < n; i++ {
		if !re.maybe(data.HashCommandID(fmt.Sprintf("wm-%d", i))) {
			t.Fatalf("restored command %d reports absent", i)
		}
	}
	// Replaying the covered seqs must not grow the filter.
	before := re.records()
	for i := 0; i < n; i++ {
		re.insert(uint64(i+1), data.HashCommandID(fmt.Sprintf("wm-%d", i)))
	}
	if got := re.records(); got != before {
		t.Fatalf("replaying covered seqs changed records: %d -> %d", before, got)
	}
	// A seq past the watermark places again.
	re.insert(n+1, data.HashCommandID("wm-new"))
	if got := re.records(); got != before+1 {
		t.Fatalf("records after a new seq: %d, want %d", got, before+1)
	}
}

// A generation whose header does not validate is dropped along with the ones
// after it, and the watermark falls back to the last trusted generation: the
// replay then rebuilds exactly the records that were lost.
func TestBloomStoreDropsDamagedGeneration(t *testing.T) {
	dir := t.TempDir()
	s := openTestBloom(t, dir)
	const n = 2500 // three generations
	for i := 0; i < n; i++ {
		s.insert(uint64(i+1), data.HashCommandID(fmt.Sprintf("dmg-%d", i)))
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt both header pages of generation 1 (seqs 1001..2000).
	path := bloomGenPath(bloomStoreDir(dir), 1)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	junk := []byte("XXXXXXXX")
	if _, err := f.WriteAt(junk, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(junk, bloomHdrPage); err != nil {
		t.Fatal(err)
	}
	f.Close()

	dropped := bloomGenDropped.Load()
	re := openTestBloom(t, dir)
	defer re.close()
	if got := re.gensCount(); got != 1 {
		t.Fatalf("generations after a damaged one: %d, want 1", got)
	}
	if re.watermark != testGenRecords {
		t.Fatalf("watermark after drop: %d, want %d", re.watermark, testGenRecords)
	}
	if bloomGenDropped.Load() == dropped {
		t.Fatal("dropping a damaged generation was not counted")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("damaged generation file still present: %v", err)
	}
	// The first generation's records are still there.
	for i := 0; i < testGenRecords; i++ {
		if !re.maybe(data.HashCommandID(fmt.Sprintf("dmg-%d", i))) {
			t.Fatalf("surviving generation lost command %d", i)
		}
	}
}

// The probe cost is what the write path pays per record: a definitely-absent
// command walks every generation. A young slot has one, a 10⁹-record slot ten.
func benchBloomMaybe(b *testing.B, gens int) {
	rng := rand.New(rand.NewSource(11))
	dir := b.TempDir()
	s, err := openBloomStoreShape(dir, 0, testGenRecords, testGenBits)
	if err != nil {
		b.Fatal(err)
	}
	defer s.close()
	for g := 0; g < gens; g++ {
		for i := 0; i < testGenRecords; i++ {
			s.insert(uint64(g*testGenRecords+i+1), rng.Uint64())
		}
	}
	misses := make([]uint64, 4096)
	for i := range misses {
		for {
			h := rng.Uint64()
			if !s.maybe(h) {
				misses[i] = h
				break
			}
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		if s.maybe(misses[j&4095]) {
			b.Fatal("a known miss answered maybe")
		}
	}
}

func BenchmarkBloomStoreMaybe1(b *testing.B)  { benchBloomMaybe(b, 1) }
func BenchmarkBloomStoreMaybe10(b *testing.B) { benchBloomMaybe(b, 10) }
