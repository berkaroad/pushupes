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

// TestBloomStoreSlotFalsePositiveRate fills one production-shaped generation
// (bloomGenRecords records over bloomGenBits bits) and measures the rate a slot
// actually sees. Opt-in: it takes ~40s and maps 256 MiB, so the default run
// skips it.
//
// The number to hold: one generation at 21.5 bits per record with bloomK probes
// lands just under 0.005%, and a slot's rate is the sum over its generations —
// ten generations (10^9 records) is still under the 0.1% budget.
func TestBloomStoreSlotFalsePositiveRate(t *testing.T) {
	if os.Getenv("PUSHUPES_BIG_BLOOM") == "" {
		t.Skip("set PUSHUPES_BIG_BLOOM=1 to run the 10^8-record fill")
	}
	dir := t.TempDir()
	s, err := openBloomStore(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()

	n := int(bloomGenRecords)
	for i := 0; i < n; i++ {
		s.insert(uint64(i+1), data.HashCommandID(fmt.Sprintf("big-%d", i)))
	}
	if got := s.gensCount(); got != 1 {
		t.Fatalf("generations after %d records: %d, want 1", n, got)
	}
	if got := s.records(); got != bloomGenRecords {
		t.Fatalf("records: %d, want %d", got, bloomGenRecords)
	}
	for i := 0; i < 1000; i++ {
		if !s.maybe(data.HashCommandID(fmt.Sprintf("big-%d", i))) {
			t.Fatalf("placed command %d reports absent", i)
		}
	}

	const probes = 1000000
	fp := 0
	for i := 0; i < probes; i++ {
		if s.maybe(data.HashCommandID(fmt.Sprintf("miss-%d", i))) {
			fp++
		}
	}
	rate := float64(fp) / probes
	t.Logf("one generation: %d records over %d bits (%.2f bits/record, k=%d): fp=%.5f%%, ten generations %.4f%%",
		bloomGenRecords, uint64(bloomGenBits), float64(bloomGenBits)/float64(bloomGenRecords), bloomK,
		100*rate, 1000*rate)
	if rate > 2e-4 {
		t.Fatalf("single-generation false-positive rate %.5f%% is above the 0.02%% shape", 100*rate)
	}
}
