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

// The per-aggregate directory entry carries two numbers that every read path
// treats as equal:
//
//	version          the claimed latest version  (settled by indexMetaLocked)
//	sealedN + n      the seqs it can actually resolve
//
// AggregateVersion and TailVersion both refuse to answer when they disagree
// ("aggregate %s desync: latest version %d but only %d versions resolvable"),
// and that refusal is durable: the aggregate cannot be read again, on any
// replica that holds the slot, from any client. A live cluster showed exactly
// that state (version=104, sealedN=0, n=59, slot LEO=389 — nothing sealed,
// nothing dropped, so the split happened at write time).
//
// These cases walk the three write paths over the version/seq shapes a replica
// and a migration target actually produce, and assert the invariant holds after
// each one. The failing case IS the bug report: whichever shape breaks it names
// the path that must stop writing a non-contiguous version.
//
// Both at-seq entry points validate the SEQ strictly and the VERSION not at all,
// while Append validates the version strictly — so the cases below exercise both
// halves deliberately.
func TestDirectoryInvariantAcrossWritePaths(t *testing.T) {
	const agg = "inv-agg"

	// rec builds one record. The command id is derived from (agg, version) so a
	// repeat of the same version is a true idempotent replay.
	rec := func(id string, version uint32) *data.EventRecord {
		return &data.EventRecord{
			AggregateID: id, Version: version,
			CommandID: id + "-v" + itoaSmall(int(version)),
			Events:    []data.Event{{Type: "E", Body: []byte("x")}},
		}
	}

	type step struct {
		name string
		run  func(t *testing.T, st *Store, slot int32)
	}

	cases := []struct {
		name  string
		steps []step
	}{
		{
			name: "local-append-contiguous",
			steps: []step{{"append v1..v3", func(t *testing.T, st *Store, slot int32) {
				for v := uint32(1); v <= 3; v++ {
					if _, err := st.Append(rec(agg, v)); err != nil {
						t.Fatalf("append v%d: %v", v, err)
					}
				}
			}}},
		},
		{
			name: "at-seq-from-empty",
			steps: []step{{"appendFrameAtSeq v1", func(t *testing.T, st *Store, slot int32) {
				fr := rec(agg, 1).EncodeBinary(nil)
				if err := st.AppendFrameAtSeq(slot, 1, fr); err != nil {
					t.Fatalf("frame v1 at seq 1: %v", err)
				}
			}}},
		},
		{
			name: "at-seq-idempotent-replay",
			steps: []step{
				{"append v1..v3", func(t *testing.T, st *Store, slot int32) {
					for v := uint32(1); v <= 3; v++ {
						if _, err := st.Append(rec(agg, v)); err != nil {
							t.Fatal(err)
						}
					}
				}},
				{"replay seq 2 with the same bytes", func(t *testing.T, st *Store, slot int32) {
					fr := rec(agg, 2).EncodeBinary(nil)
					if err := st.AppendFrameAtSeq(slot, 2, fr); err != nil {
						t.Fatalf("idempotent replay: %v", err)
					}
				}},
			},
		},
		{
			name: "at-seq-version-jump",
			steps: []step{
				{"append v1..v3", func(t *testing.T, st *Store, slot int32) {
					for v := uint32(1); v <= 3; v++ {
						if _, err := st.Append(rec(agg, v)); err != nil {
							t.Fatal(err)
						}
					}
				}},
				{"at-seq a record claiming v9 at the next seq", func(t *testing.T, st *Store, slot int32) {
					fr := rec(agg, 9).EncodeBinary(nil)
					if err := st.AppendFrameAtSeq(slot, 4, fr); err != nil {
						t.Logf("rejected (acceptable): %v", err)
					}
				}},
			},
		},
		{
			name: "at-seq-version-behind",
			steps: []step{
				{"append v1..v9", func(t *testing.T, st *Store, slot int32) {
					for v := uint32(1); v <= 9; v++ {
						if _, err := st.Append(rec(agg, v)); err != nil {
							t.Fatal(err)
						}
					}
				}},
				{"at-seq a record claiming v4 at the next seq", func(t *testing.T, st *Store, slot int32) {
					fr := rec(agg, 4).EncodeBinary(nil)
					if err := st.AppendFrameAtSeq(slot, 10, fr); err != nil {
						t.Logf("rejected (acceptable): %v", err)
					}
				}},
			},
		},
		{
			name: "at-seq-other-aggregate-shares-slot",
			steps: []step{
				{"two aggregates on one slot, interleaved", func(t *testing.T, st *Store, slot int32) {
					other := secondAggInSlot(t, st, slot, agg)
					for v := uint32(1); v <= 3; v++ {
						if _, err := st.Append(rec(agg, v)); err != nil {
							t.Fatal(err)
						}
						if _, err := st.Append(rec(other, v)); err != nil {
							t.Fatal(err)
						}
					}
				}},
				{"at-seq the target's v4 at the very next slot seq", func(t *testing.T, st *Store, slot int32) {
					fr := rec(agg, 4).EncodeBinary(nil)
					next := st.LastSeqOf(slot) + 1
					if err := st.AppendFrameAtSeq(slot, next, fr); err != nil {
						t.Logf("rejected (acceptable): %v", err)
					}
				}},
			},
		},
		{
			name: "at-seq-after-seal",
			steps: []step{
				// A tiny segment so the appends below roll the segment and seal
				// it the way a long-running slot does.
				{"append v1..v3 past a tiny segment size", func(t *testing.T, st *Store, slot int32) {
					for v := uint32(1); v <= 3; v++ {
						if _, err := st.Append(rec(agg, v)); err != nil {
							t.Fatal(err)
						}
					}
					// Force the roll: one more append is enough to cross 1 byte.
					if _, err := st.Append(rec(agg, 4)); err != nil {
						t.Fatal(err)
					}
				}},
				{"at-seq a record claiming v7 at the next seq", func(t *testing.T, st *Store, slot int32) {
					fr := rec(agg, 7).EncodeBinary(nil)
					next := st.LastSeqOf(slot) + 1
					if err := st.AppendFrameAtSeq(slot, next, fr); err != nil {
						t.Logf("rejected (acceptable): %v", err)
					}
				}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, err := OpenStore(t.TempDir(), 8, 1, FlushPolicy{})
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()

			slot := st.SlotOf(agg)
			for _, s := range tc.steps {
				s.run(t, st, slot)
			}

			claim, resolvable, detail := dirNumbers(t, st, slot, agg)
			t.Logf("%s -> claimed=%d resolvable=%d (%s)", tc.name, claim, resolvable, detail)
			if claim != resolvable {
				t.Fatalf("directory desync after %q: claimed version %d but only %d resolvable seqs "+
					"(a read of this aggregate now fails permanently: %s)",
					tc.name, claim, resolvable, detail)
			}

			// The invariant is only worth asserting if the aggregate is still
			// readable afterwards: that is what the two numbers protect.
			if _, _, err := st.ReadAggregate(agg, 1, 0, 0); err != nil {
				t.Fatalf("aggregate unreadable after %q: %v", tc.name, err)
			}
		})
	}
}

// secondAggInSlot finds an aggregate id, distinct from `avoid`, that hashes to
// the given slot. Routing is mix64(FNV-1a(id)) % slot_count, so the id must be
// SEARCHED for — never assumed from a parity or modulo trick.
func secondAggInSlot(t *testing.T, st *Store, slot int32, avoid string) string {
	t.Helper()
	for i := 0; i < 8192; i++ {
		id := fmt.Sprintf("inv-other-%d", i)
		if st.SlotOf(id) == slot && id != avoid {
			return id
		}
	}
	t.Skip("no second aggregate hashes to this slot")
	return ""
}

// itoaSmall renders a small non-negative int without importing strconv.
func itoaSmall(i int) string {
	if i == 0 {
		return "0"
	}
	var b [12]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}

// dirNumbers reads the two directory numbers that must agree.
func dirNumbers(t *testing.T, st *Store, slot int32, agg string) (uint32, uint32, string) {
	t.Helper()
	sl, err := st.SlotIfLoaded(slot)
	if err != nil || sl == nil {
		t.Fatalf("slot %d not loaded: %v", slot, err)
	}
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	e := sl.aggs[agg]
	live := make([]uint64, 0, e.n)
	for i := 0; i < e.n; i++ {
		live = append(live, e.seqAt(i))
	}
	detail := fmt.Sprintf("sealedN=%d n=%d head=%d live_seqs=%v leo=%d",
		e.sealedN, e.n, e.head, live, sl.seqCounter.Load())
	return e.version, uint32(e.sealedN) + uint32(e.n), detail
}
