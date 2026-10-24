// Copyright 2026 berkaroad
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file in compliance with the License.
// See LICENSE for the full text.

package storage

import (
	"encoding/binary"
	"hash/crc32"
	"math/rand"
	"testing"
)

// The probe masks with (bits-1). These tests pin the two sides of that
// decision: for every bit count the package can build, the mask answers
// exactly what the original modulo placement would; and a filter file that
// promises a non-power-of-two word count is refused instead of being probed
// with a placement its writers never used.

func probeMod(hash uint64, i int, words int) int {
	h1 := hash
	h2 := (hash >> 32) | 1
	return int((h1 + uint64(i)*h2) % uint64(words*64))
}

func maybeMod(f *bloomBits, hash uint64) bool {
	for i := 0; i < bloomK; i++ {
		bit := probeMod(hash, i, len(f.bits))
		if f.bits[bit/64]&(1<<uint(bit%64)) == 0 {
			return false
		}
	}
	return true
}

func TestBloomProbeMaskMatchesModuloPlacement(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for _, capacity := range []int{bloomFirstCapacity, 1 << 14, 1 << 16, 1 << 21} {
		f := newBloomBits(capacity)
		for i := 0; i < 1000; i++ {
			f.insert(rng.Uint64())
		}
		for i := 0; i < 100000; i++ {
			h := rng.Uint64()
			if f.maybe(h) != maybeMod(f, h) {
				t.Fatalf("capacity %d: probe %d (hash %x): mask=%v modulo=%v",
					capacity, i, h, f.maybe(h), maybeMod(f, h))
			}
		}
	}
}

func TestDecodeBloomSetRefusesNonPowerOfTwoWords(t *testing.T) {
	set := &bloomSet{}
	set.insertPlacedFrom(0, 0x0123456789abcdef, false)
	set.insertPlacedFrom(0, 0xfedcba9876543210, false)
	buf := set.encode()

	// The first filter header sits right after the count: k(1) pad(3)
	// words(8). Point it at a word count no writer could have produced.
	tampered := make([]byte, len(buf))
	copy(tampered, buf)
	binary.BigEndian.PutUint64(tampered[8:16], 3)
	payload := len(tampered) - 4
	binary.BigEndian.PutUint32(tampered[payload:], crc32.Checksum(tampered[:payload], segIdxCRC))

	if s := decodeBloomSet(tampered); s != nil {
		t.Fatalf("decoder accepted a filter with words=3: %+v", s.filters[0].bits)
	}
	if s := decodeBloomSet(buf); s == nil {
		t.Fatal("decoder refused the untampered set")
	}
}

// The probe cost is what the write path pays per record: a definitely-absent
// command walks every filter in the slot's chain. The chain benches model the
// two shapes seen in profiles — a young slot (one filter) and a slot with a
// dozen sealed segments (seven doubling filters).
func benchMissWalk(b *testing.B, probe func(*bloomSet, uint64) bool, depth int, fillEach int) {
	rng := rand.New(rand.NewSource(11))
	set := &bloomSet{}
	for i := 0; i < depth; i++ {
		f := newBloomBits(bloomFirstCapacity << uint(i))
		for j := 0; j < fillEach; j++ {
			f.insert(rng.Uint64())
		}
		set.filters = append(set.filters, f)
	}
	misses := make([]uint64, 4096)
	for i := range misses {
		for {
			h := rng.Uint64()
			if !set.maybe(h) {
				misses[i] = h
				break
			}
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		probe(set, misses[j&4095])
	}
}

func BenchmarkBloomMaybe1(b *testing.B) {
	benchMissWalk(b, func(s *bloomSet, h uint64) bool { return s.maybe(h) }, 1, 1<<10)
}

func BenchmarkBloomMaybe7(b *testing.B) {
	benchMissWalk(b, func(s *bloomSet, h uint64) bool { return s.maybe(h) }, 7, 1<<13)
}

func BenchmarkBloomMaybeModulo1(b *testing.B) {
	benchMissWalk(b, func(s *bloomSet, h uint64) bool { return s.unsafe || s.maybeMod(h) }, 1, 1<<10)
}

func BenchmarkBloomMaybeModulo7(b *testing.B) {
	benchMissWalk(b, func(s *bloomSet, h uint64) bool { return s.unsafe || s.maybeMod(h) }, 7, 1<<13)
}

func (s *bloomSet) maybeMod(hash uint64) bool {
	if s.unsafe || len(s.filters) == 0 {
		return true
	}
	for _, f := range s.filters {
		if maybeMod(f, hash) {
			return true
		}
	}
	return false
}
