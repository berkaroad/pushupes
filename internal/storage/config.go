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

// This file is the one home of the storage layer's shipped defaults and of the
// tuning constants its own code runs on: a reader asking "what does the WAL
// start with, and what numbers does the segment format spend" reads this file
// instead of the nine files that use the values.
//
// It sits in the storage package on purpose: the segment layout, the index
// formats and the flush policy change together, so the layer's numbers travel
// with the layer — a diff of this one file answers whether a change touched
// storage's on-disk contract. The operator-facing knobs (-segment-bytes,
// -flush-messages, -flush-interval, -flush-parallelism) take their defaults
// from the exported names below; the command seeds them into its own config.
//
// The exported Default* names are package API, read outside as well
// (internal/api's paging, the command's flag defaults, the tests). The
// unexported names are internal tuning: they never leave the package and
// change only with the code that spends them.

package storage

import (
	"runtime"
	"time"
)

// Cluster topology and WAL segment sizing: the fixed slot count, the shipped
// roll threshold and the bounds a configured segment size must satisfy.
const (
	// DefaultSlotCount is the fixed number of slots in the cluster.
	// 1024 = 2^10 is sized for the split data plane: at the target ceiling of 256
	// data nodes every node still holds 4 slots, so placement and migration stay
	// granular at full scale. Data-node counts are even, and every power of two up
	// to 256 divides 1024 exactly — those topologies give each node the same slot
	// count (ring placement keeps any other N within one slot of the mean). It is
	// also far above the expected node count: cheap slot metadata, fine-grained
	// placement and migration units. The planned node (4 cores / 16GiB / 1TiB,
	// records typically 1-4KiB) sizes this together with the segment default:
	// every slot costs a long-poll and a WAL, the count and its ceiling are one
	// decision, taken together in DESIGN §7.2.
	//
	// This is server-side cluster state. The client contract (pkg/client) carries
	// the routing ALGORITHM — the one thing a client must compute identically —
	// while the slot count reaches clients through PrefetchRoutes, so it can
	// change without stranding any client build.
	DefaultSlotCount = 1024

	// A configured segment size must be a whole number of SegmentBytesMultiple
	// bytes, between MinSegmentBytes and MaxSegmentBytes. The floor exists
	// because a smaller segment buys nothing — per-slot state, index files and
	// file descriptors all scale with the segment count, while the memory a
	// segment holds is bounded by its size either way. The ceiling exists
	// because one segment's index files, its seal-time frame walk and the
	// per-aggregate seq list it keeps in memory are all proportional to it.
	SegmentBytesMultiple = int64(64 << 20)
	MinSegmentBytes      = SegmentBytesMultiple
	MaxSegmentBytes      = int64(2 << 30)

	// DefaultSegmentBytes is the 64MiB roll threshold from the design: on the
	// planned node (4 cores / 16GiB / 1TiB, records typically 1-4KiB) the live
	// tail is 8 B/record per HOT slot and every slot on a single-data-node
	// cluster is hot — 64MiB keeps the unsealed-seq term at ~0.25GiB for 1KiB
	// records (DESIGN §7.2). Small segments cost segment count, WAL fds and
	// index files; the planned historical-segment merge reclaims the segment
	// count once it lands, which is why the default sits at the small end now.
	DefaultSegmentBytes = MinSegmentBytes
)

// The flush policy the service ships with.
const (
	// DefaultFlushInterval is the shipped timed-flush cadence: a slot with
	// pending records fsyncs at least this often. It bounds how much of the WAL
	// a crash can lose, at the price of one fsync per open slot per interval.
	DefaultFlushInterval = 100 * time.Millisecond

	// DefaultFlushMessages is the record-count half of the shipped policy: a
	// slot fsyncs every DefaultFlushMessages records or every
	// DefaultFlushInterval, whichever comes first (-flush-messages).
	DefaultFlushMessages int64 = 1000

	// DefaultFlushParallelism is how many segment fsyncs may be in flight at
	// once (-flush-parallelism): 1 is the historical one-at-a-time pipeline,
	// more lets independent segments' fsyncs overlap on the device.
	DefaultFlushParallelism = 1
)

// Admin stream paging.
const (
	// DefaultStreamPage is the page size when the caller does not ask.
	DefaultStreamPage = 200
	// MaxStreamPage bounds one response: a page is a console view, and the
	// selection cost is O(aggregates in the slot) no matter how big the page
	// is, so a runaway limit would only add serialisation work.
	MaxStreamPage = 1000
)

// Internal tuning: segment frame walking, index filters, chunking. None of
// these is an operator knob.
const (
	// indexIntervalB is how sparsely seq -> file offset is remembered in memory
	// and in the segment's .spx file. It is a seek hint: a lookup lands at the
	// nearest earlier entry and walks frames forward, so the interval bounds
	// how many frames a seek decodes — including the replica-fetch read path,
	// which seeks from the sparse hint to the slot's tail on every round.
	//
	// One entry costs 16 B, so the interval sets what the table holds: one
	// entry per interval of data, i.e. 16 B per 64 KiB, which is 0.25 B per
	// 1 KiB record and ~253 MiB on a 1 TiB node. A coarser interval trades
	// that memory back for a longer walk per seek.
	indexIntervalB = int64(64 << 10) // one entry per 64KiB

	// minRecordLen / maxRecordLen bound a plausible frame length prefix;
	// anything else is a torn tail (the same bounds the per-record readers
	// use).
	minRecordLen = 30
	maxRecordLen = 64 << 20

	// frameWindow is the read window of one segment's frame walk: one pread per
	// window instead of one per record.
	frameWindow = 1 << 20

	// The drain round's budget: at most this many units, or this much wall
	// clock, per round. Between rounds the loop re-runs the policy pass and
	// yields, which keeps a seal storm or a threshold-crossing wave from
	// holding the fsync pipeline (and the device) for as long as the queue is
	// deep.
	drainRoundMaxUnits = 8
	drainRoundMaxAge   = 200 * time.Millisecond
)

// Bloom filter shapes and the sequential read window of a block index scan.
const (
	bloomK              = 3  // hash probes per entry
	bloomBitsPerEntry   = 16 // ~0.5% false positives at k=3 (full filter)
	bloomFirstCapacity  = 1 << 10
	bloomFilterMaxBytes = 1 << 24
	// segIdxScanWindowB is the sequential read window a scan walks a block file
	// in: one pread per window instead of two per block (head, then head+body),
	// with no per-block allocation. Blocks fit in a window with room to spare
	// (a full .idx block is segIdxBlockHead+segIdxBlockMax*28 bytes), so a block
	// can only span windows by corruption and the walk re-windows at its start.
	segIdxScanWindowB = 256 << 10
)

// Per-slot memory bounds and the I/O vector cap of one scatter write.
const (
	// strandedPerAgg bounds the memory of withheld frames per aggregate: past it
	// arrivals behave exactly as before (withhold, do not remember) — the set is
	// a salvage of realistically-scrumbled chains, not a second index.
	strandedPerAgg = 1 << 16
	// seqChunkMax is one chunk of an aggregate's seq list; 8 KiB per chunk once
	// an aggregate is past 336 seqs.
	seqChunkMax = 1024
	// scatterMaxIOV is Linux's IOV_MAX: one pwritev(2) accepts at most this many
	// segments, so records with long event lists are written in batches.
	scatterMaxIOV = 1024
)

// The store's parallel recovery walk follows the process CPU width, with a
// floor: below four workers a cold start is dominated by the reads themselves.
const slotOpenWorkersMin = 4

var slotOpenWorkers = max(slotOpenWorkersMin, runtime.GOMAXPROCS(0))
