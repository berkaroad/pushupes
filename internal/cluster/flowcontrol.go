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

// Node-level flow control: ONE operator-set token budget shared by every
// slot this node LEADS — the control and its configuration live at the same
// granularity, the node. Any leader-side write to any of this node's slots
// takes one token from the node's bucket. A rejected append answers
// fail/1006 (data.ErrIDFlowControl, message "FlowControl") without touching
// the WAL. The configuration lives only in this process — set through
// POST /admin/flow-control, it is not replicated through Raft and a restart
// returns the node to unlimited.
//
// Refusals stay COUNTED PER SLOT: the bucket is the node's, but which write
// got refused happened at a concrete slot, and the admin per-slot hit array
// (and the console's slot-level marker) depends on that attribution.
//
// The bucket model is the operator's own: taking a token stamps its return
// deadline (take time + period); tokens come back when their deadline passes,
// in one sweep run on the write path before the next take. No refill
// goroutine exists — an empty deadline heap means every token is available,
// so an idle bucket costs nothing, and the sustained ceiling is tokens/period
// for the NODE.

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"
)

// FlowConfig is one node-level flow-control setting: Tokens per Period
// across every slot the node leads. A non-positive token budget or period
// means the node appends without any throttle.
type FlowConfig struct {
	Tokens int32         `json:"tokens"`
	Period time.Duration `json:"-"`
}

// Unlimited reports whether this config throttles nothing.
func (c FlowConfig) Unlimited() bool { return c.Tokens <= 0 || c.Period <= 0 }

// flowHeap is a min-heap of token return deadlines (unix nanos), one entry
// per token currently away. Swept at the head before each take.
type flowHeap []int64

func (h flowHeap) Len() int           { return len(h) }
func (h flowHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h flowHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *flowHeap) Push(x any)        { *h = append(*h, x.(int64)) }
func (h *flowHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// slotBucket is the node's single token bucket (the type name keeps "slot"
// only in its history: it gates slot-leader writes). Its shape
// (capacity + period) is frozen when the bucket is created under a config
// generation, so its mu-protected fields never re-interpret a deadline
// stamped under different rules: a config change rotates the generation and
// the write path builds fresh buckets instead of mutating these.
//
// Representation: capacity minus the tokens currently away. Away = the
// un-expired heap entries MINUS `refunded` — a refund (the write it paid for
// never landed) marks one deadline already honoured without removing it from
// the heap, so the sweep keeps a single, order-preserving pass at the head
// and the budget comes back immediately. free = capacity - away.
type slotBucket struct {
	capacity int32
	period   int64 // nanos

	mu  sync.Mutex
	due flowHeap
	// refunded counts the deadlines whose token has ALREADY been given back;
	// the sweep consumes the mark by popping past that many head entries
	// without crediting tokens for them.
	refunded int
}

// sweep returns every token whose deadline has passed. Refund-marked tokens
// were credited when they were refunded, so their (now head-)deadlines are
// simply discarded.
func (b *slotBucket) sweep(now int64) {
	for len(b.due) > 0 && b.due[0] <= now {
		heap.Pop(&b.due)
		if b.refunded > 0 {
			b.refunded--
		}
	}
}

// away is how many tokens the bucket currently holds hostage.
func (b *slotBucket) away() int {
	n := len(b.due) - b.refunded
	if n < 0 {
		return 0
	}
	return n
}

// take consumes one token for a write about to land, reporting whether the
// bucket allowed it. Expired tokens are swept back first; a successful take
// stamps the token's return deadline. The bucket counts nothing itself: a
// refusal is attributed by the caller to the slot whose write was rejected.
func (b *slotBucket) take(now int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sweep(now)
	if int32(b.away()) >= b.capacity {
		return false
	}
	heap.Push(&b.due, now+b.period)
	return true
}

// refund gives a token back without waiting for its deadline: the write it
// paid for never landed on the leader's log (a business-rule fail), so the
// bucket must not hold the budget hostage for a full period. The mark is
// consumed by the next sweep of a head deadline; free capacity is restored
// immediately.
func (b *slotBucket) refund() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refunded++
}

// flowControl is the node's whole throttle: the current config plus the one
// bucket built under the generation that config belongs to.
type flowControl struct {
	cfg atomic.Pointer[FlowConfig]

	mu  sync.Mutex
	gen uint64
	bkt *slotBucket

	// hitCounters keeps one monotonic counter per slot: the shared bucket
	// routes a refusal's count to the slot whose write was rejected (see
	// takeFor), so per-slot attribution survives node-wide budgeting. The
	// map outlives generation swaps — history is only reset by a restart.
	hitMu       sync.Mutex
	hitCounters map[int32]*atomic.Uint64
}

// hitCounter returns the slot's persistent rejection counter.
func (f *flowControl) hitCounter(slot int32) *atomic.Uint64 {
	f.hitMu.Lock()
	defer f.hitMu.Unlock()
	if f.hitCounters == nil {
		f.hitCounters = map[int32]*atomic.Uint64{}
	}
	c := f.hitCounters[slot]
	if c == nil {
		c = &atomic.Uint64{}
		f.hitCounters[slot] = c
	}
	return c
}

// config returns the node's current flow config (never set = unlimited).
func (f *flowControl) config() FlowConfig {
	if c := f.cfg.Load(); c != nil {
		return *c
	}
	return FlowConfig{}
}

// set installs a config and rotates the generation: the bucket is dropped,
// so the next write builds a fresh one under the new budget.
func (f *flowControl) set(c FlowConfig) {
	f.cfg.Store(&c)
	f.mu.Lock()
	f.gen++
	f.bkt = nil
	f.mu.Unlock()
}

// generation reads the current bucket generation. Taken BEFORE the config:
// a rotation between the two reads makes bucketOf refuse to serve (the
// caller lets the write through and re-reads on the next append) rather
// than build a bucket whose shape mixes two generations.
func (f *flowControl) generation() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gen
}

// nodeBucket returns the node's bucket under generation gen, creating it on
// first use. gen mismatch (the config was re-set concurrently) answers nil.
func (f *flowControl) nodeBucket(cfg FlowConfig, gen uint64) *slotBucket {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.gen != gen {
		return nil
	}
	if f.bkt == nil {
		f.bkt = &slotBucket{capacity: cfg.Tokens, period: int64(cfg.Period)}
	}
	return f.bkt
}

// takeFor attempts one token from the NODE's bucket for a write at `slot`,
// answering (allowed, bucket). The caller refunds the bucket when a taken
// token's record ends up not landing. A nil bucket means flow control does
// not apply: the config is unlimited, or the generation rotated mid-call.
// A refusal increments the refusing slot's own counter: one budget, per-
// slot attribution.
func (e *Engine) takeFor(slot int32) (bool, *slotBucket) {
	gen := e.flow.generation()
	cfg := e.flow.config()
	if cfg.Unlimited() {
		return true, nil
	}
	b := e.flow.nodeBucket(cfg, gen)
	if b == nil {
		return true, nil
	}
	if b.take(time.Now().UnixNano()) {
		return true, b
	}
	e.flow.hitCounter(slot).Add(1)
	return false, b
}

// flowRefund returns a token taken for a record that did not land.
func flowRefund(b *slotBucket) {
	if b != nil {
		b.refund()
	}
}

// FlowSlotStats is one slot's cumulative throttle rejection count on this
// node, as the admin endpoint reports it.
type FlowSlotStats struct {
	Slot int32  `json:"slot"`
	Hits uint64 `json:"hits"`
}

// SetFlow installs the node-level flow config. Idempotent; an unlimited
// config drops every bucket, so from the next take writes are unthrottled.
func (e *Engine) SetFlow(cfg FlowConfig) { e.flow.set(cfg) }

// FlowStats answers the admin surface: the node's current config, the
// cumulative node-wide rejection count, and the per-slot counts (only slots
// that ever rejected appear).
func (e *Engine) FlowStats() (cfg FlowConfig, total uint64, slots []FlowSlotStats) {
	cfg = e.flow.config()
	e.flow.hitMu.Lock()
	defer e.flow.hitMu.Unlock()
	for slot, c := range e.flow.hitCounters {
		if h := c.Load(); h > 0 {
			slots = append(slots, FlowSlotStats{Slot: slot, Hits: h})
			total += h
		}
	}
	return cfg, total, slots
}

// FlowConfigForAdmin exposes the node's current config to the admin surface.
func (e *Engine) FlowConfigForAdmin() FlowConfig { return e.flow.config() }

// FlowHitsPerSlot snapshots the per-slot hit counters for /admin/writes,
// index-aligned with the other per-slot arrays: slots this node has ever
// throttled report their cumulative count, everything else reads 0. The
// console diffs successive snapshots to see whether flow control fired in
// the last window.
func (e *Engine) FlowHitsPerSlot() []uint64 {
	out := make([]uint64, e.store.SlotCount)
	e.flow.hitMu.Lock()
	for slot, c := range e.flow.hitCounters {
		if slot >= 0 && int(slot) < len(out) {
			out[slot] = c.Load()
		}
	}
	e.flow.hitMu.Unlock()
	return out
}
