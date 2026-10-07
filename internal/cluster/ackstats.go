package cluster

// Ack-chain self-view: where an acknowledged append spent its time.
//
// An append is acknowledged only after the slot's high watermark covers the
// record, so the call's latency is the sum of four things: taking the migration
// write fence and deciding the route, landing the record in the local WAL,
// waiting for the replicas' positions to move that watermark, and the rest
// (response assembly, the caller's own scheduling). "The cluster is slow" is
// only actionable once those are separate numbers, so each append/batch feeds
// them in here and GET /admin/stats.ack reports them, with ?worst=N naming the
// slots whose watermark wait is worst.
//
// Cost per call: four time.Now() and a handful of atomic increments; the
// histograms are fixed buckets over cumulative counters, so a snapshot never
// needs a lock and never allocates on the append path.

import (
	"errors"
	"sort"
	"sync/atomic"
	"time"

	"pushupes/internal/data"
)

// ackBucketUpperMS bounds the millisecond histogram buckets (upper bound,
// inclusive); the remaining bucket collects everything above the last bound.
var ackBucketUpperMS = [...]float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000}

// hpBucketUpper bounds the high-water-mark lag histogram (in records): the
// question it answers is "how many records behind was the watermark when this
// append got its ack", so its scale is records, not time.
var hpBucketUpper = [...]float64{0, 1, 2, 5, 10, 20, 50, 100, 500, 1000, 5000, 20000, 100000}

// ackHist is a fixed-bucket millisecond histogram over atomic counters.
type ackHist struct {
	buckets [len(ackBucketUpperMS) + 1]atomic.Uint64
	count   atomic.Uint64
	sumUS   atomic.Uint64 // microseconds: sub-millisecond steps must not average to 0
	maxUS   atomic.Uint64
}

func (h *ackHist) observe(d time.Duration) {
	ms := d.Seconds() * 1000
	us := uint64(d.Microseconds())
	h.buckets[bucketOf(ackBucketUpperMS[:], ms)].Add(1)
	h.count.Add(1)
	h.sumUS.Add(us)
	bumpMax(&h.maxUS, us)
}

func (h *ackHist) view() AckTiming {
	return AckTiming{
		P50MS: pickPercentile(h.buckets[:], ackBucketUpperMS[:], h.count.Load(), 0.50),
		P90MS: pickPercentile(h.buckets[:], ackBucketUpperMS[:], h.count.Load(), 0.90),
		P99MS: pickPercentile(h.buckets[:], ackBucketUpperMS[:], h.count.Load(), 0.99),
		AvgMS: avgOf(h.count.Load(), h.sumUS.Load()),
		MaxMS: float64(h.maxUS.Load()) / 1000,
	}
}

// hpHist is ackHist's counterpart for record counts (the watermark lag).
type hpHist struct {
	buckets [len(hpBucketUpper) + 1]atomic.Uint64
	count   atomic.Uint64
	max     atomic.Uint64
}

func (h *hpHist) observe(n uint64) {
	h.buckets[bucketOf(hpBucketUpper[:], float64(n))].Add(1)
	h.count.Add(1)
	bumpMax(&h.max, n)
}

func (h *hpHist) view() AckTiming {
	return AckTiming{
		P50MS: pickPercentile(h.buckets[:], hpBucketUpper[:], h.count.Load(), 0.50),
		P90MS: pickPercentile(h.buckets[:], hpBucketUpper[:], h.count.Load(), 0.90),
		P99MS: pickPercentile(h.buckets[:], hpBucketUpper[:], h.count.Load(), 0.99),
		MaxMS: float64(h.max.Load()),
	}
}

func bucketOf(bounds []float64, v float64) int {
	i := 0
	for i < len(bounds) && v > bounds[i] {
		i++
	}
	return i
}

func pickPercentile(buckets []atomic.Uint64, bounds []float64, n uint64, q float64) float64 {
	if n == 0 {
		return 0
	}
	want := uint64(float64(n)*q + 0.5)
	if want == 0 {
		want = 1
	}
	var acc uint64
	for i := range buckets {
		acc += buckets[i].Load()
		if acc >= want {
			if i >= len(bounds) {
				return bounds[len(bounds)-1]
			}
			return bounds[i]
		}
	}
	return bounds[len(bounds)-1]
}

// avgOf turns a microsecond sum into a millisecond average.
func avgOf(n, sumUS uint64) float64 {
	if n == 0 {
		return 0
	}
	return float64(sumUS) / float64(n) / 1000
}

func bumpMax(slot *atomic.Uint64, v uint64) {
	for {
		cur := slot.Load()
		if v <= cur || slot.CompareAndSwap(cur, v) {
			return
		}
	}
}

// slotAck is one slot's share of the chain. Every field is an atomic so the
// append path never takes a lock to report itself.
type slotAck struct {
	appends     atomic.Uint64
	wait        ackHist
	land        ackHist
	lag         hpHist
	lastStallNS atomic.Int64
}

// ackDiag is the engine-wide ack tally. slots is sized once at construction;
// a slot outside it (a test store with fewer slots) simply is not tracked.
type ackDiag struct {
	appends   atomic.Uint64 // records acknowledged (a batch adds its record count)
	batches   atomic.Uint64 // batch groups seen
	calls     atomic.Uint64 // appends + batch groups: the denominator for `total`
	success   atomic.Uint64
	exists    atomic.Uint64
	fail      atomic.Uint64
	redirects atomic.Uint64

	// fail split by reason: "some appends failed" is only actionable once the
	// count says whether that was the watermark deadline (a replication
	// problem) or a business rule (the client's own request).
	failTimeout atomic.Uint64 // the watermark deadline (1005, record still in the WAL)
	failVersion atomic.Uint64 // version must be +1 (1001)
	failOther   atomic.Uint64 // anything else

	fence ackHist // migration write fence + route decision
	land  ackHist // local WAL landing (business rules + write())
	wait  ackHist // high-watermark wait (replica progress)
	total ackHist // the whole Engine call
	lag   hpHist  // watermark lag in records when the wait ended

	waitersNow atomic.Int64  // calls parked in waitForHW right now
	stalls     atomic.Uint64 // waits that hit the deadline (fail/1005)
	slots      []slotAck
}

// AckTiming is one step's latency distribution, in milliseconds (the lag
// histogram reuses the shape and reports records).
type AckTiming struct {
	P50MS float64 `json:"p50_ms"`
	P90MS float64 `json:"p90_ms"`
	P99MS float64 `json:"p99_ms"`
	AvgMS float64 `json:"avg_ms,omitempty"`
	MaxMS float64 `json:"max_ms"`
}

// AckSlotView is one slot's chain, listed by AckView.Worst.
type AckSlotView struct {
	Slot          int32     `json:"slot"`
	Appends       uint64    `json:"appends"`
	HwWait        AckTiming `json:"hw_wait"`
	WalLand       AckTiming `json:"wal_land"`
	HwLagPeak     uint64    `json:"hw_lag_peak"`
	LastStallAgeS float64   `json:"last_stall_age_s"`
}

// AckView is the whole ack-chain self-view. The four timings partition one
// call: fence_route + wal_land + hw_wait + (the rest) = total.
type AckView struct {
	Node      string `json:"node"`
	Appends   uint64 `json:"appends"`
	Batches   uint64 `json:"batches"`
	Calls     uint64 `json:"calls"`
	Success   uint64 `json:"success"`
	Exists    uint64 `json:"exists"`
	Fail      uint64 `json:"fail"`
	Redirects uint64 `json:"redirects"`

	FailTimeout uint64 `json:"fail_hw_timeout"`
	FailVersion uint64 `json:"fail_version_conflict"`
	FailOther   uint64 `json:"fail_other"`

	FenceRoute AckTiming `json:"fence_route"`
	WalLand    AckTiming `json:"wal_land"`
	HwWait     AckTiming `json:"hw_wait"`
	Total      AckTiming `json:"total"`

	WaitersNow   int64     `json:"waiters_now"`
	HwStalls     uint64    `json:"hw_stalls"`
	HwLagRecords AckTiming `json:"hw_lag_records"`

	Worst []AckSlotView `json:"worst,omitempty"`
}

// AckStats snapshots the ack chain.
func (e *Engine) AckStats() AckView { return e.AckStatsTop(0) }

// AckStatsTop is AckStats with the worstN slots listed by watermark wait p99.
// Safe to call while the node serves: only atomics are read.
func (e *Engine) AckStatsTop(worstN int) AckView {
	d := &e.ack
	v := AckView{
		Node:      e.self,
		Appends:   d.appends.Load(),
		Batches:   d.batches.Load(),
		Calls:     d.calls.Load(),
		Success:   d.success.Load(),
		Exists:    d.exists.Load(),
		Fail:      d.fail.Load(),
		Redirects: d.redirects.Load(),

		FailTimeout: d.failTimeout.Load(),
		FailVersion: d.failVersion.Load(),
		FailOther:   d.failOther.Load(),

		FenceRoute: d.fence.view(),
		WalLand:    d.land.view(),
		HwWait:     d.wait.view(),
		Total:      d.total.view(),

		WaitersNow:   d.waitersNow.Load(),
		HwStalls:     d.stalls.Load(),
		HwLagRecords: d.lag.view(),
	}

	if worstN <= 0 {
		return v
	}
	now := time.Now()
	var slots []AckSlotView
	for i := range d.slots {
		s := &d.slots[i]
		if s.appends.Load() == 0 {
			continue
		}
		slots = append(slots, AckSlotView{
			Slot:          int32(i),
			Appends:       s.appends.Load(),
			HwWait:        s.wait.view(),
			WalLand:       s.land.view(),
			HwLagPeak:     uint64(s.lag.view().MaxMS),
			LastStallAgeS: ageSeconds(s.lastStallNS.Load(), now),
		})
	}
	sort.Slice(slots, func(i, j int) bool {
		if slots[i].HwWait.P99MS != slots[j].HwWait.P99MS {
			return slots[i].HwWait.P99MS > slots[j].HwWait.P99MS
		}
		return slots[i].HwLagPeak > slots[j].HwLagPeak
	})
	if len(slots) > worstN {
		slots = slots[:worstN]
	}
	v.Worst = slots
	return v
}

// slotAckOf returns the per-slot tally, or nil for a slot this node's store
// does not have (a test store with a smaller slot count).
func (e *Engine) slotAckOf(slot int32) *slotAck {
	if slot < 0 || int(slot) >= len(e.ack.slots) {
		return nil
	}
	return &e.ack.slots[slot]
}

// noteCall books the first step of a client call: taking the slot's migration
// write fence and deciding the route (start -> fenced), plus the record count
// the call carries (a batch counts all of its records).
func (e *Engine) noteCall(start, fenced time.Time, recs int, batch bool) {
	e.ack.fence.observe(fenced.Sub(start))
	e.ack.calls.Add(1)
	e.ack.appends.Add(uint64(recs))
	if batch {
		e.ack.batches.Add(1)
	}
}

// noteCallEnd books the whole Engine call and whether it was redirected to
// another node (MOVED/ASK/NOT_LEADER: the client's latency, not this node's).
func (e *Engine) noteCallEnd(start time.Time, redirected bool) {
	e.ack.total.observe(time.Since(start))
	if redirected {
		e.ack.redirects.Add(1)
	}
}

// noteOutcome books one response's status. The batch path settles every
// response's final status first (the watermark deadline demotes the records
// above it), so this is called on the settled responses.
func (e *Engine) noteOutcome(resp *data.AppendResponse) {
	switch {
	case resp == nil:
	case resp.Status == data.StatusSuccess:
		e.ack.success.Add(1)
	case resp.Status == data.StatusExists:
		e.ack.exists.Add(1)
	default:
		e.ack.fail.Add(1)
		switch resp.ErrID {
		case data.ErrIDNotLeader:
			e.ack.failTimeout.Add(1)
		case data.ErrIDVersionConflict:
			e.ack.failVersion.Add(1)
		default:
			e.ack.failOther.Add(1)
		}
	}
}

// redirected reports whether err carries a routing verdict for the client.
func redirected(err error) bool {
	var red *RedirectError
	return err != nil && errors.As(err, &red)
}

// hwLagRecords is how far the watermark still trails this node's own log when
// a wait ends: the records a waiting append is still short of its ack.
func (e *Engine) hwLagRecords(slot int32, hw uint64) uint64 {
	if leo := e.store.LastSeqOf(slot); leo > hw {
		return leo - hw
	}
	return 0
}

// noteLand records one record's (or one group's) local landing time.
func (e *Engine) noteLand(slot int32, d time.Duration) {
	e.ack.land.observe(d)
	if s := e.slotAckOf(slot); s != nil {
		s.land.observe(d)
	}
}

// noteWait records one watermark wait: how long it took, how far behind the
// watermark still was when it ended (records), and whether it ran out of time.
func (e *Engine) noteWait(slot int32, d time.Duration, lag uint64, timedOut bool) {
	e.ack.wait.observe(d)
	e.ack.lag.observe(lag)
	s := e.slotAckOf(slot)
	if s == nil {
		return
	}
	s.wait.observe(d)
	s.lag.observe(lag)
	if timedOut {
		s.lastStallNS.Store(time.Now().UnixNano())
	}
}
