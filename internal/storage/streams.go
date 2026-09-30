package storage

import (
	"container/heap"
	"fmt"
	"sort"
)

// StreamVersion is one event stream of a slot: its aggregate id and the
// version of its latest committed record.
type StreamVersion struct {
	AggregateID string `json:"aggregate_id"`
	Version     uint32 `json:"version"`
}

// StreamList is one page of a slot's event streams.
//
// It is answered from the slot's in-memory index, and that index is built when
// the slot is opened, from record FRAME HEADS only — aggregate id + version
// parsed out of the frame header, event bodies neither copied nor decoded (see
// data.DecodeRecordMeta, the same walk the follower's frame landing uses).
// Serving a page therefore reads no WAL file at all.
//
// Loaded=false is the honest answer for a slot this node has not opened: the
// index does not exist here, and building it would mean walking every segment
// of the slot — the whole-file scan this listing exists to avoid. Callers ask
// a node that holds the slot instead (it is placed on its leader and replicas).
type StreamList struct {
	Slot      int32           `json:"slot"`
	Loaded    bool            `json:"loaded"`
	Total     int             `json:"total"`
	Streams   []StreamVersion `json:"streams"`
	NextAfter string          `json:"next_after"`
}

// StreamCount returns how many event streams (aggregates) the slot holds.
func (s *Slot) StreamCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.aggVersions)
}

// SlotGauges returns per-slot console gauges indexed by slot id: bytes of WAL
// on disk and the number of event streams.
//
// Both come straight out of the loaded slot (segment sizes plus the index
// length), and a slot this node has not loaded reads as zero rather than being
// opened: opening walks every segment of the slot, and this poll runs every
// couple of seconds from every console.
func (st *Store) SlotGauges() (bytes []int64, streams []int) {
	n := int(st.SlotCount)
	bytes = make([]int64, n)
	streams = make([]int, n)
	for i := 0; i < n; i++ {
		sl := st.slots[i].Load()
		if sl == nil {
			continue
		}
		bytes[i] = sl.TotalSize()
		streams[i] = sl.StreamCount()
	}
	return bytes, streams
}

const (
	// DefaultStreamPage is the page size when the caller does not ask.
	DefaultStreamPage = 200
	// MaxStreamPage bounds one response: a page is a console view, and the
	// selection cost is O(aggregates in the slot) no matter how big the page
	// is, so a runaway limit would only add serialisation work.
	MaxStreamPage = 1000
)

// StreamPage lists one page of a slot's event streams, ordered by aggregate id,
// starting after the `after` cursor ("" starts at the beginning). Slots this
// node has not opened come back with Loaded=false and no page.
func (st *Store) StreamPage(slotID int32, after string, limit int) (StreamList, error) {
	if slotID < 0 || slotID >= st.SlotCount {
		return StreamList{}, fmt.Errorf("slot %d out of range [0,%d)", slotID, st.SlotCount)
	}
	if limit <= 0 || limit > MaxStreamPage {
		limit = DefaultStreamPage
	}
	out := StreamList{Slot: slotID, Streams: []StreamVersion{}}

	// Deliberately NOT st.Slot(slotID): that opens a cold slot, which loads
	// every segment and walks its frames. A listing must not cause file I/O.
	s := st.slots[slotID].Load()
	if s == nil {
		return out, nil
	}
	out.Loaded = true
	s.streamPage(after, limit, &out)
	return out, nil
}

// streamPage fills one ordered page out of the index.
//
// The index has no order of its own, so every aggregate is examined, but only
// the page is materialised: a bounded max-heap keeps the `limit` smallest ids
// after the cursor, so memory is O(limit) regardless of how many aggregates the
// slot holds (a slot can hold hundreds of thousands), and the cursor lets the
// console walk the whole slot in pages.
func (s *Slot) streamPage(after string, limit int, out *StreamList) {
	s.mu.RLock()
	out.Total = len(s.aggVersions)
	h := make(streamHeap, 0, limit)
	more := false
	for id, v := range s.aggVersions {
		if id <= after {
			continue
		}
		if len(h) < limit {
			h = append(h, StreamVersion{AggregateID: id, Version: v})
			if len(h) == limit {
				heap.Init(&h)
			}
			continue
		}
		// Full page: keep the smallest ids, remember that the page is cut.
		if id < h[0].AggregateID {
			h[0] = StreamVersion{AggregateID: id, Version: v}
			heap.Fix(&h, 0)
		}
		more = true
	}
	s.mu.RUnlock()

	// Pop yields the largest first; an id-ordered page is what the cursor
	// ("give me what is after this id") needs to walk the slot without gaps.
	// Ordering is unconditional: a slot with fewer streams than one page never
	// fills (or builds) the heap, and its map order is arbitrary.
	page := make([]StreamVersion, 0, len(h))
	for h.Len() > 0 {
		page = append(page, heap.Pop(&h).(StreamVersion))
	}
	sort.Slice(page, func(i, j int) bool { return page[i].AggregateID < page[j].AggregateID })
	out.Streams = page
	if more && len(page) > 0 {
		out.NextAfter = page[len(page)-1].AggregateID
	}
}

// streamHeap is a max-heap by aggregate id, used to bound one page.
type streamHeap []StreamVersion

func (h streamHeap) Len() int           { return len(h) }
func (h streamHeap) Less(i, j int) bool { return h[i].AggregateID > h[j].AggregateID }
func (h streamHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *streamHeap) Push(x any)        { *h = append(*h, x.(StreamVersion)) }
func (h *streamHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}
