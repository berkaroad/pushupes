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

package payloadcodec

import (
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"

	pushupesv1 "github.com/berkaroad/pushupes/internal/grpcapi/pushupes/v1"
)

// carryMinBytes is the size from which a bytes field is emitted as its own
// segment instead of being copied into the scratch buffer, and the size from
// which scattering a message is worth doing at all. Below it the extra segment
// (and its wrapper) costs more than the copy it saves — a message of many small
// bodies is cheaper through the stock marshaller's single pooled buffer.
//
// Measured on the 1KiB-body workload: with a 512-byte threshold every fetched
// payload became its own segment (~70 per response), which lost 4-5% of
// throughput against the stock codec.
const carryMinBytes = 16 << 10

// Field numbers of the whitelisted messages, mirroring
// proto/pushupes/v1/{events,peer}.proto. codec_test.go fails if they drift.
const (
	eventTypeField = 1
	eventBodyField = 2

	recordAggregateIDField = 1
	recordVersionField     = 2
	recordUnixTimeField    = 3
	recordCommandIDField   = 4
	recordEventsField      = 5
	recordSeqField         = 6

	readStreamRecordsField     = 1
	readStreamNextVersionField = 2
	readStreamLastVersionField = 3

	byCommandFoundField  = 1
	byCommandRecordField = 2

	fetchItemSlotField    = 1
	fetchItemFromSeqField = 2
	fetchItemNextSeqField = 3
	fetchItemPayloadField = 4

	mfetchFollowerField = 1
	mfetchItemsField    = 2

	replicateSlotField    = 1
	replicateSeqField     = 2
	replicatePayloadField = 3

	segmentsSlotField = 1
	segmentsNameField = 2
	segmentsSizeField = 3
	segmentsDataField = 4
)

// scatterBuilder lays out one message as an ordered list of segments. Literal
// bytes (tags, varints, ids, small bodies) accumulate in buf; a large body is
// flushed around so that it becomes a segment of its own and never gets copied.
type scatterBuilder struct {
	buf   []byte
	live  int // start of the literal run that is not yet a segment
	parts mem.BufferSlice
}

func newScatterBuilder(capacity int) *scatterBuilder {
	return &scatterBuilder{buf: make([]byte, 0, capacity)}
}

func (b *scatterBuilder) varint(v uint64) { b.buf = protowire.AppendVarint(b.buf, v) }
func (b *scatterBuilder) tag(num protowire.Number, typ protowire.Type) {
	b.buf = protowire.AppendTag(b.buf, num, typ)
}

// str emits a string field, skipping the empty value the way proto3 does.
func (b *scatterBuilder) str(num protowire.Number, s string) {
	if s == "" {
		return
	}
	b.tag(num, protowire.BytesType)
	b.buf = protowire.AppendString(b.buf, s)
}

// uvarint emits a varint field, skipping zero the way proto3 does.
func (b *scatterBuilder) uvarint(num protowire.Number, v uint64) {
	if v == 0 {
		return
	}
	b.tag(num, protowire.VarintType)
	b.buf = protowire.AppendVarint(b.buf, v)
}

func (b *scatterBuilder) bool(num protowire.Number, v bool) {
	if !v {
		return
	}
	b.tag(num, protowire.VarintType)
	b.buf = protowire.AppendVarint(b.buf, 1)
}

// bytes emits a bytes field: small values are copied into the literal run, big
// ones become their own segment.
func (b *scatterBuilder) bytes(num protowire.Number, v []byte) {
	if len(v) == 0 {
		return
	}
	if len(v) < carryMinBytes {
		b.tag(num, protowire.BytesType)
		b.buf = protowire.AppendBytes(b.buf, v)
		return
	}
	b.tag(num, protowire.BytesType)
	b.buf = protowire.AppendVarint(b.buf, uint64(len(v)))
	b.flush()
	b.parts = append(b.parts, mem.SliceBuffer(v))
}

// nested emits a length-delimited submessage. The child is built first so its
// length is known before the header is written; after the child flushes, every
// byte it holds is covered by exactly one segment, so the sum of the segment
// lengths is its encoded size.
func (b *scatterBuilder) nested(num protowire.Number, fn func(*scatterBuilder)) {
	child := newScatterBuilder(64)
	fn(child)
	child.flush()
	total := 0
	for _, p := range child.parts {
		total += p.Len()
	}
	b.tag(num, protowire.BytesType)
	b.buf = protowire.AppendVarint(b.buf, uint64(total))
	b.flush()
	b.parts = append(b.parts, child.parts...)
}

func (b *scatterBuilder) flush() {
	if b.live < len(b.buf) {
		b.parts = append(b.parts, mem.SliceBuffer(b.buf[b.live:]))
		b.live = len(b.buf)
	}
}

func (b *scatterBuilder) finish() mem.BufferSlice {
	b.flush()
	return b.parts
}

// ---- messages --------------------------------------------------------------

func encodeEvent(b *scatterBuilder, ev *pushupesv1.Event) {
	b.str(eventTypeField, ev.Type)
	b.bytes(eventBodyField, ev.Body)
}

func encodeRecord(b *scatterBuilder, r *pushupesv1.Record) {
	b.str(recordAggregateIDField, r.AggregateId)
	b.uvarint(recordVersionField, uint64(r.Version))
	b.uvarint(recordUnixTimeField, uint64(r.UnixTime))
	b.str(recordCommandIDField, r.CommandId)
	for _, ev := range r.Events {
		b.nested(recordEventsField, func(c *scatterBuilder) { encodeEvent(c, ev) })
	}
	b.uvarint(recordSeqField, r.Seq)
}

func encodeFetchItem(b *scatterBuilder, it *pushupesv1.FetchItem) {
	b.uvarint(fetchItemSlotField, uint64(it.Slot))
	b.uvarint(fetchItemFromSeqField, it.FromSeq)
	b.uvarint(fetchItemNextSeqField, it.NextSeq)
	b.bytes(fetchItemPayloadField, it.Payload)
}

// encodeMFetchResponse emits a fetch response with every slot payload referenced
// rather than copied: this is the leader->follower bulk path.
func encodeMFetchResponse(m *pushupesv1.MFetchResponse) mem.BufferSlice {
	b := newScatterBuilder(64 + 64*len(m.Items))
	b.str(mfetchFollowerField, m.Follower)
	for _, it := range m.Items {
		b.nested(mfetchItemsField, func(c *scatterBuilder) { encodeFetchItem(c, it) })
	}
	return b.finish()
}

func encodeReadStreamResponse(m *pushupesv1.ReadStreamResponse) mem.BufferSlice {
	b := newScatterBuilder(64 + 256*len(m.Records))
	for _, rec := range m.Records {
		b.nested(readStreamRecordsField, func(c *scatterBuilder) { encodeRecord(c, rec) })
	}
	b.uvarint(readStreamNextVersionField, uint64(m.NextVersion))
	b.uvarint(readStreamLastVersionField, uint64(m.LastVersion))
	return b.finish()
}

func encodeReadByCommandResponse(m *pushupesv1.ReadByCommandResponse) mem.BufferSlice {
	b := newScatterBuilder(64 + 256)
	b.bool(byCommandFoundField, m.Found)
	if m.Record != nil {
		b.nested(byCommandRecordField, func(c *scatterBuilder) { encodeRecord(c, m.Record) })
	}
	return b.finish()
}

func encodeReplicateRequest(m *pushupesv1.ReplicateRequest) mem.BufferSlice {
	b := newScatterBuilder(64)
	b.uvarint(replicateSlotField, uint64(m.Slot))
	b.uvarint(replicateSeqField, m.Seq)
	b.bytes(replicatePayloadField, m.Payload)
	return b.finish()
}

func encodePushSegmentsRequest(m *pushupesv1.PushSegmentsRequest) mem.BufferSlice {
	b := newScatterBuilder(64)
	b.uvarint(segmentsSlotField, uint64(m.Slot))
	b.str(segmentsNameField, m.Name)
	b.uvarint(segmentsSizeField, m.Size)
	b.bytes(segmentsDataField, m.Data)
	return b.finish()
}

// ---- "is scattering worth it" predicates -----------------------------------
//
// Each mirrors the fields its encoder carries: scattering a message whose
// bodies are all small is a loss, so those go to the stock marshaller instead.

func mfetchHasCarriedBytes(m *pushupesv1.MFetchResponse) bool {
	for _, it := range m.Items {
		if len(it.Payload) >= carryMinBytes {
			return true
		}
	}
	return false
}

func readStreamHasCarriedBytes(m *pushupesv1.ReadStreamResponse) bool {
	for _, r := range m.Records {
		if recordHasCarriedBytes(r) {
			return true
		}
	}
	return false
}

func recordHasCarriedBytes(r *pushupesv1.Record) bool {
	if r == nil {
		return false
	}
	for _, ev := range r.Events {
		if len(ev.Body) >= carryMinBytes {
			return true
		}
	}
	return false
}
