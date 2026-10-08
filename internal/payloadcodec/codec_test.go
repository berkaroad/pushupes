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
	"bytes"
	"testing"

	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	pushupesv1 "github.com/berkaroad/pushupes/internal/grpcapi/pushupes/v1"
	"github.com/berkaroad/pushupes/internal/lease"
)

// ---- helpers ---------------------------------------------------------------

func stockMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("stock marshal: %v", err)
	}
	return b
}

func joinParts(parts mem.BufferSlice) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p.ReadOnlyData()...)
	}
	return out
}

func ourMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	parts, err := (payloadCodec{}).Marshal(m)
	if err != nil {
		t.Fatalf("codec marshal: %v", err)
	}
	return joinParts(parts)
}

// stockUnmarshal decodes bytes the way a client without our codec does, so the
// tests can prove our encoders produce ordinary protobuf.
func stockUnmarshal(t *testing.T, raw []byte, m proto.Message) error {
	t.Helper()
	return proto.Unmarshal(raw, m)
}

// stripUnknown clears preserved unknown fields: the stock decoder keeps them,
// the aliasing decoder skips them (these messages are only ever decoded, never
// re-encoded, so nothing would carry them anywhere).
func stripUnknown(m proto.Message) proto.Message {
	c := proto.Clone(m)
	c.ProtoReflect().SetUnknown(nil)
	return c
}

func bigBody(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i * 7)
	}
	return b
}

// ---- cases -----------------------------------------------------------------

// everyType lists one fully populated message per whitelisted type: all fields
// set, bodies on both sides of the carry threshold, nested records/events.
func everyType(t *testing.T) map[string]proto.Message {
	t.Helper()
	ev := func(typ string, n int) *pushupesv1.Event {
		return &pushupesv1.Event{Type: typ, Body: bigBody(n)}
	}
	rec := func(agg string, seq uint64) *pushupesv1.Record {
		return &pushupesv1.Record{
			AggregateId: agg, Version: 3, UnixTime: 1750000000123456789, CommandId: "cmd-" + agg,
			Events: []*pushupesv1.Event{ev("small", 16), ev("big", 4<<10)}, Seq: seq,
		}
	}
	return map[string]proto.Message{
		"AppendRequest": &pushupesv1.AppendRequest{
			AggregateId: "agg-1", Version: 7, UnixTime: -2, CommandId: "cmd-1",
			Events: []*pushupesv1.Event{ev("a", 0), ev("b", 16), ev("c", 100<<10)},
		},
		"MFetchResponse": &pushupesv1.MFetchResponse{
			Follower: "node-2",
			Items: []*pushupesv1.FetchItem{
				{Slot: 7, FromSeq: 1, NextSeq: 9, Payload: bigBody(100 << 10)},
				{Slot: 0, Payload: nil},
				{Slot: 4095, FromSeq: 3, NextSeq: 3, Payload: bigBody(600)},
			},
		},
		"ReadStreamResponse": &pushupesv1.ReadStreamResponse{
			Records:     []*pushupesv1.Record{rec("agg-1", 1), rec("agg-2", 9)},
			NextVersion: 4, LastVersion: 9,
		},
		"ReadByCommandResponse": &pushupesv1.ReadByCommandResponse{Found: true, Record: rec("agg-3", 11)},
		"ReplicateRequest":      &pushupesv1.ReplicateRequest{Slot: 12, Seq: 34, Payload: bigBody(4 << 10)},
		// all bodies below the carry threshold: the stock marshaller's row
		"ReadStreamResponse small": &pushupesv1.ReadStreamResponse{
			Records: []*pushupesv1.Record{{
				AggregateId: "agg-s", Version: 1, CommandId: "cmd-s", Seq: 5,
				Events: []*pushupesv1.Event{ev("t", 8), ev("u", 128)},
			}},
			NextVersion: 2, LastVersion: 5,
		},
		"PushSegmentsRequest": &pushupesv1.PushSegmentsRequest{Slot: 5, Name: "0000000000000001.wal", Size: 1234, Data: bigBody(100 << 10)},
	}
}

// The encoders must produce exactly what the stock marshaller produces: same
// bytes, in the same field order.
func TestMarshalMatchesStock(t *testing.T) {
	for name, msg := range everyType(t) {
		want := stockMarshal(t, msg)
		got := ourMarshal(t, msg)
		if !bytes.Equal(got, want) {
			t.Errorf("%s: our encoding differs from protobuf (%d vs %d bytes)", name, len(got), len(want))
		}
	}
}

// Both directions must round-trip: ours reads stock bytes and stock reads ours.
func TestRoundTripThroughStockCodec(t *testing.T) {
	for name, msg := range everyType(t) {
		raw := stockMarshal(t, msg)

		switch m := msg.(type) {
		case *pushupesv1.AppendRequest:
			var got pushupesv1.AppendRequest
			if err := (payloadCodec{}).Unmarshal(mem.BufferSlice{mem.SliceBuffer(raw)}, &got); err != nil {
				t.Fatalf("%s: decode: %v", name, err)
			}
			defer lease.Release(&got)
			if !proto.Equal(&got, m) {
				t.Errorf("%s: decoded message differs", name)
			}
		case *pushupesv1.MFetchResponse:
			var got pushupesv1.MFetchResponse
			if err := (payloadCodec{}).Unmarshal(mem.BufferSlice{mem.SliceBuffer(raw)}, &got); err != nil {
				t.Fatalf("%s: decode: %v", name, err)
			}
			defer lease.Release(&got)
			if !proto.Equal(&got, m) {
				t.Errorf("%s: decoded message differs", name)
			}
		case *pushupesv1.ReplicateRequest:
			var got pushupesv1.ReplicateRequest
			if err := (payloadCodec{}).Unmarshal(mem.BufferSlice{mem.SliceBuffer(raw)}, &got); err != nil {
				t.Fatalf("%s: decode: %v", name, err)
			}
			defer lease.Release(&got)
			if !proto.Equal(&got, m) {
				t.Errorf("%s: decoded message differs", name)
			}
		case *pushupesv1.PushSegmentsRequest:
			var got pushupesv1.PushSegmentsRequest
			if err := (payloadCodec{}).Unmarshal(mem.BufferSlice{mem.SliceBuffer(raw)}, &got); err != nil {
				t.Fatalf("%s: decode: %v", name, err)
			}
			defer lease.Release(&got)
			if !proto.Equal(&got, m) {
				t.Errorf("%s: decoded message differs", name)
			}
		default:
			// send-only message: what a client sees is what stock protobuf parses
			fresh := proto.Clone(msg)
			proto.Reset(fresh)
			if err := stockUnmarshal(t, raw, fresh); err != nil {
				t.Fatalf("%s: stock decode of our bytes: %v", name, err)
			}
			if !proto.Equal(fresh, msg) {
				t.Errorf("%s: stock decode of our bytes differs", name)
			}
		}
	}
}

// The point of the receive path: bodies must point into the receive buffer, not
// into a copy of it.
func TestUnmarshalAliasesBodies(t *testing.T) {
	body := bigBody(8 << 10)
	req := &pushupesv1.AppendRequest{
		AggregateId: "agg", Version: 1, CommandId: "cmd",
		Events: []*pushupesv1.Event{{Type: "e", Body: body}},
	}
	raw := stockMarshal(t, req)
	var got pushupesv1.AppendRequest
	if err := (payloadCodec{}).Unmarshal(mem.BufferSlice{mem.SliceBuffer(raw)}, &got); err != nil {
		t.Fatal(err)
	}
	defer lease.Release(&got)

	off := bytes.Index(raw, body)
	if off < 0 {
		t.Fatal("body not found in the encoding")
	}
	copy(raw[off:], bytes.Repeat([]byte{0x5a}, len(body)))
	if !bytes.Equal(got.Events[0].Body, bytes.Repeat([]byte{0x5a}, len(body))) {
		t.Fatal("event body was copied instead of aliased")
	}
}

// A message small enough that copying is cheaper than aliasing must not hold a
// lease: the body is copied out and the caller has nothing to release.
func TestUnmarshalCopiesSmallMessages(t *testing.T) {
	body := bigBody(64)
	req := &pushupesv1.AppendRequest{
		AggregateId: "agg", Version: 1, CommandId: "cmd",
		Events: []*pushupesv1.Event{{Type: "e", Body: body}},
	}
	raw := stockMarshal(t, req)
	var got pushupesv1.AppendRequest
	if err := (payloadCodec{}).Unmarshal(mem.BufferSlice{mem.SliceBuffer(raw)}, &got); err != nil {
		t.Fatal(err)
	}
	if lease.Release(&got) {
		t.Fatal("a small message must not hold a lease")
	}
	copy(raw[bytes.Index(raw, body):], bytes.Repeat([]byte{0x5a}, len(body)))
	if bytes.Equal(got.Events[0].Body, bytes.Repeat([]byte{0x5a}, len(body))) {
		t.Fatal("small body was aliased instead of copied")
	}
	if !proto.Equal(&got, req) {
		t.Fatal("small message decoded differently")
	}
}

// A message that arrives split across several frame buffers is coalesced once
// (not per field) and still decodes correctly.
func TestUnmarshalMultiFragment(t *testing.T) {
	req := &pushupesv1.AppendRequest{
		AggregateId: "agg", Version: 2, CommandId: "cmd",
		Events: []*pushupesv1.Event{{Type: "e", Body: bigBody(8 << 10)}},
	}
	raw := stockMarshal(t, req)
	cut := len(raw) / 3
	data := mem.BufferSlice{mem.SliceBuffer(raw[:cut]), mem.SliceBuffer(raw[cut:])}
	var got pushupesv1.AppendRequest
	if err := (payloadCodec{}).Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(&got, req) {
		lease.Release(&got)
		t.Fatal("multi-fragment decode differs")
	}
	if !lease.Release(&got) {
		t.Fatal("multi-fragment decode held no lease")
	}
	if lease.Outstanding() != 0 {
		t.Fatalf("leases outstanding after release: %d", lease.Outstanding())
	}
}

// Whatever the stock decoder does with a message, ours must agree: same result
// or the same failure. That covers unknown fields, wrong wire types (which fall
// back), and truncation.
func TestUnmarshalAgreesWithStock(t *testing.T) {
	base := &pushupesv1.ReplicateRequest{Slot: 3, Seq: 4, Payload: bigBody(700)}
	raw := stockMarshal(t, base)

	cases := map[string][]byte{
		"as-is": raw,
		"unknown field": protowire.AppendVarint(
			protowire.AppendTag(append([]byte{}, raw...), 99, protowire.VarintType), 7),
		"wrong wire type": protowire.AppendString(
			protowire.AppendTag([]byte{}, 2, protowire.BytesType), "not-a-varint"),
		"truncated": raw[:len(raw)-5],
		"empty":     {},
		"only tags": []byte{0x08},
	}
	for name, in := range cases {
		var stock pushupesv1.ReplicateRequest
		stockErr := stockUnmarshal(t, in, &stock)

		var ours pushupesv1.ReplicateRequest
		ourErr := (payloadCodec{}).Unmarshal(mem.BufferSlice{mem.SliceBuffer(append([]byte{}, in...))}, &ours)
		lease.Release(&ours)

		if (stockErr == nil) != (ourErr == nil) {
			t.Errorf("%s: stock err=%v, ours err=%v", name, stockErr, ourErr)
			continue
		}
		if stockErr == nil && !proto.Equal(stripUnknown(&stock), stripUnknown(&ours)) {
			t.Errorf("%s: decoded differently: stock %v ours %v", name, &stock, &ours)
		}
	}
}

// A lease must be held while the caller uses the message and dropped on
// release; an empty message holds none.
func TestLeaseAccounting(t *testing.T) {
	before := lease.Outstanding()

	req := &pushupesv1.PushSegmentsRequest{Slot: 1, Name: "seg", Data: bigBody(8 << 10)}
	raw := stockMarshal(t, req)
	var got pushupesv1.PushSegmentsRequest
	if err := (payloadCodec{}).Unmarshal(mem.BufferSlice{mem.SliceBuffer(raw)}, &got); err != nil {
		t.Fatal(err)
	}
	if lease.Outstanding() != before+1 {
		t.Fatalf("lease not held: %d outstanding", lease.Outstanding())
	}
	if !lease.Release(&got) {
		t.Fatal("release reported no lease")
	}
	if lease.Outstanding() != before {
		t.Fatalf("lease not released: %d outstanding", lease.Outstanding())
	}
	if lease.Release(&got) {
		t.Fatal("double release must be a no-op")
	}

	var empty pushupesv1.PushSegmentsRequest
	if err := (payloadCodec{}).Unmarshal(mem.BufferSlice{}, &empty); err != nil {
		t.Fatalf("empty decode: %v", err)
	}
	if lease.Outstanding() != before {
		t.Fatal("empty message must not hold a lease")
	}
}

// Non-whitelisted messages keep working through the stock fallback.
func TestStockFallback(t *testing.T) {
	msg := &pushupesv1.PingResponse{Node: "node-1"}
	raw := stockMarshal(t, msg)
	if got := ourMarshal(t, msg); !bytes.Equal(got, raw) {
		t.Fatal("fallback marshal differs from stock")
	}
	var got pushupesv1.PingResponse
	if err := (payloadCodec{}).Unmarshal(mem.BufferSlice{mem.SliceBuffer(raw)}, &got); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(&got, msg) {
		t.Fatal("fallback unmarshal differs")
	}
}

func TestCodecNameIsProto(t *testing.T) {
	if name := (payloadCodec{}).Name(); name != "proto" {
		t.Fatalf("codec name %q: clients would be stranded on another content-subtype", name)
	}
}
