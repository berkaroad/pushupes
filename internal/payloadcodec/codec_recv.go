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
	"errors"

	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"

	pushupesv1 "pushupes/internal/grpcapi/pushupes/v1"
	"pushupes/internal/lease"
)

// errUnexpectedWireType aborts a fast parse so the caller can fall back to the
// stock codec: a message that does not look the way we expect (a field with
// another wire type, a truncated value) must never be guessed at.
var errUnexpectedWireType = errors.New("pushupes codec: unexpected wire type")

// The decoders below alias the large bytes fields (event bodies, WAL payloads)
// straight into the receive buffer instead of copying them, and keep only the
// lease alive: see holdLease/ReleaseAlias in codec.go. Small fields are copied
// the way protobuf does, and unknown fields are skipped rather than preserved —
// these messages are only ever decoded by us, never re-encoded, so there is
// nothing for a preserved unknown field to survive into.

func unmarshalAppendRequest(data mem.BufferSlice, m *pushupesv1.AppendRequest) error {
	if data.Len() < aliasMinBytes {
		return unmarshalStock(data, m) // small: copying beats aliasing
	}
	raw, hold, err := adopt(data)
	if err != nil {
		return err
	}
	if err := decodeAppendRequest(raw, m); err != nil {
		hold.Free()
		return unmarshalStock(data, m)
	}
	lease.Hold(m, hold)
	return nil
}

func decodeAppendRequest(b []byte, m *pushupesv1.AppendRequest) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		switch num {
		case 1:
			v, n, err := consumeString(b, typ)
			if err != nil {
				return err
			}
			m.AggregateId, b = v, b[n:]
		case 2:
			v, n, err := consumeVarintField(b, typ)
			if err != nil {
				return err
			}
			m.Version, b = uint32(v), b[n:]
		case 3:
			v, n, err := consumeVarintField(b, typ)
			if err != nil {
				return err
			}
			m.UnixTime, b = int64(v), b[n:]
		case 4:
			v, n, err := consumeString(b, typ)
			if err != nil {
				return err
			}
			m.CommandId, b = v, b[n:]
		case 5:
			ev, n, err := consumeEvent(b, typ)
			if err != nil {
				return err
			}
			m.Events, b = append(m.Events, ev), b[n:]
		case 6:
			// reserved field number: skip it like any unknown one
			n = protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
		default:
			n = protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	return nil
}

func unmarshalMFetchResponse(data mem.BufferSlice, m *pushupesv1.MFetchResponse) error {
	if data.Len() < aliasMinBytes {
		return unmarshalStock(data, m) // small: copying beats aliasing
	}
	raw, hold, err := adopt(data)
	if err != nil {
		return err
	}
	if err := decodeMFetchResponse(raw, m); err != nil {
		hold.Free()
		return unmarshalStock(data, m)
	}
	lease.Hold(m, hold)
	return nil
}

func decodeMFetchResponse(b []byte, m *pushupesv1.MFetchResponse) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		switch num {
		case mfetchFollowerField:
			v, n, err := consumeString(b, typ)
			if err != nil {
				return err
			}
			m.Follower, b = v, b[n:]
		case mfetchItemsField:
			it, n, err := consumeFetchItem(b, typ)
			if err != nil {
				return err
			}
			m.Items, b = append(m.Items, it), b[n:]
		default:
			n = protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	return nil
}

func unmarshalReplicateRequest(data mem.BufferSlice, m *pushupesv1.ReplicateRequest) error {
	if data.Len() < aliasMinBytes {
		return unmarshalStock(data, m) // small: copying beats aliasing
	}
	raw, hold, err := adopt(data)
	if err != nil {
		return err
	}
	if err := decodeReplicateRequest(raw, m); err != nil {
		hold.Free()
		return unmarshalStock(data, m)
	}
	lease.Hold(m, hold)
	return nil
}

func decodeReplicateRequest(b []byte, m *pushupesv1.ReplicateRequest) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		switch num {
		case replicateSlotField:
			v, n, err := consumeVarintField(b, typ)
			if err != nil {
				return err
			}
			m.Slot, b = int32(v), b[n:]
		case replicateSeqField:
			v, n, err := consumeVarintField(b, typ)
			if err != nil {
				return err
			}
			m.Seq, b = v, b[n:]
		case replicatePayloadField:
			v, n, err := consumeBytes(b, typ)
			if err != nil {
				return err
			}
			m.Payload, b = v, b[n:]
		default:
			n = protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	return nil
}

func unmarshalPushSegmentsRequest(data mem.BufferSlice, m *pushupesv1.PushSegmentsRequest) error {
	if data.Len() < aliasMinBytes {
		return unmarshalStock(data, m) // small: copying beats aliasing
	}
	raw, hold, err := adopt(data)
	if err != nil {
		return err
	}
	if err := decodePushSegmentsRequest(raw, m); err != nil {
		hold.Free()
		return unmarshalStock(data, m)
	}
	lease.Hold(m, hold)
	return nil
}

func decodePushSegmentsRequest(b []byte, m *pushupesv1.PushSegmentsRequest) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		switch num {
		case segmentsSlotField:
			v, n, err := consumeVarintField(b, typ)
			if err != nil {
				return err
			}
			m.Slot, b = int32(v), b[n:]
		case segmentsNameField:
			v, n, err := consumeString(b, typ)
			if err != nil {
				return err
			}
			m.Name, b = v, b[n:]
		case segmentsSizeField:
			v, n, err := consumeVarintField(b, typ)
			if err != nil {
				return err
			}
			m.Size, b = v, b[n:]
		case segmentsDataField:
			v, n, err := consumeBytes(b, typ)
			if err != nil {
				return err
			}
			m.Data, b = v, b[n:]
		default:
			n = protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	return nil
}

// ---- field helpers ---------------------------------------------------------

func consumeVarintField(b []byte, typ protowire.Type) (uint64, int, error) {
	if typ != protowire.VarintType {
		return 0, 0, errUnexpectedWireType
	}
	v, n := protowire.ConsumeVarint(b)
	if n < 0 {
		return 0, 0, protowire.ParseError(n)
	}
	return v, n, nil
}

func consumeString(b []byte, typ protowire.Type) (string, int, error) {
	if typ != protowire.BytesType {
		return "", 0, errUnexpectedWireType
	}
	v, n := protowire.ConsumeString(b)
	if n < 0 {
		return "", 0, protowire.ParseError(n)
	}
	return v, n, nil
}

// consumeBytes returns a slice aliasing b (no copy).
func consumeBytes(b []byte, typ protowire.Type) ([]byte, int, error) {
	if typ != protowire.BytesType {
		return nil, 0, errUnexpectedWireType
	}
	v, n := protowire.ConsumeBytes(b)
	if n < 0 {
		return nil, 0, protowire.ParseError(n)
	}
	return v, n, nil
}

func consumeEvent(b []byte, typ protowire.Type) (*pushupesv1.Event, int, error) {
	body, n, err := consumeBytes(b, typ)
	if err != nil {
		return nil, 0, err
	}
	ev := &pushupesv1.Event{}
	for len(body) > 0 {
		num, t, m := protowire.ConsumeTag(body)
		if m < 0 {
			return nil, 0, protowire.ParseError(m)
		}
		body = body[m:]
		switch num {
		case eventTypeField:
			v, m, err := consumeString(body, t)
			if err != nil {
				return nil, 0, err
			}
			ev.Type, body = v, body[m:]
		case eventBodyField:
			v, m, err := consumeBytes(body, t)
			if err != nil {
				return nil, 0, err
			}
			ev.Body, body = v, body[m:] // aliases the receive buffer
		default:
			m = protowire.ConsumeFieldValue(num, t, body)
			if m < 0 {
				return nil, 0, protowire.ParseError(m)
			}
			body = body[m:]
		}
	}
	return ev, n, nil
}

func consumeFetchItem(b []byte, typ protowire.Type) (*pushupesv1.FetchItem, int, error) {
	body, n, err := consumeBytes(b, typ)
	if err != nil {
		return nil, 0, err
	}
	it := &pushupesv1.FetchItem{}
	for len(body) > 0 {
		num, t, m := protowire.ConsumeTag(body)
		if m < 0 {
			return nil, 0, protowire.ParseError(m)
		}
		body = body[m:]
		switch num {
		case fetchItemSlotField:
			v, m, err := consumeVarintField(body, t)
			if err != nil {
				return nil, 0, err
			}
			it.Slot, body = int32(v), body[m:]
		case fetchItemFromSeqField:
			v, m, err := consumeVarintField(body, t)
			if err != nil {
				return nil, 0, err
			}
			it.FromSeq, body = v, body[m:]
		case fetchItemNextSeqField:
			v, m, err := consumeVarintField(body, t)
			if err != nil {
				return nil, 0, err
			}
			it.NextSeq, body = v, body[m:]
		case fetchItemPayloadField:
			v, m, err := consumeBytes(body, t)
			if err != nil {
				return nil, 0, err
			}
			it.Payload, body = v, body[m:] // aliases the receive buffer
		default:
			m = protowire.ConsumeFieldValue(num, t, body)
			if m < 0 {
				return nil, 0, protowire.ParseError(m)
			}
			body = body[m:]
		}
	}
	return it, n, nil
}
