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
	"fmt"

	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"

	pushupesv1 "github.com/berkaroad/pushupes/internal/grpcapi/pushupes/v1"
)

// payloadCodec is a gRPC CodecV2 that keeps the large bytes fields of this
// service's messages out of the copy path.
//
// Two directions, both transparent to clients:
//
// Send (scatter): a response whose big fields are event bodies or WAL payloads
// is emitted as a list of segments — the small headers go into one scratch
// buffer, each large body is referenced in place. protobuf's own marshaller
// copies every bytes field into the output buffer; this does not.
//
// Receive (alias): a request carrying a large body (an append's event body, a
// follower's fetched payload, a migration chunk) is parsed with the field
// pointing straight into the receive buffer, which is held by a lease that the
// consumer releases once the bytes have been written.
//
// The wire format is unchanged (plain protobuf, content-subtype "proto"), so
// any client — grpcurl, another language, an older build — keeps working. Only
// the messages listed in the type switches below take the fast path; everything
// else falls back to the stock marshalling.
//
// MAINTENANCE: the encoders and decoders spell out field numbers and wire
// types. Adding a field to a whitelisted message without updating them here
// makes codec_test.go fail (it compares both directions against the stock
// codec on fully populated messages) — fix the codec, not the test.
//
// payloadCodec implements encoding.CodecV2.
type payloadCodec struct{}

// Name must stay "proto": it is the gRPC content-subtype, and clients derive
// application/grpc+proto from it. A different name would strand them.
func (payloadCodec) Name() string { return "proto" }

// InstallCodec registers the payload codec for the whole process, server and
// client side. It must run while the process is initializing (grpc resolves the
// codec through the registry, so later registrations are not seen by servers
// that already started).
func InstallCodec() { encoding.RegisterCodecV2(payloadCodec{}) }

func (payloadCodec) Marshal(v any) (mem.BufferSlice, error) {
	// Scatter only pays when there is a large body to reference: for messages
	// of small fields it would add a segment (and its wrapper) per field where
	// the stock marshaller copies into one pooled buffer, which measured slower
	// on the small-body workload.
	switch m := v.(type) {
	case *pushupesv1.MFetchResponse:
		if mfetchHasCarriedBytes(m) {
			return encodeMFetchResponse(m), nil
		}
	case *pushupesv1.ReadStreamResponse:
		if readStreamHasCarriedBytes(m) {
			return encodeReadStreamResponse(m), nil
		}
	case *pushupesv1.ReadByCommandResponse:
		if recordHasCarriedBytes(m.Record) {
			return encodeReadByCommandResponse(m), nil
		}
	case *pushupesv1.ReplicateRequest:
		if len(m.Payload) >= carryMinBytes {
			return encodeReplicateRequest(m), nil
		}
	case *pushupesv1.PushSegmentsRequest:
		if len(m.Data) >= carryMinBytes {
			return encodePushSegmentsRequest(m), nil
		}
	}
	return marshalStock(v)
}

func (payloadCodec) Unmarshal(data mem.BufferSlice, v any) error {
	switch m := v.(type) {
	case *pushupesv1.AppendRequest:
		return unmarshalAppendRequest(data, m)
	case *pushupesv1.MFetchResponse:
		return unmarshalMFetchResponse(data, m)
	case *pushupesv1.ReplicateRequest:
		return unmarshalReplicateRequest(data, m)
	case *pushupesv1.PushSegmentsRequest:
		return unmarshalPushSegmentsRequest(data, m)
	}
	return unmarshalStock(data, v)
}

// aliasMinBytes is the message size from which decoding aliases the receive
// buffer instead of copying the bytes fields out of it. Below it the copies are
// small and the lease (a held buffer plus its release) costs more than it saves.
const aliasMinBytes = 4 << 10

// Receive-side leases: gRPC frees the receive buffer as soon as the codec
// returns, so a message whose fields alias that buffer holds a lease on it
// (internal/lease). Consumers release it once the bytes are on disk — see the
// Append handler, the Replicate handler and the fetch round.
//
// adopt returns the bytes of data to parse and the buffer that keeps them
// alive. A message that arrived in one piece is used as-is (one Ref, no copy);
// a message split across several frame buffers is coalesced into one pooled
// buffer — still one copy for the whole message instead of one per field.
func adopt(data mem.BufferSlice) ([]byte, mem.Buffer, error) {
	switch len(data) {
	case 0:
		return nil, nil, nil
	case 1:
		b := data[0]
		b.Ref()
		return b.ReadOnlyData(), b, nil
	default:
		buf := data.MaterializeToBuffer(grpcBufPool)
		return buf.ReadOnlyData(), buf, nil
	}
}

// ---- stock marshalling fallback -------------------------------------------

// marshalStock mirrors the built-in proto codec (pooled buffer, cached size).
func marshalStock(v any) (mem.BufferSlice, error) {
	m, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("pushupes codec: %T is not a proto.Message", v)
	}
	size := proto.Size(m)
	if size < 1 {
		return mem.BufferSlice{mem.SliceBuffer(nil)}, nil
	}
	bufp := grpcBufPool.Get(size)
	out, err := proto.MarshalOptions{UseCachedSize: true}.MarshalAppend((*bufp)[:0], m)
	if err != nil {
		grpcBufPool.Put(bufp)
		return nil, err
	}
	*bufp = out
	return mem.BufferSlice{mem.NewBuffer(bufp, grpcBufPool)}, nil
}

// unmarshalStock mirrors the built-in proto codec: materialize, then unmarshal
// into the message (which copies bytes fields, as protobuf always does).
func unmarshalStock(data mem.BufferSlice, v any) error {
	m, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("pushupes codec: %T is not a proto.Message", v)
	}
	if len(data) == 0 {
		return nil
	}
	raw, buf, err := adopt(data)
	if err != nil {
		return err
	}
	defer buf.Free()
	return proto.Unmarshal(raw, m)
}
