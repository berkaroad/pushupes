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

package client

// Writer-side gzip: the compression half of the record codec lives HERE,
// not in the cluster. The server stamps whatever codec a request declares
// into the frame's record-header flags byte and persists the body bytes
// verbatim — it never inflates, on the write path, on the read path, or in
// follower catch-up (the replication protocol ships the stored frames as
// they are, which is how compression shrinks fetch bandwidth instead of
// taxing it). Readers get Record.compressed back and the same library
// turns it into plaintext before the call returns.
//
// The decision is per record and binary: when compression is configured,
// every event body of a record is gzipped together (the frame flags carry
// one codec per record) or the record stores raw. A record whose combined
// bodies are below the configured floor is left raw — a 1 KiB gzip stream
// of a 2 KiB JSON is noise, and per-body streams of tiny bodies would pay
// the 10-byte gzip header each. A compression that would not pay for
// itself (>= 90% of the original size: already-compact payloads like
// images or protobufs) is discarded and the record stores raw.
//
// Decompression is guarded: a stream that inflates beyond one stored-body
// ceiling (data.MaxBodySize) is refused mid-read. gzip's own CRC validates
// integrity on top of that; the ceiling is what keeps a hostile or buggy
// writer's zip bomb from costing a reader more than a MiB.

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	pushupesv1 "github.com/berkaroad/pushupes/pkg/grpcapi/pushupes/v1"
)

// maxInflatedBody bounds one decompressed event body. It mirrors
// data.MaxBodySize (the ceiling the server enforces on stored bytes): a
// plaintext body that could never have been written raw can never be read
// inflated either. Declared as a local constant because internal/data
// imports this package's error table (a package import would cycle).
const maxInflatedBody = int64(1 << 20) // == data.MaxBodySize

// compressRecord bodies a single request: returns nil when the record is
// not worth compressing (floor, ratio, already stamped, or an encoder
// failure — every fallback stores what the caller gave us).
func (c *Client) compressRecord(req *pushupesv1.AppendRequest) *pushupesv1.AppendRequest {
	if c.cfg.CompressMinTotalBody <= 0 {
		return nil
	}
	if req.GetCompression() != pushupesv1.Compression_COMPRESSION_UNSPECIFIED {
		return nil // caller already prepared the bodies
	}
	var total int
	for _, ev := range req.Events {
		total += len(ev.GetBody())
		if total < 0 {
			return nil // absurd size: not our problem to compress
		}
	}
	if total < c.cfg.CompressMinTotalBody {
		return nil
	}
	level := c.cfg.CompressLevel
	if level == 0 {
		level = gzip.BestSpeed
	}
	encoded := make([][]byte, len(req.Events))
	var enc int
	for i, ev := range req.Events {
		var buf bytes.Buffer
		w, err := gzip.NewWriterLevel(&buf, level)
		if err != nil {
			return nil
		}
		if _, err := w.Write(ev.GetBody()); err != nil {
			return nil
		}
		if err := w.Close(); err != nil {
			return nil
		}
		encoded[i] = buf.Bytes()
		enc += len(encoded[i])
	}
	if enc*10 >= total*9 { // >= 90% of the original: compression does not pay
		return nil
	}
	out := &pushupesv1.AppendRequest{
		AggregateId: req.AggregateId,
		Version:     req.Version,
		UnixTime:    req.UnixTime,
		CommandId:   req.CommandId,
		Compression: pushupesv1.Compression_COMPRESSION_GZIP,
		Events:      make([]*pushupesv1.Event, len(req.Events)),
	}
	for i, ev := range req.Events {
		out.Events[i] = &pushupesv1.Event{Type: ev.GetType(), Body: encoded[i]}
	}
	return out
}

// applyCompression returns the send list for BatchAppend: each eligible
// record replaced by a compressed copy, everything else passed through
// unchanged (caller protos are never mutated).
func (c *Client) applyCompression(recs []*pushupesv1.AppendRequest) []*pushupesv1.AppendRequest {
	if c.cfg.CompressMinTotalBody <= 0 {
		return recs
	}
	changed := false
	out := make([]*pushupesv1.AppendRequest, len(recs))
	for i, r := range recs {
		if cr := c.compressRecord(r); cr != nil {
			out[i] = cr
			changed = true
		} else {
			out[i] = r
		}
	}
	if !changed {
		return recs
	}
	return out
}

// inflateRecord decodes a compressed stored record in place: bodies become
// plaintext and the codec flag clears. A raw record is a no-op — callers
// hand it every record they receive.
func inflateRecord(rec *pushupesv1.Record) error {
	if rec == nil || !rec.GetCompressed() {
		return nil
	}
	for _, ev := range rec.Events {
		if len(ev.GetBody()) < 2 || ev.Body[0] != 0x1f || ev.Body[1] != 0x8b {
			return fmt.Errorf("record %s/%d: gzip codec but body lacks magic", rec.GetAggregateId(), rec.GetVersion())
		}
		r, err := gzip.NewReader(bytes.NewReader(ev.Body))
		if err != nil {
			return fmt.Errorf("record %s/%d: gzip header: %w", rec.GetAggregateId(), rec.GetVersion(), err)
		}
		plain, err := io.ReadAll(io.LimitReader(r, maxInflatedBody+1))
		closeErr := r.Close()
		if err != nil {
			return fmt.Errorf("record %s/%d: inflate: %w", rec.GetAggregateId(), rec.GetVersion(), err)
		}
		if int64(len(plain)) > maxInflatedBody {
			return fmt.Errorf("record %s/%d: inflated body exceeds the %d byte ceiling",
				rec.GetAggregateId(), rec.GetVersion(), maxInflatedBody)
		}
		if closeErr != nil { // gzip validates CRC at Close
			return fmt.Errorf("record %s/%d: gzip checksum: %w", rec.GetAggregateId(), rec.GetVersion(), closeErr)
		}
		ev.Body = plain
	}
	rec.Compressed = false
	return nil
}

// inflateResponses clears the codec on everything BatchAppend answered
// (the exists records carry stored bytes).
func inflateBatchResponses(resps []*pushupesv1.AppendResponse) error {
	for _, r := range resps {
		if r != nil && r.GetStatus() == pushupesv1.AppendResponse_STATUS_EXISTS {
			if err := inflateRecord(r.GetRecord()); err != nil {
				return err
			}
		}
	}
	return nil
}
