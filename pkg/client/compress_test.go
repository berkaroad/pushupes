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

import (
	"bytes"
	"compress/gzip"
	"context"
	"math/rand"
	"strings"
	"testing"

	pushupesv1 "github.com/berkaroad/pushupes/pkg/grpcapi/pushupes/v1"
)

func gz(s string) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Write([]byte(s))
	w.Close()
	return buf.Bytes()
}

func plainBody(n int) []byte {
	return []byte(strings.Repeat("pushupes-event-payload,", n)[:n])
}

func incompressible(n int) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewSource(7))
	r.Read(b)
	return b
}

// With compression configured, a record past the floor goes on the wire as
// gzip with the codec stamped; the caller's proto is not mutated; a record
// below the floor passes through untouched.
func TestBatchAppendCompressesEligibleRecords(t *testing.T) {
	nodes, addrs := fakeCluster(t, 1)
	c, err := New(addrs, &Config{CompressMinTotalBody: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	big := &pushupesv1.AppendRequest{
		AggregateId: "a1", Version: 1, CommandId: "c1",
		Events: []*pushupesv1.Event{{Type: "ev", Body: plainBody(2048)}},
	}
	small := &pushupesv1.AppendRequest{
		AggregateId: "a2", Version: 1, CommandId: "c2",
		Events: []*pushupesv1.Event{{Type: "ev", Body: plainBody(64)}},
	}
	if _, err := c.BatchAppend(context.Background(), []*pushupesv1.AppendRequest{big, small}); err != nil {
		t.Fatal(err)
	}
	calls := nodes[0].calls()
	if len(calls) != 1 || len(calls[0].Records) != 2 {
		t.Fatalf("fake saw %d batch calls", len(calls))
	}
	got := calls[0].Records[0]
	if got.Compression != pushupesv1.Compression_COMPRESSION_GZIP {
		t.Fatalf("eligible record not stamped: %v", got.Compression)
	}
	body := got.Events[0].Body
	if len(body) < 2 || body[0] != 0x1f || body[1] != 0x8b {
		t.Fatal("body must be a gzip stream")
	}
	if len(body) >= len(plainBody(2048)) {
		t.Fatalf("repetitive 2KiB payload must compress: stored %d", len(body))
	}
	if got2 := calls[0].Records[1]; got2.Compression != pushupesv1.Compression_COMPRESSION_UNSPECIFIED ||
		!bytes.Equal(got2.Events[0].Body, small.Events[0].Body) {
		t.Fatal("below-floor record must pass through untouched")
	}
	// The caller's objects were never mutated.
	if big.Events[0].Body[0] == 0x1f && big.Events[0].Body[1] == 0x8b {
		t.Fatal("caller's request proto was mutated")
	}
	if big.Compression != pushupesv1.Compression_COMPRESSION_UNSPECIFIED {
		t.Fatal("caller's stamp was mutated")
	}
}

// Incompressible payloads are not made bigger for nothing: the ratio guard
// stores them raw.
func TestIncompressibleStaysRaw(t *testing.T) {
	nodes, addrs := fakeCluster(t, 1)
	c, _ := New(addrs, &Config{CompressMinTotalBody: 256})
	defer c.Close()
	req := &pushupesv1.AppendRequest{
		AggregateId: "a1", Version: 1, CommandId: "c1",
		Events: []*pushupesv1.Event{{Type: "bin", Body: incompressible(2048)}},
	}
	if _, err := c.BatchAppend(context.Background(), []*pushupesv1.AppendRequest{req}); err != nil {
		t.Fatal(err)
	}
	got := nodes[0].calls()[0].Records[0]
	if got.Compression != pushupesv1.Compression_COMPRESSION_UNSPECIFIED {
		t.Fatal("ratio guard must store raw")
	}
}

// A caller that pre-stamps the codec is authoritative: the library never
// re-compresses (floor, level and ratio are all the caller's by then).
func TestPreStampedRecordPassesThrough(t *testing.T) {
	c := &Client{cfg: Config{CompressMinTotalBody: 1}}
	req := &pushupesv1.AppendRequest{
		AggregateId: "a", Version: 1, CommandId: "c",
		Compression: pushupesv1.Compression_COMPRESSION_GZIP,
		Events:      []*pushupesv1.Event{{Type: "ev", Body: gz("already")}},
	}
	if out := c.applyCompression([]*pushupesv1.AppendRequest{req}); out[0] != req {
		t.Fatal("stamped record must pass through by pointer")
	}
}

// The exists records BatchAppend hands back arrive plaintext again — the
// stored bytes never escape the library's decompression gate.
func TestExistsRecordAutoInflates(t *testing.T) {
	nodes, addrs := fakeCluster(t, 1)
	c, _ := New(addrs, &Config{CompressMinTotalBody: 64})
	defer c.Close()
	plain := string(plainBody(4096))
	nodes[0].onAppend = func(n int, r *pushupesv1.BatchAppendRequest) *pushupesv1.BatchAppendResponse {
		rec := r.Records[0] // already compressed by the library at this point
		return &pushupesv1.BatchAppendResponse{Results: []*pushupesv1.BatchAppendResult{
			{AggregateId: rec.AggregateId, Response: &pushupesv1.AppendResponse{
				Status: pushupesv1.AppendResponse_STATUS_EXISTS,
				Record: &pushupesv1.Record{
					AggregateId: rec.AggregateId, Version: 1, CommandId: rec.CommandId,
					Compressed: true,
					Events:     []*pushupesv1.Event{{Type: "ev", Body: rec.Events[0].Body}},
				},
			}},
		}}
	}
	req := &pushupesv1.AppendRequest{
		AggregateId: "a1", Version: 1, CommandId: "c1",
		Events: []*pushupesv1.Event{{Type: "ev", Body: []byte(plain)}},
	}
	resps, err := c.BatchAppend(context.Background(), []*pushupesv1.AppendRequest{req})
	if err != nil {
		t.Fatal(err)
	}
	rec := resps[0].GetRecord()
	if rec == nil || rec.GetCompressed() {
		t.Fatalf("exists record must come back inflated, got %+v", rec)
	}
	if string(rec.Events[0].Body) != plain {
		t.Fatal("inflated body differs from the original")
	}
}

// Reads answer compressed records and the library inflates them in place.
func TestReadStreamAutoInflates(t *testing.T) {
	nodes, addrs := fakeCluster(t, 1)
	nodes[0].onReadStream = func(n int, r *pushupesv1.ReadStreamRequest) *pushupesv1.ReadStreamResponse {
		return &pushupesv1.ReadStreamResponse{Records: []*pushupesv1.Record{{
			AggregateId: r.AggregateId, Version: 1, Compressed: true,
			Events: []*pushupesv1.Event{{Type: "ev", Body: gz("stored gzip")}},
		}}}
	}
	c, _ := New(addrs, &Config{})
	defer c.Close()
	resp, err := c.ReadStream(context.Background(), "agg-0", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	got := resp.Records[0]
	if got.GetCompressed() || string(got.Events[0].Body) != "stored gzip" {
		t.Fatalf("read record not inflated: %+v", got)
	}
}

// The zip-bomb ceiling: an inflation beyond one stored-body cap is refused
// mid-read, never materialised.
func TestInflateBombRefused(t *testing.T) {
	bomb := gz(strings.Repeat("A", 3<<20)) // 3 MiB plain: above the 1 MiB ceiling
	rec := &pushupesv1.Record{
		AggregateId: "a", Version: 1, Compressed: true,
		Events: []*pushupesv1.Event{{Type: "ev", Body: bomb}},
	}
	if err := inflateRecord(rec); err == nil || !strings.Contains(err.Error(), "ceiling") {
		t.Fatalf("bomb must be refused by the ceiling, got %v", err)
	}
}

// A record claiming the gzip codec without a gzip stream is refused with a
// pointed error, not misdecoded.
func TestInflateRejectsNonGzipBody(t *testing.T) {
	rec := &pushupesv1.Record{
		AggregateId: "a", Version: 1, Compressed: true,
		Events: []*pushupesv1.Event{{Type: "ev", Body: []byte("plain")}},
	}
	if err := inflateRecord(rec); err == nil {
		t.Fatal("fake codec must error")
	}
	// Raw records are a no-op.
	raw := &pushupesv1.Record{Events: []*pushupesv1.Event{{Body: []byte("x")}}}
	if err := inflateRecord(raw); err != nil {
		t.Fatalf("raw inflate: %v", err)
	}
}
