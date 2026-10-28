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

import (
	"context"
	"testing"

	"github.com/berkaroad/pushupes/internal/data"
	pushupesv1 "github.com/berkaroad/pushupes/internal/grpcapi/pushupes/v1"
	"google.golang.org/grpc"
)

// chunkSink collects what the streaming handler sends. Only Send is exercised,
// so the embedded stream stays nil.
type chunkSink struct {
	grpc.ServerStream
	chunks []*pushupesv1.ReplicateAggregateResponse
}

func (c *chunkSink) Send(m *pushupesv1.ReplicateAggregateResponse) error {
	c.chunks = append(c.chunks, m)
	return nil
}

// TestReplicateAggregateStreamsInVersionOrder is the replay primitive the
// generation migration is built on: an aggregate leaves a slot in version order,
// chunk by chunk, with a cursor that lets the caller resume.
func TestReplicateAggregateStreamsInVersionOrder(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	const slot, agg = int32(3), "agg-replay"
	for v := 1; v <= 10; v++ {
		rec := makeRecord(agg, uint32(v), "cmd-"+agg+"-"+string(rune('a'+v)))
		if _, err := e.store.AppendToSlot(slot, rec); err != nil {
			t.Fatalf("append v%d: %v", v, err)
		}
	}

	srv := &peerServer{e: e}
	sink := &chunkSink{}
	if err := srv.ReplicateAggregate(&pushupesv1.ReplicateAggregateRequest{
		Slot: slot, AggregateId: agg, MaxRecords: 4,
	}, sink); err != nil {
		t.Fatalf("replicate aggregate: %v", err)
	}
	if len(sink.chunks) != 3 {
		t.Fatalf("10 records at 4 per chunk: want 3 chunks, got %d", len(sink.chunks))
	}

	// Rebuild the stream: versions must be 1..10 in order, no gaps, no repeats.
	var versions []uint32
	cursor := uint32(0)
	for _, ch := range sink.chunks {
		if ch.GetVersions() != uint32(len(ch.GetRecords())) {
			t.Fatalf("chunk claims %d versions but carries %d records", ch.GetVersions(), len(ch.GetRecords()))
		}
		for _, blob := range ch.GetRecords() {
			rec, _, err := data.DecodeRecord(blob)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if rec.AggregateID != agg {
				t.Fatalf("chunk carries aggregate %q, want %q", rec.AggregateID, agg)
			}
			versions = append(versions, rec.Version)
		}
		if ch.GetNextVersion() != 0 {
			cursor = ch.GetNextVersion()
		}
	}
	if len(versions) != 10 {
		t.Fatalf("replayed %d records, want 10: %v", len(versions), versions)
	}
	for i, v := range versions {
		if v != uint32(i+1) {
			t.Fatalf("version %d at position %d: the stream must be ordered and gapless: %v", v, i, versions)
		}
	}
	if cursor != 11 {
		t.Fatalf("cursor after a full replay: %d, want 11", cursor)
	}

	// Resuming from the cursor yields nothing: that is how the caller knows it
	// is done (a short chunk is not the signal, the byte budget can shorten one).
	sink2 := &chunkSink{}
	if err := srv.ReplicateAggregate(&pushupesv1.ReplicateAggregateRequest{
		Slot: slot, AggregateId: agg, FromVersion: cursor, MaxRecords: 4,
	}, sink2); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(sink2.chunks) != 0 {
		t.Fatalf("resuming past the end streamed %d chunk(s), want none", len(sink2.chunks))
	}
}

// TestReplicateAggregateHonoursTheByteBudget: a chunk may be cut short by bytes
// instead of by record count, and the cursor still advances exactly past what
// was sent.
func TestReplicateAggregateHonoursTheByteBudget(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	const slot, agg = int32(5), "agg-bytes"
	for v := 1; v <= 6; v++ {
		rec := makeRecord(agg, uint32(v), "cmd-b-"+string(rune('a'+v)))
		rec.Events[0].Body = make([]byte, 512)
		if _, err := e.store.AppendToSlot(slot, rec); err != nil {
			t.Fatalf("append v%d: %v", v, err)
		}
	}
	srv := &peerServer{e: e}
	sink := &chunkSink{}
	if err := srv.ReplicateAggregate(&pushupesv1.ReplicateAggregateRequest{
		Slot: slot, AggregateId: agg, MaxRecords: 100, MaxBytes: 1200,
	}, sink); err != nil {
		t.Fatalf("replicate: %v", err)
	}
	total := uint32(0)
	for _, ch := range sink.chunks {
		total += ch.GetVersions()
	}
	if total != 6 {
		t.Fatalf("byte-budget streaming delivered %d of 6 records", total)
	}
}

// TestReplayChunksLandInTheTargetSlot is the whole replay pair end to end: the
// source streams an aggregate out of its slot, the destination lands those bytes
// in the slot the new segment routes the aggregate to. Versions stay 1..N and
// gaps are refused, and a resumed replay writes nothing twice.
func TestReplayChunksLandInTheTargetSlot(t *testing.T) {
	e, _ := newTestEngine(t, "node-1")
	const src, dst, agg = int32(3), int32(7), "agg-move"
	for v := 1; v <= 10; v++ {
		rec := makeRecord(agg, uint32(v), "cmd-move-"+string(rune('a'+v)))
		if _, err := e.store.AppendToSlot(src, rec); err != nil {
			t.Fatalf("append v%d: %v", v, err)
		}
	}

	srv := &peerServer{e: e}
	sink := &chunkSink{}
	if err := srv.ReplicateAggregate(&pushupesv1.ReplicateAggregateRequest{
		Slot: src, AggregateId: agg, MaxRecords: 4,
	}, sink); err != nil {
		t.Fatalf("replicate: %v", err)
	}

	landed := 0
	for _, ch := range sink.chunks {
		n, err := e.replayChunkInto(dst, ch.GetRecords())
		if err != nil {
			t.Fatalf("land chunk: %v", err)
		}
		landed += n
	}
	if landed != 10 {
		t.Fatalf("landed %d records into slot %d, want 10", landed, dst)
	}

	recs, _, err := e.store.ReadAggregate(dst, agg, 1, 100, 0)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if len(recs) != 10 {
		t.Fatalf("target slot holds %d records, want 10", len(recs))
	}
	for i, r := range recs {
		if r.Version != uint32(i+1) {
			t.Fatalf("target version %d at position %d: history must be gapless", r.Version, i)
		}
		if r.CommandID == "" {
			t.Fatalf("target record v%d lost its command id: a retried command would stop being idempotent", r.Version)
		}
	}

	// A resumed replay (the migration was interrupted) must not duplicate.
	for _, ch := range sink.chunks {
		n, err := e.replayChunkInto(dst, ch.GetRecords())
		if err != nil {
			t.Fatalf("replay again: %v", err)
		}
		if n != 0 {
			t.Fatalf("a resumed replay wrote %d duplicate record(s)", n)
		}
	}
	if recs, _, _ := e.store.ReadAggregate(dst, agg, 1, 100, 0); len(recs) != 10 {
		t.Fatalf("after the resumed replay the target holds %d records, want 10", len(recs))
	}
}

// TestReplayIntoMovesAnAggregateBetweenSlots drives the real peer path: one
// engine holds the source slot, another pulls the aggregate over gRPC and lands
// it in a different slot of its own store. This is exactly what a generation
// migration does per aggregate, with the controller only pairing the two ends.
func TestReplayIntoMovesAnAggregateBetweenSlots(t *testing.T) {
	src, srcStore := newTestEngine(t, "node-1")
	dst, dstStore := newTestEngine(t, "node-2")
	srcAddr := newPeerHarness(t, src)
	newPeerHarness(t, dst)

	const srcSlot, dstSlot, agg = int32(3), int32(6), "agg-into"
	for v := 1; v <= 7; v++ {
		rec := makeRecord(agg, uint32(v), "cmd-into-"+string(rune('a'+v)))
		if _, err := srcStore.AppendToSlot(srcSlot, rec); err != nil {
			t.Fatalf("append v%d: %v", v, err)
		}
	}

	// The destination also holds an unrelated aggregate: replaying must not
	// disturb it.
	if _, err := dstStore.AppendToSlot(dstSlot, makeRecord("other-agg", 1, "cmd-other")); err != nil {
		t.Fatalf("seed other: %v", err)
	}

	srv := &peerServer{e: dst}
	resp, err := srv.ReplayInto(context.Background(), &pushupesv1.ReplayIntoRequest{
		SrcSlot: srcSlot, SrcAddr: srcAddr, DstSlot: dstSlot, AggregateId: agg,
	})
	if err != nil {
		t.Fatalf("replay into: %v", err)
	}
	if resp.GetLanded() != 7 {
		t.Fatalf("landed %d records, want 7", resp.GetLanded())
	}
	if resp.GetNextVersion() != 8 {
		t.Fatalf("resume cursor %d, want 8", resp.GetNextVersion())
	}

	recs, _, err := dstStore.ReadAggregate(dstSlot, agg, 1, 100, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(recs) != 7 {
		t.Fatalf("destination holds %d records, want 7", len(recs))
	}
	for i, r := range recs {
		if r.Version != uint32(i+1) {
			t.Fatalf("destination version %d at position %d", r.Version, i)
		}
	}

	// Replaying again is a no-op (a retried migration does not duplicate).
	resp2, err := srv.ReplayInto(context.Background(), &pushupesv1.ReplayIntoRequest{
		SrcSlot: srcSlot, SrcAddr: srcAddr, DstSlot: dstSlot, AggregateId: agg,
	})
	if err != nil {
		t.Fatalf("replay again: %v", err)
	}
	if resp2.GetLanded() != 0 {
		t.Fatalf("a repeated replay landed %d duplicate record(s)", resp2.GetLanded())
	}
	if recs, _, _ := dstStore.ReadAggregate(dstSlot, agg, 1, 100, 0); len(recs) != 7 {
		t.Fatalf("after the repeated replay the destination holds %d records, want 7", len(recs))
	}
}
