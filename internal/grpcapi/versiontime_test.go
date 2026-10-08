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

package grpcapi

import (
	"context"
	"testing"

	pushupesv1 "github.com/berkaroad/pushupes/internal/grpcapi/pushupes/v1"
)

// ReadVersionByTime is the point-in-time anchor for client history replay:
// the highest version whose record is stamped at or before the asked time
// (a record written exactly AT the queried time is part of the answer),
// so the client can ReadStream from it. The cases pin the wire semantics:
// the <= boundary, the no-hit answer (0), the tail answer, and argument validation.
func TestGRPCReadVersionByTime(t *testing.T) {
	cli, _ := newTestClient(t)
	ctx := context.Background()

	const agg = "time-probe"
	// Explicit stamps: version v carries unix_time = 2000+v (the Append
	// request sets UnixTime; 0 would default to the server clock).
	for v := uint32(1); v <= 5; v++ {
		r, err := cli.Append(ctx, &pushupesv1.AppendRequest{
			AggregateId: agg, Version: v, UnixTime: int64(2000 + v),
			CommandId: "tc-" + string(rune('a'+v)),
			Events:    []*pushupesv1.Event{{Type: "T", Body: []byte("{}")}},
		})
		if err != nil || r.Status != pushupesv1.AppendResponse_STATUS_SUCCESS {
			t.Fatalf("append v%d: %+v %v", v, r, err)
		}
	}

	cases := []struct {
		name string
		at   int64
		want uint32
	}{
		{"exact-stamp-3", 2003, 3}, // the record written AT the time is included
		{"exact-stamp-4", 2004, 4},
		{"past-tail", 9999, 5},
		{"before-head", 2000, 0}, // strictly before the first stamp: nothing
		{"at-first-stamp", 2001, 1},
	}
	for _, c := range cases {
		resp, err := cli.ReadVersionByTime(ctx, &pushupesv1.ReadVersionByTimeRequest{
			AggregateId: agg, UnixTime: c.at,
		})
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if resp.Version != c.want {
			t.Fatalf("%s: want %d got %d", c.name, c.want, resp.Version)
		}
	}

	// The anchor composes with ReadStream: replaying "as of" a time means
	// reading versions 1..anchor.
	resp, err := cli.ReadVersionByTime(ctx, &pushupesv1.ReadVersionByTimeRequest{
		AggregateId: agg, UnixTime: 2004,
	})
	if err != nil || resp.Version != 4 {
		t.Fatalf("anchor: %+v %v", resp, err)
	}
	rs, err := cli.ReadStream(ctx, &pushupesv1.ReadStreamRequest{
		AggregateId: agg, FromVersion: 1, Limit: uint64(resp.Version),
	})
	if err != nil || len(rs.Records) != 4 || rs.LastVersion != 4 {
		t.Fatalf("replay to anchor: %+v %v", rs, err)
	}

	// Unknown aggregate: 0, no error.
	unk, err := cli.ReadVersionByTime(ctx, &pushupesv1.ReadVersionByTimeRequest{
		AggregateId: "no-such-agg", UnixTime: 9999,
	})
	if err != nil || unk.Version != 0 {
		t.Fatalf("unknown agg: %+v %v", unk, err)
	}

	// Missing aggregate_id is InvalidArgument like ReadStream.
	if _, err := cli.ReadVersionByTime(ctx, &pushupesv1.ReadVersionByTimeRequest{}); err == nil {
		t.Fatal("missing aggregate_id must be InvalidArgument")
	}
}
