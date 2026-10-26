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
)

// TestAckWaitsLeaderFlush pins the hard rule at the client ack boundary: an
// append that answers success has already passed an fsync on the leader's own
// disk, so a whole-cluster power loss cannot take back a confirmed record.
// The test store runs FlushPolicy{} — no timed round, no threshold — so a
// success here also proves the parked acknowledgement drove the flush that
// covers it (see the storage-side controls in durable_test.go).
func TestAckWaitsLeaderFlush(t *testing.T) {
	e, st := newTestEngine(t, "node-1")

	resp, err := e.SubmitAppend(context.Background(), makeRecord("ackdur-agg", 1, "ackdur-1"))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if resp.Status != data.StatusSuccess {
		t.Fatalf("append failed: %s err_id=%d %s", resp.Status, resp.ErrID, resp.Err)
	}
	if got := st.DurableSeqOf(resp.Slot); got < resp.Seq {
		t.Fatalf("ack returned with the leader fsync watermark at %d, below the confirmed seq %d", got, resp.Seq)
	}

	// The batch leg: the group's answer vouches for its last record, so the
	// watermark already covers the whole batch when the first success of the
	// next group is observed.
	recs := []*data.EventRecord{
		makeRecord("ackdur-batch", 1, "ackdur-b1"),
		makeRecord("ackdur-batch", 2, "ackdur-b2"),
	}
	out, err := e.SubmitBatch(context.Background(), e.SlotOf("ackdur-batch"), recs)
	if err != nil {
		t.Fatalf("submit batch: %v", err)
	}
	var maxSeq uint64
	for _, r := range out {
		if r.Status != data.StatusSuccess {
			t.Fatalf("batch append failed: %s err_id=%d %s", r.Status, r.ErrID, r.Err)
		}
		if r.Seq > maxSeq {
			maxSeq = r.Seq
		}
	}
	if got := st.DurableSeqOf(out[0].Slot); got < maxSeq {
		t.Fatalf("batch ack returned with the leader fsync watermark at %d, below the confirmed seq %d", got, maxSeq)
	}
}
