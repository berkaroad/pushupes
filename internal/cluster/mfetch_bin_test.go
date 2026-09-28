package cluster

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
)

func TestMRequestRoundTrip(t *testing.T) {
	cases := []MFetchRequest{
		{}, // empty
		{Follower: "node-1", WaitMS: 5000, Items: []FetchItem{{Slot: 0, FromSeq: 1}, {Slot: -1, FromSeq: 1}}},
		{Follower: "node-2", WaitMS: 0, Items: []FetchItem{{Slot: 4095, FromSeq: 1 << 40}, {Slot: 2048, FromSeq: 7}}},
	}
	for _, want := range cases {
		got, err := DecodeMRequest(EncodeMRequest(&want))
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Follower != want.Follower || got.WaitMS != want.WaitMS {
			t.Fatalf("header mismatch: %+v vs %+v", got, want)
		}
		if len(got.Items) != len(want.Items) {
			t.Fatalf("item count: %d vs %d", len(got.Items), len(want.Items))
		}
		for i := range want.Items {
			if got.Items[i].Slot != want.Items[i].Slot || got.Items[i].FromSeq != want.Items[i].FromSeq {
				t.Fatalf("item %d: %+v vs %+v", i, got.Items[i], want.Items[i])
			}
		}
	}
}

func TestMResponseRoundTrip(t *testing.T) {
	rnd := rand.New(rand.NewSource(7))
	payload := make([]byte, 4096)
	rnd.Read(payload) // binary junk, must survive verbatim
	want := &MFetchResponse{
		Follower: "node-3",
		Items: []FetchItem{
			{Slot: 0, NextSeq: 0},
			{Slot: 42, NextSeq: 9, Payload: payload, LeaderLEO: 9, LeaderHW: 5},
			{Slot: -1, NextSeq: 1},
			{Slot: 4095, NextSeq: 1234567, Payload: []byte{0x00, 0xff, 'n', '\n', '"'}, LeaderLEO: 1, LeaderHW: 1},
		},
	}
	got, err := DecodeMResponse(EncodeMResponse(want))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestMFetchDecodeGarbage(t *testing.T) {
	for _, bad := range [][]byte{nil, {}, {1, 2, 3}, bytes.Repeat([]byte{0xff}, 32)} {
		if _, err := DecodeMRequest(bad); err == nil {
			t.Fatalf("request decode should fail on %v", bad)
		}
		if _, err := DecodeMResponse(bad); err == nil {
			t.Fatalf("response decode should fail on %v", bad)
		}
	}
	// truncated response (chop a valid one)
	ok := EncodeMResponse(&MFetchResponse{Items: []FetchItem{{Slot: 1, NextSeq: 2, Payload: bytes.Repeat([]byte{7}, 100), LeaderLEO: 2, LeaderHW: 2}}})
	for i := 5; i < len(ok); i++ {
		if _, err := DecodeMResponse(ok[:i]); err == nil {
			t.Fatalf("truncated response at %d must fail", i)
		}
	}
}
