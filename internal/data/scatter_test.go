package data

import (
	"bytes"
	"testing"
)

func scatterConcat(t *testing.T, rec *EventRecord) []byte {
	t.Helper()
	parts, total := rec.EncodeScatter(make([]byte, rec.ScatterSize()), nil)
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	if len(out) != total || total != rec.EncodedSize() {
		t.Fatalf("total = %d (want %d), concatenated %d", total, rec.EncodedSize(), len(out))
	}
	return out
}

// The scattered layout must be the contiguous encoding, byte for byte: the
// WAL, migration hashes and recovery all depend on that.
func TestEncodeScatterMatchesEncodeBinary(t *testing.T) {
	cases := []*EventRecord{
		sampleRecordBytes(),
		{AggregateID: "a", Version: 1, UnixTime: 42, CommandID: "c",
			Events: []Event{{Type: "t", Body: []byte{}}}},
		{AggregateID: "agg-with-a-longer-name", Version: 4294967295, UnixTime: -7, CommandID: "cmd",
			Events: []Event{
				{Type: "first", Body: bytes.Repeat([]byte{0xff, 0x00}, 1024)},
				{Type: "second", Body: nil},
				{Type: "third", Body: []byte("small")},
			}},
	}
	for i, rec := range cases {
		want := rec.EncodeBinary(nil)
		got := scatterConcat(t, rec)
		if !bytes.Equal(got, want) {
			t.Fatalf("case %d: scattered encoding differs from EncodeBinary (len %d vs %d)", i, len(got), len(want))
		}
	}
}

// Parts must reference the caller's own body slices, not copies: that is the
// whole point of the scatter path.
func TestEncodeScatterAliasesBodies(t *testing.T) {
	body := bytes.Repeat([]byte{0x41}, 4096)
	rec := &EventRecord{AggregateID: "agg", Version: 3, CommandID: "cmd",
		Events: []Event{{Type: "e", Body: body}}}
	parts, _ := rec.EncodeScatter(make([]byte, rec.ScatterSize()), nil)
	found := false
	for _, p := range parts {
		if len(p) == len(body) && &p[0] == &body[0] {
			found = true
		}
	}
	if !found {
		t.Fatal("event body was copied instead of referenced")
	}
}

func sampleRecordBytes() *EventRecord {
	return &EventRecord{
		AggregateID: "agg-1",
		Version:     7,
		UnixTime:    1750000000123456789,
		CommandID:   "cmd-1",
		Events: []Event{
			{Type: "ev.json", Body: []byte(`{"pad":"aaa"}`)},
			{Type: "ev.bin", Body: bytes.Repeat([]byte{0x00, 0xff, 0x41}, 4096)},
		},
	}
}
