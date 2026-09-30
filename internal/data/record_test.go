package data

import (
	"bytes"
	"testing"
)

func sampleFrame() []byte {
	rec := &EventRecord{
		AggregateID: "agg-1",
		Version:     7,
		UnixTime:    1750000000123456789,
		CommandID:   "cmd-1",
		Events: []Event{
			{Type: "ev.json", Body: []byte(`{"pad":"aaa"}`)},
			{Type: "ev.bin", Body: bytes.Repeat([]byte{0x00, 0xff, 0x41}, 4096)},
		},
	}
	return rec.EncodeBinary(nil)
}

func TestDecodeRecordMetaMatchesFullDecode(t *testing.T) {
	frame := sampleFrame()
	full, n1, err := DecodeRecord(frame)
	if err != nil {
		t.Fatalf("DecodeRecord: %v", err)
	}
	meta, n2, err := DecodeRecordMeta(frame)
	if err != nil {
		t.Fatalf("DecodeRecordMeta: %v", err)
	}
	if n1 != len(frame) || n2 != len(frame) {
		t.Fatalf("consumed = %d/%d want %d", n1, n2, len(frame))
	}
	if meta.AggregateID != full.AggregateID || meta.Version != full.Version ||
		meta.UnixTime != full.UnixTime || meta.CommandHash != HashCommandID(full.CommandID) {
		t.Fatalf("meta %+v does not match record %+v", meta, full)
	}
}

// The meta path must accept and reject exactly what the full decode does: it is
// the validation gate for every byte a follower lands in its WAL.
func TestDecodeRecordMetaErrorParity(t *testing.T) {
	frame := sampleFrame()
	for cut := 0; cut < len(frame); cut++ {
		buf := frame[:cut]
		_, n1, err1 := DecodeRecord(buf)
		_, n2, err2 := DecodeRecordMeta(buf)
		if (err1 == nil) != (err2 == nil) {
			t.Fatalf("cut %d: full err=%v meta err=%v", cut, err1, err2)
		}
		if err1 == nil && n1 != n2 {
			t.Fatalf("cut %d: consumed %d vs %d", cut, n1, n2)
		}
	}
	// trailing bytes: both must stop after the first frame
	extra := append(append([]byte{}, frame...), frame...)
	_, n1, _ := DecodeRecord(extra)
	_, n2, _ := DecodeRecordMeta(extra)
	if n1 != len(frame) || n2 != len(frame) {
		t.Fatalf("with trailing bytes consumed = %d/%d want %d", n1, n2, len(frame))
	}
}

func TestDecodeRecordMetaRejectsTooManyEvents(t *testing.T) {
	rec := &EventRecord{AggregateID: "a", Version: 1, CommandID: "c"}
	for i := 0; i < MaxEventsPerRec+1; i++ {
		rec.Events = append(rec.Events, Event{Type: "t", Body: []byte{1}})
	}
	frame := rec.EncodeBinary(nil)
	if _, _, err := DecodeRecordMeta(frame); err == nil {
		t.Fatal("expected event-count rejection from DecodeRecordMeta")
	}
	if _, _, err := DecodeRecord(frame); err == nil {
		t.Fatal("expected event-count rejection from DecodeRecord")
	}
}

// DecodeRecord must copy bodies out of the frame, and DecodeRecordMeta must
// copy the header strings: both outlive the buffer they were parsed from.
func TestDecodeRecordCopiesBodies(t *testing.T) {
	frame := sampleFrame()
	meta, _, err := DecodeRecordMeta(frame)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := DecodeRecord(frame)
	if err != nil {
		t.Fatal(err)
	}
	orig := append([]byte(nil), got.Events[0].Body...)
	for i := range frame {
		frame[i] = 0x5a
	}
	if !bytes.Equal(got.Events[0].Body, orig) {
		t.Fatal("decoded body aliases the frame buffer")
	}
	if meta.AggregateID != "agg-1" || meta.CommandHash != HashCommandID("cmd-1") || meta.Version != 7 {
		t.Fatalf("meta aliases the frame buffer: %+v", meta)
	}
}
