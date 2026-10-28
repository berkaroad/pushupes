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

package data

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"testing"
)

// v1Frame assembles a legacy frame (no flags byte) by hand: the fixture the
// version dispatch must keep reading after CurrentFrameVer moved to 2.
func v1Frame(agg, cmd, typ string, body []byte, version uint32, unix int64) []byte {
	var payload bytes.Buffer
	binary.Write(&payload, binary.BigEndian, uint16(len(agg)))
	payload.WriteString(agg)
	binary.Write(&payload, binary.BigEndian, version)
	binary.Write(&payload, binary.BigEndian, uint64(unix))
	binary.Write(&payload, binary.BigEndian, uint16(len(cmd)))
	payload.WriteString(cmd)
	binary.Write(&payload, binary.BigEndian, uint16(1)) // eventCount
	binary.Write(&payload, binary.BigEndian, uint16(len(typ)))
	payload.WriteString(typ)
	binary.Write(&payload, binary.BigEndian, uint32(len(body)))
	payload.Write(body)
	out := make([]byte, 4+payload.Len())
	binary.BigEndian.PutUint32(out[:4], uint32(payload.Len()))
	copy(out[4:], payload.Bytes())
	return out
}

func gzipBody(s string) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Write([]byte(s))
	w.Close()
	return buf.Bytes()
}

func TestV2RoundTripCarriesCodec(t *testing.T) {
	rec := &EventRecord{
		AggregateID: "agg-1", Version: 1, UnixTime: 123,
		CommandID: "cmd-1", Codec: CodecGzip,
		Events: []Event{{Type: "ev", Body: gzipBody("hello")}},
	}
	frame := rec.EncodeBinary(nil)
	if len(frame) != rec.EncodedSize() {
		t.Fatalf("frame length %d want %d", len(frame), rec.EncodedSize())
	}
	got, n, err := DecodeRecord(frame)
	if err != nil || n != len(frame) {
		t.Fatalf("decode: %v n=%d", err, n)
	}
	if got.Codec != CodecGzip || !got.Compressed() {
		t.Fatalf("codec lost: %d", got.Codec)
	}
	if !bytes.Equal(got.Events[0].Body, rec.Events[0].Body) {
		t.Fatal("stored bytes must be verbatim (the server never inflates)")
	}
	meta, _, err := DecodeRecordMeta(frame)
	if err != nil || meta.AggregateID != "agg-1" || meta.Version != 1 ||
		meta.CommandHash != HashCommandID("cmd-1") {
		t.Fatalf("meta: %+v err %v", meta, err)
	}
	cmd, err := CommandIDBytes(frame)
	if err != nil || string(cmd) != "cmd-1" {
		t.Fatalf("CommandIDBytes must stay layout-stable: %q %v", cmd, err)
	}
}

func TestV1FrameReadsUnderLegacyOnly(t *testing.T) {
	frame := v1Frame("agg-1", "cmd-1", "ev.json", []byte(`{"a":1}`), 3, 999)
	rec, n, err := DecodeRecordVer(frame, FrameVerLegacy)
	if err != nil || n != len(frame) {
		t.Fatalf("v1 decode: %v", err)
	}
	if rec.Codec != CodecRaw || rec.Version != 3 || rec.CommandID != "cmd-1" {
		t.Fatalf("v1 record wrong: %+v", rec)
	}
	// The same bytes under v2 must NOT silently succeed: the flags/type
	// offsets shift and every bound check that catches it is fine.
	if _, _, err := DecodeRecordVer(frame, FrameVerFlags); err == nil {
		t.Fatal("a v1 frame decoded as v2 without error")
	}
}

func TestV2RejectsUnknownCodecsAndReservedBits(t *testing.T) {
	for _, codec := range []uint8{0x02, 0x0F, 0x10, 0xFF} {
		rec := &EventRecord{
			AggregateID: "a", Version: 1, CommandID: "c", Codec: codec,
			Events: []Event{{Type: "t", Body: []byte("b")}},
		}
		frame := rec.EncodeBinary(nil) // bypasses Validate on purpose
		if _, _, err := DecodeRecord(frame); err == nil {
			t.Fatalf("codec %#x: reader must refuse the frame", codec)
		}
	}
}

func TestValidateGzipGate(t *testing.T) {
	bad := &EventRecord{
		AggregateID: "a", Version: 1, CommandID: "c", Codec: CodecGzip,
		Events: []Event{{Type: "t", Body: []byte("not gzip")}},
	}
	if err := bad.Validate(); err == nil {
		t.Fatal("gzip codec without magic must fail the gate")
	}
	ok := &EventRecord{
		AggregateID: "a", Version: 1, CommandID: "c", Codec: CodecGzip,
		Events: []Event{{Type: "t", Body: gzipBody("hello")}},
	}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid gzip body must pass: %v", err)
	}
	rsvd := &EventRecord{
		AggregateID: "a", Version: 1, CommandID: "c", Codec: 0x20,
		Events: []Event{{Type: "t", Body: []byte("b")}},
	}
	if err := rsvd.Validate(); err == nil {
		t.Fatal("reserved bits must fail the gate")
	}
}

func TestScatterMatchesBinaryWithCodec(t *testing.T) {
	rec := &EventRecord{
		AggregateID: "agg-s", Version: 2, UnixTime: 7,
		CommandID: "cmd-s", Codec: CodecGzip,
		Events: []Event{
			{Type: "ev.one", Body: gzipBody("one")},
			{Type: "ev.two", Body: gzipBody("two")},
		},
	}
	want := rec.EncodeBinary(nil)
	scratch := make([]byte, rec.ScatterSize())
	parts, total := rec.EncodeScatter(scratch, nil)
	var got []byte
	for _, p := range parts {
		got = append(got, p...)
	}
	if total != len(want) || !bytes.Equal(got, want) {
		t.Fatalf("scatter/binary diverge (total %d want %d)", total, len(want))
	}
}
