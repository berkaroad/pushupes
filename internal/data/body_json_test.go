package data

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestBodyJSONWrapRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		body []byte
	}{
		{"json object", []byte(`{"a":1}`)},
		{"utf8 json", []byte(`{"g":"世界"}`)},
		{"raw binary", []byte{0x00, 0x01, 0xff, 'A'}},
		{"empty", []byte{}},
		{"json string", []byte(`"hello"`)},
		{"deliberate b64-shaped doc", []byte(`{"other":"x"}`)},
	}
	for _, tc := range cases {
		wrapped := encodeBodyJSON(tc.body)
		if !json.Valid(wrapped) {
			t.Fatalf("%s: wrapped form not valid JSON: %s", tc.name, wrapped)
		}
		// the wrapper must be encodable inside a full response document
		doc := RecordJSON{Events: []EventJSON{{Type: "T", Body: wrapped}}}
		if _, err := json.Marshal(&doc); err != nil {
			t.Fatalf("%s: enclosing marshal failed: %v", tc.name, err)
		}
		got := DecodeBodyJSON(wrapped)
		if !reflect.DeepEqual(got, tc.body) && !(len(got) == 0 && len(tc.body) == 0) {
			t.Fatalf("%s: round-trip got %v want %v", tc.name, got, tc.body)
		}
	}
}

func TestToRecordJSONKeepsBinaryBody(t *testing.T) {
	rec := &EventRecord{
		AggregateID: "a", Version: 1, CommandID: "c",
		Events: []Event{{Type: "B", Body: []byte{0x00, 0xff}}},
	}
	rj := rec.ToRecordJSON(7)
	if rj.Events[0].Body[0] != '{' {
		t.Fatalf("binary body should be wrapped as JSON object: %s", rj.Events[0].Body)
	}
	raw, err := json.Marshal(rj)
	if err != nil {
		t.Fatalf("RecordJSON with binary body must be marshalable: %v", err)
	}
	_ = raw
	back := DecodeBodyJSON(rj.Events[0].Body)
	if !reflect.DeepEqual(back, []byte{0x00, 0xff}) {
		t.Fatalf("decode back: %v", back)
	}
}
