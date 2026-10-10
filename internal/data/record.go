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

// Package data defines the core domain types for the pushupes: the
// EventRecord appended to slot WALs, its binary encoding, the CRC16 slot
// routing, and the wire-level error IDs.
package data

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/berkaroad/pushupes/pkg/client"
)

// ---- Field limits ---------------------------------------------------------

const (
	MaxAggregateIDLen = 256
	MaxCommandIDLen   = 256
	MaxEventTypeLen   = 256
	MaxBodySize       = 1 << 20 // 1 MiB per event body
	MaxEventsPerRec   = 1024
)

// ---- Domain types ---------------------------------------------------------

// Event is a single domain event inside a record's event list.
type Event struct {
	Type string
	Body []byte
}

// EventRecord is one entry of an event stream: a command's resulting events
// for one aggregate at one contiguous version. Records are appended to the
// WAL of the slot that aggregate_id hashes to.
type EventRecord struct {
	AggregateID string
	Version     uint32
	UnixTime    int64
	CommandID   string
	Events      []Event
}

// RecordMeta is the header portion of an encoded record frame: exactly the
// fields the WAL indexes need, with no event bodies retained.
//
// It carries the command id as a hash rather than the string: the slot index
// keeps one entry per record (tens of millions on a large node) and needs the
// id only to find a record, which it then reads and checks against the real id.
// Materialising a string per record at recovery time is exactly the allocation
// this avoids.
type RecordMeta struct {
	AggregateID string
	Version     uint32
	UnixTime    int64
	CommandHash uint64
}

// FNV-1a: small, no allocation, and stable enough for an index whose hits are
// verified against the record they point to.
const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

func hashCommandBytes(b []byte) uint64 {
	h := uint64(fnvOffset64)
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime64
	}
	return h
}

// HashCommandID hashes a command id the same way the frame decoder does, so a
// request's id can be looked up in the slot index without building anything.
func HashCommandID(id string) uint64 {
	h := uint64(fnvOffset64)
	for i := 0; i < len(id); i++ {
		h ^= uint64(id[i])
		h *= fnvPrime64
	}
	return h
}

// CommandIDBytes returns the command id of an encoded record frame without
// decoding or copying anything else. The slice aliases frame (do not retain
// it); it exists so the replication path can confirm that a replayed frame
// really is the record already stored at that seq, given that the header-only
// metadata keeps just the hash.
func CommandIDBytes(frame []byte) ([]byte, error) {
	rest := frame
	if len(rest) < 4 {
		return nil, errors.New("truncated record frame")
	}
	recLen := int(binary.BigEndian.Uint32(rest[:4]))
	rest = rest[4:]
	if len(rest) < recLen {
		return nil, errors.New("truncated record frame")
	}
	rest = rest[:recLen]
	if len(rest) < 2 {
		return nil, errors.New("truncated record header")
	}
	aggLen := int(binary.BigEndian.Uint16(rest[:2]))
	rest = rest[2:]
	if len(rest) < aggLen+4+8+2 {
		return nil, errors.New("truncated record header")
	}
	rest = rest[aggLen+4+8:]
	cmdLen := int(binary.BigEndian.Uint16(rest[:2]))
	rest = rest[2:]
	if len(rest) < cmdLen {
		return nil, errors.New("truncated command_id")
	}
	return rest[:cmdLen], nil
}

// ---- Binary encoding ------------------------------------------------------
//
// Record layout (all integers big-endian):
//
//	recLen(4) aggLen(2) aggregate_id
//	version(4) unix_time(8)
//	cmdLen(2) command_id
//	eventCount(2) { typeLen(2) type bodyLen(4) body }*
//
// recLen covers everything after itself.
func (r *EventRecord) EncodedSize() int {
	n := 2 + len(r.AggregateID) + 4 + 8 + 2 + len(r.CommandID) + 2
	for i := range r.Events {
		n += 2 + len(r.Events[i].Type) + 4 + len(r.Events[i].Body)
	}
	return 4 + n
}

// ScatterSize is the number of bytes EncodeScatter needs in its scratch
// buffer: everything in the frame except the event bodies.
func (r *EventRecord) ScatterSize() int {
	n := 4 + 2 + len(r.AggregateID) + 4 + 8 + 2 + len(r.CommandID) + 2
	for i := range r.Events {
		n += 2 + len(r.Events[i].Type) + 4
	}
	return n
}

// Validate checks field-length limits and non-empty events.
func (r *EventRecord) Validate() error {
	if r.AggregateID == "" {
		return fmt.Errorf("%w: empty aggregate_id", ErrBadRequest)
	}
	if len(r.AggregateID) > MaxAggregateIDLen {
		return fmt.Errorf("%w: aggregate_id too long", ErrBadRequest)
	}
	if r.CommandID == "" {
		return fmt.Errorf("%w: empty command_id", ErrBadRequest)
	}
	if len(r.CommandID) > MaxCommandIDLen {
		return fmt.Errorf("%w: command_id too long", ErrBadRequest)
	}
	if r.Version == 0 {
		return fmt.Errorf("%w: version must be >= 1", ErrBadRequest)
	}
	if len(r.Events) == 0 {
		return fmt.Errorf("%w: empty events", ErrBadRequest)
	}
	if len(r.Events) > MaxEventsPerRec {
		return fmt.Errorf("%w: too many events", ErrBadRequest)
	}
	for i := range r.Events {
		if len(r.Events[i].Type) > MaxEventTypeLen {
			return fmt.Errorf("%w: event type too long", ErrBadRequest)
		}
		if len(r.Events[i].Body) > MaxBodySize {
			return fmt.Errorf("%w: event body too large", ErrBadRequest)
		}
	}
	return nil
}

// EncodeBinary serialises the record including its 4-byte length prefix.
func (r *EventRecord) EncodeBinary(buf []byte) []byte {
	size := r.EncodedSize()
	if len(buf) < size {
		buf = make([]byte, size)
	}
	rest := buf[4:size]
	binary.BigEndian.PutUint32(buf[0:4], uint32(size-4))
	putU16(&rest, uint16(len(r.AggregateID)))
	putString(&rest, r.AggregateID)
	binary.BigEndian.PutUint32(rest, r.Version)
	rest = rest[4:]
	rest = putU64(rest, uint64(r.UnixTime))
	putU16(&rest, uint16(len(r.CommandID)))
	putString(&rest, r.CommandID)
	putU16(&rest, uint16(len(r.Events)))
	for i := range r.Events {
		putU16(&rest, uint16(len(r.Events[i].Type)))
		putString(&rest, r.Events[i].Type)
		binary.BigEndian.PutUint32(rest, uint32(len(r.Events[i].Body)))
		rest = rest[4:]
		putBytes(&rest, r.Events[i].Body)
	}
	return buf[:size]
}

// scanRecord parses one length-prefixed record frame from buf, validating the
// full frame shape. When withBodies is false the event bodies are validated
// but not copied — replication lands the very bytes it received.
func scanRecord(buf []byte, withBodies bool) (EventRecord, RecordMeta, int, error) {
	var r EventRecord
	var m RecordMeta
	if len(buf) < 4 {
		return r, m, 0, errors.New("truncated record length")
	}
	recLen := int(binary.BigEndian.Uint32(buf[0:4]))
	if recLen < 0 || recLen > 64<<20 || 4+recLen > len(buf) {
		return r, m, 0, fmt.Errorf("record body unavailable: need %d have %d", 4+recLen, len(buf))
	}
	rest := buf[4 : 4+recLen]

	aggLen, err := takeU16(&rest)
	if err != nil {
		return r, m, 0, err
	}
	if int(aggLen) > MaxAggregateIDLen || len(rest) < int(aggLen) {
		return r, m, 0, errors.New("truncated aggregate_id")
	}
	m.AggregateID = string(rest[:aggLen])
	rest = rest[aggLen:]

	m.Version, err = takeU32(&rest)
	if err != nil {
		return r, m, 0, err
	}
	var ts uint64
	ts, err = takeU64(&rest)
	if err != nil {
		return r, m, 0, err
	}
	m.UnixTime = int64(ts)

	cmdLen, err := takeU16(&rest)
	if err != nil {
		return r, m, 0, err
	}
	if int(cmdLen) > MaxCommandIDLen || len(rest) < int(cmdLen) {
		return r, m, 0, errors.New("truncated command_id")
	}
	m.CommandHash = hashCommandBytes(rest[:cmdLen])
	if withBodies {
		r.CommandID = string(rest[:cmdLen])
	}
	rest = rest[cmdLen:]

	count, err := takeU16(&rest)
	if err != nil {
		return r, m, 0, err
	}
	if int(count) > MaxEventsPerRec {
		return r, m, 0, fmt.Errorf("event count %d exceeds max %d", count, MaxEventsPerRec)
	}
	r.AggregateID, r.Version, r.UnixTime = m.AggregateID, m.Version, m.UnixTime
	if withBodies {
		r.Events = make([]Event, 0, count)
	}
	for i := 0; i < int(count); i++ {
		var ev Event
		typeLen, err := takeU16(&rest)
		if err != nil {
			return r, m, 0, err
		}
		if int(typeLen) > MaxEventTypeLen || len(rest) < int(typeLen) {
			return r, m, 0, errors.New("truncated event type")
		}
		ev.Type = string(rest[:typeLen])
		rest = rest[typeLen:]
		bodyLen, err := takeU32(&rest)
		if err != nil {
			return r, m, 0, err
		}
		if int(bodyLen) > MaxBodySize || len(rest) < int(bodyLen) {
			return r, m, 0, errors.New("truncated event body")
		}
		if withBodies {
			ev.Body = append([]byte(nil), rest[:bodyLen]...)
			r.Events = append(r.Events, ev)
		}
		rest = rest[bodyLen:]
	}
	return r, m, 4 + recLen, nil
}

// DecodeRecord parses one length-prefixed record from buf, materialising its
// event bodies as fresh slices (safe to retain after buf is reused).
func DecodeRecord(buf []byte) (EventRecord, int, error) {
	r, _, n, err := scanRecord(buf, true)
	return r, n, err
}

// DecodeRecordMeta validates one length-prefixed record frame and returns only
// its header fields plus the frame's total length (4+recLen). Event bodies are
// checked for bounds but never copied and never retained: the replication hot
// path indexes from the metadata and writes the received bytes straight
// through.
func DecodeRecordMeta(buf []byte) (RecordMeta, int, error) {
	_, m, n, err := scanRecord(buf, false)
	return m, n, err
}

// EncodeScatter describes the record frame as a list of segments whose
// concatenation is byte-identical to EncodeBinary's output, but with the event
// bodies referenced in place instead of copied: the fixed-width headers and
// the ids/types go into scratch. Callers that can write scattered segments
// (pwritev) then hand a large body straight to the kernel.
//
// scratch must be at least ScatterSize() bytes; parts is reused (grown if
// needed) and returned.
func (r *EventRecord) EncodeScatter(scratch []byte, parts [][]byte) ([][]byte, int) {
	size := r.ScatterSize()
	if len(scratch) < size {
		scratch = make([]byte, size)
	}
	buf := scratch[:size]
	off := 0
	binary.BigEndian.PutUint32(buf[off:], uint32(r.EncodedSize()-4))
	off += 4
	binary.BigEndian.PutUint16(buf[off:], uint16(len(r.AggregateID)))
	off += 2
	off += copy(buf[off:], r.AggregateID)
	binary.BigEndian.PutUint32(buf[off:], r.Version)
	off += 4
	binary.BigEndian.PutUint64(buf[off:], uint64(r.UnixTime))
	off += 8
	binary.BigEndian.PutUint16(buf[off:], uint16(len(r.CommandID)))
	off += 2
	off += copy(buf[off:], r.CommandID)
	binary.BigEndian.PutUint16(buf[off:], uint16(len(r.Events)))
	off += 2

	parts = parts[:0]
	parts = append(parts, buf[:off])
	for i := range r.Events {
		ev := &r.Events[i]
		hdr := off
		binary.BigEndian.PutUint16(buf[off:], uint16(len(ev.Type)))
		off += 2
		off += copy(buf[off:], ev.Type)
		binary.BigEndian.PutUint32(buf[off:], uint32(len(ev.Body)))
		off += 4
		parts = append(parts, buf[hdr:off])
		if len(ev.Body) > 0 {
			parts = append(parts, ev.Body)
		}
	}
	return parts, r.EncodedSize()
}

// ---- small binary helpers -------------------------------------------------

func putU16(rest *[]byte, v uint16) []byte {
	binary.BigEndian.PutUint16(*rest, v)
	out := (*rest)[2:]
	*rest = out
	return out
}

func putU32(rest []byte, v uint32) []byte {
	binary.BigEndian.PutUint32(rest, v)
	return rest[4:]
}

func putU64(rest []byte, v uint64) []byte {
	binary.BigEndian.PutUint64(rest, v)
	return rest[8:]
}

func putBytes(rest *[]byte, b []byte) {
	copy(*rest, b)
	*rest = (*rest)[len(b):]
}

func putString(rest *[]byte, s string) {
	copy(*rest, s) // copy() accepts a string source
	*rest = (*rest)[len(s):]
}

func takeU16(rest *[]byte) (uint16, error) {
	if len(*rest) < 2 {
		return 0, errors.New("truncated uint16")
	}
	v := binary.BigEndian.Uint16(*rest)
	*rest = (*rest)[2:]
	return v, nil
}

func takeU32(rest *[]byte) (uint32, error) {
	if len(*rest) < 4 {
		return 0, errors.New("truncated uint32")
	}
	v := binary.BigEndian.Uint32(*rest)
	*rest = (*rest)[4:]
	return v, nil
}

func takeU64(rest *[]byte) (uint64, error) {
	if len(*rest) < 8 {
		return 0, errors.New("truncated uint64")
	}
	v := binary.BigEndian.Uint64(*rest)
	*rest = (*rest)[8:]
	return v, nil
}

// ByteRange describes a contiguous byte range within one WAL segment file
// (used by replica fetch and migration snapshot to ship data zero-copy).
type ByteRange struct {
	Segment string `json:"segment"`
	Start   int64  `json:"start"`
	End     int64  `json:"end"`
}

// ---- Slot routing ----------------------------------------------------------

// DefaultSlotCount is the fixed number of slots in the cluster (configurable).
// 1680 = 105 * 16 is sized for a joint 3/5/7-node deployment: with a multiple of
// lcm(3,5,7) the leader of an aggregate is a pure function of hash%N, so 3, 5
// and 7 node clusters all distribute their leaders evenly and changing the slot
// count later cannot move an aggregate to another node. It is also far above
// the expected node count: cheap slot metadata, fine-grained placement and
// migration units.
//
// This is server-side cluster state. The client contract (pkg/client) carries
// the routing ALGORITHM — the one thing a client must compute identically —
// while the slot count reaches clients through PrefetchRoutes, so it can
// change without stranding any client build.
const DefaultSlotCount = 1680

// SlotOf routes an aggregate ID to its slot. The algorithm lives in
// pkg/client (see that package's doc): the client computes the same slot the
// server routes by, from the same code. slotCount is the cluster's slot count
// — the caller's view of DefaultSlotCount / the store's SlotCount.
func SlotOf(aggregateID string, slotCount int) int32 {
	return client.SlotOf(aggregateID, slotCount)
}

// ---- Error IDs and domain errors ------------------------------------------

// Wire-level error IDs returned in AppendResponse.ErrID on fail/MOVED/ASK.
// The table lives in pkg/client (the client-facing wire contract — the
// client side is where these ids are branched on); these are the server-side
// names for the same numbers.
const (
	ErrIDVersionConflict = client.ErrIDVersionConflict
	ErrIDBadRequest      = client.ErrIDBadRequest
	ErrIDSlotNotLocal    = client.ErrIDSlotNotLocal // MOVED
	ErrIDMigrating       = client.ErrIDMigrating    // ASK
	ErrIDNotLeader       = client.ErrIDNotLeader
	ErrIDFlowControl     = client.ErrIDFlowControl // the node's flow control refused the append
)

var (
	// ErrBadRequest maps to error ID 1002.
	ErrBadRequest = errors.New("bad request")
	// ErrVersionConflict maps to 1001.
	ErrVersionConflict = errors.New("version conflict")
	// ErrSlotNotLocal maps to 1003 (MOVED).
	ErrSlotNotLocal = errors.New("slot not owned by this node")
	// ErrMigrating maps to 1004 (ASK).
	ErrMigrating = errors.New("slot is migrating")
	// ErrNotLeader maps to 1005.
	ErrNotLeader = errors.New("not leader for this slot")
	// ErrRecordNotFound is returned by point lookups.
	ErrRecordNotFound = errors.New("record not found")
)

// ---- Append result --------------------------------------------------------
//
// The client-facing request/response message shapes live in the gRPC proto
// (pushupes.v1.EventService); the engine works on EventRecord values, and
// the stored record an "exists" replays travels as a plain *EventRecord:
// event bodies are arbitrary bytes end to end, no JSON wrapping plane.

// Status values for AppendResponse.Status.
const (
	StatusSuccess = "success"
	StatusExists  = "exists"
	StatusFail    = "fail"
)

// AppendResponse reports the outcome of an append. On "exists" the stored
// record is returned; on "fail" ErrID explains why.
type AppendResponse struct {
	Status         string       `json:"status"`
	ErrID          int          `json:"err_id,omitempty"`
	Err            string       `json:"error,omitempty"`
	CurrentVersion uint32       `json:"current_version,omitempty"`
	Seq            uint64       `json:"seq,omitempty"`
	Slot           int32        `json:"slot"`
	Node           string       `json:"node,omitempty"`   // redirect target for MOVED/ASK/NOT_LEADER
	Record         *EventRecord `json:"record,omitempty"` // set on success/exists
}
