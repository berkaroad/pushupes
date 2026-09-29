// Package data defines the core domain types for the pushupes: the
// EventRecord appended to slot WALs, its binary encoding, the CRC16 slot
// routing, and the wire-level error IDs.
package data

import (
	"encoding/binary"
	"errors"
	"fmt"
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
type RecordMeta struct {
	AggregateID string
	Version     uint32
	UnixTime    int64
	CommandID   string
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
	m.CommandID = string(rest[:cmdLen])
	rest = rest[cmdLen:]

	count, err := takeU16(&rest)
	if err != nil {
		return r, m, 0, err
	}
	if int(count) > MaxEventsPerRec {
		return r, m, 0, fmt.Errorf("event count %d exceeds max %d", count, MaxEventsPerRec)
	}
	r.AggregateID, r.Version, r.UnixTime, r.CommandID = m.AggregateID, m.Version, m.UnixTime, m.CommandID
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
// 4096 is sized far above the expected node count: cheap slot metadata,
// fine-grained placement/migration units. Routing is
// CRC16(aggregate)%slot_count regardless of N.
const DefaultSlotCount = 4096

// crc16Table is the CRC16/XMODEM (polynomial 0x1021) lookup table used by
// slot hashing.
var crc16Table = func() [256]uint16 {
	var t [256]uint16
	for i := 0; i < 256; i++ {
		crc := uint16(i) << 8
		for j := 0; j < 8; j++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
		t[i] = crc
	}
	return t
}()

// CRC16 computes the CRC16/XMODEM checksum of b.
func CRC16(b []byte) uint16 {
	crc := uint16(0)
	for _, c := range b {
		crc = (crc << 8) ^ crc16Table[byte(crc>>8)^c]
	}
	return crc
}

// SlotOf routes an aggregate ID to its slot.
func SlotOf(aggregateID string, slotCount int) int32 {
	if slotCount <= 0 {
		panic("slotCount must be positive")
	}
	return int32(CRC16([]byte(aggregateID)) % uint16(slotCount))
}

// ---- Error IDs and domain errors ------------------------------------------

// Wire-level error IDs returned in AppendResponse.ErrID on fail/MOVED/ASK.
const (
	ErrIDVersionConflict = 1001
	ErrIDBadRequest      = 1002
	ErrIDSlotNotLocal    = 1003 // MOVED
	ErrIDMigrating       = 1004 // ASK
	ErrIDNotLeader       = 1005
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
