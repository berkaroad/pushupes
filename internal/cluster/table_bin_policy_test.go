package cluster

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"testing"
)

// A live cluster's snapshot directory holds v3 tables: they predate the policy
// tier, and their factor was the fault-tolerance derivation — restoring one
// must say "high" (so the controller re-derives the factor the snapshot
// already holds and the table converges with no churn), keep the factor and
// the layout exactly, and stay readable forever.

func TestTableBinaryDecodesV3Snapshots(t *testing.T) {
	// Hand-build a minimal v3 payload: header + empty directory + one slot.
	var body bytes.Buffer
	body.WriteString(tableMagic)
	body.WriteByte(3) // the v3 version byte
	u32 := func(v uint32) {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], v)
		body.Write(b[:])
	}
	u32(2)            // slotCount
	u32(2)            // replicas (the derived factor of a 3-member cluster)
	body.WriteByte(1) // one peer
	putStr(&body, "node-1")
	putStr(&body, "127.0.0.1:8391")
	putStr(&body, "http://127.0.0.1:8091")
	putStr(&body, "http://127.0.0.1:8591")
	body.WriteByte(0) // not down
	// dictionary: one name
	body.WriteByte(1)
	putStr(&body, "node-1")
	// entries: two slots, both led by node-1 (dictionary index 0)
	body.WriteByte(2)
	u32(0) // slot 0
	body.WriteByte(0)
	body.WriteByte(stateCodes[SlotStable])
	body.WriteByte(2) // epoch = 2
	body.WriteByte(1) // one replica
	body.WriteByte(0) // replicas[0] = dictionary index 0
	body.WriteByte(noNode)
	u32(1) // slot 1
	body.WriteByte(0)
	body.WriteByte(stateCodes[SlotStable])
	body.WriteByte(1) // epoch = 1
	body.WriteByte(1)
	body.WriteByte(0)
	body.WriteByte(noNode)

	var comp bytes.Buffer
	zw := gzip.NewWriter(&comp)
	if _, err := zw.Write(body.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	tbl, err := DecodeTableBinary(comp.Bytes())
	if err != nil {
		t.Fatalf("a v3 snapshot must stay readable: %v", err)
	}
	if tbl.Policy != ReplicaPolicyHigh {
		t.Errorf("v3 factor was the fault-tolerance derivation: restored policy = %q, want high", tbl.Policy)
	}
	if tbl.Replicas != 2 || tbl.SlotCount != 2 {
		t.Errorf("v3 fields changed: factor %d, slot count %d", tbl.Replicas, tbl.SlotCount)
	}
	if len(tbl.Slots) != 2 || tbl.Slots[0].Leader != "node-1" || tbl.Slots[1].Epoch != 1 {
		t.Fatalf("v3 layout did not survive: %+v", tbl.Slots)
	}
	// The restored table must be immediately stable under the controller's
	// derive-and-reconcile rule: high over one member is one copy, but the
	// snapshot's factor (2) is what the LAYOUT was planned with, and the
	// controller only touches the table when policy+members disagree — here
	// the seed/derive step keeps high and converges the sets on the next
	// replan, exactly like a membership shrink. What matters for compat is
	// the round trip through the v4 encoder afterwards.
	out, err := DecodeTableBinary(tbl.EncodeTableBinary())
	if err != nil {
		t.Fatalf("v3 -> v4 re-encode broke: %v", err)
	}
	if out.Policy != ReplicaPolicyHigh || out.Replicas != 2 {
		t.Fatalf("policy/factor lost across the upgrade: %q/%d", out.Policy, out.Replicas)
	}
}

// TestTableBinaryRoundTripsPolicy pins that v4 carries the tier itself: a
// low-policy table re-encodes and restores as low (not the fault-tolerance
// default), including an explicit factor outside what the tier would derive.
func TestTableBinaryRoundTripsPolicy(t *testing.T) {
	tbl := NewTableWithPolicy(4, ReplicaPolicyLow)
	tbl.Replicas = 4 // deliberately off-tier: the snapshot is a record, not a re-derivation
	tbl.Slots[0] = &Placement{Leader: "node-1", Replicas: []string{"node-1"}, Epoch: 1, State: SlotStable}
	out, err := DecodeTableBinary(tbl.EncodeTableBinary())
	if err != nil {
		t.Fatal(err)
	}
	if out.Policy != ReplicaPolicyLow {
		t.Errorf("policy = %q, want low", out.Policy)
	}
	if out.Replicas != 4 {
		t.Errorf("factor = %d, want the recorded 4", out.Replicas)
	}
}
