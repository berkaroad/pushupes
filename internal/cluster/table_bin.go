package cluster

// Binary slot-table encoding for Raft snapshots.
//
// The JSON form of a 4096-slot table costs ~350KB (node ids repeated per
// slot, map keys as strings). Snapshots ride the control plane, so the
// encoding is a string dictionary plus fixed-layout varint entries:
//
//	"PTAB" ver(1) slotCount(u32) replicas(u32)
//	peers: n(u16) { id, peer_addr, admin_addr, client_addr } as len-prefixed strings
//	nodes: n(u16) { name } (dictionary; slot entries reference indexes)
//	entries: n(u32) { slot(u32) leaderIdx(u8) state(u8) epoch(uvarint)
//	                 replicaIdx(u8)* migratingToIdx(u8, 255=none) }
//
// Repeated leader/replica patterns compress the result to a few KB with
// gzip; the whole thing is <100KB uncompressed even at 4096 slots.

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

const (
	tableMagic = "PTAB"
	tableVer   = byte(2)
	noNode     = byte(255)
)

var stateCodes = map[SlotState]byte{
	SlotStable:       0,
	SlotMigratingOut: 1,
	SlotImportingIn:  2,
	SlotBackingUp:    3,
}

func stateFromCode(c byte) (SlotState, error) {
	for s, v := range stateCodes {
		if v == c {
			return s, nil
		}
	}
	return "", fmt.Errorf("table: unknown state code %d", c)
}

func putStr(buf *bytes.Buffer, s string) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(s)))
	buf.Write(tmp[:n])
	buf.WriteString(s)
}

func putU32(buf *bytes.Buffer, v uint32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	buf.Write(tmp[:])
}

// EncodeTableBinary serialises the table in the compact binary form (gzipped).
func (t *Table) EncodeTableBinary() []byte {
	// node dictionary: peer ids plus every node name used in placements
	idx := map[string]byte{}
	names := []string{}
	nodeID := func(name string) byte {
		if i, ok := idx[name]; ok {
			return i
		}
		if len(names) >= 255 {
			return noNode // dictionary overflow: leader-less entry (dev bound)
		}
		idx[name] = byte(len(names))
		names = append(names, name)
		return byte(len(names) - 1)
	}

	// stable ordering for determinism (snapshots must decode identically)
	peerIDs := t.PeerIDs()

	var body bytes.Buffer
	body.WriteString(tableMagic)
	body.WriteByte(tableVer)
	putU32(&body, uint32(t.SlotCount))
	putU32(&body, uint32(t.Replicas))

	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(peerIDs)))
	body.Write(tmp[:n])
	for _, id := range peerIDs {
		p := t.Peers[id]
		putStr(&body, p.ID)
		putStr(&body, p.PeerAddr)
		putStr(&body, p.AdminAddr)
		putStr(&body, p.ClientAddr)
	}
	// pre-register peer ids so common case entries hit the dictionary
	for _, id := range peerIDs {
		nodeID(id)
	}
	n = binary.PutUvarint(tmp[:], uint64(len(names)))
	body.Write(tmp[:n])
	for _, name := range names {
		putStr(&body, name)
	}

	// entries sorted by slot for deterministic snapshots
	slots := make([]int, 0, len(t.Slots))
	for s := range t.Slots {
		slots = append(slots, int(s))
	}
	sort.Ints(slots)
	n = binary.PutUvarint(tmp[:], uint64(len(slots)))
	body.Write(tmp[:n])
	for _, s := range slots {
		p := t.Slots[int32(s)]
		putU32(&body, uint32(s))
		body.WriteByte(nodeID(p.Leader))
		code, ok := stateCodes[p.State]
		if !ok {
			code = 255
		}
		body.WriteByte(code)
		n = binary.PutUvarint(tmp[:], uint64(p.Epoch))
		body.Write(tmp[:n])
		n = binary.PutUvarint(tmp[:], uint64(len(p.Replicas)))
		body.Write(tmp[:n])
		for _, r := range p.Replicas {
			body.WriteByte(nodeID(r))
		}
		if p.MigratingTo != "" {
			body.WriteByte(nodeID(p.MigratingTo))
		} else {
			body.WriteByte(noNode)
		}
	}

	var out bytes.Buffer
	zw := gzip.NewWriter(&out)
	_, _ = zw.Write(body.Bytes())
	_ = zw.Close()
	return out.Bytes()
}

// DecodeTableBinary restores a table from EncodeTableBinary output.
func DecodeTableBinary(b []byte) (*Table, error) {
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("table: snapshot gzip: %w", err)
	}
	raw, err := io.ReadAll(io.LimitReader(zr, 64<<20))
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(raw)
	hdr := make([]byte, 5)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, fmt.Errorf("table: short header: %w", err)
	}
	if string(hdr[0:4]) != tableMagic || hdr[4] != tableVer {
		return nil, fmt.Errorf("table: bad snapshot magic/version")
	}
	var tmp4 [4]byte
	readU32 := func() (uint32, error) {
		if _, err := io.ReadFull(r, tmp4[:]); err != nil {
			return 0, err
		}
		return binary.BigEndian.Uint32(tmp4[:]), nil
	}
	slotCount, err := readU32()
	if err != nil {
		return nil, err
	}
	replicas, err := readU32()
	if err != nil {
		return nil, err
	}
	peerN, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	t := NewTable(int32(slotCount), int(replicas))
	for i := uint64(0); i < peerN; i++ {
		id, err := readStr(r)
		if err != nil {
			return nil, err
		}
		peerAddr, err := readStr(r)
		if err != nil {
			return nil, err
		}
		adminAddr, err := readStr(r)
		if err != nil {
			return nil, err
		}
		clientAddr, err := readStr(r)
		if err != nil {
			return nil, err
		}
		t.Peers[id] = Peer{ID: id, PeerAddr: peerAddr, AdminAddr: adminAddr, ClientAddr: clientAddr}
	}
	nameN, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, nameN)
	for i := uint64(0); i < nameN; i++ {
		s, err := readStr(r)
		if err != nil {
			return nil, err
		}
		names = append(names, s)
	}
	entryN, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	node := func(i byte) (string, error) {
		if i == noNode {
			return "", nil // dictionary overflow or "none" marker
		}
		if int(i) >= len(names) {
			return "", fmt.Errorf("table: node index %d out of range", i)
		}
		return names[i], nil
	}
	for i := uint64(0); i < entryN; i++ {
		slot, err := readU32()
		if err != nil {
			return nil, err
		}
		leaderI, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		leader, err := node(leaderI)
		if err != nil {
			return nil, err
		}
		stateC, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		state, err := stateFromCode(stateC)
		if err != nil {
			return nil, err
		}
		ev, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		rn, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		if rn > 255 {
			return nil, fmt.Errorf("table: too many replicas %d", rn)
		}
		reps := make([]string, 0, rn)
		for j := uint64(0); j < rn; j++ {
			ri, err := r.ReadByte()
			if err != nil {
				return nil, err
			}
			name, err := node(ri)
			if err != nil {
				return nil, err
			}
			reps = append(reps, name)
		}
		mig := ""
		mi, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if mi != noNode {
			if mig, err = node(mi); err != nil {
				return nil, err
			}
		}
		t.Slots[int32(slot)] = &Placement{Leader: leader, Replicas: reps, Epoch: int64(ev), State: state, MigratingTo: mig}
	}
	return t, nil
}

func readStr(r *bytes.Reader) (string, error) {
	l, err := binary.ReadUvarint(r)
	if err != nil {
		return "", err
	}
	if l > 1<<20 {
		return "", fmt.Errorf("table: string too long %d", l)
	}
	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}
