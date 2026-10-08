package cluster

// Binary slot-table encoding for Raft snapshots.
//
// The JSON form of the whole slot table costs a few hundred KB (node ids repeated per
// slot, map keys as strings). Snapshots ride the control plane, so the
// encoding is a string dictionary plus fixed-layout varint entries:
//
//	"PTAB" ver(1) slotCount(u32) replicas(u32) policy(len-prefixed string)
//	peers: n(u16) { id, peer_addr, admin_addr, client_addr } as len-prefixed strings
//	nodes: n(u16) { name } (dictionary; slot entries reference indexes)
//	entries: n(u32) { slot(u32) leaderIdx(u8) state(u8) epoch(uvarint)
//	                 replicaIdx(u8)* migratingToIdx(u8, 255=none) }
//
// Repeated leader/replica patterns compress the result to a few KB with
// gzip; the whole thing is <100KB uncompressed even at the default slot count.

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
	// v4 adds the replica policy tier after the replica count in the header.
	tableVer = byte(4)
	// tableVerPolicy is the v3 form: it predates the policy tier, and its
	// factor was the fault-tolerance derivation — the high tier says exactly
	// that, so a v3 snapshot restores as high and the controller re-derives
	// the same factor the table already carries.
	tableVerPolicy = byte(3)
	noNode         = byte(255)
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

	// TWO passes over the table: every name the payload can reference must be
	// in the dictionary BEFORE it is written, because the dictionary is one
	// block ahead of the entries. A name registered while the entries are
	// being walked (a placement that names a node the directory no longer
	// holds — a leave_node that landed while a migration to that node was
	// committing, or a leader-less slot registering the empty name) would get
	// an index that never reaches the file, and DecodeTableBinary would refuse
	// the entire snapshot: every restart of a node holding it dies in Raft's
	// snapshot restore. Forensics on a real breakage: the table held six peers
	// (node-1..node-5, node-7) while 238 placements named the removed node-6,
	// so the dictionary was written with six entries and entry index 6 was
	// unreadable. Register first, then serialise.
	for _, id := range peerIDs {
		nodeID(id)
	}
	// entries sorted by slot for deterministic snapshots
	slots := make([]int, 0, len(t.Slots))
	for s := range t.Slots {
		slots = append(slots, int(s))
	}
	sort.Ints(slots)
	for _, s := range slots {
		p := t.Slots[int32(s)]
		nodeID(p.Leader)
		for _, r := range p.Replicas {
			nodeID(r)
		}
		if p.MigratingTo != "" {
			nodeID(p.MigratingTo)
		}
	}

	var body bytes.Buffer
	body.WriteString(tableMagic)
	body.WriteByte(tableVer)
	putU32(&body, uint32(t.SlotCount))
	putU32(&body, uint32(t.Replicas))
	policy := string(t.Policy)
	if policy == "" {
		policy = string(DefaultReplicaPolicy)
	}
	putStr(&body, policy)

	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(peerIDs)))
	body.Write(tmp[:n])
	for _, id := range peerIDs {
		p := t.Peers[id]
		putStr(&body, p.ID)
		putStr(&body, p.PeerAddr)
		putStr(&body, p.AdminAddr)
		putStr(&body, p.ClientAddr)
		// Down flag (v3): the controller's liveness verdict must survive a
		// snapshot, or a bootstrapped follower would consider a dead peer
		// active until the controller's next sweep re-marked it.
		if p.Down {
			body.WriteByte(1)
		} else {
			body.WriteByte(0)
		}
	}
	n = binary.PutUvarint(tmp[:], uint64(len(names)))
	body.Write(tmp[:n])
	for _, name := range names {
		putStr(&body, name)
	}

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
	if string(hdr[0:4]) != tableMagic || (hdr[4] != tableVer && hdr[4] != tableVerPolicy) {
		return nil, fmt.Errorf("table: bad snapshot magic/version")
	}
	policyInHeader := hdr[4] == tableVer // v4 carries the tier in the header
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
	// v3 snapshots carry no tier: their factor was the fault-tolerance
	// derivation itself, so the restored table says high and the controller
	// re-derives the factor the snapshot already holds.
	policy := ReplicaPolicyHigh
	if policyInHeader {
		policyStr, err := readStr(r)
		if err != nil {
			return nil, err
		}
		policy, err = NormalizeReplicaPolicy(policyStr)
		if err != nil {
			return nil, fmt.Errorf("table: snapshot: %w", err)
		}
	}
	t := NewTableWithPolicy(int32(slotCount), policy)
	t.Replicas = int(replicas)
	peerN, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
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
		// Down flag (v3): one byte, matches the encode side.
		var db [1]byte
		if _, err := io.ReadFull(r, db[:]); err != nil {
			return nil, err
		}
		t.Peers[id] = Peer{ID: id, PeerAddr: peerAddr, AdminAddr: adminAddr, ClientAddr: clientAddr, Down: db[0] == 1}
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
