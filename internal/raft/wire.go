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

// wire.go — the peer-plane wire format for the self-developed Raft.
//
// A connection opens with a one-byte handshake magic (0x9E) that must not be
// 'P' (0x50): the peer port demuxes on the first byte, and 'P' is the HTTP/2
// preface that belongs to the inter-node gRPC plane. After the handshake the
// connection carries length-prefixed frames, each tagged with a request id so
// requests and responses can share one long-lived connection.
//
// Handshake (dialer -> acceptor):
//
//	magic=0x9E | version=1 | nodeID (u8 len + bytes)
//
// Acceptor replies: magic=0x9E | version=1 | nodeID.
//
// Frame:
//
//	len u32 | reqID u64 | kind u8 | type u8 | payload
//
// len counts everything after itself (8+1+1+payload).
package raft

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	wireMagic   = 0x9E
	wireVersion = 1
	maxFrame    = 64 << 20

	frameKindRequest  byte = 0
	frameKindResponse byte = 1
)

// RPC message types.
const (
	msgRequestVote         byte = 1
	msgRequestVoteResp     byte = 2
	msgAppendEntries       byte = 3
	msgAppendEntriesResp   byte = 4
	msgInstallSnapshot     byte = 5
	msgInstallSnapshotResp byte = 6
	msgError               byte = 255 // handler-level failure, payload is the message
)

type requestVote struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type requestVoteResp struct {
	Term    uint64
	Granted bool
}

type appendEntries struct {
	Term         uint64
	LeaderID     string
	PrevIndex    uint64
	PrevTerm     uint64
	Entries      []Entry
	LeaderCommit uint64
}

type appendEntriesResp struct {
	Term          uint64
	Success       bool
	LastIndex     uint64 // follower's last log index (used as a hint)
	ConflictIndex uint64 // first index of the conflicting term (fast backtrack)
}

type installSnapshot struct {
	Term      uint64
	LeaderID  string
	LastIndex uint64
	LastTerm  uint64
	// Voters is the configuration in force at LastIndex, as recorded in the
	// snapshot file being sent: the receiver adopts it, because the log
	// entries that carried it are not part of what it is being given.
	Voters []Voter
	Data   []byte
}

type installSnapshotResp struct {
	Term      uint64
	Success   bool
	LastIndex uint64
}

// ---- handshake ----

func writeHandshake(c net.Conn, nodeID string, timeout time.Duration) error {
	if len(nodeID) > 255 {
		return fmt.Errorf("raft: node id too long")
	}
	buf := make([]byte, 0, 2+1+len(nodeID))
	buf = append(buf, wireMagic, wireVersion)
	buf = append(buf, byte(len(nodeID)))
	buf = append(buf, nodeID...)
	if timeout > 0 {
		_ = c.SetWriteDeadline(time.Now().Add(timeout))
	}
	if _, err := c.Write(buf); err != nil {
		return err
	}
	if timeout > 0 {
		_ = c.SetWriteDeadline(time.Time{})
	}
	return nil
}

func readHandshake(c net.Conn, timeout time.Duration) (string, error) {
	if timeout > 0 {
		_ = c.SetReadDeadline(time.Now().Add(timeout))
	}
	defer func() {
		if timeout > 0 {
			_ = c.SetReadDeadline(time.Time{})
		}
	}()
	var head [3]byte
	if _, err := io.ReadFull(c, head[:]); err != nil {
		return "", err
	}
	if head[0] != wireMagic {
		return "", fmt.Errorf("raft: bad handshake magic 0x%02x", head[0])
	}
	if head[1] != wireVersion {
		return "", fmt.Errorf("raft: unsupported protocol version %d", head[1])
	}
	id := make([]byte, int(head[2]))
	if _, err := io.ReadFull(c, id); err != nil {
		return "", err
	}
	return string(id), nil
}

// ---- frames ----

func writeFrame(w *bufio.Writer, kind byte, reqID uint64, typ byte, payload []byte) error {
	body := 8 + 1 + 1 + len(payload)
	var hdr [4 + 8 + 1 + 1]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(body))
	binary.BigEndian.PutUint64(hdr[4:12], reqID)
	hdr[12] = kind
	hdr[13] = typ
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	return w.Flush()
}

func readFrame(r *bufio.Reader) (kind byte, reqID uint64, typ byte, payload []byte, err error) {
	var lb [4]byte
	if _, err = io.ReadFull(r, lb[:]); err != nil {
		return
	}
	body := int64(binary.BigEndian.Uint32(lb[:]))
	if body < 10 || body > maxFrame {
		err = fmt.Errorf("raft: bad frame length %d", body)
		return
	}
	buf := make([]byte, body)
	if _, err = io.ReadFull(r, buf); err != nil {
		return
	}
	reqID = binary.BigEndian.Uint64(buf[0:8])
	kind = buf[8]
	typ = buf[9]
	payload = buf[10:]
	return
}

// ---- message codecs ----

func encodeRequestVote(m requestVote) []byte {
	b := binary.BigEndian.AppendUint64(nil, m.Term)
	b = appendLenBytes(b, []byte(m.CandidateID), 1)
	b = binary.BigEndian.AppendUint64(b, m.LastLogIndex)
	b = binary.BigEndian.AppendUint64(b, m.LastLogTerm)
	return b
}

func decodeRequestVote(b []byte) (requestVote, error) {
	var m requestVote
	if len(b) < 8 {
		return m, fmt.Errorf("raft: short RequestVote")
	}
	m.Term = binary.BigEndian.Uint64(b[0:8])
	id, rest, err := readLenBytes(b[8:], 1)
	if err != nil {
		return m, err
	}
	m.CandidateID = string(id)
	if len(rest) < 16 {
		return m, fmt.Errorf("raft: short RequestVote")
	}
	m.LastLogIndex = binary.BigEndian.Uint64(rest[0:8])
	m.LastLogTerm = binary.BigEndian.Uint64(rest[8:16])
	return m, nil
}

func encodeRequestVoteResp(m requestVoteResp) []byte {
	b := binary.BigEndian.AppendUint64(nil, m.Term)
	b = append(b, boolByte(m.Granted))
	return b
}

func decodeRequestVoteResp(b []byte) (requestVoteResp, error) {
	var m requestVoteResp
	if len(b) < 9 {
		return m, fmt.Errorf("raft: short RequestVoteResp")
	}
	m.Term = binary.BigEndian.Uint64(b[0:8])
	m.Granted = b[8] != 0
	return m, nil
}

func encodeAppendEntries(m appendEntries) []byte {
	b := binary.BigEndian.AppendUint64(nil, m.Term)
	b = appendLenBytes(b, []byte(m.LeaderID), 1)
	b = binary.BigEndian.AppendUint64(b, m.PrevIndex)
	b = binary.BigEndian.AppendUint64(b, m.PrevTerm)
	b = binary.BigEndian.AppendUint32(b, uint32(len(m.Entries)))
	for _, e := range m.Entries {
		b = binary.BigEndian.AppendUint64(b, e.Index)
		b = binary.BigEndian.AppendUint64(b, e.Term)
		b = append(b, e.Kind)
		b = binary.BigEndian.AppendUint32(b, uint32(len(e.Data)))
		b = append(b, e.Data...)
	}
	b = binary.BigEndian.AppendUint64(b, m.LeaderCommit)
	return b
}

func decodeAppendEntries(b []byte) (appendEntries, error) {
	var m appendEntries
	if len(b) < 8+1 {
		return m, fmt.Errorf("raft: short AppendEntries")
	}
	m.Term = binary.BigEndian.Uint64(b[0:8])
	id, rest, err := readLenBytes(b[8:], 1)
	if err != nil {
		return m, err
	}
	m.LeaderID = string(id)
	if len(rest) < 20 {
		return m, fmt.Errorf("raft: short AppendEntries")
	}
	m.PrevIndex = binary.BigEndian.Uint64(rest[0:8])
	m.PrevTerm = binary.BigEndian.Uint64(rest[8:16])
	n := int(binary.BigEndian.Uint32(rest[16:20]))
	rest = rest[20:]
	entries := make([]Entry, 0, n)
	for i := 0; i < n; i++ {
		if len(rest) < 21 {
			return m, fmt.Errorf("raft: short entry")
		}
		e := Entry{
			Index: binary.BigEndian.Uint64(rest[0:8]),
			Term:  binary.BigEndian.Uint64(rest[8:16]),
			Kind:  rest[16],
		}
		dlen := int(binary.BigEndian.Uint32(rest[17:21]))
		if len(rest) < 21+dlen {
			return m, fmt.Errorf("raft: short entry data")
		}
		if dlen > 0 {
			e.Data = append([]byte(nil), rest[21:21+dlen]...)
		}
		entries = append(entries, e)
		rest = rest[21+dlen:]
	}
	if len(rest) < 8 {
		return m, fmt.Errorf("raft: short AppendEntries commit")
	}
	m.LeaderCommit = binary.BigEndian.Uint64(rest[0:8])
	m.Entries = entries
	return m, nil
}

func encodeAppendEntriesResp(m appendEntriesResp) []byte {
	b := binary.BigEndian.AppendUint64(nil, m.Term)
	b = append(b, boolByte(m.Success))
	b = binary.BigEndian.AppendUint64(b, m.LastIndex)
	b = binary.BigEndian.AppendUint64(b, m.ConflictIndex)
	return b
}

func decodeAppendEntriesResp(b []byte) (appendEntriesResp, error) {
	var m appendEntriesResp
	if len(b) < 25 {
		return m, fmt.Errorf("raft: short AppendEntriesResp")
	}
	m.Term = binary.BigEndian.Uint64(b[0:8])
	m.Success = b[8] != 0
	m.LastIndex = binary.BigEndian.Uint64(b[9:17])
	m.ConflictIndex = binary.BigEndian.Uint64(b[17:25])
	return m, nil
}

func encodeInstallSnapshot(m installSnapshot) []byte {
	b := binary.BigEndian.AppendUint64(nil, m.Term)
	b = appendLenBytes(b, []byte(m.LeaderID), 1)
	b = binary.BigEndian.AppendUint64(b, m.LastIndex)
	b = binary.BigEndian.AppendUint64(b, m.LastTerm)
	b = append(b, encodeVoters(m.Voters)...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(m.Data)))
	b = append(b, m.Data...)
	return b
}

func decodeInstallSnapshot(b []byte) (installSnapshot, error) {
	var m installSnapshot
	if len(b) < 8 {
		return m, fmt.Errorf("raft: short InstallSnapshot")
	}
	m.Term = binary.BigEndian.Uint64(b[0:8])
	id, rest, err := readLenBytes(b[8:], 1)
	if err != nil {
		return m, err
	}
	m.LeaderID = string(id)
	if len(rest) < 20 {
		return m, fmt.Errorf("raft: short InstallSnapshot")
	}
	m.LastIndex = binary.BigEndian.Uint64(rest[0:8])
	m.LastTerm = binary.BigEndian.Uint64(rest[8:16])
	voters, rest, err := decodeVotersRest(rest[16:])
	if err != nil {
		return m, err
	}
	m.Voters = voters
	dlen := int(binary.BigEndian.Uint32(rest[0:4]))
	if len(rest) < 4+dlen {
		return m, fmt.Errorf("raft: short InstallSnapshot data")
	}
	if dlen > 0 {
		m.Data = append([]byte(nil), rest[4:4+dlen]...)
	}
	return m, nil
}

func encodeInstallSnapshotResp(m installSnapshotResp) []byte {
	b := binary.BigEndian.AppendUint64(nil, m.Term)
	b = append(b, boolByte(m.Success))
	b = binary.BigEndian.AppendUint64(b, m.LastIndex)
	return b
}

func decodeInstallSnapshotResp(b []byte) (installSnapshotResp, error) {
	var m installSnapshotResp
	if len(b) < 17 {
		return m, fmt.Errorf("raft: short InstallSnapshotResp")
	}
	m.Term = binary.BigEndian.Uint64(b[0:8])
	m.Success = b[8] != 0
	m.LastIndex = binary.BigEndian.Uint64(b[9:17])
	return m, nil
}

func boolByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}
