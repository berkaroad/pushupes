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

// log.go — the in-memory Raft log backed by the WAL.
//
// The log is a base-indexed slice of entries plus the WAL that persists it.
// Writes go through the log (which owns the payload format); reads may come
// from any goroutine, so every method takes the log's own mutex. The runLoop
// is the only writer, which is why there are no ...Locked variants exposed.
//
// The WAL is append-only, so a follower that must discard a conflicting suffix
// writes a truncate marker instead of rewriting history: replay drops every
// in-memory entry at or above that index before continuing.
package raft

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"
)

// hardState is the term/voteFor pair that must survive a crash.
type hardState struct {
	Term uint64
	Vote string
}

type raftLog struct {
	mu sync.Mutex

	wal      *WAL
	entries  []Entry
	lsns     []uint64 // parallel to entries
	truncLSN uint64   // LSN of the newest truncate marker (0 = none)
	hsLSN    uint64
	confLSN  uint64

	snapIndex uint64
	snapTerm  uint64
	snapFile  string
	// snapVoters is the voter set recorded in the snapshot: the configuration
	// in force at snapIndex, which is the base a recovered node starts from.
	snapVoters []Voter

	hs      hardState
	voters  []Voter
	hasConf bool
}

func openLog(raftDir string, opts WALOptions) (*raftLog, error) {
	l := &raftLog{}
	idx, term, file, voters, _, ok, err := loadLatestSnapshot(raftDir)
	if err != nil {
		return nil, err
	}
	if ok {
		l.snapIndex, l.snapTerm, l.snapFile, l.snapVoters = idx, term, file, voters
	}
	wal, err := OpenWAL(filepath.Join(raftDir, "wal"), opts)
	if err != nil {
		return nil, err
	}
	l.wal = wal
	if err := l.replay(wal.Records()); err != nil {
		wal.Close()
		return nil, err
	}
	return l, nil
}

func (l *raftLog) replay(recs []Record) error {
	for _, r := range recs {
		switch r.Type {
		case RecordEntry:
			e, err := decodeEntry(r.Payload)
			if err != nil {
				return err
			}
			if e.Index <= l.snapIndex {
				continue // already covered by the snapshot
			}
			if len(l.entries) > 0 && e.Index != l.entries[len(l.entries)-1].Index+1 {
				// Defensive: a gap means the slice is not a faithful log.
				l.entries = nil
				l.lsns = nil
			}
			l.entries = append(l.entries, e)
			l.lsns = append(l.lsns, r.LSN)
		case RecordTruncate:
			if len(r.Payload) != 8 {
				return fmt.Errorf("raft: bad truncate record")
			}
			l.dropFromLocked(binary.BigEndian.Uint64(r.Payload))
			l.truncLSN = r.LSN
		case RecordHardState:
			hs, err := decodeHardState(r.Payload)
			if err != nil {
				return err
			}
			l.hs = hs
			l.hsLSN = r.LSN
		case RecordConf:
			v, err := decodeVoters(r.Payload)
			if err != nil {
				return err
			}
			l.voters = v
			l.hasConf = true
			l.confLSN = r.LSN
		default:
			return fmt.Errorf("raft: unknown wal record type %d", r.Type)
		}
	}
	return nil
}

// appendEntries persists entries and waits for durability. Use it when the
// caller must not proceed until the records are on disk (the leader's own
// appends).
func (l *raftLog) appendEntries(es []Entry) error {
	lsn, err := l.appendEntriesAsync(es)
	if err != nil {
		return err
	}
	if lsn == 0 {
		return nil
	}
	return l.wal.Wait(lsn)
}

// appendEntriesAsync persists entries into the WAL and updates the in-memory
// log without waiting for the fsync; it returns the LSN of the last record, so
// the caller can defer its acknowledgement until the flusher reports that LSN
// durable. Followers use it to answer AppendEntries without blocking the run
// loop on I/O.
func (l *raftLog) appendEntriesAsync(es []Entry) (uint64, error) {
	if len(es) == 0 {
		return 0, nil
	}
	for i := 1; i < len(es); i++ {
		if es[i].Index != es[i-1].Index+1 {
			return 0, fmt.Errorf("raft: non-contiguous append %d then %d", es[i-1].Index, es[i].Index)
		}
	}

	l.mu.Lock()
	first := es[0].Index
	needTruncate := len(l.entries) > 0 && first <= l.entries[len(l.entries)-1].Index
	l.mu.Unlock()

	lsns := make([]uint64, 0, len(es))
	var markerLSN uint64
	if needTruncate {
		lsn, err := l.wal.Append(RecordTruncate, u64b(first))
		if err != nil {
			return 0, err
		}
		markerLSN = lsn
	}
	for i := range es {
		lsn, err := l.wal.Append(RecordEntry, encodeEntry(es[i]))
		if err != nil {
			return 0, err
		}
		lsns = append(lsns, lsn)
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if needTruncate {
		l.dropFromLocked(first)
		l.truncLSN = markerLSN
	}
	if len(l.entries) > 0 && es[0].Index != l.entries[len(l.entries)-1].Index+1 {
		// Defensive reset rather than silently corrupting the slice.
		l.entries = nil
		l.lsns = nil
	}
	for i := range es {
		l.entries = append(l.entries, es[i])
		l.lsns = append(l.lsns, lsns[i])
	}
	return lsns[len(lsns)-1], nil
}

// setHardState persists term/voteFor.
func (l *raftLog) setHardState(hs hardState) error {
	lsn, err := l.wal.Append(RecordHardState, encodeHardState(hs))
	if err != nil {
		return err
	}
	if err := l.wal.Wait(lsn); err != nil {
		return err
	}
	l.mu.Lock()
	l.hs = hs
	l.hsLSN = lsn
	l.mu.Unlock()
	return nil
}

// setVoters persists the current voter set. It is written on a first start
// (the configured membership) and again whenever a membership change commits —
// confLSN is the replay seed's answer to "what is the configuration", so every
// such change must be persisted with it.
func (l *raftLog) setVoters(vs []Voter) error {
	lsn, err := l.wal.Append(RecordConf, encodeVoters(vs))
	if err != nil {
		return err
	}
	if err := l.wal.Wait(lsn); err != nil {
		return err
	}
	l.mu.Lock()
	l.voters = vs
	l.hasConf = true
	l.confLSN = lsn
	l.mu.Unlock()
	return nil
}

func (l *raftLog) HardState() hardState {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.hs
}

func (l *raftLog) Voters() []Voter {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Voter(nil), l.voters...)
}

func (l *raftLog) HasConf() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.hasConf
}

func (l *raftLog) LastIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastIndexLocked()
}

func (l *raftLog) lastIndexLocked() uint64 {
	if n := len(l.entries); n > 0 {
		return l.entries[n-1].Index
	}
	return l.snapIndex
}

func (l *raftLog) FirstIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.snapIndex + 1
}

func (l *raftLog) LastTerm() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if n := len(l.entries); n > 0 {
		return l.entries[n-1].Term
	}
	return l.snapTerm
}

func (l *raftLog) Term(i uint64) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.termLocked(i)
}

func (l *raftLog) termLocked(i uint64) uint64 {
	if i == 0 {
		return 0
	}
	if l.snapIndex != 0 && i == l.snapIndex {
		return l.snapTerm
	}
	if i < l.snapIndex || len(l.entries) == 0 {
		return 0
	}
	base := l.entries[0].Index
	if i < base || i > l.entries[len(l.entries)-1].Index {
		return 0
	}
	return l.entries[i-base].Term
}

func (l *raftLog) Entry(i uint64) (Entry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) == 0 {
		return Entry{}, false
	}
	base := l.entries[0].Index
	if i < base || i > l.entries[len(l.entries)-1].Index {
		return Entry{}, false
	}
	return l.entries[i-base], true
}

// EntryLSN returns the LSN of an in-memory entry (0 when it is not in the
// slice, e.g. already compacted).
func (l *raftLog) EntryLSN(index uint64) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) == 0 {
		return 0
	}
	base := l.entries[0].Index
	if index < base || index > l.entries[len(l.entries)-1].Index {
		return 0
	}
	return l.lsns[index-base]
}

// Entries returns up to max entries starting at from.
func (l *raftLog) Entries(from uint64, max int) []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) == 0 || max <= 0 {
		return nil
	}
	base := l.entries[0].Index
	if from < base {
		from = base
	}
	off := int(from - base)
	if off >= len(l.entries) {
		return nil
	}
	end := len(l.entries)
	if end-off > max {
		end = off + max
	}
	return append([]Entry(nil), l.entries[off:end]...)
}

// dropFromLocked discards every in-memory entry at or above index (truncation).
func (l *raftLog) dropFromLocked(index uint64) {
	n := len(l.entries)
	if n == 0 {
		return
	}
	base := l.entries[0].Index
	if index <= base {
		l.entries = nil
		l.lsns = nil
		return
	}
	off := int(index - base)
	if off >= n {
		return
	}
	l.entries = l.entries[:off]
	l.lsns = l.lsns[:off]
}

// dropThroughLocked discards every in-memory entry at or below index (the part
// a snapshot now covers), keeping the suffix.
func (l *raftLog) dropThroughLocked(index uint64) {
	n := len(l.entries)
	if n == 0 {
		return
	}
	last := l.entries[n-1].Index
	if index >= last {
		l.entries = nil
		l.lsns = nil
		return
	}
	base := l.entries[0].Index
	if index < base {
		return
	}
	off := int(index-base) + 1
	l.entries = l.entries[off:]
	l.lsns = l.lsns[off:]
}

// setSnapshot records the new snapshot baseline and drops memory at or below it.
func (l *raftLog) setSnapshot(index, term uint64, file string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.snapIndex = index
	l.snapTerm = term
	l.snapFile = file
	l.dropThroughLocked(index)
}

// resetToSnapshot replaces the whole log with a snapshot received from the
// leader: entries above the snapshot belong to a history the leader has already
// abandoned, so they are discarded too.
func (l *raftLog) resetToSnapshot(index, term uint64, file string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.snapIndex = index
	l.snapTerm = term
	l.snapFile = file
	l.entries = nil
	l.lsns = nil
}

func (l *raftLog) Snapshot() (index, term uint64, file string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.snapIndex, l.snapTerm, l.snapFile
}

// SnapshotVoters is the configuration recorded in the snapshot (empty when the
// node has no snapshot).
func (l *raftLog) SnapshotVoters() []Voter {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Voter(nil), l.snapVoters...)
}

// compact deletes WAL segments that only hold records below what is still
// needed: the first in-memory entry, plus the newest conf/hardstate/truncate
// markers (which must survive for replay).
func (l *raftLog) compact() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	keep := l.wal.LastLSN() + 1
	if len(l.lsns) > 0 {
		keep = l.lsns[0]
	}
	for _, mark := range []uint64{l.hsLSN, l.confLSN, l.truncLSN} {
		if mark != 0 && mark < keep {
			keep = mark
		}
	}
	if keep <= 1 {
		return nil
	}
	return l.wal.Compact(keep)
}

func (l *raftLog) close() error { return l.wal.Close() }

// ---- payload codecs ----

func u64b(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func decodeHardState(b []byte) (hardState, error) {
	if len(b) < 9 {
		return hardState{}, fmt.Errorf("raft: bad hardstate record")
	}
	hs := hardState{Term: binary.BigEndian.Uint64(b[0:8])}
	vote, _, err := readLenBytes(b[8:], 1)
	if err != nil {
		return hardState{}, err
	}
	hs.Vote = string(vote)
	return hs, nil
}

func encodeHardState(hs hardState) []byte {
	b := make([]byte, 0, 8+1+len(hs.Vote))
	b = binary.BigEndian.AppendUint64(b, hs.Term)
	b = appendLenBytes(b, []byte(hs.Vote), 1)
	return b
}

func encodeEntry(e Entry) []byte {
	b := make([]byte, 0, 21+len(e.Data))
	b = binary.BigEndian.AppendUint64(b, e.Index)
	b = binary.BigEndian.AppendUint64(b, e.Term)
	b = append(b, e.Kind)
	b = binary.BigEndian.AppendUint32(b, uint32(len(e.Data)))
	b = append(b, e.Data...)
	return b
}

func decodeEntry(b []byte) (Entry, error) {
	if len(b) < 21 {
		return Entry{}, fmt.Errorf("raft: bad entry record")
	}
	e := Entry{
		Index: binary.BigEndian.Uint64(b[0:8]),
		Term:  binary.BigEndian.Uint64(b[8:16]),
		Kind:  b[16],
	}
	dlen := int(binary.BigEndian.Uint32(b[17:21]))
	if len(b) != 21+dlen {
		return Entry{}, fmt.Errorf("raft: bad entry payload length")
	}
	if dlen > 0 {
		e.Data = append([]byte(nil), b[21:21+dlen]...)
	}
	return e, nil
}

func encodeVoters(vs []Voter) []byte {
	b := binary.BigEndian.AppendUint32(nil, uint32(len(vs)))
	for _, v := range vs {
		b = appendLenBytes(b, []byte(v.ID), 1)
		b = appendLenBytes(b, []byte(v.Addr), 2)
	}
	return b
}

func decodeVoters(b []byte) ([]Voter, error) {
	vs, _, err := decodeVotersRest(b)
	return vs, err
}

// decodeVotersRest decodes a voter list from the front of b and returns the
// bytes after it — the InstallSnapshot message continues with the payload, so
// its parser needs the remainder rather than a second length prefix.
func decodeVotersRest(b []byte) ([]Voter, []byte, error) {
	if len(b) < 4 {
		return nil, nil, fmt.Errorf("raft: bad conf record")
	}
	n := int(binary.BigEndian.Uint32(b[0:4]))
	// Each voter needs at least a length byte, an id and a two-byte length for
	// the empty address, so a count larger than the remaining bytes is a
	// corrupt (or hostile) record — refuse it before allocating for it.
	if n < 0 || n > len(b)-4 {
		return nil, nil, fmt.Errorf("raft: conf record claims %d voters in %d bytes", n, len(b)-4)
	}
	rest := b[4:]
	vs := make([]Voter, 0, n)
	for i := 0; i < n; i++ {
		id, r, err := readLenBytes(rest, 1)
		if err != nil {
			return nil, nil, err
		}
		addr, r2, err := readLenBytes(r, 2)
		if err != nil {
			return nil, nil, err
		}
		vs = append(vs, Voter{ID: string(id), Addr: string(addr)})
		rest = r2
	}
	return vs, rest, nil
}

// ---- membership change payload ----
//
// A conf change rides a log entry as its Data. The leader encodes it; every
// node decodes it and applies it to its own voter set when the entry commits.
// Unknown op bytes are rejected rather than ignored: silently skipping a
// membership change would leave the node with the wrong quorum.
//
// confAdd/confRemove are the operator's changes. confSet installs a whole voter
// list and is never issued by an operator: it is how the configuration enters
// the log at bootstrap, and how a snapshot carries the configuration of the
// point it was taken at. The initial configuration has to be in the log — the
// per-node conf record is enough for the nodes that were configured at the
// start (each writes its own copy) and for nobody else: a member added at
// runtime catches up by replicating the leader's log or a snapshot, and would
// otherwise see no configuration entry except the one adding itself.

const (
	confAdd    byte = 1
	confRemove byte = 2
	confSet    byte = 3
)

type confChange struct {
	op byte
	v  Voter
	// voters is the complete voter set, for confSet only.
	voters []Voter
}

func encodeConfChange(c confChange) []byte {
	b := []byte{c.op}
	if c.op == confSet {
		return append(b, encodeVoters(c.voters)...)
	}
	b = appendLenBytes(b, []byte(c.v.ID), 1)
	b = appendLenBytes(b, []byte(c.v.Addr), 2)
	return b
}

func decodeConfChange(b []byte) (confChange, error) {
	if len(b) < 1 {
		return confChange{}, fmt.Errorf("raft: empty conf change")
	}
	op := b[0]
	switch op {
	case confSet:
		vs, err := decodeVoters(b[1:])
		if err != nil {
			return confChange{}, err
		}
		if len(vs) == 0 {
			return confChange{}, fmt.Errorf("raft: empty voter set in conf change")
		}
		return confChange{op: op, voters: vs}, nil
	case confAdd, confRemove:
	default:
		return confChange{}, fmt.Errorf("raft: unknown conf change op %d", op)
	}
	id, rest, err := readLenBytes(b[1:], 1)
	if err != nil {
		return confChange{}, err
	}
	addr, _, err := readLenBytes(rest, 2)
	if err != nil {
		return confChange{}, err
	}
	return confChange{op: op, v: Voter{ID: string(id), Addr: string(addr)}}, nil
}

func appendLenBytes(b, v []byte, lenSize int) []byte {
	switch lenSize {
	case 1:
		b = append(b, byte(len(v)))
	case 2:
		b = binary.BigEndian.AppendUint16(b, uint16(len(v)))
	case 4:
		b = binary.BigEndian.AppendUint32(b, uint32(len(v)))
	}
	return append(b, v...)
}

func readLenBytes(b []byte, lenSize int) ([]byte, []byte, error) {
	var n int
	var off int
	switch lenSize {
	case 1:
		if len(b) < 1 {
			return nil, nil, fmt.Errorf("raft: short length prefix")
		}
		n = int(b[0])
		off = 1
	case 2:
		if len(b) < 2 {
			return nil, nil, fmt.Errorf("raft: short length prefix")
		}
		n = int(binary.BigEndian.Uint16(b[0:2]))
		off = 2
	case 4:
		if len(b) < 4 {
			return nil, nil, fmt.Errorf("raft: short length prefix")
		}
		n = int(binary.BigEndian.Uint32(b[0:4]))
		off = 4
	}
	if len(b) < off+n {
		return nil, nil, fmt.Errorf("raft: truncated field")
	}
	return b[off : off+n], b[off+n:], nil
}
