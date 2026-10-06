// snapshot.go — single-file snapshots of the replicated state machine.
//
// Snapshots are written to <raftDir>/snapshot/<index>.snap. There is no
// meta.json pointer: the newest usable snapshot is simply the highest-numbered
// file whose header and payload CRC check out. Removing the pointer removes a
// crash window (an atomic-ish rename plus a second metadata write that could
// disagree with each other).
//
// File layout:
//
//	magic "PURF" (4) | ver (1) | index u64 | term u64
//	| confLen u32 | voters | dataLen u64 | crc32c(data) u32 | data
//
// The voter set is part of the snapshot because the configuration is part of
// the log: a snapshot replaces the log up to its index, so a node that catches
// up from one (a member added at runtime, either because the leader has already
// compacted the entries it lacks or because it is far behind) would otherwise
// lose the configuration that log carried. The set stored here is the one in
// force at the snapshot's index.
package raft

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	snapshotMagic   = "PURF"
	snapshotVersion = 2
	// snapshotHeader is the fixed part of the header; the voter set it points
	// at (confLen bytes) sits between it and the payload's length/crc.
	snapshotHeader = 4 + 1 + 8 + 8 + 4 + 8 + 4
	snapshotSuffix = ".snap"
	snapshotKeep   = 2
)

func snapshotDir(raftDir string) string { return filepath.Join(raftDir, "snapshot") }

// saveSnapshot writes a snapshot file atomically and prunes older ones.
func saveSnapshot(raftDir string, index, term uint64, voters []Voter, data []byte) (string, error) {
	dir := snapshotDir(raftDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	conf := encodeVoters(voters)
	hdr := make([]byte, snapshotHeader+len(conf))
	copy(hdr[0:4], snapshotMagic)
	hdr[4] = snapshotVersion
	binary.BigEndian.PutUint64(hdr[5:13], index)
	binary.BigEndian.PutUint64(hdr[13:21], term)
	binary.BigEndian.PutUint32(hdr[21:25], uint32(len(conf)))
	copy(hdr[25:25+len(conf)], conf)
	off := 25 + len(conf)
	binary.BigEndian.PutUint64(hdr[off:off+8], uint64(len(data)))
	binary.BigEndian.PutUint32(hdr[off+8:off+12], crc32c(data))

	final := filepath.Join(dir, fmt.Sprintf("%020d%s", index, snapshotSuffix))
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return "", err
	}
	if _, err := f.Write(hdr); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return "", err
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return "", err
	}
	if err := syncDir(dir); err != nil {
		return "", err
	}
	pruneSnapshots(raftDir)
	return final, nil
}

// loadLatestSnapshot returns the newest valid snapshot, if any.
func loadLatestSnapshot(raftDir string) (index, term uint64, file string, voters []Voter, data []byte, ok bool, err error) {
	dir := snapshotDir(raftDir)
	ents, rerr := os.ReadDir(dir)
	if rerr != nil {
		if os.IsNotExist(rerr) {
			return 0, 0, "", nil, nil, false, nil
		}
		return 0, 0, "", nil, nil, false, rerr
	}
	var idxs []uint64
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), snapshotSuffix) {
			continue
		}
		base := strings.TrimSuffix(e.Name(), snapshotSuffix)
		n, perr := strconv.ParseUint(base, 10, 64)
		if perr != nil {
			continue
		}
		idxs = append(idxs, n)
	}
	sort.Slice(idxs, func(i, j int) bool { return idxs[i] > idxs[j] })
	for _, n := range idxs {
		path := filepath.Join(dir, fmt.Sprintf("%020d%s", n, snapshotSuffix))
		i, t, vs, d, rerr := readSnapshot(path)
		if rerr != nil {
			continue // skip a corrupt snapshot and try the next one
		}
		return i, t, path, vs, d, true, nil
	}
	return 0, 0, "", nil, nil, false, nil
}

func readSnapshot(path string) (index, term uint64, voters []Voter, data []byte, err error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, nil, nil, err
	}
	if len(raw) < snapshotHeader {
		return 0, 0, nil, nil, fmt.Errorf("raft: snapshot %s is truncated", path)
	}
	if string(raw[0:4]) != snapshotMagic {
		return 0, 0, nil, nil, fmt.Errorf("raft: snapshot %s has a bad magic", path)
	}
	if raw[4] != snapshotVersion {
		return 0, 0, nil, nil, fmt.Errorf("raft: snapshot %s is version %d, this build writes %d (delete the data directory and restart if it predates the format)", path, raw[4], snapshotVersion)
	}
	index = binary.BigEndian.Uint64(raw[5:13])
	term = binary.BigEndian.Uint64(raw[13:21])
	clen := int(binary.BigEndian.Uint32(raw[21:25]))
	off := 25
	if len(raw) < off+clen+12 {
		return 0, 0, nil, nil, fmt.Errorf("raft: snapshot %s is truncated", path)
	}
	if clen > 0 {
		if voters, err = decodeVoters(raw[off : off+clen]); err != nil {
			return 0, 0, nil, nil, fmt.Errorf("raft: snapshot %s: %w", path, err)
		}
	}
	off += clen
	dlen := binary.BigEndian.Uint64(raw[off : off+8])
	want := binary.BigEndian.Uint32(raw[off+8 : off+12])
	off += 12
	if uint64(len(raw)-off) != dlen {
		return 0, 0, nil, nil, fmt.Errorf("raft: snapshot %s length mismatch", path)
	}
	data = raw[off:]
	if crc32c(data) != want {
		return 0, 0, nil, nil, fmt.Errorf("raft: snapshot %s failed crc", path)
	}
	return index, term, voters, data, nil
}

// pruneSnapshots keeps the newest snapshotKeep snapshots and removes the rest.
func pruneSnapshots(raftDir string) {
	dir := snapshotDir(raftDir)
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	var idxs []uint64
	for _, e := range ents {
		if e.IsDir() || !strings.HasSuffix(e.Name(), snapshotSuffix) {
			continue
		}
		n, perr := strconv.ParseUint(strings.TrimSuffix(e.Name(), snapshotSuffix), 10, 64)
		if perr != nil {
			continue
		}
		idxs = append(idxs, n)
	}
	sort.Slice(idxs, func(i, j int) bool { return idxs[i] > idxs[j] })
	for i := snapshotKeep; i < len(idxs); i++ {
		os.Remove(filepath.Join(dir, fmt.Sprintf("%020d%s", idxs[i], snapshotSuffix)))
	}
}
