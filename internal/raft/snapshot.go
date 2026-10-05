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
//	magic "PURF" (4) | ver (1) | index u64 | term u64 | dataLen u64 | crc32c(data) u32 | data
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
	snapshotMagic  = "PURF"
	snapshotHeader = 4 + 1 + 8 + 8 + 8 + 4
	snapshotSuffix = ".snap"
	snapshotKeep   = 2
)

func snapshotDir(raftDir string) string { return filepath.Join(raftDir, "snapshot") }

// saveSnapshot writes a snapshot file atomically and prunes older ones.
func saveSnapshot(raftDir string, index, term uint64, data []byte) (string, error) {
	dir := snapshotDir(raftDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	hdr := make([]byte, snapshotHeader)
	copy(hdr[0:4], snapshotMagic)
	hdr[4] = 1
	binary.BigEndian.PutUint64(hdr[5:13], index)
	binary.BigEndian.PutUint64(hdr[13:21], term)
	binary.BigEndian.PutUint64(hdr[21:29], uint64(len(data)))
	binary.BigEndian.PutUint32(hdr[29:33], crc32c(data))

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
func loadLatestSnapshot(raftDir string) (index, term uint64, file string, data []byte, ok bool, err error) {
	dir := snapshotDir(raftDir)
	ents, rerr := os.ReadDir(dir)
	if rerr != nil {
		if os.IsNotExist(rerr) {
			return 0, 0, "", nil, false, nil
		}
		return 0, 0, "", nil, false, rerr
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
		i, t, d, rerr := readSnapshot(path)
		if rerr != nil {
			continue // skip a corrupt snapshot and try the next one
		}
		return i, t, path, d, true, nil
	}
	return 0, 0, "", nil, false, nil
}

func readSnapshot(path string) (index, term uint64, data []byte, err error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, nil, err
	}
	if len(raw) < snapshotHeader {
		return 0, 0, nil, fmt.Errorf("raft: snapshot %s is truncated", path)
	}
	if string(raw[0:4]) != snapshotMagic {
		return 0, 0, nil, fmt.Errorf("raft: snapshot %s has a bad magic", path)
	}
	index = binary.BigEndian.Uint64(raw[5:13])
	term = binary.BigEndian.Uint64(raw[13:21])
	dlen := binary.BigEndian.Uint64(raw[21:29])
	want := binary.BigEndian.Uint32(raw[29:33])
	if uint64(len(raw)-snapshotHeader) != dlen {
		return 0, 0, nil, fmt.Errorf("raft: snapshot %s length mismatch", path)
	}
	data = raw[snapshotHeader:]
	if crc32c(data) != want {
		return 0, 0, nil, fmt.Errorf("raft: snapshot %s failed crc", path)
	}
	return index, term, data, nil
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
