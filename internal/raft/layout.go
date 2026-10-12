package raft

// LayoutSlice is the control plane's answer to a data-plane node (role=learner,
// see DESIGN §12.4) catching up on the replicated metadata: either the entries
// that follow the index the caller already applied, or — when this group's
// history no longer holds that point — the bytes to rebuild from plus the index
// they stand at.
//
// The learner is not a voter: it never appears in a quorum, never receives a
// vote request and never applies an uncommitted entry. What it needs is exactly
// the state machine's input, so the slice carries committed entries only.
type LayoutSlice struct {
	// Term and CommitIndex are this node's current view: the caller reports
	// them in its next request and uses CommitIndex as the "applied through"
	// horizon.
	Term        uint64
	CommitIndex uint64
	// FirstIndex is the lowest log index this node can still serve (snapshot
	// point + 1).
	FirstIndex uint64
	// Snapshot, when non-nil, supersedes Entries: the caller must reset its
	// derived state to exactly Data and resume at Index+1. Index 0 with empty
	// Data means "the group has no usable history for you: start empty".
	Snapshot *LayoutSnapshot
	// Entries are contiguous committed entries, in index order, starting at
	// the requested index+1.
	Entries []Entry
}

// LayoutSnapshot is a state-machine image as of Index (the same bytes the
// consensus layer hands a lagging voter when it installs a snapshot).
type LayoutSnapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// LayoutSlice serves a data-plane node's request for the replicated metadata
// after (from, fromTerm). It answers with entries when this node's log still
// covers from+1 and the term at `from` matches (the same consistency rule an
// AppendEntries carries); otherwise it answers with a snapshot, which is the
// only way back when the history diverged — a learner that stored a longer or
// differently-termed history (it joined this cluster keeping its old slot data,
// see DESIGN §12.4) cannot be brought to parity by an append.
//
// max bounds the entries returned per call; the caller asks again as the log
// grows.
func (n *Node) LayoutSlice(from, fromTerm uint64, max int) (LayoutSlice, error) {
	if max <= 0 {
		max = maxAppendBatch
	}
	v := n.readView()
	si, st, file := n.log.Snapshot()
	out := LayoutSlice{Term: v.term, CommitIndex: v.commit, FirstIndex: si + 1}

	// snapshot is the answer whenever an append cannot work: the image plus the
	// point it stands at. Index 0 / empty data is a legitimate answer — it says
	// "rebuild from nothing", which is what a node whose applied point is not in
	// this group's history must do.
	snapshot := func() (LayoutSlice, error) {
		snap := &LayoutSnapshot{Index: si, Term: st}
		if si > 0 && file != "" {
			idx, term, _, data, err := readSnapshot(file)
			if err != nil {
				return LayoutSlice{}, err
			}
			snap.Index, snap.Term, snap.Data = idx, term, data
		}
		out.Snapshot = snap
		out.Entries = nil
		return out, nil
	}

	switch {
	case from > v.commit:
		// The caller is ahead of this group: its history is not ours.
		return snapshot()
	case from == 0 && out.FirstIndex > 1:
		// Nothing applied yet on the caller's side and the log floor has moved
		// past index 1: only an image can put it on a real footing.
		return snapshot()
	case from > 0 && from+1 < out.FirstIndex:
		// The entries the caller needs are gone; resuming exactly at the
		// snapshot point (from == si) is still fine — termLocked knows it.
		return snapshot()
	case from > 0 && n.log.Term(from) != fromTerm:
		return snapshot()
	}

	want := from + 1
	entries := n.log.Entries(want, max)
	for len(entries) > 0 && entries[len(entries)-1].Index > v.commit {
		entries = entries[:len(entries)-1]
	}
	// The log clamps a request below its floor instead of failing; a caller that
	// gets entries starting later than it asked for would silently skip the gap,
	// so treat that (and an empty answer where the commit index promises one) as
	// needing an image instead.
	if v.commit >= want && (len(entries) == 0 || entries[0].Index != want) {
		return snapshot()
	}
	out.Entries = entries
	return out, nil
}
