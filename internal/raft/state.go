// state.go — the run loop and every protocol state transition.
//
// One goroutine owns all mutable protocol state. RPC handlers, replicators and
// callers never touch it directly: they post an event and wait for a reply on a
// buffered channel. The election/heartbeat/snapshot timers live in the loop's
// own select and are held in Node fields so any transition can reset them —
// resetting a timer from a handler can therefore never race with the loop (the
// class of bug that shows up as "the leader is fine but Apply always times
// out").
package raft

import (
	"time"
)

// ---- events ----

type applyReq struct {
	cmd  []byte
	resp chan applyResp
}

type applyResp struct {
	value any
	err   error
}

type barrierReq struct {
	resp chan error
}

type requestVoteReq struct {
	msg  requestVote
	resp chan requestVoteResp
}

type appendEntriesReq struct {
	msg  appendEntries
	resp chan appendEntriesResp
}

type installSnapshotReq struct {
	msg  installSnapshot
	resp chan installSnapshotResp
}

type voteResult struct {
	peer    string
	term    uint64
	granted bool
	err     error
}

type appendResult struct {
	peer string
	term uint64
	resp appendEntriesResp
	err  error
}

type installResult struct {
	peer string
	term uint64
	resp installSnapshotResp
	err  error
}

type snapshotDone struct {
	index uint64
	term  uint64
	file  string
	err   error
}

type applyWaiter struct {
	index uint64
	ch    chan applyResp
}

type barrierWait struct {
	index uint64
	ch    chan error
}

// pendingAck is a follower AppendEntries reply waiting for its record to be
// fsync'd. Deferring the reply keeps the run loop off the durability critical
// path.
type pendingAck struct {
	lsn          uint64
	leaderCommit uint64
	resp         chan appendEntriesResp
}

// ---- timers ----

func stopTimer(t *time.Timer) {
	if t == nil {
		return
	}
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}

func resetTimer(t *time.Timer, d time.Duration) {
	if t == nil {
		return
	}
	stopTimer(t)
	t.Reset(d)
}

// ---- the loop ----

func (n *Node) run() {
	defer close(n.loopDone)
	n.electionTimer = time.NewTimer(n.randomElectionTimeout())
	n.heartbeatTimer = time.NewTimer(n.cfg.HeartbeatTimeout)
	stopTimer(n.heartbeatTimer)
	stopTimer(n.snapTimer)
	stopTimer(n.confTimer)
	defer stopTimer(n.electionTimer)
	defer stopTimer(n.heartbeatTimer)
	defer stopTimer(n.snapTimer)
	defer stopTimer(n.confTimer)
	n.armTimers(time.Now())

	for {
		select {
		case <-n.done:
			n.onShutdown()
			return
		case ev := <-n.events:
			n.handleEvent(ev)
		case req := <-n.applyCh:
			n.onApplyBatch(req)
		case req := <-n.confCh:
			n.onConfRequest(req)
		case <-n.durableSig:
			n.onDurable(n.durableSeq.Load())
		case <-n.electionTimer.C:
			n.onElectionTimeout()
		case <-n.heartbeatTimer.C:
			n.onHeartbeatTick()
		case <-n.snapTimer.C:
			n.onSnapshotTick()
		case <-n.confTimer.C:
			n.sweepConfDeadlines(time.Now())
		}
		// Every case above can move `applied` or the conf deadlines, which is
		// what decides whether the two on-demand timers are needed at all; one
		// re-evaluation per loop turn keeps them exact without scattering arm
		// calls through the handlers. Re-arming with a duration measured to the
		// SAME absolute deadline (or the snapArmed guard) never pushes a timer's
		// firing time out, so events cannot starve either timer.
		n.armTimers(time.Now())
	}
}

// armTimers re-arms the two on-demand run-loop timers after loop work that can
// change whether they are needed. The snapshot backstop exists only while
// applied entries sit ahead of the last snapshot; the membership sweep exists
// only while a change has an outstanding deadline. An idle node holds neither,
// so it wakes for no timer at all — these two plus the WAL's group-commit
// window were the process's entire idle wakeups.
func (n *Node) armTimers(now time.Time) {
	si, _, _ := n.log.Snapshot()
	if n.applied > si {
		if !n.snapArmed {
			n.snapArmed = true
			n.snapTimer.Reset(n.cfg.SnapshotInterval)
		}
	} else if n.snapArmed {
		n.snapArmed = false
		stopTimer(n.snapTimer)
	}
	if n.confNextDeadline.IsZero() {
		stopTimer(n.confTimer)
		return
	}
	d := n.confNextDeadline.Sub(now)
	if d < confSweepInterval {
		d = confSweepInterval
	}
	n.confTimer.Reset(d)
}

// onSnapshotTick is the time backstop: snapshot what has been applied even if
// the entry-count threshold was never reached. The loop's armTimers re-arms
// (the async snapshot has not landed yet) or disarms (nothing sits ahead of
// the snapshot) after this case returns.
func (n *Node) onSnapshotTick() {
	n.snapArmed = false
	if n.applied > 0 {
		n.maybeSnapshot(true)
	}
}

func (n *Node) handleEvent(ev any) {
	switch e := ev.(type) {
	case *barrierReq:
		n.onBarrier(e)
	case *requestVoteReq:
		n.onRequestVote(e)
	case *appendEntriesReq:
		n.onAppendEntries(e)
	case *installSnapshotReq:
		n.onInstallSnapshot(e)
	case voteResult:
		n.onVoteResult(e)
	case appendResult:
		n.onAppendResult(e)
	case installResult:
		n.onInstallResult(e)
	case snapshotDone:
		n.onSnapshotDone(e)
	default:
		n.logger.Warnf("raft: unknown event %T", ev)
	}
}

// ---- elections ----

func (n *Node) onElectionTimeout() {
	if n.state == Leader {
		resetTimer(n.electionTimer, n.randomElectionTimeout())
		return
	}
	// A node that knows no configuration must not stand for election. Its
	// voter set is empty because it has not learned the cluster's
	// configuration yet — a runtime-joined seed before the leader's first
	// entries/snapshot reach it, or a node whose data directory was rebuilt
	// while its id is still a member. quorum(0) is 1, so such a node would
	// elect itself on the spot: it bumps the term (forcing the real leader to
	// step down) and, having no voters to replicate to, writes a no-op at
	// index 1 of its own log. A lower-term entry never overwrites an existing
	// one, so the cluster's real index-1 configuration entry can then never
	// reach it: it applies everything above that point with no configuration
	// as the base and ends up with a partial voter set (observed:
	// [node-4..node-7] on a node whose own id was node-3) — and on the next
	// restart the binary refuses to start it, because it is not in the
	// membership it recorded.
	//
	// The configuration arrives through AppendEntries / InstallSnapshot, so
	// waiting is all this node has to do.
	if len(n.voters) == 0 {
		if !n.noConfWarned {
			n.noConfWarned = true
			n.logger.Warnf("raft: %s knows no configuration yet; waiting for the leader instead of standing for election", n.cfg.NodeID)
		}
		resetTimer(n.electionTimer, n.randomElectionTimeout())
		return
	}
	n.becomeCandidate()
}

func (n *Node) becomeCandidate() {
	n.state = Candidate
	n.term++
	n.vote = n.cfg.NodeID
	n.leaderID = ""
	n.votes = map[string]bool{n.cfg.NodeID: true}

	if err := n.log.setHardState(hardState{Term: n.term, Vote: n.vote}); err != nil {
		n.logger.Errorf("raft: persist hardstate: %v", err)
	}
	resetTimer(n.electionTimer, n.randomElectionTimeout())
	n.publishView()

	if quorum(len(n.voters)) <= 1 {
		n.becomeLeader()
		return
	}
	lastIdx, lastTerm := n.log.LastIndex(), n.log.LastTerm()
	for _, v := range n.voters {
		if v.ID == n.cfg.NodeID {
			continue
		}
		go n.sendRequestVote(v.ID, n.term, lastIdx, lastTerm)
	}
}

func (n *Node) onVoteResult(r voteResult) {
	if r.err != nil {
		n.logger.Debugf("raft: vote request to %s failed: %v", r.peer, r.err)
		return
	}
	if r.term > n.term {
		n.adoptTerm(r.term)
		n.publishView()
		return
	}
	if n.state != Candidate || r.term != n.term || !r.granted {
		return
	}
	if n.votes == nil {
		n.votes = map[string]bool{}
	}
	n.votes[r.peer] = true
	if len(n.votes) >= quorum(len(n.voters)) {
		n.becomeLeader()
	}
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.leaderID = n.cfg.NodeID
	n.votes = nil
	stopTimer(n.electionTimer)
	resetTimer(n.heartbeatTimer, n.cfg.HeartbeatTimeout)

	last := n.log.LastIndex()
	for _, v := range n.voters {
		if v.ID == n.cfg.NodeID {
			continue
		}
		n.startReplicator(v.ID, v.Addr)
	}
	// Learners that were accepted before this node became leader keep being
	// replicated to: dropping them here would strand a half-added member.
	n.learnerMu.RLock()
	learners := append([]Voter(nil), n.learners...)
	n.learnerMu.RUnlock()
	for _, l := range learners {
		if l.ID == n.cfg.NodeID || n.isVoter(l.ID) {
			continue
		}
		n.startReplicator(l.ID, l.Addr)
	}
	// Commit an entry from the current term (a no-op) so entries left over
	// from previous terms can be committed safely.
	if err := n.log.appendEntries([]Entry{{Index: last + 1, Term: n.term, Kind: KindNoop}}); err != nil {
		n.logger.Errorf("raft: append leader no-op: %v", err)
	}
	n.publishView()
	n.kickAll()
	n.maybeAdvanceCommit()
	n.applyCommitted()
}

func (n *Node) onHeartbeatTick() {
	// The heartbeat timer exists only to drive a leader's periodic kick. A
	// follower must not re-arm it: it is started by becomeLeader, and without
	// this guard a node that once led (then stepped down) would keep waking
	// every heartbeat interval doing nothing.
	if n.state != Leader {
		return
	}
	n.kickAll()
	n.advancePendingAdd()
	n.maybeAdvanceCommit()
	n.applyCommitted()
	resetTimer(n.heartbeatTimer, n.cfg.HeartbeatTimeout)
}

func (n *Node) adoptTerm(term uint64) {
	n.term = term
	n.vote = ""
	n.state = Follower
	n.leaderID = ""
	n.votes = nil
	if err := n.log.setHardState(hardState{Term: n.term, Vote: n.vote}); err != nil {
		n.logger.Errorf("raft: persist hardstate: %v", err)
	}
	// A follower must always be able to time out into an election, including
	// when it was leader a moment ago (whose election timer was stopped).
	resetTimer(n.electionTimer, n.randomElectionTimeout())
}

func (n *Node) logUpToDate(idx, term uint64) bool {
	myTerm := n.log.LastTerm()
	if term != myTerm {
		return term > myTerm
	}
	return idx >= n.log.LastIndex()
}

// ---- request vote ----

func (n *Node) onRequestVote(req *requestVoteReq) {
	m := req.msg
	if m.Term < n.term {
		req.resp <- requestVoteResp{Term: n.term, Granted: false}
		return
	}
	if m.Term > n.term {
		n.adoptTerm(m.Term)
	}
	granted := false
	if (n.vote == "" || n.vote == m.CandidateID) && n.logUpToDate(m.LastLogIndex, m.LastLogTerm) {
		granted = true
		n.vote = m.CandidateID
		if err := n.log.setHardState(hardState{Term: n.term, Vote: n.vote}); err != nil {
			n.logger.Errorf("raft: persist vote: %v", err)
			granted = false
		} else {
			resetTimer(n.electionTimer, n.randomElectionTimeout())
		}
	}
	n.publishView()
	req.resp <- requestVoteResp{Term: n.term, Granted: granted}
}

// ---- append entries ----

func (n *Node) onAppendEntries(req *appendEntriesReq) {
	m := req.msg
	if m.Term < n.term {
		req.resp <- appendEntriesResp{Term: n.term, Success: false, LastIndex: n.log.LastIndex()}
		return
	}
	if m.Term > n.term {
		n.adoptTerm(m.Term)
	}
	if n.state != Follower {
		n.state = Follower
		n.votes = nil
	}
	n.leaderID = m.LeaderID
	resetTimer(n.electionTimer, n.randomElectionTimeout())

	last := n.log.LastIndex()
	if m.PrevIndex > last {
		req.resp <- appendEntriesResp{Term: n.term, Success: false, LastIndex: last, ConflictIndex: last + 1}
		n.publishView()
		return
	}
	if m.PrevIndex > 0 && n.log.Term(m.PrevIndex) != m.PrevTerm {
		t := n.log.Term(m.PrevIndex)
		ci := m.PrevIndex
		first := n.log.FirstIndex()
		for ci > first && n.log.Term(ci-1) == t {
			ci--
		}
		req.resp <- appendEntriesResp{Term: n.term, Success: false, LastIndex: last, ConflictIndex: ci}
		n.publishView()
		return
	}

	var ackLSN uint64
	if len(m.Entries) > 0 {
		es := m.Entries
		if es[0].Index <= n.commitIndex {
			// Never rewrite what is already committed: keep only the suffix
			// above the commit point. A correct leader rarely sends such a
			// prefix (a stale retry), and replaying it must not truncate
			// committed history.
			keep := make([]Entry, 0, len(es))
			for _, e := range es {
				if e.Index > n.commitIndex {
					keep = append(keep, e)
				}
			}
			es = keep
		}
		if len(es) > 0 {
			lsn, err := n.log.appendEntriesAsync(es)
			if err != nil {
				n.logger.Errorf("raft: append entries: %v", err)
				req.resp <- appendEntriesResp{Term: n.term, Success: false, LastIndex: n.log.LastIndex()}
				return
			}
			ackLSN = lsn
		}
	}

	if ackLSN == 0 {
		// Heartbeat, or everything the leader sent was already committed.
		n.advanceFollowerCommit(m.LeaderCommit)
		n.applyCommitted()
		n.publishView()
		req.resp <- appendEntriesResp{Term: n.term, Success: true, LastIndex: n.log.LastIndex()}
		return
	}

	// Durability before acknowledgement, without blocking the loop on I/O: the
	// reply is released when the flusher reports this record on disk.
	n.pendingAcks = append(n.pendingAcks, pendingAck{
		lsn:          ackLSN,
		leaderCommit: m.LeaderCommit,
		resp:         req.resp,
	})
	n.publishView()
}

// advanceFollowerCommit moves the follower's commit index up to what the
// leader has committed (never past its own log).
func (n *Node) advanceFollowerCommit(leaderCommit uint64) {
	if leaderCommit <= n.commitIndex {
		return
	}
	c := leaderCommit
	if l := n.log.LastIndex(); c > l {
		c = l
	}
	if c > n.commitIndex {
		n.commitIndex = c
	}
}

// onDurable releases every deferred follower acknowledgement and commit step
// whose record is now on disk.
func (n *Node) onDurable(lsn uint64) {
	if lsn > n.durableLSN {
		n.durableLSN = lsn
	}
	if len(n.pendingAcks) == 0 {
		return
	}
	kept := n.pendingAcks[:0]
	for _, pa := range n.pendingAcks {
		if pa.lsn > n.durableLSN {
			kept = append(kept, pa)
			continue
		}
		n.advanceFollowerCommit(pa.leaderCommit)
		n.applyCommitted()
		n.publishView()
		pa.resp <- appendEntriesResp{Term: n.term, Success: true, LastIndex: n.log.LastIndex()}
	}
	n.pendingAcks = kept
}

func (n *Node) onAppendResult(r appendResult) {
	if r.err != nil {
		n.logger.Debugf("raft: append to %s failed: %v", r.peer, r.err)
		return
	}
	if r.resp.Term > n.term {
		n.adoptTerm(r.resp.Term)
		n.publishView()
		return
	}
	if n.state != Leader || n.term != r.term {
		return
	}
	if r.resp.Success {
		mi := r.resp.LastIndex
		if l := n.log.LastIndex(); mi > l {
			mi = l
		}
		if mi > n.matchIndex[r.peer] {
			n.matchIndex[r.peer] = mi
		}
		n.nextIndex[r.peer] = n.matchIndex[r.peer] + 1
		n.finishConf(r.peer)
		n.advancePendingAdd()
		n.maybeAdvanceCommit()
		n.applyCommitted()
		if n.matchIndex[r.peer] < n.log.LastIndex() {
			n.kick(r.peer)
		}
	} else {
		ci := r.resp.ConflictIndex
		if ci == 0 || ci > r.resp.LastIndex+1 {
			ci = r.resp.LastIndex + 1
		}
		if ci < 1 {
			ci = 1
		}
		n.nextIndex[r.peer] = ci
		n.kick(r.peer)
	}
	n.publishView()
}

// ---- install snapshot ----

func (n *Node) onInstallSnapshot(req *installSnapshotReq) {
	m := req.msg
	if m.Term < n.term {
		req.resp <- installSnapshotResp{Term: n.term, Success: false, LastIndex: n.log.LastIndex()}
		return
	}
	if m.Term > n.term {
		n.adoptTerm(m.Term)
	}
	if n.state != Follower {
		n.state = Follower
		n.votes = nil
	}
	n.leaderID = m.LeaderID
	resetTimer(n.electionTimer, n.randomElectionTimeout())

	// Refuse a snapshot that is not NEWER than what this node already holds.
	// Adopting one rolls the node backward in three ways at once: the
	// snapshot's voter set replaces the live configuration (a member added at
	// runtime disappears from it while the slot table that named it stays
	// applied), the log is reset to the snapshot's index (dropping entries
	// above it — possibly committed ones), and the FSM is restored to an older
	// state, while applied/commit only ever move forward. The node then holds
	// a state machine that neither its configuration nor its log supports.
	// Only a stale leader can send such a message, so refusing is safe; the
	// sender falls back to ordinary log replication (see onInstallResult).
	if si, _, _ := n.log.Snapshot(); m.LastIndex <= si {
		n.logger.Warnf("raft: refusing out-of-date snapshot from %s at index %d/term %d: this node already has a snapshot at index %d",
			m.LeaderID, m.LastIndex, m.LastTerm, si)
		req.resp <- installSnapshotResp{Term: n.term, Success: false, LastIndex: n.log.LastIndex()}
		return
	}
	if m.LastIndex < n.commitIndex {
		n.logger.Warnf("raft: refusing out-of-date snapshot from %s at index %d/term %d: this node already committed to index %d",
			m.LeaderID, m.LastIndex, m.LastTerm, n.commitIndex)
		req.resp <- installSnapshotResp{Term: n.term, Success: false, LastIndex: n.log.LastIndex()}
		return
	}

	if err := n.fsm.Restore(m.Data); err != nil {
		n.logger.Errorf("raft: restore installed snapshot at %d: %v", m.LastIndex, err)
		req.resp <- installSnapshotResp{Term: n.term, Success: false, LastIndex: n.log.LastIndex()}
		return
	}
	file, err := saveSnapshot(n.cfg.DataDir, m.LastIndex, m.LastTerm, m.Voters, m.Data)
	if err != nil {
		n.logger.Errorf("raft: persist installed snapshot at %d: %v", m.LastIndex, err)
		req.resp <- installSnapshotResp{Term: n.term, Success: false, LastIndex: n.log.LastIndex()}
		return
	}
	// The snapshot carries the configuration of the point it was taken at, and
	// the log that held it is being replaced wholesale — so adopt it, and
	// persist it the same way a committed conf entry does. Without this a node
	// whose first catch-up is a snapshot (the entries it lacks are already
	// compacted on the leader) would know no voters at all, and the only
	// configuration entry it would ever apply is the one that adds itself.
	if len(m.Voters) > 0 {
		n.applyConfLocally(confChange{op: confSet, voters: m.Voters}, m.LastIndex)
		if serr := n.log.setVoters(n.Voters()); serr != nil {
			n.logger.Errorf("raft: persist membership from installed snapshot: %v", serr)
		}
	}
	n.log.resetToSnapshot(m.LastIndex, m.LastTerm, file)
	n.commitIndex = maxU64(n.commitIndex, m.LastIndex)
	n.applied = maxU64(n.applied, m.LastIndex)
	if err := n.log.compact(); err != nil {
		n.logger.Warnf("raft: compact wal: %v", err)
	}
	n.publishView()
	req.resp <- installSnapshotResp{Term: n.term, Success: true, LastIndex: n.log.LastIndex()}
}

func (n *Node) onInstallResult(r installResult) {
	if r.err != nil {
		n.logger.Debugf("raft: install snapshot to %s failed: %v", r.peer, r.err)
		return
	}
	if r.resp.Term > n.term {
		n.adoptTerm(r.resp.Term)
		n.publishView()
		return
	}
	if n.state != Leader || n.term != r.term {
		return
	}
	if !r.resp.Success {
		// The follower refused the snapshot (it was not newer than what it
		// already holds — see onInstallSnapshot) and reported what it does
		// have. Re-sending the same file every round would loop forever, so
		// resume ordinary log replication from there — but only when our log
		// can still cover that range (the receiver's index must be above our
		// own snapshot, or AppendEntries has nothing to offer it).
		// matchIndex is deliberately untouched: only an accepted
		// AppendEntries raises it.
		if si, _, _ := n.log.Snapshot(); r.resp.LastIndex+1 > n.nextIndex[r.peer] && r.resp.LastIndex+1 > si {
			n.nextIndex[r.peer] = r.resp.LastIndex + 1
			n.kick(r.peer)
		}
		return
	}
	if r.resp.LastIndex > n.matchIndex[r.peer] {
		n.matchIndex[r.peer] = r.resp.LastIndex
	}
	n.nextIndex[r.peer] = n.matchIndex[r.peer] + 1
	n.maybeAdvanceCommit()
	n.applyCommitted()
	if n.matchIndex[r.peer] < n.log.LastIndex() {
		n.kick(r.peer)
	}
	n.publishView()
}

// ---- commit / apply ----

func (n *Node) maybeAdvanceCommit() {
	if n.state != Leader {
		return
	}
	last := n.log.LastIndex()
	for idx := last; idx > n.commitIndex; idx-- {
		if n.log.Term(idx) != n.term {
			break // only entries from the current term may be committed by count
		}
		count := 1
		for _, v := range n.voters {
			if v.ID == n.cfg.NodeID {
				continue
			}
			if n.matchIndex[v.ID] >= idx {
				count++
			}
		}
		if count >= quorum(len(n.voters)) {
			n.commitIndex = idx
			break
		}
	}
}

func (n *Node) applyCommitted() {
	// Invariant: commit can never be ahead of the log. Clamp defensively so a
	// waiter can never be left waiting on an index that does not exist.
	if last := n.log.LastIndex(); n.commitIndex > last {
		n.commitIndex = last
	}
	changed := false
	for n.applied < n.commitIndex {
		next := n.applied + 1
		e, ok := n.log.Entry(next)
		if !ok {
			// Either the entry is already covered by the snapshot or the log
			// lost it; either way a waiter must never be left hanging.
			n.applied = next
			n.respondAt(next, nil)
			changed = true
			continue
		}
		var res any
		switch e.Kind {
		case KindCommand:
			res = n.fsm.Apply(e)
		case KindConf:
			// A membership change is local configuration, not FSM state: the
			// consensus layer applies it to itself, and nothing is handed to
			// the state machine.
			n.onConfCommitted(e, next)
		}
		n.applied = next
		n.respondAt(next, res)
		changed = true
	}
	if changed {
		n.publishView()
		n.releaseBarriers()
		if n.applied > 0 {
			n.maybeSnapshot(false)
		}
	}
}

func (n *Node) respondAt(index uint64, res any) {
	if len(n.waiters) == 0 {
		return
	}
	kept := n.waiters[:0]
	for _, w := range n.waiters {
		if w.index <= index {
			w.ch <- applyResp{value: res}
			continue
		}
		kept = append(kept, w)
	}
	n.waiters = kept
}

func (n *Node) releaseBarriers() {
	if len(n.barriers) == 0 {
		return
	}
	kept := n.barriers[:0]
	for _, b := range n.barriers {
		if b.index <= n.applied {
			b.ch <- nil
			continue
		}
		kept = append(kept, b)
	}
	n.barriers = kept
}

func (n *Node) onApplyBatch(first *applyReq) {
	batch := []*applyReq{first}
	const maxBatch = 256
	for len(batch) < maxBatch {
		select {
		case req := <-n.applyCh:
			batch = append(batch, req)
			continue
		default:
		}
		break
	}

	if n.state != Leader {
		for _, r := range batch {
			r.resp <- applyResp{err: ErrNotLeader}
		}
		return
	}

	// All of the batch's entries go into the WAL together: one write, one
	// fsync, one durability wait for the whole group. That is the whole point
	// of group commit — a batch of concurrent applies must not serialise into
	// one fsync each.
	base := n.log.LastIndex()
	entries := make([]Entry, 0, len(batch))
	for i, r := range batch {
		entries = append(entries, Entry{Index: base + uint64(i) + 1, Term: n.term, Kind: KindCommand, Data: r.cmd})
	}
	// Append to the log and hand the entries to the replicators before waiting
	// for our own fsync: the follower round trip then overlaps the local
	// durability wait instead of being serialised behind it. The commit step
	// below still waits for durability, so nothing is acknowledged early.
	lsn, err := n.log.appendEntriesAsync(entries)
	if err != nil {
		for _, r := range batch {
			r.resp <- applyResp{err: err}
		}
		return
	}
	for i, r := range batch {
		n.waiters = append(n.waiters, applyWaiter{index: base + uint64(i) + 1, ch: r.resp})
	}
	n.publishView()
	n.kickAll()
	if werr := n.log.wal.Wait(lsn); werr != nil {
		n.logger.Errorf("raft: waiting for local durability: %v", werr)
	}
	n.maybeAdvanceCommit()
	n.applyCommitted()
}

func (n *Node) onBarrier(req *barrierReq) {
	target := n.commitIndex
	if n.applied >= target {
		req.resp <- nil
		return
	}
	n.barriers = append(n.barriers, barrierWait{index: target, ch: req.resp})
}

// ---- snapshots ----

func (n *Node) maybeSnapshot(force bool) {
	if n.snapRunning || n.applied <= 0 {
		return
	}
	si, _, _ := n.log.Snapshot()
	if n.applied <= si {
		return
	}
	if !force && n.applied-si < n.cfg.SnapshotThreshold {
		return
	}
	data, err := n.fsm.Snapshot()
	if err != nil {
		n.logger.Warnf("raft: fsm snapshot: %v", err)
		return
	}
	index, term := n.applied, n.log.Term(n.applied)
	// The configuration in force at the snapshot point rides the snapshot: a
	// node that catches up from it never sees the log entries that carried it.
	voters := n.Voters()
	n.snapRunning = true
	// Tracked so Close can wait for it: the write lands in the node's data
	// directory, and a shutdown that returns while a file is still being
	// renamed into place leaves a snapshot nobody accounted for.
	n.snapWG.Add(1)
	go func() {
		defer n.snapWG.Done()
		file, serr := saveSnapshot(n.cfg.DataDir, index, term, voters, data)
		select {
		case n.events <- snapshotDone{index: index, term: term, file: file, err: serr}:
		case <-n.done:
		}
	}()
}

func (n *Node) onSnapshotDone(d snapshotDone) {
	n.snapRunning = false
	if d.err != nil {
		n.logger.Warnf("raft: write snapshot at %d: %v", d.index, d.err)
		return
	}
	si, _, _ := n.log.Snapshot()
	if d.index <= si {
		return
	}
	n.log.setSnapshot(d.index, d.term, d.file)
	if err := n.log.compact(); err != nil {
		n.logger.Warnf("raft: compact wal: %v", err)
	}
	n.publishView()
}

// ---- shutdown ----

func (n *Node) onShutdown() {
	for _, r := range n.replicators {
		close(r.stop)
	}
	n.replicators = map[string]*replicator{}
	for _, w := range n.waiters {
		w.ch <- applyResp{err: ErrClosed}
	}
	n.waiters = nil
	for _, b := range n.barriers {
		b.ch <- ErrClosed
	}
	n.barriers = nil
	// Drain anything that arrived after the loop stopped selecting.
	for {
		select {
		case r := <-n.applyCh:
			r.resp <- applyResp{err: ErrClosed}
		default:
			return
		}
	}
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
