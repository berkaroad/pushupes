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

// raft.go — the public membership API: runtime add/remove of cluster members.
//
// The change is a log entry of kind KindConf: the leader appends it, it
// replicates like any other entry, and each node folds it into its own voter set
// when the entry commits (never earlier — an uncommitted configuration is not a
// configuration). Commit is what makes it history, so a node that restarts
// re-derives exactly the same membership by replaying its log.
//
// Adding a member goes through a learner phase: the leader starts replicating to
// the new node and only appends the KindConf entry once the learner has caught
// up to the leader's commit index (or a wall-clock bound elapses, for a node
// that can never answer). Without that phase a newly added voter could count
// towards quorum while still holding an empty log — and the leader could then be
// replaced by a node that cannot serve reads.
package raft

import (
	"fmt"
	"time"
)

// AddMember adds a node to the cluster membership at runtime. It is
// leader-only: a follower answers ErrNotLeader, and nothing is forwarded. It
// blocks until the change is applied (or the timeout elapses) and is
// idempotent — adding a current member is a no-op.
func (n *Node) AddMember(id, addr string, timeout time.Duration) error {
	return n.changeMembership(confAdd, Voter{ID: id, Addr: addr}, timeout)
}

// RemoveMember drops a node from the cluster membership at runtime. Refusing to
// remove the last member keeps a cluster from being configured out of
// existence.
func (n *Node) RemoveMember(id string, timeout time.Duration) error {
	return n.changeMembership(confRemove, Voter{ID: id}, timeout)
}

// Members returns the current membership. A node that has not learned a
// configuration yet (it was started to be joined into a running cluster)
// reports its configured seed instead — minus this node itself, which is by
// definition not a member yet — so a caller can still tell there is a cluster
// out there to join without mistaking the seed for a live membership.
func (n *Node) Members() []Voter {
	n.votersMu.RLock()
	defer n.votersMu.RUnlock()
	if len(n.voters) > 0 {
		return append([]Voter(nil), n.voters...)
	}
	out := make([]Voter, 0, len(n.cfg.Voters))
	for _, v := range n.cfg.Voters {
		if v.ID != n.cfg.NodeID {
			out = append(out, v)
		}
	}
	return out
}

// learnerCatchUpBudget bounds the learner phase: long enough for a node with a
// wall of log to catch up over a healthy link, short enough that a node which
// will never answer cannot hold an operator's change open.
const learnerCatchUpBudget = 20 * time.Second

// confSweepInterval is how often the run loop checks whether a membership
// change has blown its deadline.
const confSweepInterval = 200 * time.Millisecond

// confWaiter is a membership change waiting for its entry to be applied. The
// loop answers it from onConfCommitted; the deadline is swept by the loop so a
// change that never commits still surfaces to its caller.
type confWaiter struct {
	ch         chan error
	deadline   time.Time
	cc         confChange
	timeoutMsg string
}

// confReq is a membership change posted to the run loop.
type confReq struct {
	cc      confChange
	resp    chan error
	timeout time.Duration
}

// changeMembership posts a membership change and waits for the run loop to
// apply it.
func (n *Node) changeMembership(op byte, v Voter, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	req := &confReq{cc: confChange{op: op, v: v}, resp: make(chan error, 1), timeout: timeout}
	select {
	case n.confCh <- req:
	case <-n.done:
		return ErrClosed
	}
	select {
	case err := <-req.resp:
		return err
	case <-n.done:
		return ErrClosed
	}
}

// onConfRequest validates a change against the current state and starts it.
// Every refusal lives here so the run loop stays the single authority: a
// follower answers ErrNotLeader, a remove cannot empty the cluster, and an add
// of a node that answers as somebody else is refused with the id it reports.
func (n *Node) onConfRequest(req *confReq) {
	if n.state != Leader {
		req.resp <- ErrNotLeader
		return
	}
	cc := req.cc
	switch cc.op {
	case confAdd:
		if cc.v.ID == "" || cc.v.Addr == "" {
			req.resp <- fmt.Errorf("%w: add voter needs an id and an address", ErrBadChange)
			return
		}
		if n.isVoter(cc.v.ID) {
			req.resp <- nil // idempotent
			return
		}
		if len(n.voters) == 0 {
			req.resp <- fmt.Errorf("%w: this node has no membership yet; it has to be joined into a running cluster", ErrBadChange)
			return
		}
		if other, ok := n.conflictFor(cc.v); ok {
			req.resp <- fmt.Errorf("%w: %s is already the peer address of member %q; a node that is not a member yet must be started with the -peers list of the running cluster",
				ErrBadChange, cc.v.Addr, other.ID)
			return
		}
	case confRemove:
		if cc.v.ID == "" {
			req.resp <- fmt.Errorf("%w: remove voter needs an id", ErrBadChange)
			return
		}
		if !n.isVoter(cc.v.ID) {
			req.resp <- nil // idempotent
			return
		}
		if len(n.voters) <= 1 {
			req.resp <- fmt.Errorf("%w: refusing to remove %s: it is the last voter", ErrBadChange, cc.v.ID)
			return
		}
	default:
		req.resp <- fmt.Errorf("%w: %d is not an operator-issued membership change", ErrBadChange, cc.op)
		return
	}

	if cc.op == confAdd {
		n.startAdd(cc, req)
		return
	}
	n.startRemove(cc, req)
}

// conflictFor reports an existing member reachable at the address a new node
// announces under a different id. That is the failure mode of starting a node
// that is not a member yet with the members' -peers list: it does not refuse to
// start any more (that list is only its bootstrap seed), so the mistake has to
// surface here instead of as two nodes fighting over one identity.
func (n *Node) conflictFor(v Voter) (Voter, bool) {
	if v.Addr == "" {
		return Voter{}, false
	}
	for _, m := range n.voters {
		if m.Addr == v.Addr && m.ID != v.ID {
			return m, true
		}
	}
	return Voter{}, false
}

// startAdd runs the learner phase: replicate to the new node, then commit the
// entry that makes it a voter.
func (n *Node) startAdd(cc confChange, req *confReq) {
	id := cc.v.ID
	already := false
	n.learnerMu.Lock()
	for _, l := range n.learners {
		if l.ID == id {
			already = true
		}
	}
	if !already {
		n.learners = append(n.learners, cc.v)
	}
	n.learnerMu.Unlock()

	n.pendingAddrs[id] = cc.v.Addr
	if !already {
		n.pendingAdd = id
		n.pendingCommit = n.commitIndex
	}
	n.ensureReplicator(id, cc.v.Addr)
	n.kick(id)

	// The change goes in once the learner's log has reached this node's
	// commit index, or when the budget runs out.
	n.waitConf(cc, req, time.Now().Add(learnerCatchUpBudget))
}

// startRemove appends the configuration entry immediately, but hands the
// removed member's replicator over to `closing` so it keeps receiving entries
// until it has the change in its own log (see finishConf).
func (n *Node) startRemove(cc confChange, req *confReq) {
	n.closing[cc.v.ID] = true
	n.waitConf(cc, req, time.Now().Add(req.timeout))
}

// advancePendingAdd promotes a caught-up learner: once its log has reached the
// commit index this node had when the add was accepted, the configuration
// entry that makes it a voter is safe to append.
func (n *Node) advancePendingAdd() {
	if n.pendingAdd == "" {
		return
	}
	if n.matchIndex[n.pendingAdd] < n.pendingCommit {
		return
	}
	id := n.pendingAdd
	v := Voter{ID: id, Addr: n.pendingAddrs[id]}
	if v.Addr == "" {
		if known, ok := n.voterByID(id); ok {
			v.Addr = known.Addr
		}
	}
	if v.Addr == "" {
		return
	}
	n.logger.Infof("raft: learner %s caught up (%d >= %d); promoting it to voter", id, n.matchIndex[id], n.pendingCommit)
	n.pendingAdd = ""
	cc := confChange{op: confAdd, v: v}
	n.waitConf(cc, &confReq{cc: cc, resp: make(chan error, 1)}, time.Now().Add(n.cfg.ApplyTimeout))
}

// waitConf appends the configuration entry and registers the waiter. It never
// blocks the run loop: the answer comes from onConfCommitted when the entry is
// applied, and from the conf deadline timer (swept by the loop) when it never
// commits. Blocking here would wedge the very loop that has to drive the
// replication the change depends on.
func (n *Node) waitConf(cc confChange, req *confReq, deadline time.Time) {
	idx := n.log.LastIndex() + 1
	if err := n.log.appendEntries([]Entry{{Index: idx, Term: n.term, Kind: KindConf, Data: encodeConfChange(cc)}}); err != nil {
		n.clearPending(cc)
		req.resp <- err
		return
	}
	n.confWaiters[idx] = confWaiter{ch: req.resp, deadline: deadline, cc: cc, timeoutMsg: "raft: membership change did not commit in time (it is in the log and commits once the cluster has quorum again)"}
	// A no-op of this term right behind it: a change appended by a fresh
	// leader cannot commit on its own (Raft commits entries of the current
	// term by counting only), and the no-op makes the change commit as soon
	// as the new configuration has reached a quorum.
	noop := idx + 1
	if _, err := n.log.appendEntriesAsync([]Entry{{Index: noop, Term: n.term, Kind: KindNoop}}); err != nil {
		n.logger.Warnf("raft: append no-op after conf change: %v", err)
	}
	n.confSeq.Store(noop)
	n.confNextDeadline = earliest(n.confNextDeadline, deadline)
	n.kickAll()
	n.maybeAdvanceCommit()
	n.applyCommitted()
}

// sweepConfDeadlines answers membership changes that never committed. It runs
// from the loop's select (see confTick), so a timed-out change surfaces to the
// caller without ever blocking the protocol.
func (n *Node) sweepConfDeadlines(now time.Time) {
	if n.confNextDeadline.IsZero() || now.Before(n.confNextDeadline) {
		return
	}
	next := time.Time{}
	for idx, w := range n.confWaiters {
		if now.Before(w.deadline) {
			next = earliest(next, w.deadline)
			continue
		}
		delete(n.confWaiters, idx)
		n.clearPending(w.cc)
		w.ch <- fmt.Errorf("%s", w.timeoutMsg)
	}
	n.confNextDeadline = next
}

func earliest(a, b time.Time) time.Time {
	if a.IsZero() || (!b.IsZero() && b.Before(a)) {
		return b
	}
	return a
}

// clearPending undoes the local bookkeeping a change that never committed
// installed: the learner, its pending flag, and the closing flag. confPending /
// closingAcked are deliberately untouched: a remove that DID commit keeps
// replicating to the member until it acknowledges, and that bookkeeping is
// cleared by dropClosing instead.
func (n *Node) clearPending(cc confChange) {
	delete(n.pendingAddrs, cc.v.ID)
	if n.pendingAdd == cc.v.ID {
		n.pendingAdd = ""
	}
	if _, ok := n.confPending[cc.v.ID]; !ok {
		delete(n.closing, cc.v.ID)
	}
	n.learnerMu.Lock()
	kept := n.learners[:0]
	for _, l := range n.learners {
		if l.ID != cc.v.ID {
			kept = append(kept, l)
		}
	}
	n.learners = kept
	n.learnerMu.Unlock()
}

// onConfCommitted folds a committed configuration entry into the live voter
// set, persists the new configuration, and answers a waiting change.
func (n *Node) onConfCommitted(e Entry, index uint64) {
	cc, err := decodeConfChange(e.Data)
	if err != nil {
		n.logger.Errorf("raft: decode committed conf change at %d: %v", index, err)
	} else {
		n.applyConfLocally(cc, index)
		if serr := n.log.setVoters(n.Voters()); serr != nil {
			n.logger.Errorf("raft: persist membership after conf change: %v", serr)
		}
		switch cc.op {
		case confAdd:
			n.logger.Infof("raft: %s joined the cluster as a voter (%d voters, %d learners)", cc.v.ID, len(n.voters), n.learnerCount())
		case confRemove:
			n.logger.Infof("raft: %s left the cluster (%d voters)", cc.v.ID, len(n.voters))
		}
	}
	if w, ok := n.confWaiters[index]; ok {
		delete(n.confWaiters, index)
		w.ch <- nil
	}
}

// applyConfLocally mutates the voter set. It is deterministic and idempotent:
// every node applies the same committed entries in the same order, and a replay
// after a restart re-derives the identical set.
func (n *Node) applyConfLocally(cc confChange, index uint64) {
	switch cc.op {
	case confSet:
		// The whole voter set, as it stood when the entry was written: this is
		// the bootstrap entry and the configuration a snapshot carries. It
		// replaces the local set outright — it IS the configuration at that
		// point in the log.
		n.publishVoters(canonicalVoters(cc.voters))
		n.rebuildClients(n.closing)
	case confAdd:
		for i := range n.voters {
			if n.voters[i].ID == cc.v.ID {
				if cc.v.Addr != "" && n.voters[i].Addr != cc.v.Addr {
					// A copy, not an in-place edit: readers hold the slice.
					moved := append([]Voter(nil), n.voters...)
					moved[i].Addr = cc.v.Addr
					n.publishVoters(moved)
					n.rebuildClients(n.closing)
				}
				n.clearPending(cc)
				return
			}
		}
		n.publishVoters(append(append([]Voter(nil), n.voters...), cc.v))
		n.rebuildClients(n.closing)
		n.forgetLearner(cc.v.ID)
		n.clearPending(cc)
	case confRemove:
		out := make([]Voter, 0, len(n.voters))
		for _, v := range n.voters {
			if v.ID != cc.v.ID {
				out = append(out, v)
			}
		}
		n.publishVoters(out)
		// The removed member keeps its replicator until it has the change AND
		// has been told to commit it (finishConf); the index of the entry is
		// what that acknowledgement is measured against.
		if _, ok := n.confPending[cc.v.ID]; !ok {
			n.confPending[cc.v.ID] = index
		}
	}
}

// publishVoters installs a new membership. The run loop is the only writer, but
// the reader side (Members/Voters/IsMember) takes votersMu and copies, so the
// write has to hold the same lock — an unlocked assignment races with a reader
// mid-copy (and every element of a slice that is read while being rebuilt).
func (n *Node) publishVoters(vs []Voter) {
	n.votersMu.Lock()
	n.voters = vs
	n.votersMu.Unlock()
}

// forgetLearner drops id from the accepted-but-not-voting set.
func (n *Node) forgetLearner(id string) {
	n.learnerMu.Lock()
	defer n.learnerMu.Unlock()
	kept := n.learners[:0]
	for _, l := range n.learners {
		if l.ID != id {
			kept = append(kept, l)
		}
	}
	n.learners = kept
}

// finishConf is called when a probe reply moves a member's matchIndex: a member
// being removed keeps its replicator until it has ACKNOWLEDGED the
// configuration entry, so it can still be told to commit it. Dropping the
// replicator the moment the leader commits would strand the removed node with
// an uncommitted tail — it would keep believing in the old membership and could
// campaign against the survivors.
func (n *Node) finishConf(id string) {
	if !n.closing[id] {
		return
	}
	idx, ok := n.confPending[id]
	if !ok {
		return
	}
	if n.matchIndex[id] < idx {
		return // not acknowledged yet: keep replicating
	}
	// Acknowledged. One more round is still owed: the member needs an
	// AppendEntries carrying the new leaderCommit before its own commit index
	// can move past the change.
	if n.closingAcked[id] {
		n.dropClosing(id)
		return
	}
	n.closingAcked[id] = true
	n.kick(id)
}

func (n *Node) dropClosing(id string) {
	delete(n.closing, id)
	delete(n.confPending, id)
	delete(n.closingAcked, id)
	n.stopReplicatorFor(id)
}

// learnerCount is the number of accepted non-voting members.
func (n *Node) learnerCount() int {
	n.learnerMu.RLock()
	defer n.learnerMu.RUnlock()
	return len(n.learners)
}

// isVoter is the loop-owned membership test used by protocol decisions.
func (n *Node) isVoter(id string) bool {
	for _, v := range n.voters {
		if v.ID == id {
			return true
		}
	}
	return false
}

// ensureReplicator starts (or re-points) a replicator at one peer.
func (n *Node) ensureReplicator(id, addr string) {
	n.startReplicator(id, addr)
	n.putPeerClient(id, addr, n.tr)
}

// startReplicator brings up the per-peer replication goroutine (and its
// indexes) if it is not running yet.
func (n *Node) startReplicator(id, addr string) {
	if id == "" || id == n.cfg.NodeID {
		return
	}
	if n.peerClientOf(id) == nil {
		if addr == "" {
			addr = n.cfg.voterAddr(id)
		}
		if addr == "" {
			n.logger.Warnf("raft: no peer address known for %s; it cannot be replicated to yet", id)
			return
		}
		n.putPeerClient(id, addr, n.tr)
	}
	if n.replicators[id] != nil {
		return
	}
	if _, ok := n.nextIndex[id]; !ok {
		n.nextIndex[id] = n.log.LastIndex() + 1
	}
	if _, ok := n.matchIndex[id]; !ok {
		n.matchIndex[id] = 0
	}
	n.replicators[id] = newReplicator(id, n)
	go n.runReplicator(n.replicators[id])
}

// stopReplicatorFor stops a peer's replicator without closing the peer client
// (a removed member costs one goroutine less to keep alive for a moment).
func (n *Node) stopReplicatorFor(id string) {
	if r, ok := n.replicators[id]; ok {
		close(r.stop)
		delete(n.replicators, id)
	}
}
