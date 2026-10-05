// replicate.go — one goroutine per follower keeps it caught up.
//
// The run loop is the only reader of nextIndex/matchIndex; it hands each
// replicator a self-contained round (term, prev*, entries, leaderCommit) so
// the replicator never touches loop-owned state. Results come back as events.
package raft

import (
	"errors"
	"fmt"
	"time"
)

const (
	rpcTimeout         = 5 * time.Second
	snapshotRPCTimeout = 30 * time.Second
	maxAppendBatch     = 64
)

type replicateRound struct {
	term         uint64
	leaderID     string
	prevIndex    uint64
	prevTerm     uint64
	entries      []Entry
	leaderCommit uint64
	install      *installSnapshot
}

type replicator struct {
	peer string
	kick chan replicateRound
	stop chan struct{}
}

func newReplicator(peer string, _ *Node) *replicator {
	return &replicator{
		peer: peer,
		kick: make(chan replicateRound, 1),
		stop: make(chan struct{}),
	}
}

// offer replaces any queued round with the newest one: rounds are stateless
// snapshots of the leader's view, so a stale one is worthless.
func (r *replicator) offer(round replicateRound) {
	select {
	case r.kick <- round:
		return
	default:
	}
	select {
	case <-r.kick:
	default:
	}
	select {
	case r.kick <- round:
	default:
	}
}

func (n *Node) runReplicator(r *replicator) {
	for {
		select {
		case <-r.stop:
			return
		case round := <-r.kick:
			n.sendRound(r.peer, round)
		}
	}
}

func (n *Node) kick(peer string) {
	r := n.replicators[peer]
	if r == nil {
		return
	}
	r.offer(n.buildRound(peer))
}

func (n *Node) kickAll() {
	if n.state != Leader {
		return
	}
	for id := range n.replicators {
		n.kick(id)
	}
}

func (n *Node) buildRound(peer string) replicateRound {
	rd := replicateRound{term: n.term, leaderID: n.cfg.NodeID, leaderCommit: n.commitIndex}
	next := n.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	if si, _, file := n.log.Snapshot(); si > 0 && next <= si && file != "" {
		if idx, term, data, err := readSnapshot(file); err == nil {
			rd.install = &installSnapshot{
				Term:      n.term,
				LeaderID:  n.cfg.NodeID,
				LastIndex: idx,
				LastTerm:  term,
				Data:      data,
			}
			return rd
		}
		n.logger.Warnf("raft: cannot read snapshot %s to catch up %s", file, peer)
	}
	rd.prevIndex = next - 1
	rd.prevTerm = n.log.Term(next - 1)
	rd.entries = n.log.Entries(next, maxAppendBatch)
	return rd
}

func (n *Node) sendRound(peer string, rd replicateRound) {
	c := n.clients[peer]
	if c == nil {
		return
	}
	if rd.install != nil {
		typ, payload, err := c.call(msgInstallSnapshot, encodeInstallSnapshot(*rd.install), snapshotRPCTimeout)
		if err != nil {
			n.post(installResult{peer: peer, term: rd.term, err: err})
			return
		}
		if typ == msgError {
			n.post(installResult{peer: peer, term: rd.term, err: fmt.Errorf("raft: %s", payload)})
			return
		}
		resp, derr := decodeInstallSnapshotResp(payload)
		n.post(installResult{peer: peer, term: rd.term, resp: resp, err: derr})
		return
	}
	m := appendEntries{
		Term:         rd.term,
		LeaderID:     rd.leaderID,
		PrevIndex:    rd.prevIndex,
		PrevTerm:     rd.prevTerm,
		Entries:      rd.entries,
		LeaderCommit: rd.leaderCommit,
	}
	typ, payload, err := c.call(msgAppendEntries, encodeAppendEntries(m), rpcTimeout)
	if err != nil {
		n.post(appendResult{peer: peer, term: rd.term, err: err})
		return
	}
	if typ == msgError {
		n.post(appendResult{peer: peer, term: rd.term, err: fmt.Errorf("raft: %s", payload)})
		return
	}
	resp, derr := decodeAppendEntriesResp(payload)
	n.post(appendResult{peer: peer, term: rd.term, resp: resp, err: derr})
}

func (n *Node) sendRequestVote(peer string, term, lastIdx, lastTerm uint64) {
	c := n.clients[peer]
	if c == nil {
		n.post(voteResult{peer: peer, term: term, err: errors.New("raft: unknown peer")})
		return
	}
	msg := requestVote{Term: term, CandidateID: n.cfg.NodeID, LastLogIndex: lastIdx, LastLogTerm: lastTerm}
	typ, payload, err := c.call(msgRequestVote, encodeRequestVote(msg), rpcTimeout)
	if err != nil {
		n.post(voteResult{peer: peer, term: term, err: err})
		return
	}
	if typ == msgError {
		n.post(voteResult{peer: peer, term: term, err: fmt.Errorf("raft: %s", payload)})
		return
	}
	resp, derr := decodeRequestVoteResp(payload)
	n.post(voteResult{peer: peer, term: resp.Term, granted: resp.Granted, err: derr})
}
