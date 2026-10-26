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

// Data-plane address registration ("self-discovery" for admin/client ports).
//
// Operators configure peers with a single address each — the Raft peer
// endpoint: "node-1=http://192.168.1.1:9891,...". The admin (management)
// and client (gRPC event plane) addresses are NOT configured: each node
// knows its own listeners, so it announces them into the routing table via
// a Raft-committed OpRegister command. MOVED targets and the liveness
// sweep then see correct cross-machine addresses without extra config.
//
// The announcement rides the PeerService gRPC surface on the SAME peer
// port as Raft (proto/pushupes/v1/peer.proto): the peer listener splits
// connections by their first byte — the HTTP/2 preface ('P') goes to
// gRPC, everything else to the Raft transport. Followers relay
// registrations to the current leader's peer address; the leader commits
// them. Announcements are idempotent and retried periodically, which
// makes any bootstrap-order race (voter not yet joined into the table)
// self-healing.
package cluster

import (
	"context"
	"fmt"
	"time"

	pushupesv1 "github.com/berkaroad/pushupes/internal/grpcapi/pushupes/v1"
)

const (
	registerRetryFast   = time.Second
	registerRetrySteady = 10 * time.Second
	registerRPCTimeout  = 5 * time.Second
)

// Registration announces one node's own data-plane addresses.
type Registration struct {
	ID         string
	AdminAddr  string
	ClientAddr string
	// Role is the node's self-announcement of its place in the cluster
	// (RoleVoter or RoleLearner, always stated). The node's own -role flag
	// is the only authority on it.
	Role string
}

func (r *Registration) validate() error {
	if r.ID == "" || r.AdminAddr == "" || r.ClientAddr == "" {
		return fmt.Errorf("register: incomplete announcement %+v", *r)
	}
	if _, err := NormalizeRole(r.Role); err != nil {
		return err
	}
	return nil
}

// sendRegistration delivers a registration to a peer's peer-plane address:
// if that peer is the leader it commits, otherwise it relays onward.
func (e *Engine) sendRegistration(ctx context.Context, peerAddr string, reg *Registration) error {
	cctx, cancel := context.WithTimeout(ctx, registerRPCTimeout)
	defer cancel()
	c, err := e.peerRPC(peerAddr)
	if err != nil {
		return err
	}
	_, err = c.Register(cctx, &pushupesv1.RegisterRequest{
		Id: reg.ID, AdminAddr: reg.AdminAddr, ClientAddr: reg.ClientAddr, Role: reg.Role,
	})
	return err
}

// acceptRegistration commits locally when we are the Raft leader, else
// relays to the leader's peer address over the peer plane.
func (e *Engine) acceptRegistration(reg *Registration) error {
	if err := reg.validate(); err != nil {
		return err
	}
	if e.node.IsLeader() {
		// An announcement is also recovery evidence (see noteRecovery): the
		// peer it comes from is running, whatever the probe's cached
		// connection says.
		e.noteRecovery(reg)
		// The directory entry must exist BEFORE the announcement is committed.
		// OpRegister patches an existing entry and never creates one (membership
		// belongs to the configuration/directory path), so committing an
		// announcement for an id the table does not hold writes a log entry no
		// state machine can apply: the leader's own apply fails, and every data
		// node replaying that log stalls its feed on it ("apply layout entry N:
		// register: unknown peer node-4" in a reconnect loop). It is a real
		// ordering, not a bug elsewhere — a node announces its addresses from
		// the moment it starts (1s fast rounds) while its own join offer waits
		// on a 3s adopt interval, so on a cold boot the announcement routinely
		// beats the command that admits it. Answering "not in the directory
		// yet" leaves the announcement on its heartbeat until the join lands.
		if _, ok := e.TableSnapshot().Peers[reg.ID]; !ok {
			return fmt.Errorf("register: %s is not in the directory yet; the cluster joins it first", reg.ID)
		}
		// Registrations are retried on a steady heartbeat (registerRetrySteady,
		// every 10s), so an announcement that matches what the table already
		// holds is the common case — and committing it would only advance the
		// Raft commit index, which the console renders as a "commit" number
		// that creeps up on a completely idle cluster. Skip the no-op commit:
		// only a real change (a node restarted with different ports) reaches
		// Raft, while the heartbeat still heals that change within one round.
		if e.registrationMatches(reg) {
			return nil
		}
		return e.submit(&Command{Op: OpRegister, Peer: &Peer{
			ID: reg.ID, AdminAddr: reg.AdminAddr, ClientAddr: reg.ClientAddr, Role: reg.Role,
		}})
	}
	if addr := e.leaderPeerAddr(); addr != "" {
		return e.sendRegistration(context.Background(), addr, reg)
	}
	return fmt.Errorf("register: leader unknown yet")
}

// noteRecovery treats a self-announcement from a peer the directory holds as
// down as recovery evidence and marks it up. This event, not the liveness probe,
// is what bounds how long a restarted node stays out of the layout: the probe
// reads a cached connection that carries the backoff it built up during the
// outage (up to gRPC's two-minute ceiling), so on its own it can keep calling a
// recovered node dead for a minute.
//
// The announcement alone does not seat the node back in the layout: the
// controller must also reach its peer port. A node that can talk to us while we
// cannot reach it would be marked up here and down again three probes later,
// flapping leadership every announcement — so the reachability dial stays the
// authority on "can this node serve". Either way the announcement clears the
// accumulated probe strikes and the refusal clock, and drops the stale
// connection, so the next probe judges the node on its merits.
func (e *Engine) noteRecovery(reg *Registration) {
	p, ok := e.TableSnapshot().Peers[reg.ID]
	if !ok || !p.Down || p.PeerAddr == "" {
		return
	}
	listening, _ := dialPeerPort(p.PeerAddr)
	e.failMu.Lock()
	e.failStreak[reg.ID] = 0
	delete(e.refusedSince, reg.ID) // it is listening (or at least announcing): the refusal clock starts over
	e.failMu.Unlock()
	if !listening {
		return
	}
	e.resetPeerConn(p.PeerAddr)
	if err := e.submit(&Command{Op: OpMarkUp, NodeID: reg.ID}); err != nil {
		e.loggerf("register: %s announced itself but the mark_up did not commit: %v (the sweep retries)", reg.ID, err)
		return
	}
	e.loggerf("peer %s announced itself and answers on its peer port again; marking it up", reg.ID)
}

// registrationMatches reports whether the routing table already carries
// exactly this announcement, i.e. committing it would mutate nothing. A
// peer that is absent from the table never matches (the first announcement
// must be committed so the address becomes known).
func (e *Engine) registrationMatches(reg *Registration) bool {
	tbl := e.TableSnapshot()
	if tbl == nil {
		return false
	}
	p, ok := tbl.Peers[reg.ID]
	if !ok || p.AdminAddr != reg.AdminAddr || p.ClientAddr != reg.ClientAddr {
		return false
	}
	return p.Role == reg.Role
}

// leaderPeerAddr maps the Raft leader id onto its peer address — the one
// address every node knows statically for every configured peer.
func (e *Engine) leaderPeerAddr() string {
	id := e.node.LeaderID()
	if id == "" {
		return ""
	}
	if id == e.self {
		return e.node.Cfg().PeerAddr
	}
	return e.peerAddrForNode(id)
}

// peerAddrForNode resolves a node id to its peer-plane address: the
// replicated directory first (it carries members admitted at runtime, which
// the static -peers list cannot know), the static list as fallback.
func (e *Engine) peerAddrForNode(id string) string {
	if p, ok := e.TableSnapshot().Peers[id]; ok && p.PeerAddr != "" {
		return p.PeerAddr
	}
	for _, p := range e.node.Cfg().Peers {
		if p.ID == id {
			return p.PeerAddr
		}
	}
	return ""
}

// RegisterAnnouncer periodically announces this node's data-plane
// addresses over the peer ports of the members it can reach.
type RegisterAnnouncer struct {
	e    *Engine
	reg  Registration
	join []string // the -join list: the only addresses a joiner knows statically
}

// NewRegisterAnnouncer builds the announcer. joinTargets is the node's -join
// list (empty for the author, which knows the cluster through -peers): a voter
// JOINER deliberately starts with an EMPTY peer list — its raft configuration
// arrives from the leader's log, so a seeded voter set would be a fiction —
// and the join list is therefore the only address its announcement loop can
// start from. Deriving the targets from the configured peers alone left every
// voter joiner silent: the configuration named it as a voter while the
// directory kept the author's placeholder (no admin/client address, no role),
// so the control plane showed one node and anything routing by the controller's
// admin address had nothing to dial as soon as a joiner won the election.
func (e *Engine) NewRegisterAnnouncer(joinTargets ...string) *RegisterAnnouncer {
	reg := Registration{
		ID:         e.self,
		AdminAddr:  e.node.Cfg().AdminAddr,
		ClientAddr: e.node.Cfg().ClientAddr,
		Role:       e.node.Cfg().Role,
	}
	join := make([]string, 0, len(joinTargets))
	for _, t := range joinTargets {
		if a := HostPort(NormalizeAddr(t)); a != "" {
			join = append(join, a)
		}
	}
	return &RegisterAnnouncer{e: e, reg: reg, join: join}
}

// Run announces until ctx closes. Early rounds retry fast so a booting
// cluster converges within seconds; afterwards it is a cheap steady-state
// heartbeat that also heals addresses when a node restarts with new
// ports.
func (a *RegisterAnnouncer) Run(ctx context.Context) {
	if len(a.targets()) == 0 {
		return // nobody to announce to: single node, and the controller join carries our addresses
	}
	interval := registerRetryFast
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
		if a.done() {
			interval = registerRetrySteady
		}
		a.announceOnce(ctx)
	}
}

// targets lists where one announcement may go this round, the most direct
// first: the controller's peer address (it commits the announcement), then the
// -join list, the configured peer directory, and finally every peer address the
// replicated directory knows — so a static list that has gone stale (or a join
// seed that has died since) cannot strand the heartbeat. The node's own address
// is never a target.
//
// It is derived per round rather than once at startup because the address a
// joiner can use changes as it learns the cluster: before admission it knows
// only -join, afterwards the directory names the controller.
func (a *RegisterAnnouncer) targets() []string {
	self := HostPort(NormalizeAddr(a.e.node.Cfg().PeerAddr))
	out := make([]string, 0, len(a.join)+len(a.e.node.Cfg().Peers)+1)
	seen := make(map[string]bool, cap(out))
	add := func(raw string) {
		addr := HostPort(NormalizeAddr(raw))
		if addr == "" || addr == self || seen[addr] {
			return
		}
		seen[addr] = true
		out = append(out, addr)
	}
	add(a.e.leaderPeerAddr())
	for _, addr := range a.join {
		add(addr)
	}
	for _, p := range a.e.node.Cfg().Peers {
		add(p.PeerAddr)
	}
	if tbl := a.e.TableSnapshot(); tbl != nil {
		for _, p := range tbl.Peers {
			add(p.PeerAddr)
		}
	}
	return out
}

func (a *RegisterAnnouncer) done() bool {
	tbl := a.e.TableSnapshot()
	p, ok := tbl.Peers[a.reg.ID]
	return ok && p.AdminAddr == a.reg.AdminAddr && p.ClientAddr == a.reg.ClientAddr &&
		p.Role == a.reg.Role
}

func (a *RegisterAnnouncer) announceOnce(ctx context.Context) {
	for _, addr := range a.targets() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := a.e.sendRegistration(ctx, addr, &a.reg); err == nil {
			return // accepted or relayed; done for this round
		}
	}
}
