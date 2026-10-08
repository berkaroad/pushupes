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
}

func (r *Registration) validate() error {
	if r.ID == "" || r.AdminAddr == "" || r.ClientAddr == "" {
		return fmt.Errorf("register: incomplete announcement %+v", *r)
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
		Id: reg.ID, AdminAddr: reg.AdminAddr, ClientAddr: reg.ClientAddr,
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
			ID: reg.ID, AdminAddr: reg.AdminAddr, ClientAddr: reg.ClientAddr,
		}})
	}
	if addr := e.leaderPeerAddr(); addr != "" {
		return e.sendRegistration(context.Background(), addr, reg)
	}
	return fmt.Errorf("register: leader unknown yet")
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
	return ok && p.AdminAddr == reg.AdminAddr && p.ClientAddr == reg.ClientAddr
}

// leaderPeerAddr maps the Raft leader id onto its peer address — the one
// address every node knows statically for every configured peer.
func (e *Engine) leaderPeerAddr() string {
	id := e.node.raft.LeaderID()
	if id == "" {
		return ""
	}
	if id == e.self {
		return e.node.cfg.PeerAddr
	}
	for _, p := range e.node.cfg.Peers {
		if p.ID == id {
			return p.PeerAddr
		}
	}
	return ""
}

// RegisterAnnouncer periodically announces this node's data-plane
// addresses over the peer ports of every configured entry point.
type RegisterAnnouncer struct {
	e     *Engine
	reg   Registration
	ports []string
}

func (e *Engine) NewRegisterAnnouncer() *RegisterAnnouncer {
	reg := Registration{
		ID:         e.self,
		AdminAddr:  e.node.cfg.AdminAddr,
		ClientAddr: e.node.cfg.ClientAddr,
	}
	targets := make([]string, 0, len(e.node.cfg.Peers)+1)
	for _, p := range e.node.cfg.Peers {
		if p.PeerAddr != "" && p.ID != e.self {
			targets = append(targets, p.PeerAddr)
		}
	}
	return &RegisterAnnouncer{e: e, reg: reg, ports: targets}
}

// Run announces until ctx closes. Early rounds retry fast so a booting
// cluster converges within seconds; afterwards it is a cheap steady-state
// heartbeat that also heals addresses when a node restarts with new
// ports.
func (a *RegisterAnnouncer) Run(ctx context.Context) {
	if len(a.ports) == 0 {
		return // single-node: the controller join carries our addresses
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

func (a *RegisterAnnouncer) done() bool {
	tbl := a.e.TableSnapshot()
	p, ok := tbl.Peers[a.reg.ID]
	return ok && p.AdminAddr == a.reg.AdminAddr && p.ClientAddr == a.reg.ClientAddr
}

func (a *RegisterAnnouncer) announceOnce(ctx context.Context) {
	for _, addr := range a.ports {
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
