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

// members.go — the admin-facing runtime membership API.
//
// The Raft configuration (which nodes vote) and the replicated peer directory
// (the slot table's member list) are two different things, and this file keeps
// them in step. Adding a node is: make it a voter in the consensus layer, then
// let the ordinary controller reconcile pick it up (the directory sync turns
// every voter into a join_node entry, registration fills in its data-plane
// addresses, and replan_slots tops the replica sets up to the factor).
//
// Removing a node reverses it, with one extra rule: only an offline member can
// be removed. The consensus layer keeps talking to the removed member until it
// has the configuration entry in its own log, so it can serve its slots one
// last time while the controller moves their leadership to live replicas.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	pushupesv1 "github.com/berkaroad/pushupes/internal/grpcapi/pushupes/v1"
	"github.com/berkaroad/pushupes/internal/raft"
)

// AddMemberTimeout bounds one runtime membership change.
const AddMemberTimeout = 30 * time.Second

// DefaultAdoptInterval is how often a node that is not a member yet retries
// offering itself through -join.
const DefaultAdoptInterval = 3 * time.Second

// ErrBadMember is a membership change the operator has to fix (missing id,
// address already in use, removing the last voter).
var ErrBadMember = errors.New("cluster: invalid membership change")

// AddMember adds a node to the running cluster. It is controller-only: a
// follower refuses with a *NotControllerError naming the controller, so the
// caller retries there (the admin plane never forwards commands).
//
// The address is the new node's peer-plane address — the raft/gossip endpoint,
// the only one membership needs. Its admin and client addresses are NOT passed:
// a member announces those itself through the ordinary registration protocol
// once it is in the configuration.
func (e *Engine) AddMember(ctx context.Context, id, peerAddr, role string) error {
	return e.admitMember(ctx, id, peerAddr, role, true)
}

// AdmitSelf is the adoption path — a node offering ITSELF at startup. It is
// the same directory admission minus one power: a self-offer never withdraws
// a drain. The removal was the operator's call (the DELETE that marked it);
// a node rebooting into its own scale-down must not cancel it by coming
// back — that would make every drain racing a restart loop forever. Only
// the operator's AddMember (admin endpoint, console button, cluster.sh join)
// says "keep this one after all".
func (e *Engine) AdmitSelf(ctx context.Context, id, peerAddr, role string) error {
	return e.admitMember(ctx, id, peerAddr, role, false)
}

func (e *Engine) admitMember(ctx context.Context, id, peerAddr, role string, withdraw bool) error {
	if e.node == nil {
		return &NotControllerError{}
	}
	if id == "" || peerAddr == "" {
		return fmt.Errorf("%w: id and peer addr are required", ErrBadMember)
	}
	if role != RoleVoter && role != RoleLearner {
		return fmt.Errorf("%w: role must be %s or %s", ErrBadMember, RoleVoter, RoleLearner)
	}
	if role == RoleLearner {
		// Learner admission is a replicated DIRECTORY command: the raft
		// configuration never names a learner, and the layout pool takes it
		// from the entry's role (see Table.HostingIDs). Idempotent on the
		// directory: a re-admission of the same clean entry is a no-op — but
		// a re-admission of a DRAINING entry is the withdrawal of the
		// scale-down itself (the DELETE pipeline has no other cancel), so it
		// must commit: join_node over the entry, both marks cleared, the
		// announced addresses kept (they are the node's own registration;
		// dropping them here would blind the layout until the next
		// heartbeat re-announces).
		if p, ok := e.TableSnapshot().Peers[id]; ok && p.Role == RoleLearner {
			if !p.Draining && !p.PendingRemove {
				return nil
			}
			if !withdraw {
				return nil // self-offer: the marks (and the removal riding them) stand
			}
			back := p
			back.Draining = false
			back.PendingRemove = false
			if back.PeerAddr == "" {
				back.PeerAddr = NormalizeAddr(peerAddr)
			}
			if err := e.submit(&Command{Op: OpJoinNode, Peer: &back}); err != nil {
				if errors.Is(err, ErrNotLeader) {
					cid, caddr := e.Controller()
					return &NotControllerError{LeaderID: cid, AdminAddr: caddr}
				}
				return err
			}
			e.loggerf("node %s re-admitted: drain and removal marks withdrawn, it serves again", id)
			return nil
		}
		if e.node.IsMember(id) {
			return fmt.Errorf("%w: %s is already a raft voter; it cannot also be admitted as a learner", ErrBadMember, id)
		}
		if err := e.submit(&Command{Op: OpJoinNode, Peer: &Peer{
			ID: id, PeerAddr: NormalizeAddr(peerAddr), Role: RoleLearner,
		}}); err != nil {
			if errors.Is(err, ErrNotLeader) {
				cid, caddr := e.Controller()
				return &NotControllerError{LeaderID: cid, AdminAddr: caddr}
			}
			return err
		}
		e.loggerf("learner %s admitted to the directory (peer addr %s); it registers its admin/client addresses itself", id, HostPort(NormalizeAddr(peerAddr)))
		return nil
	}
	if e.node.IsMember(id) {
		return nil // already a member: idempotent
	}
	if p, ok := e.TableSnapshot().Peers[id]; ok && p.Role == RoleLearner {
		return fmt.Errorf("%w: %s is already a directory learner; it cannot also be admitted as a voter", ErrBadMember, id)
	}
	addr := HostPort(NormalizeAddr(peerAddr))
	err := e.node.AddMember(id, addr, AddMemberTimeout)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			cid, caddr := e.Controller()
			return &NotControllerError{LeaderID: cid, AdminAddr: caddr}
		}
		if errors.Is(err, raft.ErrBadChange) {
			// An operator mistake (an address another member already owns, a
			// node with no membership to add into): report it as a bad
			// request, not as a server failure.
			return fmt.Errorf("%w: %v", ErrBadMember, err)
		}
		return err
	}
	e.loggerf("member %s added to the raft configuration (peer addr %s); it registers its admin/client addresses itself", id, addr)
	return nil
}

// RemoveMember is the scale-down in ONE command, and the drain-first,
// remove-second order runs entirely server-side. Whether the node is up or
// dead is never asked: the directory plus the slot table is the whole truth,
// and the removal pipeline is the same for both — DELETE starts the drain
// (the drain mark takes the node out of every layout role: no new leader, no
// new seat, no migration target, and no reads/writes ROUTED TO IT as such —
// while the slots it still leads serve normally until each handover commits,
// which is what keeps running clients uninterrupted across the whole
// window), and the rebalancer's drain rounds — which run even when the
// operator turned rebalancing off, because a drain is what they exist for —
// move the leadership out. The moment the node leads nothing and holds no
// slot mid-handover, the removal commits: its raft configuration (voters)
// and its directory entry. The intent to remove at that point rides the
// table (Peer.PendingRemove, second field of the same drain_node command),
// so a controller change mid-drain resumes the job instead of losing it, and
// a re-admission of the node (join_node) clears both marks — bringing a node
// back is exactly how an operator cancels its removal.
//
// Return values: "removed" when the node is out of the cluster (this call
// committed it), "draining" while handovers are in flight. A repeat DELETE
// during the drain is a progress report, not a new command.
func (e *Engine) RemoveMember(ctx context.Context, id string) (string, error) {
	if e.node == nil {
		return "", &NotControllerError{}
	}
	if id == "" {
		return "", fmt.Errorf("%w: id is required", ErrBadMember)
	}
	tbl := e.TableSnapshot()
	p, inDir := tbl.Peers[id]
	if !e.node.IsMember(id) {
		if !inDir {
			return "removed", nil // already gone: idempotent
		}
		if p.Role != RoleLearner {
			return "removed", nil // an unseeded name only: the voter sync owns it
		}
	}
	leading, inflight := e.drainProgress(tbl, id)
	if leading == 0 && inflight == 0 {
		if err := e.finishRemoval(ctx, id); err != nil {
			return "", err
		}
		return "removed", nil
	}
	// Still holds work: mark the drain (idempotent — a node already marked
	// just reports progress). The mark excludes it from the layout pools;
	// the drain rounds the rebalancer runs do the moving.
	if p.Draining && p.PendingRemove {
		return "draining", nil
	}
	if err := e.submit(&Command{Op: OpDrainNode, NodeID: id, Drain: true, ThenRemove: true}); err != nil {
		if errors.Is(err, ErrNotLeader) {
			cid, caddr := e.Controller()
			return "", &NotControllerError{LeaderID: cid, AdminAddr: caddr}
		}
		return "", err
	}
	e.loggerf("node %s draining (%d leading slots, %d mid-handover) before removal; it no longer takes part in the layout",
		id, leading, inflight)
	return "draining", nil
}

// DrainProgressOf reports how much of the slot table still ties to a node —
// the same count the removal pipeline and the admin DELETE read to answer
// "drained yet?" without either re-deriving it.
func (e *Engine) DrainProgressOf(id string) (leading, inflight int) {
	return e.drainProgress(e.TableSnapshot(), id)
}

// drainProgress counts what still ties the slot table to a node: slots it
// leads and slots whose migration involves it (staging, importing, backing
// up). A handover that commits in several commands keeps the count above
// zero between steps by design — the removal must not race the last data
// copy it is waiting on.
func (e *Engine) drainProgress(tbl *Table, id string) (leading, inflight int) {
	for _, pl := range tbl.Slots {
		if pl == nil {
			continue
		}
		if pl.Leader == id {
			if pl.State == SlotStable {
				leading++
			} else {
				inflight++
			}
			continue
		}
		if pl.State != SlotStable && containsNode(pl.Replicas, id) {
			inflight++
		}
	}
	return leading, inflight
}

// finishRemoval commits the formal removal of a drained (or empty) node: the
// raft configuration change for voters, the directory leave for everyone.
// The online question is gone on purpose — the drain already moved whatever
// the node held; a live process with no slots left has nothing to disrupt.
func (e *Engine) finishRemoval(ctx context.Context, id string) error {
	p, inDir := e.TableSnapshot().Peers[id]
	if !e.node.IsMember(id) {
		if !inDir || p.Role != RoleLearner {
			return nil // nothing to do (idempotent, or a voter name the sync owns)
		}
		if err := e.submit(&Command{Op: OpLeaveNode, NodeID: id}); err != nil {
			if errors.Is(err, ErrNotLeader) {
				cid, caddr := e.Controller()
				return &NotControllerError{LeaderID: cid, AdminAddr: caddr}
			}
			return err
		}
		e.loggerf("learner %s revoked from the directory; its seats re-plan to the remaining holders", id)
		return nil
	}
	if err := e.node.RemoveMember(id, AddMemberTimeout); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			cid, caddr := e.Controller()
			return &NotControllerError{LeaderID: cid, AdminAddr: caddr}
		}
		if errors.Is(err, raft.ErrBadChange) {
			return fmt.Errorf("%w: %v", ErrBadMember, err)
		}
		return err
	}
	if inDir {
		if err := e.submit(&Command{Op: OpLeaveNode, NodeID: id}); err != nil {
			if errors.Is(err, ErrNotLeader) {
				cid, caddr := e.Controller()
				return &NotControllerError{LeaderID: cid, AdminAddr: caddr}
			}
			return err
		}
	}
	e.loggerf("member %s removed from the raft configuration; its slots fail over to live replicas", id)
	return nil
}

// Members returns the runtime membership as the admin API reports it: the
// consensus voter set with the peer-directory addresses attached.
func (e *Engine) Members() []MemberView {
	voters := e.node.Members()
	tbl := e.TableSnapshot()
	out := make([]MemberView, 0, len(voters)+len(tbl.Peers))
	seen := map[string]bool{}
	for _, v := range voters {
		mv := MemberView{ID: v.ID, PeerAddr: v.Addr, Role: RoleVoter}
		if p, ok := tbl.Peers[v.ID]; ok {
			mv.AdminAddr = p.AdminAddr
			mv.ClientAddr = p.ClientAddr
			mv.Down = p.Down
			// The self-announced role wins over the config-derived voter: an
			// announced voter says voter either way, and a directory learner
			// never enters the raft configuration (see AddMember), so the two
			// sources can only disagree on the not-yet-announced window.
			if p.Role != "" {
				mv.Role = p.Role
			}
		}
		seen[v.ID] = true
		out = append(out, mv)
	}
	// Learners live in the directory only — the raft configuration never
	// names them, so the voter walk above cannot find them.
	for id, p := range tbl.Peers {
		if seen[id] || p.Role != RoleLearner {
			continue
		}
		out = append(out, MemberView{ID: id, PeerAddr: p.PeerAddr,
			AdminAddr: p.AdminAddr, ClientAddr: p.ClientAddr, Role: p.Role, Down: p.Down})
	}
	return out
}

// MemberView is one cluster member as the admin API reports it.
type MemberView struct {
	ID         string `json:"id"`
	PeerAddr   string `json:"peer_addr"`
	AdminAddr  string `json:"admin_addr,omitempty"`
	ClientAddr string `json:"client_addr,omitempty"`
	Role       string `json:"role,omitempty"`
	Down       bool   `json:"down,omitempty"`
}

// Adoption is a node offering itself to the cluster.
type Adoption struct {
	ID       string
	PeerAddr string
	// Role is the shape to be admitted with: RoleVoter (the raft-configuration
	// admission) or RoleLearner (data plane, no vote). One of the two is
	// required — an offer without a role is refused.
	Role string
}

// sendAdoption delivers an offer to a peer's peer-plane address; a non-leader
// relays it onward. offerAddr is the JOINING node's peer address (what the
// leader adds); targetAddr is the member being asked (which may be any node).
func (e *Engine) sendAdoption(ctx context.Context, id, offerAddr, targetAddr, role string) error {
	cctx, cancel := context.WithTimeout(ctx, registerRPCTimeout)
	defer cancel()
	c, err := e.peerRPC(targetAddr)
	if err != nil {
		return err
	}
	_, err = c.Adopt(cctx, &pushupesv1.AdoptRequest{
		Id: id, PeerAddr: HostPort(NormalizeAddr(offerAddr)), Role: role,
	})
	return err
}

// Joiner is the joining node's loop: offer yourself through one of the cluster
// members until the leader has put you into the configuration.
type Joiner struct {
	e        *Engine
	targets  []string // members to offer through, tried in rotation
	selfAddr string   // this node's own peer address (what it announces)
	interval time.Duration
}

// NewJoiner builds the joining node's offer loop. targets are the peer
// addresses of running members: an explicit -join address, or — for a node
// that is simply not the author of the cluster's configuration — the -peers
// list it was configured with. None of them is announced as this node's
// address; the node announces its own peer address.
//
// Several targets are the norm now: a joiner comes up while the cluster it is
// joining may still be starting, so one unreachable seed must not stop the
// offer.
func (e *Engine) NewJoiner(targets []string, interval time.Duration) *Joiner {
	if interval <= 0 {
		interval = DefaultAdoptInterval
	}
	cleaned := make([]string, 0, len(targets))
	seen := map[string]bool{}
	for _, t := range targets {
		a := HostPort(NormalizeAddr(t))
		if a == "" || seen[a] {
			continue
		}
		seen[a] = true
		cleaned = append(cleaned, a)
	}
	return &Joiner{
		e:        e,
		targets:  cleaned,
		selfAddr: HostPort(NormalizeAddr(e.node.Cfg().PeerAddr)),
		interval: interval,
	}
}

// Run offers this node to the cluster until it is a member, or ctx closes. The
// targets are tried in rotation: a member that is mid-restart costs one round,
// not the join.
func (j *Joiner) Run(ctx context.Context) {
	if len(j.targets) == 0 {
		j.e.loggerf("join: no member address to offer through (empty -peers and no -join); this node stays out of the cluster")
		return
	}
	for i := 0; ; i++ {
		if j.e.node != nil && j.e.node.IsMember(j.e.self) {
			j.e.loggerf("joined the cluster: %s is now a raft voter", j.e.self)
			return
		}
		target := j.targets[i%len(j.targets)]
		if err := j.e.sendAdoption(ctx, j.e.self, j.selfAddr, target, j.e.node.Cfg().Role); err != nil {
			j.e.loggerf("join offer to %s failed: %v", target, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(j.interval):
		}
	}
}

// acceptAdoption commits locally when we are the leader, else relays.
func (e *Engine) acceptAdoption(ctx context.Context, ad *Adoption) error {
	if ad.ID == "" || ad.PeerAddr == "" {
		return fmt.Errorf("adopt: incomplete offer")
	}
	if ad.Role != RoleVoter && ad.Role != RoleLearner {
		return fmt.Errorf("adopt: role %q must be %s or %s", ad.Role, RoleVoter, RoleLearner)
	}
	if e.node == nil {
		return fmt.Errorf("adopt: no raft node")
	}
	if e.node.IsLeader() {
		if ad.Role == RoleLearner {
			// Self-admission of a data-plane node: the same directory
			// command the admin endpoint posts, WITHOUT the withdraw power
			// (see AdmitSelf: a rebooting node does not cancel its own
			// scale-down).
			return e.AdmitSelf(ctx, ad.ID, ad.PeerAddr, RoleLearner)
		}
		if e.node.IsMember(ad.ID) {
			return nil
		}
		if err := e.node.AddMember(ad.ID, HostPort(NormalizeAddr(ad.PeerAddr)), AddMemberTimeout); err != nil {
			return err
		}
		e.loggerf("adopted %s (%s) into the raft configuration", ad.ID, ad.PeerAddr)
		return nil
	}
	if addr := e.leaderPeerAddr(); addr != "" {
		return e.sendAdoption(ctx, ad.ID, ad.PeerAddr, addr, ad.Role)
	}
	return fmt.Errorf("adopt: leader unknown yet")
}
