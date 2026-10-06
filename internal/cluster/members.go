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

	pushupesv1 "pushupes/internal/grpcapi/pushupes/v1"
	"pushupes/internal/raft"
)

// AddMemberTimeout bounds one runtime membership change.
const AddMemberTimeout = 30 * time.Second

// DefaultAdoptInterval is how often a node that is not a member yet retries
// offering itself through -join.
const DefaultAdoptInterval = 3 * time.Second

// ErrBadMember is a membership change the operator has to fix (missing id,
// address already in use, removing the last voter).
var ErrBadMember = errors.New("cluster: invalid membership change")

// ErrOnlineMember is a refused removal: the target still serves clients — it
// announced its client address and the controller has not marked it down — so
// the operator has to take it offline first. Removal is reserved for dead
// members precisely because a live one holds slot leadership and replica seats
// the cluster is using; evicting it through the raft configuration would move
// clients onto whatever the reconciler picks next, instead of onto a placement
// the operator chose.
var ErrOnlineMember = errors.New("cluster: node is online; only offline members can be removed")

// AddMember adds a node to the running cluster. It is controller-only: a
// follower refuses with a *NotControllerError naming the controller, so the
// caller retries there (the admin plane never forwards commands).
//
// The address is the new node's peer-plane address — the raft/gossip endpoint,
// the only one membership needs. Its admin and client addresses are NOT passed:
// a member announces those itself through the ordinary registration protocol
// once it is in the configuration.
func (e *Engine) AddMember(ctx context.Context, id, peerAddr string) error {
	if e.node == nil {
		return &NotControllerError{}
	}
	if id == "" || peerAddr == "" {
		return fmt.Errorf("%w: id and peer addr are required", ErrBadMember)
	}
	if e.node.IsMember(id) {
		return nil // already a member: idempotent
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

// RemoveMember drops a node from the running cluster. Controller-only, like
// AddMember. Only an offline member can go: a node the peer directory still
// marks reachable (ErrOnlineMember) must be taken offline first, so removal is
// the operator's cleanup of a dead member, never a way to reshuffle live
// placements. The peer directory entry, its slots' leadership and its replica
// seats are reconciled by the ordinary controller loop (and the rebalancer)
// once the configuration change commits.
func (e *Engine) RemoveMember(ctx context.Context, id string) error {
	if e.node == nil {
		return &NotControllerError{}
	}
	if id == "" {
		return fmt.Errorf("%w: id is required", ErrBadMember)
	}
	if !e.node.IsMember(id) {
		return nil // already gone: idempotent
	}
	if p, ok := e.TableSnapshot().Peers[id]; ok && !p.Offline() {
		return fmt.Errorf("%w: %s", ErrOnlineMember, id)
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
	e.loggerf("member %s removed from the raft configuration; its slots fail over to live replicas", id)
	return nil
}

// Members returns the runtime membership as the admin API reports it: the
// consensus voter set with the peer-directory addresses attached.
func (e *Engine) Members() []MemberView {
	voters := e.node.Members()
	tbl := e.TableSnapshot()
	out := make([]MemberView, 0, len(voters))
	for _, v := range voters {
		mv := MemberView{ID: v.ID, PeerAddr: v.Addr}
		if p, ok := tbl.Peers[v.ID]; ok {
			mv.AdminAddr = p.AdminAddr
			mv.ClientAddr = p.ClientAddr
			mv.Down = p.Down
		}
		out = append(out, mv)
	}
	return out
}

// MemberView is one cluster member as the admin API reports it.
type MemberView struct {
	ID         string `json:"id"`
	PeerAddr   string `json:"peer_addr"`
	AdminAddr  string `json:"admin_addr,omitempty"`
	ClientAddr string `json:"client_addr,omitempty"`
	Down       bool   `json:"down,omitempty"`
}

// Adoption is a node offering itself to the cluster.
type Adoption struct {
	ID       string
	PeerAddr string
}

// sendAdoption delivers an offer to a peer's peer-plane address; a non-leader
// relays it onward. offerAddr is the JOINING node's peer address (what the
// leader adds); targetAddr is the member being asked (which may be any node).
func (e *Engine) sendAdoption(ctx context.Context, id, offerAddr, targetAddr string) error {
	cctx, cancel := context.WithTimeout(ctx, registerRPCTimeout)
	defer cancel()
	c, err := e.peerRPC(targetAddr)
	if err != nil {
		return err
	}
	_, err = c.Adopt(cctx, &pushupesv1.AdoptRequest{Id: id, PeerAddr: HostPort(NormalizeAddr(offerAddr))})
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
		selfAddr: HostPort(NormalizeAddr(e.node.cfg.PeerAddr)),
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
		if err := j.e.sendAdoption(ctx, j.e.self, j.selfAddr, target); err != nil {
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
	if e.node == nil {
		return fmt.Errorf("adopt: no raft node")
	}
	if e.node.IsLeader() {
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
		return e.sendAdoption(ctx, ad.ID, ad.PeerAddr, addr)
	}
	return fmt.Errorf("adopt: leader unknown yet")
}
