package cluster

import "fmt"

// NotControllerError is returned when a command that mutates replicated (Raft)
// state is submitted on a node that is not the controller — the Raft leader.
//
// pushupes never hands such a command to another node: the admin plane is not
// a proxy for the controller, and the peer plane carries node-to-node
// replication, not operator commands. So a follower refuses the command
// outright, and the refusal has to be actionable — the caller must be able to
// retry it against the right node without guessing. This error therefore
// carries the controller's node id and, once it has announced itself, its
// admin address; Error() spells both out.
//
// It unwraps to ErrNotLeader, so callers that only match the class of failure
// (errors.Is(err, ErrNotLeader)) keep working unchanged.
type NotControllerError struct {
	// LeaderID is the controller's node id; empty before an election settles.
	LeaderID string
	// AdminAddr is the controller's admin address; empty while the leader has
	// not registered yet (registration is what teaches every node the admin
	// addresses, so a freshly elected leader is nameless for a moment).
	AdminAddr string
}

func (e *NotControllerError) Error() string {
	who := e.LeaderID
	if who == "" {
		who = "unknown (no leader elected yet)"
	}
	if e.AdminAddr == "" {
		return fmt.Sprintf(
			"not the controller (Raft leader): the controller is %s and its admin address is not known yet; retry this command against that node",
			who)
	}
	return fmt.Sprintf(
		"not the controller (Raft leader): the controller is %s at %s; send this command directly to that admin address (this node does not forward it)",
		who, e.AdminAddr)
}

// Is keeps errors.Is(err, ErrNotLeader) working for controller-only commands.
func (e *NotControllerError) Is(target error) bool { return target == ErrNotLeader }

// Controller reports where controller-only commands must be sent: the Raft
// leader's node id and its admin address. The address comes from the
// replicated peer directory (the registration announcement is what fills it
// in), so it is empty when the leader has not registered yet — the caller then
// only learns the id.
func (e *Engine) Controller() (id, adminAddr string) {
	if e.node == nil {
		return "", ""
	}
	id = e.node.LeaderID()
	if p, ok := e.TableSnapshot().Peers[id]; ok {
		adminAddr = p.AdminAddr
	}
	return id, adminAddr
}

// controllerGuard returns a NotControllerError when this node is not the
// controller, and nil when it is (or when there is no Raft node to ask, which
// cannot be a controller either — but is only reachable from tests/dev).
func (e *Engine) controllerGuard() error {
	if e.node == nil {
		return &NotControllerError{}
	}
	if e.node.IsLeader() {
		return nil
	}
	id, addr := e.Controller()
	return &NotControllerError{LeaderID: id, AdminAddr: addr}
}
