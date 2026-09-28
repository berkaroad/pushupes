// Data-plane address registration ("self-discovery" for admin/client ports).
//
// Operators configure peers with a single address each — the Raft peer
// endpoint: "node1=192.168.1.1:3001,node2=192.168.1.2:3001,...". The admin
// (replication) and client (gRPC) addresses are NOT configured: each node
// knows its own listeners, so it announces them into the routing table via
// a Raft-committed OpRegister command. MOVED targets, mfetch and liveness
// probes then see correct cross-machine addresses without extra config.
//
// The announcement rides the peer port: registration connections are
// demuxed from Raft transport traffic by the first byte of the stream
// (peerMux below). Raft's wire protocol starts every connection with an
// RPC version byte (0), so 'P' can never collide. Followers relay
// registrations to the current leader's peer address; the leader commits
// them. Announcements are idempotent and retried periodically, which makes
// any bootstrap-order race (voter not yet joined into the table)
// self-healing.
package cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// registerMagic prefixes registration connections on the peer port.
const registerMagic = byte('P')

const (
	registerPeekTimeout = 10 * time.Second
	registerIOTimeout   = 5 * time.Second
	registerDialTimeout = 2 * time.Second
	registerQueueSize   = 16
	registerRetryFast   = time.Second
	registerRetrySteady = 10 * time.Second
)

// Registration announces one node's own data-plane addresses.
type Registration struct {
	ID         string `json:"id"`
	AdminAddr  string `json:"admin_addr"`
	ClientAddr string `json:"client_addr"`
}

func (r *Registration) validate() error {
	if r.ID == "" || r.AdminAddr == "" || r.ClientAddr == "" {
		return fmt.Errorf("register: incomplete announcement %+v", *r)
	}
	return nil
}

// ---- wire protocol: magic byte + one JSON Registration, one JSON ack ----

func sendRegistration(peerAddr string, reg *Registration) error {
	conn, err := net.DialTimeout("tcp", HostPort(peerAddr), registerDialTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(registerIOTimeout))
	if _, err := conn.Write([]byte{registerMagic}); err != nil {
		return err
	}
	if err := json.NewEncoder(conn).Encode(reg); err != nil {
		return err
	}
	var ack struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&ack); err != nil {
		return err
	}
	if ack.Error != "" {
		return fmt.Errorf("register %s rejected: %s", reg.ID, ack.Error)
	}
	return nil
}

// ---- peerMux: one TCP listener, demuxed by the first byte ----

type acceptResult struct {
	conn net.Conn
	err  error
}

// peerMux implements raft.StreamLayer over a plain TCP listener, diverting
// registration connections (magic first byte) to Registrations() and
// handing everything else — including indeterminate connections — to the
// Raft transport.
type peerMux struct {
	listener  *net.TCPListener
	advertise *net.TCPAddr
	raftCh    chan acceptResult
	regCh     chan acceptResult
	closed    chan struct{}
	closeOnce sync.Once
}

// newPeerMux binds the peer listener and starts the demux loop.
func newPeerMux(bindAddr string, advertise *net.TCPAddr) (*peerMux, error) {
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	tcpLn, ok := ln.(*net.TCPListener)
	if !ok {
		ln.Close()
		return nil, fmt.Errorf("cluster: peer listener is not TCP")
	}
	m := &peerMux{
		listener:  tcpLn,
		advertise: advertise,
		raftCh:    make(chan acceptResult, registerQueueSize),
		regCh:     make(chan acceptResult, registerQueueSize),
		closed:    make(chan struct{}),
	}
	go m.demux()
	return m, nil
}

func (m *peerMux) demux() {
	defer m.listener.Close()
	for {
		c, err := m.listener.Accept()
		if err != nil {
			var res acceptResult
			select {
			case <-m.closed:
				res = acceptResult{err: net.ErrClosed}
			default:
				res = acceptResult{err: err}
			}
			// unblock waiters, then exit
			m.shutdown(res)
			return
		}
		tc, ok := c.(*net.TCPConn)
		if !ok {
			m.push(m.raftCh, acceptResult{conn: c})
			continue
		}
		_ = tc.SetNoDelay(true)
		peeked, isReg, err := peekMagic(tc)
		if err != nil || !isReg {
			// Raft (or garbage — let the transport reject it). peeked
			// replays the one consumed byte so raft still sees its
			// version header.
			m.push(m.raftCh, acceptResult{conn: peeked})
			continue
		}
		if !m.push(m.regCh, acceptResult{conn: tc}) {
			tc.Close() // registrations not being drained: drop, retried
		}
	}
}

// peekMagic reads the first byte with a deadline and returns a conn that
// replays it; the deadline is cleared before handing over (raft manages
// its own).
func peekMagic(tc *net.TCPConn) (net.Conn, bool, error) {
	_ = tc.SetReadDeadline(time.Now().Add(registerPeekTimeout))
	br := bufio.NewReader(tc)
	_, err := br.Peek(1)
	if err != nil {
		_ = tc.SetReadDeadline(time.Time{})
		return nil, false, err
	}
	magic, _ := br.ReadByte() // safe: Peek succeeded
	_ = tc.SetReadDeadline(time.Time{})
	if magic == registerMagic {
		return tc, true, nil
	}
	return &replayConn{Conn: tc, br: br, pending: []byte{magic}}, false, nil
}

// replayConn re-serves the one byte consumed during demux. Reads go
// through the buffered reader; deadlines/other methods hit the raw conn
// (raft's own deadlines apply to future kernel-side reads).
type replayConn struct {
	net.Conn
	br      *bufio.Reader
	pending []byte
}

func (c *replayConn) Read(p []byte) (int, error) {
	if len(c.pending) > 0 {
		n := copy(p, c.pending)
		c.pending = c.pending[n:]
		if n > 0 {
			return n, nil
		}
	}
	return c.br.Read(p)
}

func (m *peerMux) push(ch chan acceptResult, res acceptResult) bool {
	select {
	case ch <- res:
		return true
	case <-m.closed:
		return false
	}
}

func (m *peerMux) shutdown(res acceptResult) {
	m.closeOnce.Do(func() { close(m.closed) })
	for _, ch := range []chan acceptResult{m.raftCh, m.regCh} {
		for {
			select {
			case ch <- res:
				continue
			default:
			}
			break
		}
	}
}

// ---- raft.StreamLayer (what the transport consumes) ----

func (m *peerMux) Accept() (net.Conn, error) {
	select {
	case res := <-m.raftCh:
		return res.conn, res.err
	case <-m.closed:
		return nil, net.ErrClosed
	}
}

func (m *peerMux) Addr() net.Addr { return m.advertise }

func (m *peerMux) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", HostPort(string(address)), timeout)
}

func (m *peerMux) Close() error {
	m.shutdown(acceptResult{err: net.ErrClosed})
	return m.listener.Close()
}

// registrations is the net.Listener surface of demuxed registration
// connections.
type registrations struct{ mux *peerMux }

var _ net.Listener = (*registrations)(nil)

func (r *registrations) Accept() (net.Conn, error) {
	select {
	case res := <-r.mux.regCh:
		return res.conn, res.err
	case <-r.mux.closed:
		return nil, net.ErrClosed
	}
}

func (r *registrations) Addr() net.Addr { return r.mux.advertise }
func (r *registrations) Close() error   { return r.mux.Close() }

// ---- engine glue ----

// ServeRegistrations accepts and applies registrations until the listener
// is closed (node shutdown).
func (e *Engine) ServeRegistrations(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go e.handleRegistration(c)
	}
}

func (e *Engine) handleRegistration(c net.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(registerIOTimeout))
	// The peer mux already consumed the magic byte when classifying this
	// connection — the stream starts at the JSON body.
	var reg Registration
	if err := json.NewDecoder(bufio.NewReader(c)).Decode(&reg); err != nil {
		writeRegisterAck(c, err.Error())
		return
	}
	if err := e.acceptRegistration(&reg); err != nil {
		e.logger.WithError(err).Warnf("registration %s rejected", reg.ID)
		writeRegisterAck(c, err.Error())
		return
	}
	writeRegisterAck(c, "")
}

func writeRegisterAck(c net.Conn, errMsg string) {
	_ = json.NewEncoder(c).Encode(map[string]string{"error": errMsg})
}

// acceptRegistration commits locally when we are the Raft leader, else
// relays to the leader's peer address.
func (e *Engine) acceptRegistration(reg *Registration) error {
	if err := reg.validate(); err != nil {
		return err
	}
	if e.node.IsLeader() {
		return e.submit(&Command{Op: OpRegister, Peer: &Peer{
			ID: reg.ID, AdminAddr: reg.AdminAddr, ClientAddr: reg.ClientAddr,
		}})
	}
	if addr := e.leaderPeerAddr(); addr != "" {
		return sendRegistration(addr, reg)
	}
	return fmt.Errorf("register: leader unknown yet")
}

// leaderPeerAddr maps the Raft leader id onto its peer address — the one
// address every node knows statically for every configured peer.
func (e *Engine) leaderPeerAddr() string {
	id, _ := e.node.raft.LeaderWithID()
	if id == "" {
		return ""
	}
	if string(id) == e.self {
		return e.node.cfg.PeerAddr
	}
	for _, p := range e.node.cfg.Peers {
		if p.ID == string(id) {
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
		if err := sendRegistration(addr, &a.reg); err == nil {
			return // accepted or relayed; done for this round
		}
	}
}
