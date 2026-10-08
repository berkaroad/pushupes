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

// transport.go — TCP client/server for the peer-plane RPC.
//
// One long-lived connection per (dialer -> target) direction carries every
// request the dialer sends to that target, multiplexed by request id. The
// acceptor only answers on a connection it accepted; it never initiates on it.
// That keeps the model simple: each node dials the peers it wants to talk to
// and serves whoever dials it.
package raft

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	handshakeTimeout = 2 * time.Second
)

// Transport is the peer-plane listener. Dial returns a raw connection; the
// RPC layer performs its own handshake on it. The cluster package implements
// this over the port that also demuxes the inter-node gRPC plane.
type Transport interface {
	Accept() (net.Conn, error)
	Dial(addr string, timeout time.Duration) (net.Conn, error)
	Addr() string
	Close() error
}

type frameReply struct {
	typ     byte
	payload []byte
	err     error
}

// peerClient is the requester side: a lazily dialled, reused connection.
type peerClient struct {
	self   string
	peerID string
	addr   string
	tr     Transport
	log    Logger

	mu     sync.Mutex
	wmu    sync.Mutex
	conn   net.Conn
	bw     *bufio.Writer
	pend   map[uint64]chan frameReply
	nextID uint64
	closed bool
}

func newPeerClient(self, peerID, addr string, tr Transport, log Logger) *peerClient {
	return &peerClient{
		self:   self,
		peerID: peerID,
		addr:   addr,
		tr:     tr,
		log:    log,
		pend:   make(map[uint64]chan frameReply),
	}
}

func (c *peerClient) call(typ byte, payload []byte, timeout time.Duration) (byte, []byte, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0, nil, ErrClosed
	}
	if c.conn == nil {
		if err := c.dialLocked(timeout); err != nil {
			c.mu.Unlock()
			return 0, nil, err
		}
	}
	c.nextID++
	id := c.nextID
	ch := make(chan frameReply, 1)
	c.pend[id] = ch
	conn := c.conn
	bw := c.bw
	c.mu.Unlock()

	if err := c.write(conn, bw, id, typ, payload, timeout); err != nil {
		c.mu.Lock()
		delete(c.pend, id)
		c.mu.Unlock()
		c.reset(conn)
		return 0, nil, err
	}

	select {
	case r := <-ch:
		return r.typ, r.payload, r.err
	case <-time.After(timeout):
		c.mu.Lock()
		delete(c.pend, id)
		c.mu.Unlock()
		return 0, nil, fmt.Errorf("raft: rpc %d to %s timed out after %s", typ, c.addr, timeout)
	}
}

// dialLocked dials and handshakes. Caller holds c.mu.
func (c *peerClient) dialLocked(timeout time.Duration) error {
	dialTO := timeout
	if dialTO > handshakeTimeout {
		dialTO = handshakeTimeout
	}
	conn, err := c.tr.Dial(c.addr, dialTO)
	if err != nil {
		return err
	}
	if err := writeHandshake(conn, c.self, handshakeTimeout); err != nil {
		conn.Close()
		return err
	}
	peer, err := readHandshake(conn, handshakeTimeout)
	if err != nil {
		conn.Close()
		return err
	}
	if c.peerID != "" && peer != c.peerID {
		conn.Close()
		return fmt.Errorf("raft: peer at %s identified as %q, expected %q", c.addr, peer, c.peerID)
	}
	c.conn = conn
	c.bw = bufio.NewWriter(conn)
	go c.readLoop(conn, bufio.NewReader(conn))
	return nil
}

// write sends one frame on a connection captured under the client lock; it
// never reads the client's conn field, so a concurrent close/reset cannot race
// with it.
func (c *peerClient) write(conn net.Conn, bw *bufio.Writer, id uint64, typ byte, payload []byte, timeout time.Duration) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if conn == nil {
		return fmt.Errorf("raft: connection to %s is gone", c.addr)
	}
	if timeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(timeout))
		defer conn.SetWriteDeadline(time.Time{})
	}
	return writeFrame(bw, frameKindRequest, id, typ, payload)
}

func (c *peerClient) readLoop(conn net.Conn, br *bufio.Reader) {
	for {
		kind, id, typ, payload, err := readFrame(br)
		if err != nil {
			c.reset(conn)
			return
		}
		if kind != frameKindResponse {
			continue
		}
		c.mu.Lock()
		ch := c.pend[id]
		delete(c.pend, id)
		c.mu.Unlock()
		if ch != nil {
			ch <- frameReply{typ: typ, payload: payload}
		}
	}
}

// reset drops the connection (if it is still the current one) and fails every
// in-flight request.
func (c *peerClient) reset(conn net.Conn) {
	c.mu.Lock()
	if conn != nil && c.conn != conn {
		c.mu.Unlock()
		return
	}
	c.conn = nil
	c.bw = nil
	pend := c.pend
	c.pend = make(map[uint64]chan frameReply)
	c.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
	for _, ch := range pend {
		ch <- frameReply{err: fmt.Errorf("raft: connection to %s lost", c.addr)}
	}
}

func (c *peerClient) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	conn := c.conn
	c.conn = nil
	pend := c.pend
	c.pend = make(map[uint64]chan frameReply)
	c.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
	for _, ch := range pend {
		ch <- frameReply{err: ErrClosed}
	}
}

// rpcServer accepts connections and answers the requests they carry.
type rpcServer struct {
	selfID string
	tr     Transport
	handle func(typ byte, payload []byte) (byte, []byte, error)
	// member reports whether a handshaked peer is allowed to speak the
	// consensus protocol here (a current member or an accepted learner).
	member func(id string) bool
	log    Logger

	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	cmu   sync.Mutex
	conns map[net.Conn]struct{}
}

func newRPCServer(selfID string, tr Transport, handle func(byte, []byte) (byte, []byte, error), member func(string) bool, log Logger) *rpcServer {
	return &rpcServer{
		selfID:  selfID,
		tr:      tr,
		handle:  handle,
		member:  member,
		log:     log,
		closeCh: make(chan struct{}),
		conns:   map[net.Conn]struct{}{},
	}
}

func (s *rpcServer) run() {
	for {
		conn, err := s.tr.Accept()
		if err != nil {
			select {
			case <-s.closeCh:
				return
			default:
			}
			s.log.Warnf("raft: accept on peer plane failed: %v", err)
			select {
			case <-s.closeCh:
				return
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}
		s.cmu.Lock()
		if s.closing() {
			s.cmu.Unlock()
			conn.Close()
			continue
		}
		s.conns[conn] = struct{}{}
		s.wg.Add(1)
		s.cmu.Unlock()
		go func() {
			defer s.wg.Done()
			defer func() {
				s.cmu.Lock()
				delete(s.conns, conn)
				s.cmu.Unlock()
			}()
			s.serve(conn)
		}()
	}
}

func (s *rpcServer) closing() bool {
	select {
	case <-s.closeCh:
		return true
	default:
		return false
	}
}

func (s *rpcServer) serve(conn net.Conn) {
	defer conn.Close()
	peer, err := readHandshake(conn, handshakeTimeout)
	if err != nil {
		s.log.Debugf("raft: inbound handshake failed: %v", err)
		return
	}
	if err := writeHandshake(conn, s.selfID, handshakeTimeout); err != nil {
		return
	}
	if s.member != nil && s.member(peer) == false && peer != "" {
		// A node that left the cluster (or one that has not joined yet) must
		// not be able to talk the consensus protocol at this node: a removed
		// member that kept its old configuration would otherwise keep
		// campaigning and mutating this node's vote. The connection is served
		// read-only for nothing — it is dropped after an explanatory debug
		// line, so the sender sees a closed connection rather than silence.
		s.log.Debugf("raft: refusing consensus traffic from %s: not a member", peer)
		return
	}
	s.log.Debugf("raft: serving peer %s", peer)

	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)
	var wmu sync.Mutex
	for {
		kind, id, typ, payload, err := readFrame(br)
		if err != nil {
			return
		}
		if kind != frameKindRequest {
			return
		}
		rtyp, rpayload, herr := s.handle(typ, payload)
		if herr != nil {
			rtyp, rpayload = msgError, []byte(herr.Error())
		}
		wmu.Lock()
		werr := writeFrame(bw, frameKindResponse, id, rtyp, rpayload)
		wmu.Unlock()
		if werr != nil {
			return
		}
	}
}

func (s *rpcServer) close() {
	s.closeOnce.Do(func() { close(s.closeCh) })
	s.cmu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.cmu.Unlock()
	s.wg.Wait()
}
