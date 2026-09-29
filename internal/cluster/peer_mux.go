// peer_mux.go — one TCP listener on the peer port, demuxed by the first
// byte of every connection into two surfaces:
//
//	'P'  → the HTTP/2 client preface ("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
//	       starts with 'P'): handed to the PeerService gRPC server.
//	other → the Raft transport (its wire protocol opens with RPC version
//	       byte 0, so it can never collide with 'P').
//
// This is how one configured port per node (the peers CSV only ever names
// the peer address) carries consensus AND the full inter-node data plane.
// Both surfaces receive a conn that replays the peeked byte: Raft needs
// its version header, gRPC needs the complete HTTP/2 preface.
package cluster

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

const (
	peerPeekTimeout = 10 * time.Second
	peerAcceptQueue = 64
	peerDialTimeout = 2 * time.Second
)

type acceptResult struct {
	conn net.Conn
	err  error
}

// peerMux implements raft.StreamLayer over a plain TCP listener, diverting
// gRPC (HTTP/2 preface) connections to GRPCListener() and handing
// everything else — including indeterminate connections — to Raft.
type peerMux struct {
	listener  *net.TCPListener
	advertise *net.TCPAddr
	raftCh    chan acceptResult
	grpcCh    chan acceptResult
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
		raftCh:    make(chan acceptResult, peerAcceptQueue),
		grpcCh:    make(chan acceptResult, peerAcceptQueue),
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
		peeked, isGRPC, err := peekMagic(tc)
		if err != nil {
			// stalled or broken connection: drop it
			c.Close()
			continue
		}
		if isGRPC {
			if !m.push(m.grpcCh, acceptResult{conn: peeked}) {
				c.Close() // gRPC server not draining yet; client retries
			}
			continue
		}
		// Raft (or garbage — let the transport reject it). peeked
		// replays the consumed byte so raft still sees its version header.
		m.push(m.raftCh, acceptResult{conn: peeked})
	}
}

// peekMagic reads the first byte with a deadline and returns a conn that
// replays it (both consumers need the full stream); the deadline is
// cleared before handing over (raft and gRPC manage their own).
func peekMagic(tc *net.TCPConn) (net.Conn, bool, error) {
	_ = tc.SetReadDeadline(time.Now().Add(peerPeekTimeout))
	br := bufio.NewReader(tc)
	if _, err := br.Peek(1); err != nil {
		_ = tc.SetReadDeadline(time.Time{})
		return nil, false, err
	}
	magic, _ := br.ReadByte() // safe: Peek succeeded
	_ = tc.SetReadDeadline(time.Time{})
	return &replayConn{Conn: tc, br: br, pending: []byte{magic}}, magic == grpcMagicByte, nil
}

// replayConn re-serves the one byte consumed during demux. Reads go
// through the buffered reader; deadlines/other methods hit the raw conn
// (the consumer's own deadlines apply to future kernel-side reads).
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
	for _, ch := range []chan acceptResult{m.raftCh, m.grpcCh} {
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

// ---- gRPC listener surface ----

// GRPCListener returns the net.Listener view of the demuxed HTTP/2
// connections; feed it to Engine.ServePeer.
func (m *peerMux) GRPCListener() net.Listener { return &grpcDemux{mux: m} }

type grpcDemux struct{ mux *peerMux }

var _ net.Listener = (*grpcDemux)(nil)

func (r *grpcDemux) Accept() (net.Conn, error) {
	select {
	case res := <-r.mux.grpcCh:
		return res.conn, res.err
	case <-r.mux.closed:
		return nil, net.ErrClosed
	}
}

func (r *grpcDemux) Addr() net.Addr { return r.mux.advertise }
func (r *grpcDemux) Close() error   { return r.mux.Close() }
