package cluster

import (
	"net"
	"testing"
)

// newPeerHarness serves the engine's PeerService on a loopback TCP port and
// returns its host:port. The connection path is the exact production one:
// the real peerMux demuxes the HTTP/2 preface ('P') to gRPC and replays the
// peeked byte, so this also covers the mux classification + replayConn.
func newPeerHarness(t *testing.T, e *Engine) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := &peerMux{
		listener:  ln.(*net.TCPListener),
		advertise: ln.Addr().(*net.TCPAddr),
		raftCh:    make(chan acceptResult, peerAcceptQueue),
		grpcCh:    make(chan acceptResult, peerAcceptQueue),
		closed:    make(chan struct{}),
	}
	go mux.demux()
	srv := e.ServePeer(mux.GRPCListener())
	t.Cleanup(func() {
		srv.Stop()
		mux.Close()
	})
	return ln.Addr().String()
}
