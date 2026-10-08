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
