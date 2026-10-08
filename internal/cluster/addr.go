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

import "strings"

// Address conventions.
//
// All stored addresses (Peer.PeerAddr/AdminAddr/ClientAddr, Config, the
// routing table, status JSON) are canonical: they carry a scheme. Operators
// may write them with or without one — without a protocol the default
// "http://" is applied; an explicit protocol is kept as passed.
//
// Scheme matters per plane: admin addresses are HTTP URLs, client
// addresses are dialed by gRPC (scheme stripped to host:port), peer
// addresses ride the raw Raft TCP transport (scheme stripped to host:port).

// NormalizeAddr applies the default scheme: an address without a protocol
// gets "http://" prepended; an explicit protocol is preserved.
func NormalizeAddr(addr string) string {
	if addr == "" || strings.Contains(addr, "://") {
		return addr
	}
	return "http://" + addr
}

// HostPort strips the scheme for TCP-level use (net.Listen, net.Dial,
// grpc dial, raft transport). There are no HTTP calls between nodes
// anymore (all node-to-node traffic is PeerService gRPC on the peer port),
// so nothing here builds URLs — operators' http:// notation is pure
// configuration sugar consumed by NormalizeAddr.
func HostPort(addr string) string {
	if i := strings.Index(addr, "://"); i >= 0 {
		return addr[i+3:]
	}
	return addr
}
