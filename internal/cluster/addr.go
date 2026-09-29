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
