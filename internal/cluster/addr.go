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
// grpc dial, raft transport).
func HostPort(addr string) string {
	if i := strings.Index(addr, "://"); i >= 0 {
		return addr[i+3:]
	}
	return addr
}

// HTTPURL builds a request URL from a possibly-schemeless address,
// defaulting to http:// (lenient for tests and internal callers).
func HTTPURL(addr, path string) string {
	return NormalizeAddr(addr) + path
}
