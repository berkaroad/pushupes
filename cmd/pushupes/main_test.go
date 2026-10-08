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

package main

import (
	"strings"
	"testing"

	"github.com/berkaroad/pushupes/internal/cluster"
)

func TestParsePeers(t *testing.T) {
	// primary form: node-id=http://host:peerport — the peer (Raft) address
	// is all the config carries; admin/client land via self-registration.
	// The scheme is notation only: PeerAddr stays host:port.
	got, err := parsePeers("node1=http://192.168.1.1:3001,node2=http://192.168.1.2:3001,node3=http://192.168.1.3:3002")
	if err != nil {
		t.Fatalf("named form: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("named form: %d peers", len(got))
	}
	if got[0].ID != "node1" || got[0].PeerAddr != "http://192.168.1.1:3001" {
		t.Fatalf("named form peer-1: %+v", got[0])
	}
	for i, p := range got {
		if p.AdminAddr != "" || p.ClientAddr != "" {
			t.Fatalf("peer %d: admin/client must arrive by registration: %+v", i, p)
		}
	}

	// equals-separated id form without scheme still parses (lenient):
	// node-1=host:port
	got, err = parsePeers("node-1=127.0.0.1:9891,node-2=127.0.0.1:9892")
	if err != nil {
		t.Fatalf("id= form: %v", err)
	}
	if len(got) != 2 || got[0].ID != "node-1" || got[0].PeerAddr != "http://127.0.0.1:9891" {
		t.Fatalf("id= form: %+v", got)
	}

	// bare http://host:port without an id: the address doubles as the id
	got, err = parsePeers("http://192.168.1.1:3001")
	if err != nil {
		t.Fatalf("bare scheme form: %v", err)
	}
	if got[0].ID != "192.168.1.1:3001" || got[0].PeerAddr != "http://192.168.1.1:3001" {
		t.Fatalf("bare scheme form: %+v", got[0])
	}

	// bare ip:port without an id: the address doubles as the node id
	got, err = parsePeers("192.168.1.1:3001, 192.168.1.2:3001 ")
	if err != nil {
		t.Fatalf("bare form: %v", err)
	}
	if len(got) != 2 || got[1].ID != "192.168.1.2:3001" || got[1].PeerAddr != "http://192.168.1.2:3001" {
		t.Fatalf("bare form: %+v", got)
	}

	// legacy 4-segment form (what cluster.sh emits): host defaults to
	// 127.0.0.1, ports map to peer/admin/client in order.
	got, err = parsePeers("node-1:8391:8091:8591,node-2:8392:8092:8592")
	if err != nil {
		t.Fatalf("legacy form: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("legacy form: %d peers", len(got))
	}
	if got[1].ID != "node-2" || got[1].PeerAddr != "http://127.0.0.1:8392" ||
		got[1].AdminAddr != "http://127.0.0.1:8092" || got[1].ClientAddr != "http://127.0.0.1:8592" {
		t.Fatalf("legacy form peer-2: %+v", got[1])
	}

	// explicit-host 5-segment form: cross-machine deployments must be able
	// to carry real addresses — the routing table dials these verbatim.
	got, err = parsePeers(" node-2:10.0.0.5:8392:8092:8592 ")
	if err != nil {
		t.Fatalf("explicit host: %v", err)
	}
	if got[0].PeerAddr != "http://10.0.0.5:8392" || got[0].AdminAddr != "http://10.0.0.5:8092" ||
		got[0].ClientAddr != "http://10.0.0.5:8592" {
		t.Fatalf("explicit host: %+v", got[0])
	}

	// empty csv is legal (single-node start)
	if p, err := parsePeers("  "); err != nil || p != nil {
		t.Fatalf("empty csv: %v %v", p, err)
	}

	// malformed tokens are rejected AT PARSE TIME, not at first dial
	for _, bad := range []string{
		"node-1",                 // too few segments
		"node-1:1:2",             // 3 segments
		"node-1:1:2:3:4:5",       // 6 segments
		"node-1::8091:8591",      // empty port
		"node-1:a:8091:8591",     // non-numeric port
		"node-1:0:8091:8591",     // port 0
		"node-1:70000:8091:8591", // out of range
		":8391:8091:8591",        // empty id
		"node-1::8392:8092:8592", // explicit-host empty host
	} {
		if _, err := parsePeers(bad); err == nil {
			t.Errorf("want error for %q", bad)
		} else if !strings.Contains(err.Error(), "bad peer") {
			t.Errorf("%q: unexpected error %v", bad, err)
		}
	}
}

// Membership is written on first start, so exactly one node may author it: the
// one leading -peers (or an explicit -bootstrap). Every other configured node
// starts as a seed and offers itself through the configured members — a second
// author would write a competing entry at index 1 and split the cluster into
// raft groups that each commit on their own.
func TestBootstrapEligible(t *testing.T) {
	peers := []cluster.Peer{
		{ID: "node-3", PeerAddr: "127.0.0.1:8393"},
		{ID: "node-1", PeerAddr: "127.0.0.1:8391"},
		{ID: "node-2", PeerAddr: "127.0.0.1:8392"},
	}
	if got := canonicalFirstPeer(peers); got != "node-1" {
		t.Fatalf("canonical first = %q, want node-1 (smallest id, whatever the flag order)", got)
	}
	if !bootstrapEligible("node-1", peers, false) {
		t.Fatal("the node leading -peers authors the configuration")
	}
	for _, id := range []string{"node-2", "node-3"} {
		if bootstrapEligible(id, peers, false) {
			t.Fatalf("%s must not author a competing configuration", id)
		}
		if !bootstrapEligible(id, peers, true) {
			t.Fatalf("%s with an explicit -bootstrap must still author it", id)
		}
	}
	// A lone node (no -peers, or only itself) authors its own cluster.
	solo := []cluster.Peer{{ID: "node-1", PeerAddr: "127.0.0.1:8391"}}
	if !bootstrapEligible("node-1", solo, false) {
		t.Fatal("a single-node cluster must bootstrap")
	}
	if canonicalFirstPeer(nil) != "" {
		t.Fatal("no peers means nobody leads the list")
	}
}

func TestJoinTargets(t *testing.T) {
	peers := []cluster.Peer{
		{ID: "node-1", PeerAddr: "127.0.0.1:8391"},
		{ID: "node-2", PeerAddr: "127.0.0.1:8392"},
		{ID: "node-3", PeerAddr: ""}, // not yet addressed: nothing to dial
		{ID: "node-4", PeerAddr: "127.0.0.1:8394"},
	}
	// No -join: offer through the configured members, never through ourselves.
	got := joinTargets("node-2", peers, "")
	want := []string{"127.0.0.1:8391", "127.0.0.1:8394"}
	if len(got) != len(want) {
		t.Fatalf("targets = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("targets = %v, want %v", got, want)
		}
	}
	// An explicit -join names one member and nothing else.
	if got := joinTargets("node-2", peers, " node-1:8391 "); len(got) != 1 || got[0] != "node-1:8391" {
		t.Fatalf("-join targets = %v", got)
	}
	// Nobody to ask.
	if got := joinTargets("node-1", []cluster.Peer{{ID: "node-1", PeerAddr: "127.0.0.1:8391"}}, ""); len(got) != 0 {
		t.Fatalf("targets = %v, want none", got)
	}
}

// The env readers must accept exactly what the flags accept: a knob an
// operator can write on the command line has to be writable through the
// environment the same way, which for a size knob means the suffixed forms
// (256MiB), not only a raw byte count.
func TestEnvByteSizeOr(t *testing.T) {
	const key = "PUSHUPES_SEGMENT_BYTES"
	if got := envByteSizeOr(key, 42); got != 42 {
		t.Fatalf("unset: got %d, want the default 42", got)
	}
	for _, tc := range []struct {
		val  string
		want int64
	}{
		{"268435456", 268435456},
		{"256MiB", 256 << 20},
		{"1GiB", 1 << 30},
	} {
		t.Setenv(key, tc.val)
		if got := envByteSizeOr(key, 0); got != tc.want {
			t.Errorf("%s=%q: got %d, want %d", key, tc.val, got, tc.want)
		}
	}
	// Same input, same bytes as the flag's own value parser.
	var b byteSize
	if err := b.Set("256MiB"); err != nil {
		t.Fatal(err)
	}
	t.Setenv(key, "256MiB")
	if got := envByteSizeOr(key, 0); got != int64(b) {
		t.Fatalf("env %d != flag %d for the same size", got, int64(b))
	}
}

// envInt64Or backs -flush-messages: the default when unset, the record count
// when set, and 0 meaning "off" without being mistaken for "unset".
func TestEnvInt64Or(t *testing.T) {
	const key = "PUSHUPES_FLUSH_MESSAGES"
	if got := envInt64Or(key, 1000); got != 1000 {
		t.Fatalf("unset: got %d, want the default 1000", got)
	}
	t.Setenv(key, "2000")
	if got := envInt64Or(key, 1000); got != 2000 {
		t.Fatalf("set: got %d, want 2000", got)
	}
	t.Setenv(key, "0")
	if got := envInt64Or(key, 1000); got != 0 {
		t.Fatalf("0 must mean off, got %d", got)
	}
}
