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

package config

import (
	"runtime"
	"testing"
	"time"
)

// seeded is the shape the command hands Validate: the layer-owned fields hold
// the values their package ships (here the same magnitudes, since this test
// cannot import those packages — it would close the data → config → data
// cycle). The command's own test pins the real seeded values.
func seeded() *Config {
	c := Default()
	c.SegmentBytes = 64 << 20
	c.FlushMessages = 1000
	c.FlushInterval = 100 * time.Millisecond
	c.FlushParallelism = 1
	c.SlotCount = 1024
	c.RaftFlushInterval = 100 * time.Millisecond
	c.RaftSegmentBytes = 64 << 20
	return c
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
	var v int64
	if err := (sizeFlag{&v}).Set("256MiB"); err != nil {
		t.Fatal(err)
	}
	t.Setenv(key, "256MiB")
	if got := envByteSizeOr(key, 0); got != v {
		t.Fatalf("env %d != flag %d for the same size", got, v)
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

// Default covers the knobs this package owns and leaves the layer-owned fields
// for the command to seed: an unset duration or a zero address would silently
// turn a feature off.
func TestDefaultCoversOwnedKnobs(t *testing.T) {
	c := Default()
	durations := map[string]time.Duration{
		"AdoptInterval":     c.AdoptInterval,
		"FetchSettle":       c.FetchSettle,
		"DropAfter":         c.DropAfter,
		"RebalanceInterval": c.RebalanceInterval,
		"StartupGrace":      c.StartupGrace,
	}
	for name, d := range durations {
		if d <= 0 {
			t.Errorf("%s defaults to %v, want a positive duration", name, d)
		}
	}
	if c.GRPCMaxMsgBytes != 4<<20 {
		t.Errorf("grpc max message default = %d, want 4MiB", c.GRPCMaxMsgBytes)
	}
	if c.NodeID == "" || c.DataDir == "" || c.AdminAddr == "" || c.ClientAddr == "" || c.PeerAddr == "" {
		t.Errorf("address and path defaults must be set: %+v", c)
	}
	// The layer-owned fields are the command's to fill: Default leaves them
	// zero so a value never has two definitions.
	if c.SegmentBytes != 0 || c.FlushMessages != 0 || c.FlushInterval != 0 ||
		c.FlushParallelism != 0 || c.SlotCount != 0 || c.ReplicaPolicy != "" ||
		c.RaftFlushInterval != 0 || c.RaftSegmentBytes != 0 ||
		c.RaftHeartbeatTimeout != 0 || c.RaftElectionTimeout != 0 {
		t.Errorf("Default must not carry layer-owned values: %+v", c)
	}
}

// Validate accepts the seeded configuration the command builds, and rejects
// the shapes the flags document as startup errors.
func TestValidate(t *testing.T) {
	if err := seeded().Validate(); err != nil {
		t.Fatalf("the seeded configuration must validate: %v", err)
	}
	cases := map[string]func(*Config){
		"zero slot count":         func(c *Config) { c.SlotCount = 0 },
		"zero grpc cap":           func(c *Config) { c.GRPCMaxMsgBytes = 0 },
		"zero flush parallelism":  func(c *Config) { c.FlushParallelism = 0 },
		"negative rebalance":      func(c *Config) { c.RebalanceInterval = -time.Second },
		"negative startup grace":  func(c *Config) { c.StartupGrace = -time.Second },
		"negative fetch settle":   func(c *Config) { c.FetchSettle = -time.Microsecond },
		"zero raft segment bytes": func(c *Config) { c.RaftSegmentBytes = 0 },
	}
	for name, mutate := range cases {
		c := seeded()
		mutate(c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: Validate accepted it", name)
		}
	}
}

// The worker counts follow the process CPU width.
func TestProcsDerivedWorkers(t *testing.T) {
	procs := runtime.GOMAXPROCS(0)
	if Procs != procs {
		t.Fatalf("Procs = %d, want runtime.GOMAXPROCS(0) = %d", Procs, procs)
	}
	if BatchPoolWorkers != BatchPoolWorkersPerCPU*procs {
		t.Errorf("BatchPoolWorkers = %d, want %d x %d", BatchPoolWorkers, BatchPoolWorkersPerCPU, procs)
	}
	if ApplyWorkers != ApplyWorkersPerCPU*procs || ApplyParallelMin != procs {
		t.Errorf("apply pool = %d/%d, want %d/%d", ApplyWorkers, ApplyParallelMin, ApplyWorkersPerCPU*procs, procs)
	}
}
