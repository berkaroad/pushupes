package raft

import (
	"testing"
	"time"
)

// TestRaftTimingDefaults pins the two shipped timings and the rule the config
// enforces on them. The knobs are a ratio: a follower that waits less than
// twice the heartbeat starts an election on a single late heartbeat, which is
// the churn an operator raising these values is trying to stop — so the pair
// must be refused rather than accepted and then explained by logs.
func TestRaftTimingDefaults(t *testing.T) {
	base := func() Config {
		return Config{
			NodeID:  "node-1",
			DataDir: t.TempDir(),
			Voters:  []Voter{{ID: "node-1", Addr: "127.0.0.1:1"}},
		}
	}

	// Unset: the shipped defaults, and they satisfy the rule themselves.
	c := base()
	if err := c.withDefaults(); err != nil {
		t.Fatalf("withDefaults: %v", err)
	}
	if c.HeartbeatTimeout != DefaultHeartbeatTimeout {
		t.Fatalf("heartbeat default %s want %s", c.HeartbeatTimeout, DefaultHeartbeatTimeout)
	}
	if c.ElectionTimeout != DefaultElectionTimeout {
		t.Fatalf("election default %s want %s", c.ElectionTimeout, DefaultElectionTimeout)
	}
	if DefaultElectionTimeout < minElectionPerHeartbeat*DefaultHeartbeatTimeout {
		t.Fatalf("shipped timings %s/%s violate the rule the config enforces (%dx)",
			DefaultElectionTimeout, DefaultHeartbeatTimeout, minElectionPerHeartbeat)
	}

	// Explicit values are kept as given.
	c = base()
	c.HeartbeatTimeout, c.ElectionTimeout = 250*time.Millisecond, 3*time.Second
	if err := c.withDefaults(); err != nil {
		t.Fatalf("withDefaults: %v", err)
	}
	if c.HeartbeatTimeout != 250*time.Millisecond || c.ElectionTimeout != 3*time.Second {
		t.Fatalf("explicit timings were rewritten: %s/%s", c.HeartbeatTimeout, c.ElectionTimeout)
	}

	// The floor: 2x is the smallest accepted pair, below it the config is
	// refused (1x is what makes a cluster vote its leader out under load).
	for _, tc := range []struct {
		heartbeat, election time.Duration
		wantErr             bool
	}{
		{100 * time.Millisecond, 100 * time.Millisecond, true},
		{100 * time.Millisecond, 199 * time.Millisecond, true},
		{100 * time.Millisecond, 200 * time.Millisecond, false},
		{500 * time.Millisecond, 5 * time.Second, false},
	} {
		c := base()
		c.HeartbeatTimeout, c.ElectionTimeout = tc.heartbeat, tc.election
		err := c.withDefaults()
		if tc.wantErr && err == nil {
			t.Fatalf("heartbeat %s / election %s accepted, want refused", tc.heartbeat, tc.election)
		}
		if !tc.wantErr && err != nil {
			t.Fatalf("heartbeat %s / election %s refused: %v", tc.heartbeat, tc.election, err)
		}
	}
}
