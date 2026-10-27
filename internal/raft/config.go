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

// This file is the one home of the consensus package's shipped defaults and of
// the timing constants its own code runs on: a reader asking "what does raft
// start with" reads this file instead of the seven files that use the values.
//
// The exported Default* names are this package's defaults — the values
// Config.withDefaults applies when a field is zero, and the ones
// -raft-heartbeat-timeout / -raft-election-timeout are seeded from. The
// unexported names are the package's internal tuning: they are not operator
// knobs, they do not leave the package, and they change only with the code
// that spends them.
//
// The wire and on-disk contract (frame magic, frame kind and message type
// bytes, WAL record kinds, snapshot magic and header layout, the frame size
// bound) stays beside the encoder that defines it: those answer "what does this
// byte mean", not "how long does this wait".

package raft

import "time"

// Defaults: what Config.withDefaults applies, and the shipped timings.
const (
	DefaultHeartbeatTimeout = 100 * time.Millisecond
	DefaultElectionTimeout  = 500 * time.Millisecond
	// DefaultFlushInterval is the consensus WAL's group-commit window: an
	// append opens it, everything that arrives before it expires shares one
	// fsync. It bounds how long a committed entry can sit unsynced.
	DefaultFlushInterval = 100 * time.Millisecond
	// minElectionPerHeartbeat is the smallest ratio the two knobs may have:
	// below it one late heartbeat starts an election, which is the very churn
	// the caller raising these values is trying to stop.
	minElectionPerHeartbeat = 2

	// DefaultApplyTimeout bounds how long one committed batch may take to
	// apply before the applier reports the stall.
	DefaultApplyTimeout = 10 * time.Second
	// DefaultSnapshotThreshold is the number of log entries past the last
	// snapshot that triggers the next one.
	DefaultSnapshotThreshold uint64 = 1024
	// DefaultSnapshotInterval is the cadence the snapshot check runs at, so a
	// quiet log still gets a snapshot eventually.
	DefaultSnapshotInterval = 30 * time.Second
	// DefaultSegmentBytes is the consensus WAL's segment roll threshold: the
	// same 64MiB the data plane uses, for the same reason (per-segment index
	// and descriptor cost scales with segment count, not with bytes).
	DefaultSegmentBytes int64 = 64 << 20
)

// Internal tuning: values this package spends itself, never settable from
// outside.
const (
	// rpcTimeout bounds one replicate RPC (AppendEntries / RequestVote) to a
	// peer; snapshotRPCTimeout bounds one InstallSnapshot, which ships a whole
	// image. maxAppendBatch caps how many entries one AppendEntries carries.
	rpcTimeout         = 5 * time.Second
	snapshotRPCTimeout = 30 * time.Second
	maxAppendBatch     = 64

	// rpcHandlerTimeout bounds one inbound RPC's handler on the serving side,
	// so a wedged handler cannot pin a connection forever.
	rpcHandlerTimeout = 30 * time.Second

	// learnerCatchUpBudget is how long a newly added learner gets to catch up
	// before it is counted as still-behind for placement decisions.
	learnerCatchUpBudget = 20 * time.Second

	// confSweepInterval is the cadence of the configuration sweep that retries
	// a membership change the leader has not managed to append yet.
	confSweepInterval = 200 * time.Millisecond

	// handshakeTimeout bounds the peer transport's opening handshake: the
	// version byte exchange before any frame is accepted.
	handshakeTimeout = 2 * time.Second

	// snapshotKeep is how many snapshot files stay on disk: the current one
	// plus the one a lagging follower may still be receiving.
	snapshotKeep = 2
)
