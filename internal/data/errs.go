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

package data

import (
	"errors"

	"github.com/berkaroad/pushupes/pkg/client"
)

// Every error this package names lives in this file: the wire-level IDs a
// response carries and the domain errors the storage/replication layers return
// as `error`. They are different channels — a caller reads the ID off
// AppendResponse.ErrID, and gets an error only from functions that answer one —
// so the mapping comment next to each domain error is the only place the two
// meet.

// ---- Wire-level error IDs -------------------------------------------------

// IDs returned in AppendResponse.ErrID on fail/MOVED/ASK. The table itself
// lives in pkg/client (the client-facing wire contract — the client side is
// where these ids are branched on); these are the server-side names for the
// same numbers, aliased so the two can never drift.
const (
	ErrIDVersionConflict   = client.ErrIDVersionConflict   // 1001
	ErrIDBadRequest        = client.ErrIDBadRequest        // 1002
	ErrIDSlotNotLocal      = client.ErrIDSlotNotLocal      // 1003 MOVED
	ErrIDMigrating         = client.ErrIDMigrating         // 1004 ASK
	ErrIDFlowControl       = client.ErrIDFlowControl       // 1005 the node's flow control refused the append
	ErrIDSlotUnproven      = client.ErrIDSlotUnproven      // 1006 local copy without a proven generation (owner fence)
	ErrIDAckTimeout        = client.ErrIDAckTimeout        // 1007 watermark deadline; the record is in the leader's WAL
	ErrIDReplicaCatchingUp = client.ErrIDReplicaCatchingUp // 1008 leader holds no usable copy yet; retry later
	ErrIDNoDataPlane       = client.ErrIDNoDataPlane       // 1009 control-plane-only node; the layout is unplanned
)

// ---- Domain errors --------------------------------------------------------

// The `error` values the engine returns, each naming the wire ID it turns into
// at the response boundary.
var (
	// ErrBadRequest maps to error ID 1002.
	ErrBadRequest = errors.New("bad request")
	// ErrVersionConflict maps to 1001.
	ErrVersionConflict = errors.New("version conflict")
	// ErrSlotNotLocal maps to 1003 (MOVED).
	ErrSlotNotLocal = errors.New("slot not owned by this node")
	// ErrMigrating maps to 1004 (ASK).
	ErrMigrating = errors.New("slot is migrating")
	// ErrRecordNotFound is returned by point lookups.
	ErrRecordNotFound = errors.New("record not found")
	// ErrLeaderUnreachable is the replication fetch loop's own signal: the
	// leader it was told to poll has no peer address in this node's table yet
	// (an election just settled or registration has not arrived). It is an
	// internal sentinel, never a wire code.
	ErrLeaderUnreachable = errors.New("leader's peer address is not known yet")
)
