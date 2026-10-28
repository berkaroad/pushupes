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

package client

// Wire-level error IDs returned in AppendResponse.ErrID on fail/MOVED/ASK.
// Part of the client-facing wire contract (the server's internal/data
// re-exports these constants, so both sides name the same numbers).
//
// Numbering runs continuously from 1001 with no gaps and no reserved values:
// every id below answers a client. The admin plane does not occupy a
// data-plane number at all — "this node is not the Raft controller" is a 307
// with a Location to the controller (503 while there is none), not an err_id.
//
// What pkg/client itself branches on: 1003/1004 (follow the redirect and
// re-send) and 1005 (absorb the refusal as backpressure, see Config.FlowBackoff).
// The rest are answered to the caller — the library never hides that a record
// was refused, and it never retries a duplicate on the caller's behalf.
const (
	ErrIDVersionConflict   = 1001 // the version is not cur+1; current_version is attached
	ErrIDBadRequest        = 1002 // malformed request (empty events, oversized field, aggregate repeated in a batch)
	ErrIDSlotNotLocal      = 1003 // MOVED: another node leads the slot; node carries its client address
	ErrIDMigrating         = 1004 // ASK: the slot is mid-migration; node carries the target
	ErrIDFlowControl       = 1005 // the node's token bucket refused the append; nothing was written
	ErrIDSlotUnproven      = 1006 // the copy's provenance is unproven (owner fence, DESIGN 3.1): the leader refuses
	ErrIDAckTimeout        = 1007 // watermark deadline expired: the record IS in the leader's WAL, retry answers exists
	ErrIDReplicaCatchingUp = 1008 // this node leads the slot but holds no usable copy yet; nothing was written
	ErrIDNoDataPlane       = 1009 // control-plane-only member, layout unplanned; nothing was written, retrying cannot help
	ErrIDGroupMismatch     = 1010 // the request's layout_epoch is stale: refresh PrefetchRoutes and retry
)
