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
const (
	ErrIDVersionConflict = 1001
	ErrIDBadRequest      = 1002
	ErrIDSlotNotLocal    = 1003 // MOVED
	ErrIDMigrating       = 1004 // ASK
	ErrIDNotLeader       = 1005
	ErrIDFlowControl     = 1006 // the node's flow control refused the append
)
