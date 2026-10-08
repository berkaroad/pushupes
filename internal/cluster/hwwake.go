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

import (
	"sync"
	"time"
)

// hwGate wakes the calls parked in waitForHW when a slot's high watermark
// moves, so an acknowledged append does not pay a poll interval to notice that
// its replicas caught up.
//
// One gate per slot, owned by the engine and NOT by slotRepl: the replication
// view for a slot is replaced wholesale on migration and reload, and a waiter
// parked on a channel owned by the retired view would never be woken. A waiter
// re-reads the view after every wake, so a replaced view is picked up then.
//
// Generation channels: kick closes the current channel (waking everyone parked
// on it) and leaves the next one to be created lazily. Nothing is allocated in
// the common case of a wake with no waiter, which is the case for most of a
// busy node's report traffic.
type hwGate struct {
	mu sync.Mutex
	ch chan struct{}
}

// wait returns the channel to park on. Callers take it while they still hold
// replMu (the same lock advanceHW holds when it stores the new watermark and
// kicks), so an advance between the watermark check and the park cannot be
// missed: either the channel returned is the pre-advance one (kick closes it)
// or the watermark read was already the new one.
func (g *hwGate) wait() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ch == nil {
		g.ch = make(chan struct{})
	}
	return g.ch
}

// kick wakes every waiter parked on this gate and arms a fresh generation.
func (g *hwGate) kick() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ch == nil {
		return // nobody parked: nothing to wake, nothing to allocate
	}
	close(g.ch)
	g.ch = nil
}

// hwWaitTick is waitForHW's safety net. The watermark normally advances on a
// replica report, which kicks the gate; the transitions that produce no report
// at all — a replica going stale (isrStaleAfter with no further reports) or a
// peer being marked down — are why a parked wait still re-checks on a tick.
// It replaces a 2ms poll: 40x less work with the same worst case on those
// paths, and no poll interval at all on the common one. A variable so a test
// can push it out of the way and prove the wake comes from the event.
var hwWaitTick = 50 * time.Millisecond
