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
	"context"
	"fmt"
	"sort"

	"github.com/berkaroad/pushupes/internal/config"
)

// SetSlotGroup switches the served slot generation (controller-only).
//
// It refuses while any slot of the CURRENT generation still holds records: a
// switch moves routing to the new segment, and records left in the old one
// would become unreachable through the data plane (their slots are no longer
// named by the plan). The segment migration (DESIGN §4.5.8) moves them first;
// this command is the final, cheap step that flips the layout once the source
// segment is empty. Switching to the same generation is a no-op (idempotent),
// which is what makes a retried operator call safe.
func (e *Engine) SetSlotGroup(ctx context.Context, name string) (SlotGroup, error) {
	if err := e.controllerGuard(); err != nil {
		return SlotGroup{}, err
	}
	want, err := ParseSlotGroup(name, e.store.SlotCount)
	if err != nil {
		return SlotGroup{}, err
	}
	tbl := e.TableSnapshot()
	if len(tbl.Slots) == 0 {
		return SlotGroup{}, fmt.Errorf("slot group: the cluster has no layout yet (no data-plane member has joined)")
	}
	cur := SlotGroup{Gen: tbl.GroupGen, Base: tbl.GroupBase, Size: tbl.GroupSize, Epoch: tbl.LayoutEpoch}
	if cur.Size == want.Size {
		return cur, nil // already serving this generation
	}
	if cur.Size > 0 {
		if busy := e.segmentBusy(tbl); len(busy) > 0 {
			return cur, fmt.Errorf("slot group: %d slot(s) of the current generation still hold records (first %d); run the segment migration first", len(busy), busy[0])
		}
	}
	if err := e.submit(&Command{Op: OpSetGroup, GroupGen: want.Gen, GroupBase: want.Base, GroupSize: want.Size}); err != nil {
		return cur, fmt.Errorf("set_group: %w", err)
	}
	if err := e.awaitApplied(ctx, config.HandoverStageTimeout); err != nil {
		return cur, err
	}
	after := e.TableSnapshot()
	return SlotGroup{Gen: after.GroupGen, Base: after.GroupBase, Size: after.GroupSize, Epoch: after.LayoutEpoch}, nil
}

// segmentBusy lists the slots of the current plan that still hold records on any
// reachable replica. A slot whose replicas all fail to answer counts as busy:
// absence of evidence is not evidence of an empty segment, and switching under
// it would strand data nobody can enumerate later.
func (e *Engine) segmentBusy(tbl *Table) []int32 {
	slots := make([]int32, 0, len(tbl.Slots))
	for s := range tbl.Slots {
		slots = append(slots, s)
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })

	byNode := map[string][]int32{}
	for _, s := range slots {
		p := tbl.Slots[s]
		if p == nil {
			continue
		}
		for _, r := range p.Replicas {
			byNode[r] = append(byNode[r], s)
		}
	}
	leos := map[int32]uint64{}
	answered := map[int32]bool{}
	ctx, cancel := context.WithTimeout(context.Background(), config.FailoverPickTimeout)
	defer cancel()
	for node, ss := range byNode {
		if node == e.self {
			for _, s := range ss {
				answered[s] = true
				if v := e.store.LastSeqOf(s); v > leos[s] {
					leos[s] = v
				}
			}
			continue
		}
		pe, ok := tbl.Peers[node]
		if !ok || pe.Offline() || pe.PeerAddr == "" {
			continue
		}
		got, err := e.peerSlotLeos(ctx, pe.PeerAddr, ss)
		if err != nil || len(got) != len(ss) {
			continue
		}
		for i, s := range ss {
			answered[s] = true
			if got[i] > leos[s] {
				leos[s] = got[i]
			}
		}
	}
	var busy []int32
	for _, s := range slots {
		if !answered[s] || leos[s] > 0 {
			busy = append(busy, s)
		}
	}
	return busy
}
