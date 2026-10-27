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

package storage

import "testing"

// The slot count is a permanent layout decision (routing, placement and
// migration granularity, DESIGN §7.2), not a tuning knob: 1024 = 2^10 leaves
// every node at least four slots up to the 256-data-node ceiling and divides
// evenly over every even power-of-two node count in between. A change here is a
// cluster-wide layout change, so it has to be deliberate.
func TestDefaultSlotCountIsFixed(t *testing.T) {
	if DefaultSlotCount != 1024 {
		t.Fatalf("DefaultSlotCount drifted: %d, want the fixed 1024 (DESIGN §7.2)", DefaultSlotCount)
	}
}

// The shipped segment size has to pass the rule the service entry point applies
// to whatever an operator configures.
func TestDefaultSegmentBytesIsValid(t *testing.T) {
	if err := ValidateSegmentBytes(DefaultSegmentBytes); err != nil {
		t.Fatalf("DefaultSegmentBytes %d does not validate: %v", DefaultSegmentBytes, err)
	}
	if DefaultFlushPolicy().Interval != DefaultFlushInterval ||
		DefaultFlushPolicy().IntervalMessages != DefaultFlushMessages {
		t.Fatalf("DefaultFlushPolicy = %+v, want the shipped %d records / %s",
			DefaultFlushPolicy(), DefaultFlushMessages, DefaultFlushInterval)
	}
}
