package grpcapi

import (
	"testing"

	"github.com/berkaroad/pushupes/internal/cluster"
	"github.com/berkaroad/pushupes/internal/data"
)

// TestBatchRejection pins the pre-execution refusal of BatchAppend: a stale
// layout_epoch is retryable after a route refresh (1010), and a zero epoch is
// "no opinion" — a client that never fetched a table keeps working.
func TestBatchRejection(t *testing.T) {
	spec := cluster.SlotGroup{Gen: 10, Base: 1024, Size: 1024, Epoch: 7}

	cases := []struct {
		name   string
		epoch  uint64
		wantID int
	}{
		{"no opinion", 0, 0},
		{"current epoch", 7, 0},
		{"stale epoch", 6, data.ErrIDGroupMismatch},
		{"epoch ahead of the table", 8, data.ErrIDGroupMismatch},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotID, _ := batchRejection(c.epoch, spec)
			if gotID != c.wantID {
				t.Fatalf("batchRejection(epoch=%d) = %d, want %d", c.epoch, gotID, c.wantID)
			}
		})
	}
}
