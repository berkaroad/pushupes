package cluster

import (
	"fmt"

	"github.com/sirupsen/logrus"

	"pushupes/internal/raft"
)

// ApplyResult is the FSM reply to a consensus apply: plain bytes plus an error
// string, so the consensus layer stays independent of engine types.
type ApplyResult struct {
	Value []byte `json:"value,omitempty"`
	Error string `json:"error,omitempty"`
}

// FSM replicates only cluster metadata (the slot assignment table and the
// peer directory). Event data never enters the Raft log — it flows over the
// pull-based fetch protocol (see replication.go).
type FSM struct {
	applier Applier
	logger  *logrus.Entry
}

func (f *FSM) Apply(e raft.Entry) any {
	value, err := f.applier.ApplyCommand(e.Data)
	res := &ApplyResult{}
	if err != nil {
		res.Error = err.Error()
	} else {
		res.Value = value
	}
	return res
}

func (f *FSM) Snapshot() ([]byte, error) { return f.applier.SnapshotState() }

func (f *FSM) Restore(data []byte) error { return f.applier.RestoreState(data) }

// errNotController is returned by controller-only operations on followers.
var errNotController = fmt.Errorf("not the controller (raft leader)")
