package cluster

import (
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
)

// ApplyResult is the FSM reply to a Raft apply: plain bytes plus an error
// string, so consensus transport stays independent of engine types.
type ApplyResult struct {
	Value []byte `json:"value,omitempty"`
	Error string `json:"error,omitempty"`
}

// FSM replicates only cluster metadata (the slot assignment table and the
// peer directory). Event data never enters the Raft log — it flows over the
// pull-based fetch protocol (see replication.go), like DeadliftMQ.
type FSM struct {
	applier Applier
	logger  *logrus.Entry
}

func (f *FSM) Apply(l *raft.Log) interface{} {
	value, err := f.applier.ApplyCommand(l.Data)
	res := &ApplyResult{}
	if err != nil {
		res.Error = err.Error()
	} else {
		res.Value = value
	}
	return res
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	data, err := f.applier.SnapshotState()
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: data}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	return f.applier.RestoreState(data)
}

type fsmSnapshot struct {
	data []byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

// errNotController is returned by controller-only operations on followers.
var errNotController = fmt.Errorf("not the controller (raft leader)")
