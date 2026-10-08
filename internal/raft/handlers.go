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

// handlers.go — RPC entry points. Each one posts an event to the run loop and
// waits for the loop to produce the reply, so all protocol state stays owned
// by that single goroutine.
package raft

import (
	"errors"
	"time"
)

const rpcHandlerTimeout = 30 * time.Second

func (n *Node) handleRPC(typ byte, payload []byte) (byte, []byte, error) {
	switch typ {
	case msgRequestVote:
		m, err := decodeRequestVote(payload)
		if err != nil {
			return 0, nil, err
		}
		req := &requestVoteReq{msg: m, resp: make(chan requestVoteResp, 1)}
		if err := n.post(req); err != nil {
			return 0, nil, err
		}
		select {
		case r := <-req.resp:
			return msgRequestVoteResp, encodeRequestVoteResp(r), nil
		case <-time.After(rpcHandlerTimeout):
			return 0, nil, errors.New("raft: request vote handling timed out")
		case <-n.done:
			return 0, nil, ErrClosed
		}

	case msgAppendEntries:
		m, err := decodeAppendEntries(payload)
		if err != nil {
			return 0, nil, err
		}
		req := &appendEntriesReq{msg: m, resp: make(chan appendEntriesResp, 1)}
		if err := n.post(req); err != nil {
			return 0, nil, err
		}
		select {
		case r := <-req.resp:
			return msgAppendEntriesResp, encodeAppendEntriesResp(r), nil
		case <-time.After(rpcHandlerTimeout):
			return 0, nil, errors.New("raft: append entries handling timed out")
		case <-n.done:
			return 0, nil, ErrClosed
		}

	case msgInstallSnapshot:
		m, err := decodeInstallSnapshot(payload)
		if err != nil {
			return 0, nil, err
		}
		req := &installSnapshotReq{msg: m, resp: make(chan installSnapshotResp, 1)}
		if err := n.post(req); err != nil {
			return 0, nil, err
		}
		select {
		case r := <-req.resp:
			return msgInstallSnapshotResp, encodeInstallSnapshotResp(r), nil
		case <-time.After(rpcHandlerTimeout):
			return 0, nil, errors.New("raft: install snapshot handling timed out")
		case <-n.done:
			return 0, nil, ErrClosed
		}
	}
	return 0, nil, errors.New("raft: unknown rpc type")
}
