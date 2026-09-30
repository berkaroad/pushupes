// Package lease hands out and revokes references to receive buffers that a
// message's bytes fields alias, so a codec without copies can stay honest about
// ownership.
//
// It exists as its own package because both the gRPC layer (which holds a lease
// when it decodes a message) and the storage/cluster layers (which release it
// once the bytes are safely on disk) need it, and those two must not import
// each other.
package lease

import (
	"sync"

	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
)

var held sync.Map // proto.Message -> mem.Buffer

// Hold pins buf as long as the message m is in use. Releasing it returns the
// buffer to whichever pool it came from.
func Hold(m proto.Message, buf mem.Buffer) {
	if m == nil || buf == nil {
		return
	}
	held.Store(m, buf)
}

// Release drops the lease on m and frees the buffer. It reports whether a lease
// was held; releasing twice is harmless.
func Release(m proto.Message) bool {
	if m == nil {
		return false
	}
	if v, ok := held.LoadAndDelete(m); ok {
		v.(mem.Buffer).Free()
		return true
	}
	return false
}

// Outstanding counts leases that have not been released. A non-zero value after
// a workload has drained means a consumer forgot to release, which leaks the
// buffer (it never returns to the pool) rather than corrupting anything.
func Outstanding() int {
	n := 0
	held.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}
