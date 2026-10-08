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

package payloadcodec

import (
	"math/bits"
	"sync"

	"google.golang.org/grpc/experimental"
	"google.golang.org/grpc/mem"
)

// payloadClasses are the buffer size classes (powers of two) this service
// actually asks gRPC for: event bodies run up to the 1 MiB body limit, and
// fetch/snapshot payloads up to the 4 MiB chunk size.
var payloadClasses = []uint8{10, 12, 14, 16, 18, 20, 22} // 1KiB .. 4MiB

// payloadBufferPool is a BufferPool for gRPC's codec and transport buffers. It
// exists because the stock default pool hurt large-bodied workloads twice over:
// its classes are sparse (…32KiB, 1MiB), so a 100KiB message took a 1MiB buffer,
// and it zeroes every buffer on Get (grpc/internal/mem sizedBufferPool.Get does
// a memclr of the whole buffer) even though both users fully overwrite what
// they get: protobuf marshalling writes the entire message, and the read path
// copies the frame in. Measured on a 100KiB body: a 1MiB memclr per message
// (~10% of node CPU) and ~15% less throughput than before the grpc upgrade.
type payloadBufferPool struct {
	pools []sync.Pool
}

// grpcBufPool is the process-wide buffer pool: gRPC's default pool for codec and
// transport buffers, and the pool this package's codec borrows from.
var grpcBufPool = newPayloadBufferPool()

// InstallBufferPool replaces gRPC's default buffer pool. It must run while the
// process is still initializing (before any client or server exists), which is
// what main does it for.
func InstallBufferPool() {
	experimental.SetDefaultBufferPool(grpcBufPool)
}

func newPayloadBufferPool() *payloadBufferPool {
	p := &payloadBufferPool{pools: make([]sync.Pool, len(payloadClasses))}
	return p
}

// classFor returns the index of the smallest class that fits n, or -1 when n
// exceeds every class.
func (p *payloadBufferPool) classFor(n int) int {
	for i, exp := range payloadClasses {
		if 1<<exp >= n {
			return i
		}
	}
	return -1
}

// classOf returns the index of the class whose size equals cap, or -1.
func (p *payloadBufferPool) classOf(capacity int) int {
	if capacity <= 0 || capacity&(capacity-1) != 0 {
		return -1 // not a power of two: never handed out by us
	}
	exp := bits.TrailingZeros(uint(capacity))
	for i, e := range payloadClasses {
		if int(e) == exp {
			return i
		}
	}
	return -1
}

// Get returns a buffer of exactly length bytes. Buffers outside the classes are
// allocated per call rather than pooled, so an oversized message never leaves a
// huge buffer parked in a pool.
func (p *payloadBufferPool) Get(length int) *[]byte {
	if length < 1 {
		length = 1
	}
	if i := p.classFor(length); i >= 0 {
		if v := p.pools[i].Get(); v != nil {
			b := v.(*[]byte)
			*b = (*b)[:length]
			return b
		}
		buf := make([]byte, 1<<payloadClasses[i])
		buf = buf[:length]
		return &buf
	}
	b := make([]byte, length)
	return &b
}

// Put recycles a buffer obtained from Get. Buffers that are not class-sized are
// dropped for the garbage collector.
func (p *payloadBufferPool) Put(b *[]byte) {
	if b == nil {
		return
	}
	i := p.classOf(cap(*b))
	if i < 0 {
		return
	}
	*b = (*b)[:cap(*b)]
	p.pools[i].Put(b)
}

var _ mem.BufferPool = (*payloadBufferPool)(nil)
