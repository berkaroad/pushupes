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

//go:build linux

package storage

import (
	"errors"

	"golang.org/x/sys/unix"
)

// pwritevAt writes iovs at off with one pwritev(2).
func pwritevAt(fd int, iovs [][]byte, off int64) (int, error) {
	return unix.Pwritev(fd, iovs, off)
}

// isScatterUnsupported reports whether err means this file (filesystem, or
// kernel) cannot do scatter/gather writes at all, as opposed to a real I/O
// failure: ENOSYS (no pwritev), EINVAL (e.g. an fs that rejects iovecs),
// EOPNOTSUPP. Callers latch the answer and fall back to a contiguous write.
func isScatterUnsupported(err error) bool {
	return errors.Is(err, unix.ENOSYS) ||
		errors.Is(err, unix.EINVAL) ||
		errors.Is(err, unix.EOPNOTSUPP) ||
		errors.Is(err, unix.ENOTSUP)
}
