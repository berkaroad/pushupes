//go:build !linux

package storage

import "errors"

// pwritevAt always reports scatter writes as unsupported off Linux, so the WAL
// takes the contiguous WriteAt path.
func pwritevAt(fd int, iovs [][]byte, off int64) (int, error) {
	return 0, errScatterUnsupported
}

func isScatterUnsupported(err error) bool { return errors.Is(err, errScatterUnsupported) }
