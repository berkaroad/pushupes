package storage

import (
	"errors"
	"io"
	"os"
	"sync/atomic"
)

// scatterMaxIOV is Linux's IOV_MAX: one pwritev(2) accepts at most this many
// segments, so records with long event lists are written in batches.
const scatterMaxIOV = 1024

// errScatterUnsupported is returned when the filesystem (or kernel) refuses
// scatter/gather writes; the WAL then uses a contiguous WriteAt instead.
var errScatterUnsupported = errors.New("scatter/gather writes unsupported")

// scatterDisabled latches the first refusal so every later append takes the
// contiguous path without probing again. WAL files of a process live on one
// filesystem in practice; the bytes written are identical either way.
var scatterDisabled atomic.Bool

// pwritevAll writes parts at off as a batch of segments. It resumes at the
// advanced offset on a short write and reports errScatterUnsupported when the
// kernel refused the very first attempt (nothing was written, so the caller can
// still fall back to a contiguous write).
func pwritevAll(f *os.File, parts [][]byte, off int64) error {
	fd := int(f.Fd())
	for len(parts) > 0 {
		n := len(parts)
		if n > scatterMaxIOV {
			n = scatterMaxIOV
		}
		w, err := pwritevAt(fd, parts[:n], off)
		if err != nil {
			if w == 0 && isScatterUnsupported(err) {
				return errScatterUnsupported
			}
			return err
		}
		if w == 0 {
			return io.ErrShortWrite
		}
		off += int64(w)
		parts = advanceParts(parts, w)
	}
	return nil
}

// advanceParts drops the first n bytes from parts.
func advanceParts(parts [][]byte, n int) [][]byte {
	for len(parts) > 0 {
		if n < len(parts[0]) {
			parts[0] = parts[0][n:]
			return parts
		}
		n -= len(parts[0])
		parts = parts[1:]
	}
	return nil
}
