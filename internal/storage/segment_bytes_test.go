package storage

import "testing"

// The configured segment size is restricted to whole multiples of 64MiB up to
// 2GiB: the service entry point rejects anything else, so a typo in a flag or a
// config file fails at startup instead of producing an odd layout.
func TestValidateSegmentBytes(t *testing.T) {
	cases := []struct {
		n    int64
		want bool
	}{
		{64 << 20, true}, {128 << 20, true}, {192 << 20, true}, {256 << 20, true},
		{512 << 20, true}, {1024 << 20, true}, {2 << 30, true},
		{0, false}, {-1, false}, {32 << 20, false}, {63 << 20, false},
		{100 << 20, false}, {256<<20 + 1, false}, {2<<30 + (64 << 20), false},
		{3 << 30, false},
	}
	for _, c := range cases {
		err := ValidateSegmentBytes(c.n)
		if (err == nil) != c.want {
			t.Errorf("ValidateSegmentBytes(%d) err=%v, want valid=%v", c.n, err, c.want)
		}
	}
	if err := ValidateSegmentBytes(DefaultSegmentBytes); err != nil {
		t.Fatalf("the default must be valid: %v", err)
	}
	if DefaultSegmentBytes%SegmentBytesMultiple != 0 {
		t.Fatalf("the default %d is not a multiple of %d", DefaultSegmentBytes, SegmentBytesMultiple)
	}
}
