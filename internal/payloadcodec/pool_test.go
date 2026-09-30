package payloadcodec

import "testing"

func TestPayloadBufferPool(t *testing.T) {
	p := newPayloadBufferPool()

	// Every request gets exactly the length asked for, with a class-sized
	// capacity so recycling can tell the sizes apart.
	for _, n := range []int{1, 100, 1 << 10, (1 << 10) + 1, 100 << 10, 1 << 20, 3 << 20} {
		b := p.Get(n)
		if len(*b) != n {
			t.Fatalf("Get(%d): len=%d", n, len(*b))
		}
		c := cap(*b)
		if c&(c-1) != 0 {
			t.Fatalf("Get(%d): capacity %d is not a power of two", n, c)
		}
		if c < n {
			t.Fatalf("Get(%d): capacity %d below the request", n, c)
		}
	}

	// Class-sized buffers are recycled as-is — without zeroing, which is the
	// point: both users overwrite everything they get.
	b := p.Get(64 << 10)
	if len(*b) != 64<<10 || cap(*b) != 64<<10 {
		t.Fatalf("64KiB class: len=%d cap=%d", len(*b), cap(*b))
	}
	(*b)[0] = 0x7f
	p.Put(b)
	if got := p.Get(64 << 10); len(*got) != 64<<10 || cap(*got) != 64<<10 {
		t.Fatalf("recycled buffer: len=%d cap=%d", len(*got), cap(*got))
	}

	// Oversized requests bypass the pool entirely: never park a huge buffer.
	huge := p.Get(5 << 20)
	if len(*huge) != 5<<20 || cap(*huge) != 5<<20 {
		t.Fatalf("oversized: len=%d cap=%d", len(*huge), cap(*huge))
	}
	p.Put(huge) // dropped, not pooled

	// Buffers we never handed out must not be recycled as class buffers.
	odd := make([]byte, 1000, 1000)
	p.Put(&odd)
	got := p.Get(1000)
	if len(*got) != 1000 {
		t.Fatalf("odd-sized buffer corrupted the pool: len=%d", len(*got))
	}
	if &(*got)[0] == &odd[0] {
		t.Fatal("a non-class buffer was recycled")
	}
}
