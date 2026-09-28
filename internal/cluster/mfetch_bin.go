package cluster

// Binary framing for the multiplexed replica fetch (data plane).
//
// The JSON form forced every payload byte through base64 + string
// escaping — under bench load, encoding/decoding the concatenated WAL
// records was the largest avoidable CPU tax on both sides of the
// replication pipe. The binary layout keeps payload bytes verbatim:
//
//   request:  magic u32 | ver u8 | follower string | wait_ms uvarint
//             count varint | per item: slot varint | from_seq uvarint
//   response: magic u32 | ver u8 | follower string | count varint
//             per item: slot varint | has_data u8 | next_seq uvarint
//             payload_len uvarint | payload bytes | leader_leo uvarint |
//             leader_hw uvarint
//
// Slot ids use zigzag-free plain varint only because they are int32 and
// migration placeholders are -1 — binary.Varint zigzags, which is exactly
// what we want for signed values. Strings are [uvarint length][bytes].

import (
	"encoding/binary"
	"fmt"
)

const (
	mfetchMagic uint32 = 0x4D464553 // "MFES"
	mfetchVer   uint8  = 1
)

func appendBinString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func readBinString(b []byte, name string) (string, []byte, error) {
	n, used := binary.Uvarint(b)
	if used <= 0 || uint64(len(b)-used) < n {
		return "", nil, fmt.Errorf("mfetch: bad %s string", name)
	}
	return string(b[used : used+int(n)]), b[used+int(n):], nil
}

// EncodeMRequest serialises an MFetchRequest (wire carries Slot+FromSeq
// per item only — the response-side fields are leader-assigned).
func EncodeMRequest(req *MFetchRequest) []byte {
	b := make([]byte, 0, 48+len(req.Items)*12)
	b = binary.BigEndian.AppendUint32(b, mfetchMagic)
	b = append(b, mfetchVer)
	b = appendBinString(b, req.Follower)
	b = binary.AppendUvarint(b, uint64(req.WaitMS))
	b = binary.AppendVarint(b, int64(len(req.Items)))
	for _, it := range req.Items {
		b = binary.AppendVarint(b, int64(it.Slot))
		b = binary.AppendUvarint(b, it.FromSeq)
	}
	return b
}

// DecodeMRequest parses a request body produced by EncodeMRequest.
func DecodeMRequest(buf []byte) (*MFetchRequest, error) {
	if len(buf) < 5 || binary.BigEndian.Uint32(buf[:4]) != mfetchMagic || buf[4] != mfetchVer {
		return nil, fmt.Errorf("mfetch: bad magic/version")
	}
	follower, rest, err := readBinString(buf[5:], "follower")
	if err != nil {
		return nil, err
	}
	waitMS, n := binary.Uvarint(rest)
	if n <= 0 {
		return nil, fmt.Errorf("mfetch: bad wait_ms")
	}
	rest = rest[n:]
	cnt, n := binary.Varint(rest)
	if n <= 0 || cnt < 0 || cnt > 1<<20 {
		return nil, fmt.Errorf("mfetch: bad item count")
	}
	rest = rest[n:]
	req := &MFetchRequest{Follower: follower, WaitMS: int64(waitMS), Items: make([]FetchItem, 0, cnt)}
	for i := int64(0); i < cnt; i++ {
		slot, n1 := binary.Varint(rest)
		if n1 <= 0 {
			return nil, fmt.Errorf("mfetch: truncated item %d slot", i)
		}
		fromSeq, n2 := binary.Uvarint(rest[n1:])
		if n2 <= 0 {
			return nil, fmt.Errorf("mfetch: truncated item %d from_seq", i)
		}
		rest = rest[n1+n2:]
		req.Items = append(req.Items, FetchItem{Slot: int32(slot), FromSeq: fromSeq})
	}
	return req, nil
}

// EncodeMResponse serialises an MFetchResponse with payload bytes verbatim.
func EncodeMResponse(resp *MFetchResponse) []byte {
	sz := 48
	for _, it := range resp.Items {
		sz += 24 + len(it.Payload)
	}
	b := make([]byte, 0, sz)
	b = binary.BigEndian.AppendUint32(b, mfetchMagic)
	b = append(b, mfetchVer)
	b = appendBinString(b, resp.Follower)
	b = binary.AppendVarint(b, int64(len(resp.Items)))
	for _, it := range resp.Items {
		b = binary.AppendVarint(b, int64(it.Slot))
		hd := byte(0)
		if len(it.Payload) > 0 {
			hd = 1
		}
		b = append(b, hd)
		b = binary.AppendUvarint(b, it.NextSeq)
		b = binary.AppendUvarint(b, uint64(len(it.Payload)))
		b = append(b, it.Payload...)
		b = binary.AppendUvarint(b, it.LeaderLEO)
		b = binary.AppendUvarint(b, it.LeaderHW)
	}
	return b
}

// DecodeMResponse parses a response body produced by EncodeMResponse.
func DecodeMResponse(buf []byte) (*MFetchResponse, error) {
	if len(buf) < 5 || binary.BigEndian.Uint32(buf[:4]) != mfetchMagic || buf[4] != mfetchVer {
		return nil, fmt.Errorf("mfetch: bad magic/version")
	}
	follower, rest, err := readBinString(buf[5:], "follower")
	if err != nil {
		return nil, err
	}
	cnt, n := binary.Varint(rest)
	if n <= 0 || cnt < 0 || cnt > 1<<20 {
		return nil, fmt.Errorf("mfetch: bad item count")
	}
	rest = rest[n:]
	resp := &MFetchResponse{Follower: follower, Items: make([]FetchItem, 0, cnt)}
	for i := int64(0); i < cnt; i++ {
		slot, n1 := binary.Varint(rest)
		if n1 <= 0 || len(rest) <= n1 {
			return nil, fmt.Errorf("mfetch: truncated item %d slot", i)
		}
		hasData := rest[n1]
		rest = rest[n1+1:]
		nextSeq, n2 := binary.Uvarint(rest)
		if n2 <= 0 {
			return nil, fmt.Errorf("mfetch: bad next_seq %d", i)
		}
		rest = rest[n2:]
		plen, n3 := binary.Uvarint(rest)
		if n3 <= 0 || uint64(len(rest)-n3) < plen {
			return nil, fmt.Errorf("mfetch: bad payload len %d", i)
		}
		rest = rest[n3:]
		it := FetchItem{Slot: int32(slot), NextSeq: nextSeq}
		if hasData != 0 && plen > 0 {
			it.Payload = append([]byte(nil), rest[:plen]...)
		}
		rest = rest[plen:]
		leo, n4 := binary.Uvarint(rest)
		if n4 <= 0 {
			return nil, fmt.Errorf("mfetch: bad leader_leo %d", i)
		}
		hw, n5 := binary.Uvarint(rest[n4:])
		if n5 <= 0 {
			return nil, fmt.Errorf("mfetch: bad leader_hw %d", i)
		}
		rest = rest[n4+n5:]
		it.LeaderLEO, it.LeaderHW = leo, hw
		resp.Items = append(resp.Items, it)
	}
	return resp, nil
}
