package mst

import (
	"fmt"

	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/gt"
)

// NodeData is the on-disk CBOR representation of an MST node.
type NodeData struct {
	Left    gt.Option[cbor.CID]
	Entries []EntryData
}

// EntryData is a single entry in an MST node.
type EntryData struct {
	PrefixLen int
	KeySuffix []byte
	Value     cbor.CID
	Right     gt.Option[cbor.CID]
}

// Precomputed CBOR bytes for fixed tokens to avoid repeated encoding.
var (
	// Map header + "e" key: a2 6165
	cborMapHeader2E = []byte{0xa2, 0x61, 0x65}
	// "l" key: 616c
	cborKeyL = []byte{0x61, 0x6c}
	// Map header(4) + "k" key: a4 616b
	cborMapHeader4K = []byte{0xa4, 0x61, 0x6b}
	// "p" key: 6170
	cborKeyP = []byte{0x61, 0x70}
	// "t" key: 6174
	cborKeyT = []byte{0x61, 0x74}
	// "v" key: 6176
	cborKeyV = []byte{0x61, 0x76}
	// CBOR null: f6
	cborNull = []byte{0xf6}
)

// encodeNodeData encodes a NodeData to DAG-CBOR bytes using a specialized
// fast path that writes directly to a pre-sized buffer.
func encodeNodeData(nd *NodeData) ([]byte, error) {
	// Estimate size: header(3) + entries(~60 each) + left CID(~40) + padding
	buf := make([]byte, 0, 64+len(nd.Entries)*60)

	// Map(2) + "e" key
	buf = append(buf, cborMapHeader2E...)

	// Array header for entries
	buf = cbor.AppendArrayHeader(buf, uint64(len(nd.Entries)))

	for i := range nd.Entries {
		buf = appendEntryData(buf, &nd.Entries[i])
	}

	// "l" key
	buf = append(buf, cborKeyL...)

	// Left CID or null
	if nd.Left.HasVal() {
		left := nd.Left.Val()
		buf = cbor.AppendCIDLink(buf, &left)
	} else {
		buf = append(buf, cborNull...)
	}

	return buf, nil
}

// appendEntryData appends a CBOR-encoded EntryData to buf.
func appendEntryData(buf []byte, e *EntryData) []byte {
	// Map(4) + "k" key
	buf = append(buf, cborMapHeader4K...)

	// Key suffix as bytes
	buf = cbor.AppendBytes(buf, e.KeySuffix)

	// "p" key + prefix length as uint
	buf = append(buf, cborKeyP...)
	buf = cbor.AppendUint(buf, uint64(e.PrefixLen))

	// "t" key + right CID or null
	buf = append(buf, cborKeyT...)
	if e.Right.HasVal() {
		right := e.Right.Val()
		buf = cbor.AppendCIDLink(buf, &right)
	} else {
		buf = append(buf, cborNull...)
	}

	// "v" key + value CID
	buf = append(buf, cborKeyV...)
	buf = cbor.AppendCIDLink(buf, &e.Value)

	return buf
}

// DecodeNodeData decodes a NodeData from DAG-CBOR bytes using a specialized
// fast path that avoids the generic CBOR unmarshaler and its map allocations.
// Returns by value (not pointer) so the caller can keep it on the stack.
// KeySuffix slices point directly into data (no-copy) via ReadBytesNoCopy,
// so data must remain valid for the lifetime of the returned NodeData.
func DecodeNodeData(data []byte) (NodeData, error) {
	if len(data) < 3 {
		return NodeData{}, fmt.Errorf("mst: node data too short")
	}

	var nd NodeData
	pos := 0

	// Expect map(2): 0xa2
	if data[pos] != 0xa2 {
		return NodeData{}, fmt.Errorf("mst: expected map(2), got 0x%02x", data[pos])
	}
	pos++

	// Read 2 map entries. We know the order is "e", "l".
	for range 2 {
		if pos >= len(data) {
			return NodeData{}, fmt.Errorf("mst: unexpected end of data")
		}

		// Read key (text string, 1 byte: "e" or "l").
		keyByte := data[pos]
		if keyByte != 0x61 { // text(1)
			return NodeData{}, fmt.Errorf("mst: expected text(1) key, got 0x%02x", keyByte)
		}
		pos++
		if pos >= len(data) {
			return NodeData{}, fmt.Errorf("mst: unexpected end of data")
		}
		key := data[pos]
		pos++

		switch key {
		case 'e':
			// Array of entries.
			count, newPos, err := cbor.ReadArrayHeader(data, pos)
			if err != nil {
				return NodeData{}, err
			}
			pos = newPos
			if count > 10000 {
				return NodeData{}, fmt.Errorf("mst: too many entries: %d", count)
			}
			nd.Entries = make([]EntryData, int(count))
			for i := range int(count) {
				pos, err = decodeEntryDataFast(data, pos, &nd.Entries[i])
				if err != nil {
					return NodeData{}, fmt.Errorf("mst: entry %d: %w", i, err)
				}
			}

		case 'l':
			// Left CID or null.
			if cbor.IsNull(data, pos) {
				pos++ // null
			} else {
				cid, newPos, err := cbor.ReadCIDLink(data, pos)
				if err != nil {
					return NodeData{}, fmt.Errorf("mst: 'l': %w", err)
				}
				nd.Left = gt.Some(cid)
				pos = newPos
			}

		default:
			return NodeData{}, fmt.Errorf("mst: unexpected key %q", string(key))
		}
	}

	return nd, nil

}

// decodeEntryDataFast decodes a single entry from data at pos into ed.
func decodeEntryDataFast(data []byte, pos int, ed *EntryData) (int, error) {
	if pos >= len(data) || data[pos] != 0xa4 { // map(4)
		return 0, fmt.Errorf("expected map(4)")
	}
	pos++

	// Read 4 fields: k, p, t, v (in DAG-CBOR sorted order).
	for range 4 {
		if pos+1 >= len(data) || data[pos] != 0x61 {
			return 0, fmt.Errorf("expected text(1) key")
		}
		key := data[pos+1]
		pos += 2

		switch key {
		case 'k': // byte string — no-copy slice into the node's block data
			b, newPos, err := cbor.ReadBytesNoCopy(data, pos)
			if err != nil {
				return 0, err
			}
			ed.KeySuffix = b
			pos = newPos

		case 'p': // unsigned integer
			v, newPos, err := cbor.ReadUint(data, pos)
			if err != nil {
				return 0, err
			}
			ed.PrefixLen = int(v)
			pos = newPos

		case 't': // CID or null
			if cbor.IsNull(data, pos) {
				pos++ // null
			} else {
				cid, newPos, err := cbor.ReadCIDLink(data, pos)
				if err != nil {
					return 0, err
				}
				ed.Right = gt.Some(cid)
				pos = newPos
			}

		case 'v': // CID
			cid, newPos, err := cbor.ReadCIDLink(data, pos)
			if err != nil {
				return 0, err
			}
			ed.Value = cid
			pos = newPos

		default:
			return 0, fmt.Errorf("unexpected key %q", string(key))
		}
	}

	return pos, nil
}
