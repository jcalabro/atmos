package cbor

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DASL interop tests — test vectors from https://github.com/hyphacoop/dasl-testing
// Validates our DAG-CBOR codec against the shared DASL/dag-cbor test suite.
//
// Vectors only relevant to specs we don't implement (rfc8949 preferred
// serialization, CBOR-Core, dCBOR numeric reduction) have been removed from
// the fixture files. All remaining vectors apply to our dag-cbor codec.

type daslTestCase struct {
	Type string   `json:"type"` // "roundtrip", "invalid_in", "invalid_out"
	Data string   `json:"data"` // hex-encoded CBOR bytes
	Name string   `json:"name"`
	Tags []string `json:"tags"`
	Desc string   `json:"desc"`
}

func loadDASLFixtures(t *testing.T) []daslTestCase {
	t.Helper()
	dir := filepath.Join("testdata", "dasl-cbor")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	var all []daslTestCase
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		require.NoError(t, err)
		var tests []daslTestCase
		require.NoError(t, json.Unmarshal(data, &tests))
		all = append(all, tests...)
	}
	return all
}

func TestDASL_Roundtrip(t *testing.T) {
	t.Parallel()
	fixtures := loadDASLFixtures(t)

	ran := 0
	for _, tc := range fixtures {
		if tc.Type != "roundtrip" {
			continue
		}
		ran++
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			data, err := hex.DecodeString(tc.Data)
			require.NoError(t, err, "bad test hex")

			val, err := Unmarshal(data)
			require.NoError(t, err, "Unmarshal failed: %s", tc.Desc)

			out, err := Marshal(val)
			require.NoError(t, err, "Marshal failed: %s", tc.Desc)

			assert.True(t, bytes.Equal(data, out),
				"roundtrip mismatch: want %x, got %x (%s)", data, out, tc.Desc)
		})
	}
	t.Logf("ran %d roundtrip tests", ran)
}

func TestDASL_InvalidIn(t *testing.T) {
	t.Parallel()
	fixtures := loadDASLFixtures(t)

	ran := 0
	for _, tc := range fixtures {
		if tc.Type != "invalid_in" {
			continue
		}
		ran++
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			data, err := hex.DecodeString(tc.Data)
			require.NoError(t, err, "bad test hex")

			_, err = Unmarshal(data)
			assert.Error(t, err, "expected error for invalid input: %s", tc.Desc)
		})
	}
	t.Logf("ran %d invalid_in tests", ran)
}

// TestDASL_CID_Vectors tests specific CID vectors from the DASL suite
// against our CID parser directly (not through CBOR decode).
func TestDASL_CID_Vectors(t *testing.T) {
	t.Parallel()

	// Known valid CID from DASL suite (raw codec, SHA-256).
	// The 0x00 prefix is stripped by the CBOR decoder before calling ParseCIDBytes.
	cidHex := "015512205891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03"
	cidBytes, err := hex.DecodeString(cidHex)
	require.NoError(t, err)

	cid, err := ParseCIDBytes(cidBytes)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), cid.Version())
	assert.Equal(t, CodecRaw, cid.Codec())
	assert.Equal(t, HashSHA256, cid.HashCode())
	assert.Equal(t, 32, cid.HashLen())

	// Round-trip: bytes → CID → bytes.
	var buf [64]byte
	out := cid.AppendBytes(buf[:0])
	assert.True(t, bytes.Equal(cidBytes, out), "CID bytes roundtrip: want %x, got %x", cidBytes, out)

	// CID string round-trip.
	s := cid.String()
	assert.NotEmpty(t, s)

	// Invalid: truncated hash (31 bytes instead of 32).
	truncHex := "015512205891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be"
	truncBytes, _ := hex.DecodeString(truncHex)
	_, err = ParseCIDBytes(truncBytes)
	assert.Error(t, err, "should reject CID with truncated hash")
}
