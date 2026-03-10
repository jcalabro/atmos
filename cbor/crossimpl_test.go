package cbor

//
// This file was constructed by investigating several popular implementations
// of CBOR and importing their test vectors to ensure we're making a good
// effort to be robust in a x-library manner.
//
// Cross-implementation tests adapted from:
//   - Darkyenus/cbor-test-vectors (RFC 8949 Appendix A, 778 vectors)
//   - fxamacker/cbor (RFC 7049bis well-formedness, edge cases)
//   - DavidBuchanan314/dag-cbor-benchmark (real-world + torture tests)
//   - ipfs/go-ipld-cbor (canonicalization, CID links)
//

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// 1. RFC 8949 test vectors (Darkyenus/cbor-test-vectors)
// ---------------------------------------------------------------------------

type rfc8949Vector struct {
	Hex        string   `json:"hex"`
	Flags      []string `json:"flags"`
	Features   []string `json:"features"`
	Diagnostic string   `json:"diagnostic"`
	Canonical  string   `json:"canonical"`
}

func (v rfc8949Vector) hasFlag(f string) bool    { return slices.Contains(v.Flags, f) }
func (v rfc8949Vector) hasFeature(f string) bool { return slices.Contains(v.Features, f) }

func loadRFC8949Vectors(t *testing.T) []rfc8949Vector {
	t.Helper()
	data, err := os.ReadFile("testdata/rfc8949-vectors.json")
	require.NoError(t, err)
	var vectors []rfc8949Vector
	require.NoError(t, json.Unmarshal(data, &vectors))
	return vectors
}

// TestRFC8949_InvalidMustReject verifies that all 693 invalid vectors from the
// RFC 8949 test suite are rejected by our decoder. DAG-CBOR is stricter than
// standard CBOR, so anything invalid in standard CBOR must also fail here.
func TestRFC8949_InvalidMustReject(t *testing.T) {
	t.Parallel()
	vectors := loadRFC8949Vectors(t)

	ran := 0
	for _, v := range vectors {
		if !v.hasFlag("invalid") {
			continue
		}
		ran++
		t.Run(v.Hex, func(t *testing.T) {
			t.Parallel()
			data, err := hex.DecodeString(v.Hex)
			require.NoError(t, err, "bad test hex")
			_, err = Unmarshal(data)
			assert.Error(t, err, "expected rejection of invalid CBOR %s", v.Hex)
		})
	}
	t.Logf("ran %d invalid vector tests", ran)
}

// TestRFC8949_ValidDAGCBOR tests valid+canonical vectors that our DAG-CBOR
// decoder should accept (filtering out features we don't support).
func TestRFC8949_ValidDAGCBOR(t *testing.T) {
	t.Parallel()
	vectors := loadRFC8949Vectors(t)

	// DAG-CBOR only supports: int63-range integers, float64, no bignums,
	// no float16/32, no simple values, no tags except 42, no undefined,
	// no indefinite length.
	ran := 0
	for _, v := range vectors {
		if !v.hasFlag("valid") {
			continue
		}
		// Skip features we don't support.
		if v.hasFeature("float16") || v.hasFeature("bignum") || v.hasFeature("!bignum") ||
			v.hasFeature("simple") || v.hasFeature("int64") {
			continue
		}
		// Skip non-canonical indefinite length encodings.
		if !v.hasFlag("canonical") {
			continue
		}
		// Skip NaN, Infinity, undefined — not allowed in DAG-CBOR.
		diag := v.Diagnostic
		if diag == "NaN" || diag == "Infinity" || diag == "-Infinity" || diag == "undefined" {
			continue
		}
		// Skip float32 values (fa prefix) — DAG-CBOR only allows float64.
		if strings.HasPrefix(v.Hex, "fa") {
			continue
		}
		// Skip tags other than 42.
		if strings.HasPrefix(v.Hex, "c0") || strings.HasPrefix(v.Hex, "c1") ||
			strings.HasPrefix(v.Hex, "d7") || strings.HasPrefix(v.Hex, "d8") ||
			strings.HasPrefix(v.Hex, "d820") {
			continue
		}
		// Skip map with integer keys ({1: 2, 3: 4}).
		if v.Hex == "a201020304" {
			continue
		}
		ran++
		t.Run(v.Hex+"_"+diag, func(t *testing.T) {
			t.Parallel()
			data, err := hex.DecodeString(v.Hex)
			require.NoError(t, err, "bad test hex")

			val, err := Unmarshal(data)
			require.NoError(t, err, "Unmarshal should accept valid canonical CBOR %s (diag: %s)", v.Hex, diag)

			// Round-trip: re-encode and verify identical bytes.
			out, err := Marshal(val)
			require.NoError(t, err, "Marshal should succeed for %s", v.Hex)
			assert.Equal(t, data, out, "round-trip mismatch for %s (diag: %s)", v.Hex, diag)
		})
	}
	t.Logf("ran %d valid DAG-CBOR vector tests", ran)
}

// TestRFC8949_DAGCBORRejectsValidButNonCanonical tests that valid-but-non-canonical
// CBOR is rejected by DAG-CBOR (which requires canonical encoding).
func TestRFC8949_DAGCBORRejectsValidButNonCanonical(t *testing.T) {
	t.Parallel()
	vectors := loadRFC8949Vectors(t)

	// These are valid CBOR but use indefinite-length encoding, which DAG-CBOR forbids.
	ran := 0
	for _, v := range vectors {
		if !v.hasFlag("valid") || v.hasFlag("canonical") {
			continue
		}
		// Skip feature-gated ones.
		if v.hasFeature("float16") || v.hasFeature("bignum") || v.hasFeature("simple") {
			continue
		}
		// Skip float32/NaN/Infinity which are already tested.
		diag := v.Diagnostic
		if diag == "NaN" || diag == "Infinity" || diag == "-Infinity" {
			continue
		}
		ran++
		t.Run(v.Hex+"_"+diag, func(t *testing.T) {
			t.Parallel()
			data, err := hex.DecodeString(v.Hex)
			require.NoError(t, err)
			_, err = Unmarshal(data)
			// DAG-CBOR should reject: indefinite-length, float32, non-canonical.
			assert.Error(t, err, "DAG-CBOR should reject non-canonical valid CBOR %s (diag: %s)", v.Hex, diag)
		})
	}
	t.Logf("ran %d non-canonical rejection tests", ran)
}

// ---------------------------------------------------------------------------
// 2. Well-formedness tests from fxamacker/cbor (RFC 7049bis Appendix G)
// ---------------------------------------------------------------------------

func mustHexDecode(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(fmt.Sprintf("bad hex %q: %v", s, err))
	}
	return b
}

// TestFxamacker_TruncatedHeads tests premature EOF in CBOR headers.
func TestFxamacker_TruncatedHeads(t *testing.T) {
	t.Parallel()

	truncated := []string{
		"18",               // uint, 1-byte follow missing
		"19",               // uint, 2-byte follow missing
		"1a",               // uint, 4-byte follow missing
		"1b",               // uint, 8-byte follow missing
		"1901",             // uint, 2-byte partially present
		"1a0102",           // uint, 4-byte partially present
		"1b01020304050607", // uint, 8-byte partially present (7 of 8)
		"38",               // negint, 1-byte follow missing
		"58",               // bytes, length missing
		"78",               // text, length missing
		"98",               // array, length missing
		"9a01ff00",         // array, 4-byte length partially present
		"b8",               // map, length missing
		"d8",               // tag, number missing
		"f8",               // simple value, value missing
		"f900",             // float16, 1 of 2 bytes
		"fa0000",           // float32, 2 of 4 bytes
		"fb000000",         // float64, 4 of 8 bytes
		"fb00000000000000", // float64, 7 of 8 bytes
	}

	for _, h := range truncated {
		t.Run(h, func(t *testing.T) {
			t.Parallel()
			data := mustHexDecode(h)
			_, err := Unmarshal(data)
			assert.Error(t, err, "should reject truncated head %s", h)
		})
	}
}

// TestFxamacker_TruncatedPayloads tests premature EOF in CBOR payloads.
func TestFxamacker_TruncatedPayloads(t *testing.T) {
	t.Parallel()

	truncated := []string{
		"41",                 // bytes(1) with no data
		"61",                 // text(1) with no data
		"5affffffff00",       // bytes(4294967295) with 1 byte
		"7affffffff00",       // text(4294967295) with 1 byte
		"81",                 // array(1) empty
		"818181818181818181", // nested arrays, innermost missing
		"8200",               // array(2) with only 1 item
		"a1",                 // map(1) empty
		"a100",               // map(1) with key but no value
		"a20102",             // map(2) with 1.5 entries
		"a2000000",           // map(2) with only first entry (int keys, but tests truncation)
	}

	for _, h := range truncated {
		t.Run(h, func(t *testing.T) {
			t.Parallel()
			data := mustHexDecode(h)
			_, err := Unmarshal(data)
			assert.Error(t, err, "should reject truncated payload %s", h)
		})
	}
}

// TestFxamacker_ReservedAdditionalInfo tests that reserved additional info
// values (28, 29, 30) are rejected for all major types.
func TestFxamacker_ReservedAdditionalInfo(t *testing.T) {
	t.Parallel()

	// additional info 28=0x1c, 29=0x1d, 30=0x1e for each major type
	reserved := []string{
		"1c", "1d", "1e", // uint
		"3c", "3d", "3e", // negint
		"5c", "5d", "5e", // bytes
		"7c", "7d", "7e", // text
		"9c", "9d", "9e", // array
		"bc", "bd", "be", // map
		"dc", "dd", "de", // tag
		"fc", "fd", "fe", // simple/float
	}

	for _, h := range reserved {
		t.Run(h, func(t *testing.T) {
			t.Parallel()
			data := mustHexDecode(h)
			_, err := Unmarshal(data)
			assert.Error(t, err, "should reject reserved additional info %s", h)
		})
	}
}

// TestFxamacker_InvalidUTF8 tests rejection of invalid UTF-8 in text strings.
func TestFxamacker_InvalidUTF8(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		hex  string
	}{
		{"lone continuation", "62" + "80" + "61"},    // text(2): 0x80 'a'
		{"overlong 2-byte NUL", "62" + "c080"},       // text(2): overlong NUL
		{"truncated 2-byte", "61" + "c3"},            // text(1): start of ü but no follow
		{"surrogate half", "63" + "eda080"},          // text(3): U+D800 (surrogate)
		{"high surrogate", "63" + "edbfbf"},          // text(3): U+DFFF (surrogate)
		{"overlong 3-byte", "63" + "e08080"},         // text(3): overlong NUL
		{"4-byte above U+10FFFF", "64" + "f4908080"}, // text(4): U+110000
		{"5-byte sequence", "65" + "f8808080af"},     // text(5): invalid 5-byte
		{"lone start byte ff", "61" + "ff"},          // text(1): 0xFF
		{"lone start byte fe", "61" + "fe"},          // text(1): 0xFE
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := mustHexDecode(tc.hex)
			_, err := Unmarshal(data)
			assert.Error(t, err, "should reject invalid UTF-8: %s", tc.name)
		})
	}
}

// ---------------------------------------------------------------------------
// 3. Non-minimal encoding rejection (comprehensive)
// ---------------------------------------------------------------------------

// TestNonMinimalEncoding_AllMajorTypes tests rejection of non-minimal encoding
// for every major type and every size class boundary.
func TestNonMinimalEncoding_AllMajorTypes(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name string
		hex  string
	}

	cases := []testCase{
		// Major 0 (uint): value fits in smaller encoding
		{"uint 0 in 1-byte", "1800"},
		{"uint 23 in 1-byte", "1817"},
		{"uint 24 in 2-byte", "190018"},
		{"uint 255 in 2-byte", "1900ff"},
		{"uint 256 in 4-byte", "1a00000100"},
		{"uint 65535 in 4-byte", "1a0000ffff"},
		{"uint 65536 in 8-byte", "1b0000000000010000"},
		{"uint 0xFFFFFFFF in 8-byte", "1b00000000ffffffff"},

		// Major 1 (negint): same rules
		{"negint -1 in 1-byte", "3800"},
		{"negint -24 in 1-byte", "3817"},
		{"negint -25 in 2-byte", "390018"},
		{"negint -256 in 2-byte", "3900ff"},
		{"negint -257 in 4-byte", "3a00000100"},
		{"negint -65536 in 4-byte", "3a0000ffff"},
		{"negint -65537 in 8-byte", "3b0000000000010000"},

		// Major 2 (bytes): length non-minimal
		{"bytes len=0 in 1-byte", "5800"},
		{"bytes len=23 in 1-byte", "5817"},
		{"bytes len=0 in 2-byte", "590000"},

		// Major 3 (text): length non-minimal
		{"text len=0 in 1-byte", "7800"},
		{"text len=23 in 1-byte", "7817"},
		{"text len=0 in 2-byte", "790000"},

		// Major 4 (array): length non-minimal
		{"array len=0 in 1-byte", "9800"},
		{"array len=0 in 2-byte", "990000"},

		// Major 5 (map): length non-minimal
		{"map len=0 in 1-byte", "b800"},
		{"map len=0 in 2-byte", "b90000"},

		// Major 6 (tag): tag number non-minimal
		{"tag 0 in 1-byte", "d800"},
		{"tag 0 in 2-byte", "d90000"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := mustHexDecode(tc.hex)
			_, err := Unmarshal(data)
			assert.Error(t, err, "should reject non-minimal: %s (%s)", tc.name, tc.hex)
		})
	}
}

// ---------------------------------------------------------------------------
// 4. Integer boundary tests
// ---------------------------------------------------------------------------

func TestIntegerBoundaries(t *testing.T) {
	t.Parallel()

	// Test encoding/decoding at each size class boundary.
	boundaries := []struct {
		val int64
		hex string
	}{
		{0, "00"},
		{1, "01"},
		{23, "17"},
		{24, "1818"},
		{255, "18ff"},
		{256, "190100"},
		{65535, "19ffff"},
		{65536, "1a00010000"},
		{math.MaxInt32, "1a7fffffff"},
		{int64(math.MaxUint32), "1affffffff"},
		{int64(math.MaxUint32) + 1, "1b0000000100000000"},
		{math.MaxInt64, "1b7fffffffffffffff"},
		{-1, "20"},
		{-10, "29"},
		{-24, "37"},
		{-25, "3818"},
		{-100, "3863"},
		{-256, "38ff"},
		{-257, "390100"},
		{-65536, "39ffff"},
		{-65537, "3a00010000"},
		{math.MinInt64, "3b7fffffffffffffff"},
	}

	for _, b := range boundaries {
		t.Run(fmt.Sprintf("%d", b.val), func(t *testing.T) {
			t.Parallel()
			expected := mustHexDecode(b.hex)

			// Encode and verify bytes.
			got, err := Marshal(b.val)
			require.NoError(t, err)
			assert.Equal(t, expected, got, "encoding %d", b.val)

			// Decode and verify value.
			val, err := Unmarshal(expected)
			require.NoError(t, err)
			assert.Equal(t, b.val, val, "decoding %s", b.hex)
		})
	}
}

// ---------------------------------------------------------------------------
// 5. Float64 edge cases
// ---------------------------------------------------------------------------

func TestFloat64EdgeCases(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		val  float64
	}{
		{"zero", 0.0},
		{"negative zero", math.Copysign(0.0, -1.0)},
		{"one", 1.0},
		{"minus one", -1.0},
		{"1.1", 1.1},
		{"-4.1", -4.1},
		{"1e300", 1e300},
		{"smallest denorm", math.SmallestNonzeroFloat64},
		{"max float64", math.MaxFloat64},
		{"epsilon", math.Nextafter(1.0, 2.0) - 1.0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b, err := Marshal(tc.val)
			require.NoError(t, err)

			val, err := Unmarshal(b)
			require.NoError(t, err)
			assert.Equal(t, tc.val, val)

			// Verify always encoded as float64 (9 bytes: 0xfb + 8).
			assert.Len(t, b, 9, "float64 must always be 9 bytes in DAG-CBOR")
			assert.Equal(t, byte(0xfb), b[0])
		})
	}
}

func TestFloat64_RejectSpecialValues(t *testing.T) {
	t.Parallel()

	specials := []struct {
		name string
		hex  string
	}{
		// float64 NaN
		{"float64 NaN", "fb7ff8000000000000"},
		// float64 +Inf
		{"float64 +Inf", "fb7ff0000000000000"},
		// float64 -Inf
		{"float64 -Inf", "fbfff0000000000000"},
		// float64 signaling NaN
		{"float64 sNaN", "fb7ff0000000000001"},
		// float16 NaN
		{"float16 NaN", "f97e00"},
		// float16 +Inf
		{"float16 +Inf", "f97c00"},
		// float16 -Inf
		{"float16 -Inf", "f9fc00"},
		// float32 NaN
		{"float32 NaN", "fa7fc00000"},
		// float32 +Inf
		{"float32 +Inf", "fa7f800000"},
		// float32 -Inf
		{"float32 -Inf", "faff800000"},
	}

	for _, tc := range specials {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := mustHexDecode(tc.hex)
			_, err := Unmarshal(data)
			assert.Error(t, err, "should reject %s", tc.name)
		})
	}
}

// ---------------------------------------------------------------------------
// 6. DAG-CBOR specific: map key ordering
// ---------------------------------------------------------------------------

func TestMapKeyOrdering_Comprehensive(t *testing.T) {
	t.Parallel()

	// DAG-CBOR: shorter keys first, then lexicographic (by raw CBOR bytes).
	t.Run("single char before two char", func(t *testing.T) {
		t.Parallel()
		// {"a": 1, "bb": 2} — correct DAG-CBOR order
		good := mustHexDecode("a2" + "6161" + "01" + "6262" + "6202")
		// wrong: reversed
		bad := mustHexDecode("a2" + "6262" + "62" + "02" + "6161" + "01")
		_, err := Unmarshal(good)
		assert.NoError(t, err)
		_, err = Unmarshal(bad)
		assert.Error(t, err)
	})

	t.Run("length ordering beats lex", func(t *testing.T) {
		t.Parallel()
		// {"b": 1, "aa": 2} is correct — "b" (1 char) before "aa" (2 chars)
		good := mustHexDecode("a2" + "6162" + "01" + "6261" + "6102")
		// wrong: "aa" before "b"
		bad := mustHexDecode("a2" + "6261" + "61" + "02" + "6162" + "01")
		_, err := Unmarshal(good)
		assert.NoError(t, err)
		_, err = Unmarshal(bad)
		assert.Error(t, err)
	})

	t.Run("empty key first", func(t *testing.T) {
		t.Parallel()
		// {"": 1, "a": 2} — empty key is shortest
		good := mustHexDecode("a2" + "60" + "01" + "6161" + "02")
		_, err := Unmarshal(good)
		assert.NoError(t, err)
	})

	t.Run("encoder produces correct order", func(t *testing.T) {
		t.Parallel()
		m := map[string]any{
			"bb": int64(2),
			"a":  int64(1),
			"":   int64(0),
			"cc": int64(3),
		}
		b, err := Marshal(m)
		require.NoError(t, err)

		// Decode and re-encode should be identical.
		val, err := Unmarshal(b)
		require.NoError(t, err)
		b2, err := Marshal(val)
		require.NoError(t, err)
		assert.Equal(t, b, b2)
	})
}

// ---------------------------------------------------------------------------
// 7. DavidBuchanan314 dag-cbor-benchmark torture tests
// ---------------------------------------------------------------------------

func TestBenchmarkData_TortureCIDs(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("testdata/torture_cids.dagcbor")
	require.NoError(t, err)

	val, err := Unmarshal(data)
	require.NoError(t, err)

	// Should be an array of CIDs.
	arr, ok := val.([]any)
	require.True(t, ok, "expected array, got %T", val)
	assert.Greater(t, len(arr), 1000, "expected many CIDs")

	// Every element should be a CID.
	for i, elem := range arr {
		_, ok := elem.(CID)
		assert.True(t, ok, "element %d should be CID, got %T", i, elem)
		if !ok && i > 10 {
			break // don't spam
		}
	}

	// Round-trip.
	out, err := Marshal(val)
	require.NoError(t, err)
	assert.Equal(t, data, out)
}

// ---------------------------------------------------------------------------
// 8. Tag 42 (CID link) edge cases from ipfs/go-ipld-cbor
// ---------------------------------------------------------------------------

func TestTag42_EdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("tag 42 must wrap bytes", func(t *testing.T) {
		t.Parallel()
		// tag(42) + text string "hello"
		data := mustHexDecode("d82a" + "6568656c6c6f")
		_, err := Unmarshal(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "byte string")
	})

	t.Run("tag 42 empty bytes", func(t *testing.T) {
		t.Parallel()
		// tag(42) + bytes(0)
		data := mustHexDecode("d82a" + "40")
		_, err := Unmarshal(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty")
	})

	t.Run("tag 42 missing 0x00 prefix", func(t *testing.T) {
		t.Parallel()
		// tag(42) + bytes with no 0x00 prefix — just some random CID-like bytes
		data := mustHexDecode("d82a" + "4501" + "7112200000000000000000000000000000000000000000000000000000000000000000")
		_, err := Unmarshal(data)
		assert.Error(t, err)
	})

	t.Run("all non-42 tags rejected", func(t *testing.T) {
		t.Parallel()
		tags := []string{
			"c0",               // tag 0
			"c1",               // tag 1
			"c2",               // tag 2
			"c3",               // tag 3
			"d74401020304",     // tag 23
			"d818456449455446", // tag 24
			"d82076687474703a2f2f7777772e6578616d706c652e636f6d", // tag 32
		}
		for _, h := range tags {
			data := mustHexDecode(h)
			_, err := Unmarshal(data)
			assert.Error(t, err, "should reject tag in %s", h)
		}
	})
}

// ---------------------------------------------------------------------------
// 9. Indefinite-length encoding rejection (comprehensive)
// ---------------------------------------------------------------------------

func TestIndefiniteLength_AllTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		hex  string
	}{
		{"indef bytes", "5f42010243030405ff"},
		{"indef text", "7f657374726561646d696e67ff"},
		{"indef array empty", "9fff"},
		{"indef array items", "9f018202039f0405ffff"},
		{"indef array mixed", "9f01820203820405ff"},
		{"indef array outer only", "83018202039f0405ff"},
		{"indef array inner only", "83019f0203ff820405"},
		{"indef array 25 items", "9f0102030405060708090a0b0c0d0e0f101112131415161718181819ff"},
		{"indef map", "bf61610161629f0203ffff"},
		{"indef map in array", "826161bf61626163ff"},
		{"indef map unsorted", "bf6346756ef563416d7421ff"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := mustHexDecode(tc.hex)
			_, err := Unmarshal(data)
			assert.Error(t, err, "should reject indefinite-length: %s", tc.name)
		})
	}
}

// ---------------------------------------------------------------------------
// 10. Simple values rejection (DAG-CBOR only allows false, true, null, float64)
// ---------------------------------------------------------------------------

func TestSimpleValues_Rejected(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		hex  string
	}{
		{"undefined", "f7"},
		{"simple(0)", "e0"},
		{"simple(16)", "f0"},
		{"simple(32)", "f820"},
		{"simple(255)", "f8ff"},
		{"simple(19)", "f3"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := mustHexDecode(tc.hex)
			_, err := Unmarshal(data)
			assert.Error(t, err, "should reject simple value: %s", tc.name)
		})
	}
}

// ---------------------------------------------------------------------------
// 11. Negative integer overflow
// ---------------------------------------------------------------------------

func TestNegativeIntegerOverflow(t *testing.T) {
	t.Parallel()

	// CBOR negint -1 - 2^63 = -(2^63+1) which overflows int64.
	// Major 1, additional 27, value 2^63 = 0x8000000000000000
	data := mustHexDecode("3b8000000000000000")
	_, err := Unmarshal(data)
	assert.Error(t, err, "should reject negative integer overflow")

	// Max valid negative: -2^63 (MinInt64) = -1 - (2^63-1)
	data = mustHexDecode("3b7fffffffffffffff")
	val, err := Unmarshal(data)
	require.NoError(t, err)
	assert.Equal(t, int64(math.MinInt64), val)

	// One more overflow case: -1 - 0xFFFFFFFFFFFFFFFF
	data = mustHexDecode("3bffffffffffffffff")
	_, err = Unmarshal(data)
	assert.Error(t, err, "should reject -1 - MaxUint64")
}

// ---------------------------------------------------------------------------
// 12. Complex nested structure round-trips
// ---------------------------------------------------------------------------

func TestComplexNestedRoundTrips(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		val  any
	}{
		{
			"array of mixed types",
			[]any{int64(1), "hello", true, nil, []byte{0xde, 0xad}, float64(3.14)},
		},
		{
			"nested maps",
			map[string]any{
				"a": map[string]any{
					"b": map[string]any{
						"c": int64(42),
					},
				},
			},
		},
		{
			"array of maps",
			[]any{
				map[string]any{"x": int64(1)},
				map[string]any{"y": int64(2)},
			},
		},
		{
			"map with array values",
			map[string]any{
				"items": []any{int64(1), int64(2), int64(3)},
				"meta":  map[string]any{"count": int64(3)},
			},
		},
		{
			"deeply nested arrays",
			[]any{[]any{[]any{[]any{int64(1)}}}},
		},
		{
			"empty containers",
			map[string]any{
				"arr":  []any{},
				"map":  map[string]any{},
				"null": nil,
			},
		},
		{
			"unicode keys and values",
			map[string]any{
				"emoji":   "🎉",
				"chinese": "中文",
				"arabic":  "عربي",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b, err := Marshal(tc.val)
			require.NoError(t, err)

			val, err := Unmarshal(b)
			require.NoError(t, err)

			// Re-encode to verify determinism.
			b2, err := Marshal(val)
			require.NoError(t, err)
			assert.Equal(t, b, b2, "deterministic round-trip failed for %s", tc.name)
		})
	}
}

// ---------------------------------------------------------------------------
// 13. Huge allocations and resource limits
// ---------------------------------------------------------------------------

func TestResourceLimits(t *testing.T) {
	t.Parallel()

	t.Run("huge bytes claim", func(t *testing.T) {
		t.Parallel()
		// bytes(2^32) — claims 4GB
		data := mustHexDecode("5b" + "0000000100000000")
		_, err := Unmarshal(data)
		assert.Error(t, err)
	})

	t.Run("huge text claim", func(t *testing.T) {
		t.Parallel()
		// text(2^32)
		data := mustHexDecode("7b" + "0000000100000000")
		_, err := Unmarshal(data)
		assert.Error(t, err)
	})

	t.Run("huge map claim", func(t *testing.T) {
		t.Parallel()
		// map(2^32)
		data := mustHexDecode("bb" + "0000000100000000")
		_, err := Unmarshal(data)
		assert.Error(t, err)
	})

	t.Run("near max depth", func(t *testing.T) {
		t.Parallel()
		// MaxDepth-1 levels of nesting — should succeed (ReadValue increments before check).
		depth := MaxDepth - 1
		data := make([]byte, depth+1)
		for i := range depth {
			data[i] = 0x81 // array(1)
		}
		data[depth] = 0x00 // int 0
		_, err := Unmarshal(data)
		assert.NoError(t, err, "MaxDepth-1 nesting should be accepted")
	})

	t.Run("one past max depth", func(t *testing.T) {
		t.Parallel()
		depth := MaxDepth + 1
		data := make([]byte, depth+1)
		for i := range depth {
			data[i] = 0x81
		}
		data[depth] = 0x00
		_, err := Unmarshal(data)
		assert.Error(t, err)
	})
}

// ---------------------------------------------------------------------------
// 14. Canonical encoding verification (inspired by go-ipld-cbor)
// ---------------------------------------------------------------------------

func TestCanonicalEncoding_MapKeyOrder(t *testing.T) {
	t.Parallel()

	// Marshal a map and verify the key bytes are in DAG-CBOR order.
	m := map[string]any{
		"z":   int64(1),
		"aa":  int64(2),
		"a":   int64(3),
		"":    int64(4),
		"bbb": int64(5),
		"bb":  int64(6),
	}

	b, err := Marshal(m)
	require.NoError(t, err)

	// Decode to verify acceptance.
	val, err := Unmarshal(b)
	require.NoError(t, err)
	m2, ok := val.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, int64(1), m2["z"])
	assert.Equal(t, int64(4), m2[""])

	// Manual verification: walk the encoded bytes to extract key order.
	// The map header should be followed by keys in order: "", "a", "z", "aa", "bb", "bbb"
	reader := bytes.NewReader(b)
	dec := NewDecoder(reader)
	result, err := dec.ReadValue()
	require.NoError(t, err)
	resultMap, ok2 := result.(map[string]any)
	require.True(t, ok2)

	// Since Go maps are unordered, verify by re-encoding — should produce identical bytes.
	b2, err := Marshal(resultMap)
	require.NoError(t, err)
	assert.Equal(t, b, b2)
}

// ---------------------------------------------------------------------------
// 15. String encoding edge cases
// ---------------------------------------------------------------------------

func TestStringEncoding(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		val  string
		hex  string
	}{
		{"empty", "", "60"},
		{"a", "a", "6161"},
		{"IETF", "IETF", "6449455446"},
		{"quote backslash", "\"\\", "62225c"},
		{"u-umlaut", "\u00fc", "62c3bc"},
		{"CJK water", "\u6c34", "63e6b0b4"},
		{"linear B syllable", "\U00010151", "64f0908591"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			expected := mustHexDecode(tc.hex)
			got, err := Marshal(tc.val)
			require.NoError(t, err)
			assert.Equal(t, expected, got)

			val, err := Unmarshal(expected)
			require.NoError(t, err)
			assert.Equal(t, tc.val, val)
		})
	}
}

// ---------------------------------------------------------------------------
// 16. Byte string edge cases
// ---------------------------------------------------------------------------

func TestByteStringEncoding(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		val  []byte
		hex  string
	}{
		{"empty", []byte{}, "40"},
		{"4 bytes", []byte{1, 2, 3, 4}, "4401020304"},
		{"24 bytes", bytes.Repeat([]byte{0xAB}, 24), "5818" + strings.Repeat("ab", 24)},
		{"256 bytes", bytes.Repeat([]byte{0xCD}, 256), "590100" + strings.Repeat("cd", 256)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			expected := mustHexDecode(tc.hex)
			got, err := Marshal(tc.val)
			require.NoError(t, err)
			assert.Equal(t, expected, got)

			val, err := Unmarshal(expected)
			require.NoError(t, err)
			assert.Equal(t, tc.val, val)
		})
	}
}

// ---------------------------------------------------------------------------
// 17. Append/Read helpers round-trip stress test
// ---------------------------------------------------------------------------

func TestAppendReadHelpers_RoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("integers", func(t *testing.T) {
		t.Parallel()
		values := []int64{0, 1, -1, 23, 24, -24, -25, 255, 256, -256, -257,
			65535, 65536, -65536, math.MaxInt32, math.MinInt32,
			math.MaxInt64, math.MinInt64}

		for _, v := range values {
			buf := AppendInt(nil, v)
			got, n, err := ReadInt(buf, 0)
			require.NoError(t, err, "ReadInt(%d)", v)
			assert.Equal(t, v, got, "ReadInt mismatch")
			assert.Equal(t, len(buf), n, "ReadInt consumed bytes")
		}
	})

	t.Run("uints", func(t *testing.T) {
		t.Parallel()
		values := []uint64{0, 1, 23, 24, 255, 256, 65535, 65536,
			math.MaxUint32, math.MaxInt64}

		for _, v := range values {
			buf := AppendUint(nil, v)
			got, n, err := ReadUint(buf, 0)
			require.NoError(t, err, "ReadUint(%d)", v)
			assert.Equal(t, v, got)
			assert.Equal(t, len(buf), n)
		}
	})

	t.Run("text", func(t *testing.T) {
		t.Parallel()
		values := []string{"", "a", "hello", "🎉", strings.Repeat("x", 300)}

		for _, v := range values {
			buf := AppendText(nil, v)
			got, n, err := ReadText(buf, 0)
			require.NoError(t, err, "ReadText(%q)", v)
			assert.Equal(t, v, got)
			assert.Equal(t, len(buf), n)
		}
	})

	t.Run("bytes", func(t *testing.T) {
		t.Parallel()
		values := [][]byte{{}, {0}, {1, 2, 3}, bytes.Repeat([]byte{0xff}, 300)}

		for _, v := range values {
			buf := AppendBytes(nil, v)
			got, n, err := ReadBytes(buf, 0)
			require.NoError(t, err)
			assert.Equal(t, v, got)
			assert.Equal(t, len(buf), n)
		}
	})

	t.Run("float64", func(t *testing.T) {
		t.Parallel()
		values := []float64{0.0, math.Copysign(0.0, -1.0), 1.0, -1.0, 3.14, math.MaxFloat64, math.SmallestNonzeroFloat64}

		for _, v := range values {
			buf := AppendFloat64(nil, v)
			got, n, err := ReadFloat64(buf, 0)
			require.NoError(t, err, "ReadFloat64(%v)", v)
			assert.Equal(t, v, got)
			assert.Equal(t, len(buf), n)
		}
	})
}

// ---------------------------------------------------------------------------
// 18. SkipValue stress tests
// ---------------------------------------------------------------------------

func TestSkipValue_AllTypes(t *testing.T) {
	t.Parallel()

	values := []any{
		nil,
		true,
		false,
		int64(0),
		int64(42),
		int64(-100),
		int64(math.MaxInt64),
		int64(math.MinInt64),
		float64(3.14),
		"",
		"hello world",
		[]byte{},
		[]byte{1, 2, 3},
		[]any{},
		[]any{int64(1), "two", true},
		map[string]any{},
		map[string]any{"a": int64(1), "bb": []any{int64(2), int64(3)}},
	}

	for i, v := range values {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			b, err := Marshal(v)
			require.NoError(t, err)

			// Append a sentinel after the value.
			sentinel := byte(0x01) // int 1
			data := append(b, sentinel)

			n, err := SkipValue(data, 0)
			require.NoError(t, err)
			assert.Equal(t, len(b), n, "SkipValue should skip exactly the encoded value")
			assert.Equal(t, sentinel, data[n])
		})
	}
}

// ---------------------------------------------------------------------------
// 19. CompareCBORKeys correctness
// ---------------------------------------------------------------------------

func TestCompareCBORKeys_Exhaustive(t *testing.T) {
	t.Parallel()

	// DAG-CBOR: shorter CBOR key first, then lexicographic.
	// Since all keys are text strings, shorter length = shorter CBOR encoding.
	keys := []string{"", "a", "b", "z", "aa", "ab", "ba", "aaa", "zzz"}

	for i := range keys {
		for j := range keys {
			a, b := keys[i], keys[j]
			cmp := CompareCBORKeys(a, b)
			if i < j {
				assert.Equal(t, -1, cmp, "expected %q < %q", a, b)
			} else if i > j {
				assert.Equal(t, 1, cmp, "expected %q > %q", a, b)
			} else {
				assert.Equal(t, 0, cmp, "expected %q == %q", a, b)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// 20. Single-byte CBOR values (exhaustive scan)
// ---------------------------------------------------------------------------

// TestSingleByteExhaustive tries every possible single-byte CBOR value and
// ensures our decoder either accepts it correctly or rejects it without panic.
func TestSingleByteExhaustive(t *testing.T) {
	t.Parallel()

	for b := range 256 {
		b := byte(b)
		t.Run(fmt.Sprintf("0x%02x", b), func(t *testing.T) {
			t.Parallel()
			_, err := Unmarshal([]byte{b})
			// We don't care about the specific result, just that it doesn't panic.
			// Valid single-byte values: 0x00-0x17 (uint 0-23), 0x20-0x37 (negint),
			// 0x40 (empty bytes), 0x60 (empty text), 0x80 (empty array), 0xa0 (empty map),
			// 0xf4 (false), 0xf5 (true), 0xf6 (null).
			switch {
			case b <= 0x17: // uint 0-23
				assert.NoError(t, err)
			case b == 0x40: // empty bytes
				assert.NoError(t, err)
			case b == 0x60: // empty text
				assert.NoError(t, err)
			case b == 0x80: // empty array
				assert.NoError(t, err)
			case b == 0xa0: // empty map
				assert.NoError(t, err)
			case b == 0xf4 || b == 0xf5 || b == 0xf6: // false, true, null
				assert.NoError(t, err)
			case b >= 0x20 && b <= 0x37: // negint -1..-24
				assert.NoError(t, err)
			default:
				// Everything else should either error or need more bytes (EOF).
				// Just verify no panic occurred.
				_ = err
			}
		})
	}
}
