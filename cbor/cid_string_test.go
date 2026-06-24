package cbor

import (
	"encoding/base32"
	"testing"
)

// oldCIDString is the prior three-allocation implementation, kept as the
// reference oracle for the single-allocation String() that replaced it.
func oldCIDString(c CID) string {
	return "b" + base32Lower.EncodeToString(c.Bytes())
}

// TestCIDStringMatchesReference pins that the optimized single-alloc String()
// is byte-identical to the old "b"+base32(Bytes()) form across every codec byte
// and a spread of hashes, and that the result round-trips through
// ParseCIDString back to the same CID.
func TestCIDStringMatchesReference(t *testing.T) {
	t.Parallel()
	for codec := 0; codec < 256; codec++ {
		var c CID
		c.codec = uint8(codec)
		// Vary the hash deterministically across codecs.
		for i := range c.hash {
			c.hash[i] = byte((codec*31 + i*7) & 0xff)
		}
		got := c.String()
		want := oldCIDString(c)
		if got != want {
			t.Fatalf("codec %d: String()=%q, want %q", codec, got, want)
		}
		// Round-trip only for the codecs CIDs may actually carry (dag-cbor, raw);
		// ParseCIDString rejects others by design. The encoding equality above is
		// asserted for ALL 256 codec bytes regardless.
		if uint64(codec) == CodecDagCBOR || uint64(codec) == CodecRaw {
			parsed, err := ParseCIDString(got)
			if err != nil {
				t.Fatalf("codec %d: ParseCIDString(%q): %v", codec, got, err)
			}
			if parsed != c {
				t.Fatalf("codec %d: round-trip mismatch: %+v != %+v", codec, parsed, c)
			}
		}
	}
}

// TestCIDStringSingleAllocation asserts String() now allocates exactly once
// (the output buffer), guarding against a regression to the 3-alloc form.
// Not parallel: testing.AllocsPerRun panics if called from a parallel test.
//
//nolint:paralleltest // AllocsPerRun cannot run under t.Parallel
func TestCIDStringSingleAllocation(t *testing.T) {
	c := ComputeCID(CodecDagCBOR, []byte("alloc-count fixture"))
	allocs := testing.AllocsPerRun(1000, func() {
		_ = c.String()
	})
	if allocs > 1 {
		t.Fatalf("CID.String allocated %.0f times, want 1", allocs)
	}
}

// FuzzCIDStringMatchesReference drives arbitrary CID bytes through both String()
// implementations and requires byte-identical output.
func FuzzCIDStringMatchesReference(f *testing.F) {
	f.Add(byte(0x71), make([]byte, 32))
	f.Add(byte(0x55), []byte("0123456789abcdef0123456789abcdef"))
	f.Fuzz(func(t *testing.T, codec byte, hash []byte) {
		var c CID
		c.codec = codec
		copy(c.hash[:], hash) // hash[:32]; short hash leaves zero tail
		if c.String() != oldCIDString(c) {
			t.Fatalf("codec %d hash %x: %q != %q", codec, c.hash, c.String(), oldCIDString(c))
		}
	})
}

// sanity: the encoder used by both paths is the canonical base32lower.
var _ = base32.NoPadding
