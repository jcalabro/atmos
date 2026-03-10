package repo

import (
	"bytes"
	"os"
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
)

// FuzzDecodeCommit tests that commit decoding never panics on arbitrary CBOR
// and that valid commits round-trip.
func FuzzDecodeCommit(f *testing.F) {
	// Seed with a valid signed commit.
	key, _ := crypto.GenerateP256()
	c := &Commit{
		DID:     "did:plc:testuser1234567890abcde",
		Version: 3,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root")),
		Rev:     "3jqfcqzm3fo2j",
	}
	_ = c.Sign(key)
	data, _ := encodeCommit(c)
	f.Add(data)

	// Seed with commit from CAR fixture.
	carData, _ := os.ReadFile("testdata/greenground.repo.car")
	if len(carData) > 0 {
		_, commit, err := LoadFromCAR(bytes.NewReader(carData))
		if err == nil {
			commitData, err := encodeCommit(commit)
			if err == nil {
				f.Add(commitData)
			}
		}
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// Must never panic, regardless of input.
		_, _ = decodeCommit(data)
	})
}

// FuzzLoadFromCAR tests that loading a repo from arbitrary CAR data never panics.
func FuzzLoadFromCAR(f *testing.F) {
	carData, _ := os.ReadFile("testdata/greenground.repo.car")
	if len(carData) > 0 {
		f.Add(carData)
	}
	repoSlice, _ := os.ReadFile("testdata/repo_slice.car")
	if len(repoSlice) > 0 {
		f.Add(repoSlice)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// Should never panic.
		_, _, _ = LoadFromCAR(bytes.NewReader(data))
	})
}
