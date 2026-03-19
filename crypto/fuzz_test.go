package crypto

import (
	"bytes"
	"testing"
)

// FuzzParseDIDKey tests that DID key parsing never panics and round-trips.
func FuzzParseDIDKey(f *testing.F) {
	p256, _ := GenerateP256()
	k256, _ := GenerateK256()
	f.Add(p256.PublicKey().DIDKey())
	f.Add(k256.PublicKey().DIDKey())
	f.Add("")
	f.Add("did:key:z")
	f.Add("did:key:zDnaerDaTF5BXEavCrfRZEk316dpbLsfPDZ3WJ5hRTPFU2169")

	f.Fuzz(func(t *testing.T, s string) {
		pk, err := ParsePublicDIDKey(s)
		if err != nil {
			return
		}
		rt, err := ParsePublicDIDKey(pk.DIDKey())
		if err != nil {
			t.Fatalf("round-trip failed: %v", err)
		}
		if !pk.Equal(rt) {
			t.Fatalf("round-trip key mismatch")
		}
	})
}

// FuzzParsePublicBytesP256 tests that P-256 key parsing never panics.
func FuzzParsePublicBytesP256(f *testing.F) {
	key, _ := GenerateP256()
	f.Add(key.PublicKey().Bytes())
	f.Add([]byte{})
	f.Add([]byte{0x02}) // too short

	f.Fuzz(func(t *testing.T, data []byte) {
		pk, err := ParsePublicBytesP256(data)
		if err != nil {
			return
		}
		rt, err := ParsePublicBytesP256(pk.Bytes())
		if err != nil {
			t.Fatalf("round-trip failed: %v", err)
		}
		if !pk.Equal(rt) {
			t.Fatalf("round-trip key mismatch")
		}
	})
}

// FuzzParsePublicBytesK256 tests that K-256 key parsing never panics.
func FuzzParsePublicBytesK256(f *testing.F) {
	key, _ := GenerateK256()
	f.Add(key.PublicKey().Bytes())
	f.Add([]byte{})
	f.Add([]byte{0x03}) // too short

	f.Fuzz(func(t *testing.T, data []byte) {
		pk, err := ParsePublicBytesK256(data)
		if err != nil {
			return
		}
		rt, err := ParsePublicBytesK256(pk.Bytes())
		if err != nil {
			t.Fatalf("round-trip failed: %v", err)
		}
		if !pk.Equal(rt) {
			t.Fatalf("round-trip key mismatch")
		}
	})
}

// FuzzParsePublicMultibase tests that multibase key parsing never panics and round-trips.
func FuzzParsePublicMultibase(f *testing.F) {
	p256, _ := GenerateP256()
	k256, _ := GenerateK256()
	f.Add(p256.PublicKey().Multibase())
	f.Add(k256.PublicKey().Multibase())
	f.Add("")
	f.Add("z")

	f.Fuzz(func(t *testing.T, s string) {
		pk, err := ParsePublicMultibase(s)
		if err != nil {
			return
		}
		rt, err := ParsePublicMultibase(pk.Multibase())
		if err != nil {
			t.Fatalf("round-trip failed: %v", err)
		}
		if !pk.Equal(rt) {
			t.Fatalf("round-trip key mismatch")
		}
	})
}

// FuzzParsePrivateP256 tests that P-256 private key parsing never panics.
func FuzzParsePrivateP256(f *testing.F) {
	key, _ := GenerateP256()
	ecdh, _ := key.key.ECDH()
	f.Add(ecdh.Bytes())
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xff}, 32))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ParsePrivateP256(data)
	})
}

// FuzzParsePrivateK256 tests that K-256 private key parsing never panics.
func FuzzParsePrivateK256(f *testing.F) {
	key, _ := GenerateK256()
	f.Add(key.key.Bytes())
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xff}, 32))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ParsePrivateK256(data)
	})
}
