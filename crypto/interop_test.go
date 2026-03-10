package crypto

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Interop test vectors from:
// https://github.com/bluesky-social/atproto-interop-tests/tree/main/crypto

type signatureFixture struct {
	Comment            string   `json:"comment"`
	MessageBase64      string   `json:"messageBase64"`
	Algorithm          string   `json:"algorithm"`
	PublicKeyDid       string   `json:"publicKeyDid"`
	PublicKeyMultibase string   `json:"publicKeyMultibase"`
	SignatureBase64    string   `json:"signatureBase64"`
	ValidSignature     bool     `json:"validSignature"`
	Tags               []string `json:"tags"`
}

func TestInterop_SignatureFixtures(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("testdata/signature-fixtures.json")
	require.NoError(t, err)

	var fixtures []signatureFixture
	require.NoError(t, json.Unmarshal(data, &fixtures))

	for _, f := range fixtures {
		t.Run(f.Comment, func(t *testing.T) {
			t.Parallel()

			msg, err := base64.RawStdEncoding.DecodeString(f.MessageBase64)
			require.NoError(t, err)

			sig, err := base64.RawStdEncoding.DecodeString(f.SignatureBase64)
			require.NoError(t, err)

			pub, err := ParsePublicDIDKey(f.PublicKeyDid)
			require.NoError(t, err)

			// Strict verification (rejects high-S and DER).
			err = pub.HashAndVerify(msg, sig)
			if f.ValidSignature {
				assert.NoError(t, err, "expected valid signature")
			} else {
				assert.Error(t, err, "expected invalid signature")
			}

			// DID key round-trip.
			assert.Equal(t, f.PublicKeyDid, pub.DIDKey())
		})
	}
}

type w3cDIDKeyK256 struct {
	PrivateKeyBytesHex string `json:"privateKeyBytesHex"`
	PublicDidKey       string `json:"publicDidKey"`
}

func TestInterop_W3C_DIDKey_K256(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("testdata/w3c_didkey_K256.json")
	require.NoError(t, err)

	var fixtures []w3cDIDKeyK256
	require.NoError(t, json.Unmarshal(data, &fixtures))

	for _, f := range fixtures {
		t.Run(f.PublicDidKey, func(t *testing.T) {
			t.Parallel()

			privBytes, err := hex.DecodeString(f.PrivateKeyBytesHex)
			require.NoError(t, err)

			priv, err := ParsePrivateK256(privBytes)
			require.NoError(t, err)

			assert.Equal(t, f.PublicDidKey, priv.PublicKey().DIDKey())

			// Verify round-trip: parse the did:key back and compare.
			pub, err := ParsePublicDIDKey(f.PublicDidKey)
			require.NoError(t, err)
			assert.Equal(t, f.PublicDidKey, pub.DIDKey())
		})
	}
}

type w3cDIDKeyP256 struct {
	PrivateKeyBytesBase58 string `json:"privateKeyBytesBase58"`
	PublicDidKey          string `json:"publicDidKey"`
}

func TestInterop_W3C_DIDKey_P256(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("testdata/w3c_didkey_P256.json")
	require.NoError(t, err)

	var fixtures []w3cDIDKeyP256
	require.NoError(t, json.Unmarshal(data, &fixtures))

	for _, f := range fixtures {
		t.Run(f.PublicDidKey, func(t *testing.T) {
			t.Parallel()

			privBytes, err := base58.Decode(f.PrivateKeyBytesBase58)
			require.NoError(t, err)

			priv, err := ParsePrivateP256(privBytes)
			require.NoError(t, err)

			assert.Equal(t, f.PublicDidKey, priv.PublicKey().DIDKey())

			// Verify round-trip.
			pub, err := ParsePublicDIDKey(f.PublicDidKey)
			require.NoError(t, err)
			assert.Equal(t, f.PublicDidKey, pub.DIDKey())
		})
	}
}
