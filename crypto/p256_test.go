package crypto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestP256_GenerateSignVerify(t *testing.T) {
	t.Parallel()
	priv, err := GenerateP256()
	require.NoError(t, err)

	content := []byte("hello world")
	sig, err := priv.HashAndSign(content)
	require.NoError(t, err)
	require.Len(t, sig, 64)

	pub := priv.PublicKey()
	require.NoError(t, pub.HashAndVerify(content, sig))
}

func TestP256_VerifyWrongData(t *testing.T) {
	t.Parallel()
	priv, err := GenerateP256()
	require.NoError(t, err)

	sig, err := priv.HashAndSign([]byte("correct"))
	require.NoError(t, err)

	require.Error(t, priv.PublicKey().HashAndVerify([]byte("wrong"), sig))
}

func TestP256_CompressedBytesRoundTrip(t *testing.T) {
	t.Parallel()
	priv, err := GenerateP256()
	require.NoError(t, err)
	pub := priv.PublicKey()

	compressed := pub.Bytes()
	require.Len(t, compressed, 33)

	parsed, err := ParsePublicBytesP256(compressed)
	require.NoError(t, err)
	require.True(t, pub.Equal(parsed))
}

func TestP256_DIDKeyRoundTrip(t *testing.T) {
	t.Parallel()
	priv, err := GenerateP256()
	require.NoError(t, err)
	pub := priv.PublicKey()

	didKey := pub.DIDKey()
	require.Contains(t, didKey, "did:key:z")

	parsed, err := ParsePublicDIDKey(didKey)
	require.NoError(t, err)
	require.True(t, pub.Equal(parsed))
}

func TestP256_LowSEnforcement(t *testing.T) {
	t.Parallel()
	priv, err := GenerateP256()
	require.NoError(t, err)

	// Sign many times — all should produce low-S signatures.
	for range 50 {
		sig, err := priv.HashAndSign([]byte("test low-s"))
		require.NoError(t, err)
		require.NoError(t, priv.PublicKey().HashAndVerify([]byte("test low-s"), sig))
	}
}

func TestP256_PrivateKeyRoundTrip(t *testing.T) {
	t.Parallel()
	priv, err := GenerateP256()
	require.NoError(t, err)

	raw, err := priv.key.Bytes()
	require.NoError(t, err)
	priv2, err := ParsePrivateP256(raw)
	require.NoError(t, err)

	require.True(t, priv.PublicKey().Equal(priv2.PublicKey()))

	// Sign with one, verify with other.
	sig, err := priv.HashAndSign([]byte("cross-verify"))
	require.NoError(t, err)
	require.NoError(t, priv2.PublicKey().HashAndVerify([]byte("cross-verify"), sig))
}
