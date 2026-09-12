package crypto

import (
	"testing"

	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
)

func TestK256_GenerateSignVerify(t *testing.T) {
	t.Parallel()
	priv, err := GenerateK256()
	require.NoError(t, err)

	content := []byte("hello world")
	sig, err := priv.HashAndSign(content)
	require.NoError(t, err)
	require.Len(t, sig, 64)

	pub := priv.PublicKey()
	require.NoError(t, pub.HashAndVerify(content, sig))
}

func TestK256_VerifyWrongData(t *testing.T) {
	t.Parallel()
	priv, err := GenerateK256()
	require.NoError(t, err)

	sig, err := priv.HashAndSign([]byte("correct"))
	require.NoError(t, err)

	require.Error(t, priv.PublicKey().HashAndVerify([]byte("wrong"), sig))
}

func TestK256_CompressedBytesRoundTrip(t *testing.T) {
	t.Parallel()
	priv, err := GenerateK256()
	require.NoError(t, err)
	pub := priv.PublicKey()

	compressed := pub.Bytes()
	require.Len(t, compressed, 33)

	parsed, err := ParsePublicBytesK256(compressed)
	require.NoError(t, err)
	require.True(t, pub.Equal(parsed))
}

func TestK256_LegacyPublicMultibaseRoundTrip(t *testing.T) {
	t.Parallel()
	privateKey, err := GenerateK256()
	require.NoError(t, err)
	publicKey, ok := privateKey.PublicKey().(*K256PublicKey)
	require.True(t, ok)

	for name, raw := range map[string][]byte{
		"compressed":   publicKey.Bytes(),
		"uncompressed": publicKey.key.Point().UncompressedBytes(),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			parsed, err := ParseLegacyPublicMultibaseK256("z" + base58.Encode(raw))
			require.NoError(t, err)
			require.True(t, publicKey.Equal(parsed))
		})
	}

	for name, encoded := range map[string]string{
		"empty":                "",
		"wrong multibase":      "f00",
		"empty payload":        "z",
		"invalid compressed":   "z" + base58.Encode(make([]byte, 33)),
		"invalid uncompressed": "z" + base58.Encode(make([]byte, 65)),
		"multicodec prefixed":  publicKey.Multibase(),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseLegacyPublicMultibaseK256(encoded)
			require.Error(t, err)
		})
	}
}

func TestK256_DIDKeyRoundTrip(t *testing.T) {
	t.Parallel()
	priv, err := GenerateK256()
	require.NoError(t, err)
	pub := priv.PublicKey()

	didKey := pub.DIDKey()
	require.Contains(t, didKey, "did:key:z")

	parsed, err := ParsePublicDIDKey(didKey)
	require.NoError(t, err)
	require.True(t, pub.Equal(parsed))
}

func TestK256_LowSEnforcement(t *testing.T) {
	t.Parallel()
	priv, err := GenerateK256()
	require.NoError(t, err)

	for range 50 {
		sig, err := priv.HashAndSign([]byte("test low-s"))
		require.NoError(t, err)
		require.NoError(t, priv.PublicKey().HashAndVerify([]byte("test low-s"), sig))
	}
}

func TestK256_PrivateKeyRoundTrip(t *testing.T) {
	t.Parallel()
	priv, err := GenerateK256()
	require.NoError(t, err)

	raw := priv.key.Bytes()
	priv2, err := ParsePrivateK256(raw)
	require.NoError(t, err)

	require.True(t, priv.PublicKey().Equal(priv2.PublicKey()))

	sig, err := priv.HashAndSign([]byte("cross-verify"))
	require.NoError(t, err)
	require.NoError(t, priv2.PublicKey().HashAndVerify([]byte("cross-verify"), sig))
}
