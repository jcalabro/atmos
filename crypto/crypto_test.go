package crypto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCrossCurve_P256CannotVerifyK256(t *testing.T) {
	t.Parallel()
	p256Priv, err := GenerateP256()
	require.NoError(t, err)
	k256Priv, err := GenerateK256()
	require.NoError(t, err)

	sig, err := p256Priv.HashAndSign([]byte("test"))
	require.NoError(t, err)

	// K256 key should not verify P256 signature.
	require.Error(t, k256Priv.PublicKey().HashAndVerify([]byte("test"), sig))
}

func TestCrossCurve_K256CannotVerifyP256(t *testing.T) {
	t.Parallel()
	p256Priv, err := GenerateP256()
	require.NoError(t, err)
	k256Priv, err := GenerateK256()
	require.NoError(t, err)

	sig, err := k256Priv.HashAndSign([]byte("test"))
	require.NoError(t, err)

	require.Error(t, p256Priv.PublicKey().HashAndVerify([]byte("test"), sig))
}

func TestCrossCurve_EqualReturnsFalse(t *testing.T) {
	t.Parallel()
	p256Priv, err := GenerateP256()
	require.NoError(t, err)
	k256Priv, err := GenerateK256()
	require.NoError(t, err)

	require.False(t, p256Priv.PublicKey().Equal(k256Priv.PublicKey()))
	require.False(t, k256Priv.PublicKey().Equal(p256Priv.PublicKey()))
}

func TestDIDKey_DetectsCurve(t *testing.T) {
	t.Parallel()
	p256Priv, err := GenerateP256()
	require.NoError(t, err)
	k256Priv, err := GenerateK256()
	require.NoError(t, err)

	p256Key := p256Priv.PublicKey().DIDKey()
	k256Key := k256Priv.PublicKey().DIDKey()

	// Parse should return the correct type.
	parsedP256, err := ParsePublicDIDKey(p256Key)
	require.NoError(t, err)
	_, ok := parsedP256.(*P256PublicKey)
	require.True(t, ok)

	parsedK256, err := ParsePublicDIDKey(k256Key)
	require.NoError(t, err)
	_, ok = parsedK256.(*K256PublicKey)
	require.True(t, ok)
}

func TestDIDKey_InvalidPrefix(t *testing.T) {
	t.Parallel()
	_, err := ParsePublicDIDKey("not:a:did:key")
	require.Error(t, err)
}

func TestMultibase_InvalidPrefix(t *testing.T) {
	t.Parallel()
	_, err := ParsePublicMultibase("Bnot-z-prefix")
	require.Error(t, err)
}

func TestLenientVerify_AcceptsHighS(t *testing.T) {
	t.Parallel()
	priv, err := GenerateP256()
	require.NoError(t, err)

	sig, err := priv.HashAndSign([]byte("test"))
	require.NoError(t, err)

	// Flip to high-S.
	r, s, err := decodeCompactSig(sig)
	require.NoError(t, err)
	highS := normalizeLowS(p256Order, p256HalfOrder, s)
	// If it's already low-S, make it high-S.
	if isLowS(p256HalfOrder, highS) {
		highS.Sub(p256Order, highS)
	}
	highSSig := encodeCompactSig(r, highS)

	// Strict should reject.
	require.Error(t, priv.PublicKey().HashAndVerify([]byte("test"), highSSig))
	// Lenient should accept.
	require.NoError(t, priv.PublicKey().HashAndVerifyLenient([]byte("test"), highSSig))
}

func TestCompactSig_InvalidLength(t *testing.T) {
	t.Parallel()
	_, _, err := decodeCompactSig([]byte{0x01, 0x02})
	require.Error(t, err)
	require.Contains(t, err.Error(), "64 bytes")
}
