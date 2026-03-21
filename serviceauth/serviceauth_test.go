package serviceauth

import (
	"context"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testDirectory(did atmos.DID, pub crypto.PublicKey) *identity.Directory {
	return &identity.Directory{
		Resolver: &mockResolver{did: did, pub: pub},
	}
}

type mockResolver struct {
	did atmos.DID
	pub crypto.PublicKey
}

func (m *mockResolver) ResolveDID(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	return &identity.DIDDocument{
		ID: string(m.did),
		VerificationMethod: []identity.VerificationMethod{
			{
				ID:                 string(m.did) + "#atproto",
				Type:               keyType(m.pub),
				Controller:         string(m.did),
				PublicKeyMultibase: m.pub.Multibase(),
			},
		},
		Service: []identity.Service{
			{ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: "https://pds.example.com"},
		},
	}, nil
}

func (m *mockResolver) ResolveHandle(_ context.Context, _ atmos.Handle) (atmos.DID, error) {
	return m.did, nil
}

func keyType(pub crypto.PublicKey) string {
	switch pub.(type) {
	case *crypto.P256PublicKey:
		return "EcdsaSecp256r1VerificationKey2019"
	case *crypto.K256PublicKey:
		return "EcdsaSecp256k1VerificationKey2019"
	default:
		return ""
	}
}

func TestCreateAndVerify_P256(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)

	dir := testDirectory("did:plc:alice", priv.PublicKey())

	token, err := CreateToken(TokenParams{
		Issuer:   "did:plc:alice",
		Audience: "did:web:api.example.com",
		Exp:      time.Now().Add(60 * time.Second),
	}, priv)
	require.NoError(t, err)

	claims, err := VerifyToken(context.Background(), token, VerifyOptions{
		Audience: "did:web:api.example.com",
		Identity: dir,
	})
	require.NoError(t, err)
	assert.Equal(t, atmos.DID("did:plc:alice"), claims.Issuer)
	assert.Equal(t, "did:web:api.example.com", claims.Audience)
	assert.NotEmpty(t, claims.JTI)
	assert.Equal(t, atmos.NSID(""), claims.LexMethod)
}

func TestCreateAndVerify_K256(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateK256()
	require.NoError(t, err)

	dir := testDirectory("did:plc:bob", priv.PublicKey())

	token, err := CreateToken(TokenParams{
		Issuer:   "did:plc:bob",
		Audience: "did:web:relay.example.com",
		Exp:      time.Now().Add(60 * time.Second),
	}, priv)
	require.NoError(t, err)

	claims, err := VerifyToken(context.Background(), token, VerifyOptions{
		Audience: "did:web:relay.example.com",
		Identity: dir,
	})
	require.NoError(t, err)
	assert.Equal(t, atmos.DID("did:plc:bob"), claims.Issuer)
}

func TestVerify_ExpiredToken(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)

	dir := testDirectory("did:plc:alice", priv.PublicKey())

	token, err := CreateToken(TokenParams{
		Issuer:   "did:plc:alice",
		Audience: "did:web:api.example.com",
		Exp:      time.Now().Add(-10 * time.Second), // already expired
	}, priv)
	require.NoError(t, err)

	_, err = VerifyToken(context.Background(), token, VerifyOptions{
		Audience: "did:web:api.example.com",
		Identity: dir,
		Leeway:   gt.Some(1 * time.Second),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expired")
}

func TestVerify_WrongAudience(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)

	dir := testDirectory("did:plc:alice", priv.PublicKey())

	token, err := CreateToken(TokenParams{
		Issuer:   "did:plc:alice",
		Audience: "did:web:api.example.com",
		Exp:      time.Now().Add(60 * time.Second),
	}, priv)
	require.NoError(t, err)

	_, err = VerifyToken(context.Background(), token, VerifyOptions{
		Audience: "did:web:other.example.com",
		Identity: dir,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "aud")
}

func TestVerify_LexMethodBinding(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)

	dir := testDirectory("did:plc:alice", priv.PublicKey())

	token, err := CreateToken(TokenParams{
		Issuer:    "did:plc:alice",
		Audience:  "did:web:api.example.com",
		Exp:       time.Now().Add(60 * time.Second),
		LexMethod: "com.atproto.sync.getBlob",
	}, priv)
	require.NoError(t, err)

	// Matching method — should succeed.
	claims, err := VerifyToken(context.Background(), token, VerifyOptions{
		Audience:  "did:web:api.example.com",
		Identity:  dir,
		LexMethod: "com.atproto.sync.getBlob",
	})
	require.NoError(t, err)
	assert.Equal(t, atmos.NSID("com.atproto.sync.getBlob"), claims.LexMethod)

	// Mismatching method — should fail.
	_, err = VerifyToken(context.Background(), token, VerifyOptions{
		Audience:  "did:web:api.example.com",
		Identity:  dir,
		LexMethod: "com.atproto.repo.getRecord",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lxm mismatch")
}

func TestVerify_LexMethodMissingFromToken(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)

	dir := testDirectory("did:plc:alice", priv.PublicKey())

	// Token without lxm.
	token, err := CreateToken(TokenParams{
		Issuer:   "did:plc:alice",
		Audience: "did:web:api.example.com",
		Exp:      time.Now().Add(60 * time.Second),
	}, priv)
	require.NoError(t, err)

	// Verifier requires lxm — should fail.
	_, err = VerifyToken(context.Background(), token, VerifyOptions{
		Audience:  "did:web:api.example.com",
		Identity:  dir,
		LexMethod: "com.atproto.sync.getBlob",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required lxm")
}

func TestVerify_InvalidSignature(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)

	// Use a different key for the directory (wrong key).
	otherPriv, err := crypto.GenerateP256()
	require.NoError(t, err)

	dir := testDirectory("did:plc:alice", otherPriv.PublicKey())

	token, err := CreateToken(TokenParams{
		Issuer:   "did:plc:alice",
		Audience: "did:web:api.example.com",
		Exp:      time.Now().Add(60 * time.Second),
	}, priv)
	require.NoError(t, err)

	_, err = VerifyToken(context.Background(), token, VerifyOptions{
		Audience: "did:web:api.example.com",
		Identity: dir,
	})
	require.Error(t, err)
}

func TestVerify_TokenTooOld(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)

	dir := testDirectory("did:plc:alice", priv.PublicKey())

	// Create token with iat in the past.
	token, err := CreateToken(TokenParams{
		Issuer:   "did:plc:alice",
		Audience: "did:web:api.example.com",
		Exp:      time.Now().Add(60 * time.Second),
	}, priv)
	require.NoError(t, err)

	// Wait briefly so iat is in the past.
	time.Sleep(10 * time.Millisecond)

	// Very short maxAge — token should be considered too old.
	_, err = VerifyToken(context.Background(), token, VerifyOptions{
		Audience: "did:web:api.example.com",
		Identity: dir,
		MaxAge:   gt.Some(1 * time.Millisecond),
		Leeway:   gt.Some(1 * time.Millisecond),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too old")
}

func TestVerify_GarbageToken(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)

	dir := testDirectory("did:plc:alice", priv.PublicKey())

	_, err = VerifyToken(context.Background(), "not.a.jwt", VerifyOptions{
		Audience: "did:web:api.example.com",
		Identity: dir,
	})
	require.Error(t, err)
}

func TestCreateToken_UniqueJTI(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)

	dir := testDirectory("did:plc:alice", priv.PublicKey())

	params := TokenParams{
		Issuer:   "did:plc:alice",
		Audience: "did:web:api.example.com",
		Exp:      time.Now().Add(60 * time.Second),
	}

	tok1, err := CreateToken(params, priv)
	require.NoError(t, err)
	tok2, err := CreateToken(params, priv)
	require.NoError(t, err)

	c1, err := VerifyToken(context.Background(), tok1, VerifyOptions{
		Audience: "did:web:api.example.com",
		Identity: dir,
	})
	require.NoError(t, err)

	c2, err := VerifyToken(context.Background(), tok2, VerifyOptions{
		Audience: "did:web:api.example.com",
		Identity: dir,
	})
	require.NoError(t, err)

	assert.NotEqual(t, c1.JTI, c2.JTI, "each token should have a unique JTI")
}
