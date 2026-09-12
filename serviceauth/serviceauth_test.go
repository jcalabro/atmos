package serviceauth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/gt"
	"github.com/mr-tron/base58"
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
				PublicKeyMultibase: legacyMultibase(m.pub),
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

// legacyMultibase encodes a key the way EcdsaSecp256{k1,r1}VerificationKey2019
// DID entries carry it on the wire: raw SEC1 bytes, no multicodec prefix.
func legacyMultibase(pub crypto.PublicKey) string {
	return "z" + base58.Encode(pub.Bytes())
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

// fragmentResolver serves a DID document whose signing key lives under a
// non-default verification-method fragment (e.g. #atproto_labeler).
type fragmentResolver struct {
	did      atmos.DID
	fragment string
	pub      crypto.PublicKey
}

func (m *fragmentResolver) ResolveDID(_ context.Context, _ atmos.DID) (*identity.DIDDocument, error) {
	return &identity.DIDDocument{
		ID: string(m.did),
		VerificationMethod: []identity.VerificationMethod{{
			ID:                 string(m.did) + "#" + m.fragment,
			Type:               keyType(m.pub),
			Controller:         string(m.did),
			PublicKeyMultibase: legacyMultibase(m.pub),
		}},
	}, nil
}

func (m *fragmentResolver) ResolveHandle(_ context.Context, _ atmos.Handle) (atmos.DID, error) {
	return m.did, nil
}

// TestVerify_IssuerWithFragment asserts a service-auth token whose iss carries a
// verification-method fragment (e.g. did:plc:x#atproto_labeler) verifies against
// the keyed method — matching the TS reference, which accepts DidString#fragment.
func TestVerify_IssuerWithFragment(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateK256()
	require.NoError(t, err)

	dir := &identity.Directory{
		Resolver: &fragmentResolver{did: "did:plc:labeler", fragment: "atproto_labeler", pub: priv.PublicKey()},
	}

	token, err := CreateToken(TokenParams{
		Issuer:   "did:plc:labeler#atproto_labeler",
		Audience: "did:web:ozone.example.com",
		Exp:      time.Now().Add(60 * time.Second),
	}, priv)
	require.NoError(t, err)

	claims, err := VerifyToken(context.Background(), token, VerifyOptions{
		Audience: "did:web:ozone.example.com",
		Identity: dir,
	})
	require.NoError(t, err)
	assert.Equal(t, atmos.DID("did:plc:labeler#atproto_labeler"), claims.Issuer)
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

func TestCreateTokenUsesScalarAudience(t *testing.T) {
	t.Parallel()
	privateKey, err := crypto.GenerateP256()
	require.NoError(t, err)
	token, err := CreateToken(TokenParams{
		Issuer: "did:plc:alice", Audience: "did:web:api.example.com",
		Exp: time.Now().Add(time.Minute), LexMethod: "com.atproto.space.notifyWrite",
	}, privateKey)
	require.NoError(t, err)
	parts := strings.Split(token, ".")
	require.Len(t, parts, 3)
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)
	var claims map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &claims))
	var audience string
	require.NoError(t, json.Unmarshal(claims["aud"], &audience))
	require.Equal(t, "did:web:api.example.com", audience)
}

func TestCreateToken_UsesExplicitIssuanceTime(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Second)
	token, err := CreateToken(TokenParams{
		Issuer: "did:plc:alice", Audience: "did:web:api.example.com",
		IssuedAt: now, Exp: now.Add(time.Minute),
	}, priv)
	require.NoError(t, err)
	claims, err := VerifyToken(t.Context(), token, VerifyOptions{
		Audience: "did:web:api.example.com", Identity: testDirectory("did:plc:alice", priv.PublicKey()),
	})
	require.NoError(t, err)
	assert.True(t, now.Equal(claims.IssuedAt))
	assert.True(t, now.Add(time.Minute).Equal(claims.ExpiresAt))
}

func TestVerifyToken_RejectsUnsafeHeadersAndLifetimes(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)
	dir := testDirectory("did:plc:alice", priv.PublicKey())
	now := time.Now()

	sign := func(typ string, exp *jwt.NumericDate) string {
		c := claims{
			RegisteredClaims: jwt.RegisteredClaims{
				Issuer:    "did:plc:alice",
				Audience:  jwt.ClaimStrings{"did:web:api.example.com"},
				IssuedAt:  jwt.NewNumericDate(now),
				ExpiresAt: exp,
				ID:        "test-jti",
			},
		}
		token := jwt.NewWithClaims(sigES256, c)
		if typ != "" {
			token.Header["typ"] = typ
		}
		signed, err := token.SignedString(priv)
		require.NoError(t, err)
		return signed
	}

	tests := []struct {
		name  string
		token string
		exp   *jwt.NumericDate
	}{
		{"missing exp", sign("", nil), nil},
		{"far future exp", sign("", jwt.NewNumericDate(now.Add(10*time.Minute))), nil},
		{"oauth access token typ", sign("at+jwt", jwt.NewNumericDate(now.Add(time.Minute))), nil},
		{"oauth refresh token typ", sign("refresh+jwt", jwt.NewNumericDate(now.Add(time.Minute))), nil},
		{"dpop proof typ", sign("dpop+jwt", jwt.NewNumericDate(now.Add(time.Minute))), nil},
		{"case-varied oauth typ", sign("AT+JWT", jwt.NewNumericDate(now.Add(time.Minute))), nil},
		{"media-type-prefixed typ", sign("application/at+jwt", jwt.NewNumericDate(now.Add(time.Minute))), nil},
		{"unknown typ", sign("secevent+jwt", jwt.NewNumericDate(now.Add(time.Minute))), nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := VerifyToken(context.Background(), tt.token, VerifyOptions{
				Audience: "did:web:api.example.com",
				Identity: dir,
			})
			require.Error(t, err)
		})
	}
}

func TestVerifyToken_AcceptsCaseVariantJWTTyp(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)
	dir := testDirectory("did:plc:alice", priv.PublicKey())
	now := time.Now()

	// RFC 7515 §4.1.9: typ comparison is case-insensitive, and a value
	// without '/' is equivalent to the same value with "application/"
	// prepended — compliant senders may emit any of these.
	for _, typ := range []string{"jwt", "JWT", "application/jwt", "application/JWT"} {
		c := claims{
			RegisteredClaims: jwt.RegisteredClaims{
				Issuer:    "did:plc:alice",
				Audience:  jwt.ClaimStrings{"did:web:api.example.com"},
				IssuedAt:  jwt.NewNumericDate(now),
				ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute)),
				ID:        "test-jti-" + typ,
			},
		}
		token := jwt.NewWithClaims(sigES256, c)
		token.Header["typ"] = typ
		signed, err := token.SignedString(priv)
		require.NoError(t, err)

		_, err = VerifyToken(context.Background(), signed, VerifyOptions{
			Audience: "did:web:api.example.com",
			Identity: dir,
		})
		require.NoError(t, err, "typ %q must be accepted", typ)
	}
}

func TestVerifyToken_RejectsMalformedHeadersAndClaims(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)
	now := time.Now()
	dir := testDirectory("did:plc:alice", priv.PublicKey())

	makeToken := func(header func(*jwt.Token), tokenClaims claims) string {
		token := jwt.NewWithClaims(sigES256, tokenClaims)
		if header != nil {
			header(token)
		}
		signed, err := token.SignedString(priv)
		require.NoError(t, err)
		return signed
	}

	validClaims := claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "did:plc:alice",
			Audience:  jwt.ClaimStrings{"did:web:api.example.com"},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute)),
			ID:        "test-jti",
		},
	}
	noJTI := validClaims
	noJTI.ID = ""
	multipleFragments := validClaims
	multipleFragments.Issuer = "did:plc:alice#atproto#other"

	tests := []struct {
		name  string
		token string
	}{
		{"non-string typ", makeToken(func(token *jwt.Token) { token.Header["typ"] = []string{"JWT"} }, validClaims)},
		{"missing jti", makeToken(nil, noJTI)},
		{"multiple issuer fragments", makeToken(nil, multipleFragments)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := VerifyToken(context.Background(), tt.token, VerifyOptions{
				Audience: "did:web:api.example.com",
				Identity: dir,
			})
			require.Error(t, err)
		})
	}
}

func TestVerifyToken_RejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)
	token, err := CreateToken(TokenParams{
		Issuer:   "did:plc:alice",
		Audience: "did:web:api.example.com",
		Exp:      time.Now().Add(time.Minute),
	}, priv)
	require.NoError(t, err)

	_, err = VerifyToken(context.Background(), token, VerifyOptions{
		Audience: "did:web:api.example.com",
	})
	require.ErrorContains(t, err, "identity")

	_, err = VerifyToken(context.Background(), token, VerifyOptions{
		Audience: "",
		Identity: testDirectory("did:plc:alice", priv.PublicKey()),
	})
	require.ErrorContains(t, err, "audience")
}

func TestVerifyToken_RejectsReplayedJTI(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)
	token, err := CreateToken(TokenParams{
		Issuer:   "did:plc:alice",
		Audience: "did:web:api.example.com",
		Exp:      time.Now().Add(time.Minute),
	}, priv)
	require.NoError(t, err)

	seen := false
	opts := VerifyOptions{
		Audience: "did:web:api.example.com",
		Identity: testDirectory("did:plc:alice", priv.PublicKey()),
		UniqueJTI: func(string) bool {
			unique := !seen
			seen = true
			return unique
		},
	}

	_, err = VerifyToken(context.Background(), token, opts)
	require.NoError(t, err)
	_, err = VerifyToken(context.Background(), token, opts)
	require.ErrorContains(t, err, "replay")
}

func TestCreateToken_RejectsInvalidParameters(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateP256()
	require.NoError(t, err)

	tests := []TokenParams{
		{Issuer: "not-a-did", Audience: "did:web:api.example.com", Exp: time.Now().Add(time.Minute)},
		{Issuer: "did:plc:alice", Audience: "", Exp: time.Now().Add(time.Minute)},
		{Issuer: "did:plc:alice", Audience: "did:web:api.example.com#"},
		{Issuer: "did:plc:alice", Audience: "did:web:api.example.com", Exp: time.Time{}},
		{Issuer: "did:plc:alice", Audience: "did:web:api.example.com", Exp: time.Now().Add(time.Minute), LexMethod: "invalid NSID"},
	}

	for _, params := range tests {
		_, err := CreateToken(params, priv)
		require.Error(t, err)
	}
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
