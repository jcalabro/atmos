package credential

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/stretchr/testify/require"
)

const testSpace = atmos.SpaceRef("at://did:plc:abcdefghijklmnopqrstuvwx/space/com.example.board/main")
const testUser = atmos.DID("did:plc:zyxwvutsrqponmlkjihgfedc")

func TestThreeTokenProfilesRoundTrip(t *testing.T) {
	t.Parallel()
	now := time.Unix(2_000_000_000, 0)
	p256, err := crypto.GenerateP256()
	require.NoError(t, err)
	jwk, err := PublicJWK(p256.PublicKey())
	require.NoError(t, err)
	dpopJKT, err := JWKThumbprint(jwk)
	require.NoError(t, err)
	k256, err := crypto.GenerateK256()
	require.NoError(t, err)
	replay, err := NewMemoryReplayStore(8)
	require.NoError(t, err)

	delegation, err := CreateDelegationToken(DelegationTokenParams{
		Issuer: testUser, Subject: testSpace,
		Audience: SpaceHostAudience(testSpace.Authority()), Now: now,
	}, k256)
	require.NoError(t, err)
	unverified, err := ParseDelegationToken(delegation)
	require.NoError(t, err)
	require.Equal(t, DelegationProfile, unverified.Profile)
	verified, err := VerifyDelegationToken(context.Background(), unverified, VerifyDelegationOptions{
		Resolver: keyResolver(testUser, k256.PublicKey(), "#atproto"), Issuer: testUser, Subject: testSpace, Audience: SpaceHostAudience(testSpace.Authority()),
		Now: now.Add(time.Second), Replay: replay,
	})
	require.NoError(t, err)
	require.Equal(t, testSpace.String(), verified.Subject)
	_, err = VerifyDelegationToken(context.Background(), unverified, VerifyDelegationOptions{
		Resolver: keyResolver(testUser, k256.PublicKey(), "#atproto"), Issuer: testUser, Subject: testSpace, Audience: SpaceHostAudience(testSpace.Authority()),
		Now: now.Add(time.Second), Replay: replay,
	})
	require.ErrorIs(t, err, ErrReplay)

	attestation, err := CreateClientAttestationToken(ClientAttestationTokenParams{
		ClientID: "https://app.example/client-metadata.json", Audience: SpaceHostAudience(testSpace.Authority()), Now: now,
		KeyID: "app-key-1",
	}, p256)
	require.NoError(t, err)
	parsedAttestation, err := ParseClientAttestationToken(attestation)
	require.NoError(t, err)
	_, err = VerifyClientAttestationToken(context.Background(), parsedAttestation, VerifyClientAttestationOptions{
		Key: p256.PublicKey(), ClientID: "https://app.example/client-metadata.json", KeyID: "app-key-1",
		Audience: SpaceHostAudience(testSpace.Authority()), Now: now, Replay: replay,
	})
	require.NoError(t, err)

	credentialToken, err := CreateSpaceCredentialToken(SpaceCredentialTokenParams{
		Issuer: testSpace.Authority(), Subject: testSpace, DPoPThumbprint: dpopJKT,
		Now: now, KeyID: "#atproto_space",
	}, p256)
	require.NoError(t, err)
	parsedCredential, err := ParseSpaceCredentialToken(credentialToken)
	require.NoError(t, err)
	credential, err := VerifySpaceCredentialToken(context.Background(), parsedCredential, VerifySpaceCredentialOptions{
		Resolver: keyResolver(testSpace.Authority(), p256.PublicKey(), "#atproto_space"), Subject: testSpace, Now: now.Add(time.Second),
	})
	require.NoError(t, err)
	require.Equal(t, dpopJKT, credential.ConfirmationJKT)
}

func TestTokenStrictParsingAndTimeHardening(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	valid, err := CreateDelegationToken(DelegationTokenParams{
		Issuer: testUser, Subject: testSpace,
		Audience: SpaceHostAudience(testSpace.Authority()), Now: now,
	}, key)
	require.NoError(t, err)
	parts := strings.Split(valid, ".")
	tests := map[string]string{
		"duplicate claim":   fmt.Sprintf("%s.%s.%s", parts[0], b64(`{"iss":"`+testUser.String()+`","iss":"did:plc:other234567890123456789","sub":"`+testSpace.String()+`","aud":"`+SpaceHostAudience(testSpace.Authority())+`","iat":2000000000,"exp":2000000060,"jti":"x"}`), parts[2]),
		"fractional iat":    replacePayload(parts, `{"iss":"`+testUser.String()+`","sub":"`+testSpace.String()+`","aud":"`+SpaceHostAudience(testSpace.Authority())+`","iat":2000000000.5,"exp":2000000060,"jti":"x"}`),
		"array audience":    replacePayload(parts, `{"iss":"`+testUser.String()+`","sub":"`+testSpace.String()+`","aud":["`+SpaceHostAudience(testSpace.Authority())+`"],"iat":2000000000,"exp":2000000060,"jti":"x"}`),
		"wrong typ":         strings.Replace(valid, parts[0], b64(`{"alg":"ES256","typ":"JWT","kid":"#atproto"}`), 1),
		"critical header":   strings.Replace(valid, parts[0], b64(`{"alg":"ES256","typ":"atproto-space-delegation+jwt","kid":"#atproto","crit":["x"]}`), 1),
		"overflow lifetime": replacePayload(parts, `{"iss":"`+testUser.String()+`","sub":"`+testSpace.String()+`","aud":"`+SpaceHostAudience(testSpace.Authority())+`","iat":-9223372036854775807,"exp":9223372036854775807,"jti":"x"}`),
		"present empty cnf": replacePayload(parts, `{"iss":"`+testUser.String()+`","sub":"`+testSpace.String()+`","aud":"`+SpaceHostAudience(testSpace.Authority())+`","iat":2000000000,"exp":2000000060,"jti":"x","cnf":{}}`),
		"present null cnf":  replacePayload(parts, `{"iss":"`+testUser.String()+`","sub":"`+testSpace.String()+`","aud":"`+SpaceHostAudience(testSpace.Authority())+`","iat":2000000000,"exp":2000000060,"jti":"x","cnf":null}`),
		"present null aud":  replacePayload(parts, `{"iss":"`+testUser.String()+`","sub":"`+testSpace.String()+`","aud":null,"iat":2000000000,"exp":2000000060,"jti":"x"}`),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseDelegationToken(raw)
			require.Error(t, err)
		})
	}

	for name, offset := range map[string]time.Duration{
		"too far future":      -6 * time.Second,
		"expired beyond skew": 66 * time.Second,
	} {
		t.Run(name, func(t *testing.T) {
			parsed, parseErr := ParseDelegationToken(valid)
			require.NoError(t, parseErr)
			_, verifyErr := VerifyDelegationToken(context.Background(), parsed, VerifyDelegationOptions{
				Resolver: keyResolver(testUser, key.PublicKey(), "#atproto"), Issuer: testUser, Subject: testSpace, Audience: SpaceHostAudience(testSpace.Authority()),
				Now: now.Add(offset), Replay: mustReplay(t),
			})
			require.Error(t, verifyErr)
		})
	}
}

func TestSpaceCredentialRejectsPresentNullAudience(t *testing.T) {
	t.Parallel()
	// A JSON null aud must be treated as present and rejected, not conflated
	// with the absent aud the credential profile requires.
	header := b64(`{"alg":"ES256","typ":"` + SpaceCredentialTokenType + `","kid":"#atproto"}`)
	jkt := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	signature := base64.RawURLEncoding.EncodeToString(make([]byte, 64))
	withoutAud := `{"iss":"` + testSpace.Authority().String() + `","sub":"` + testSpace.String() + `","iat":2000000000,"exp":2000007200,"jti":"x","cnf":{"jkt":"` + jkt + `"}}`
	_, err := ParseSpaceCredentialToken(header + "." + b64(withoutAud) + "." + signature)
	require.NoError(t, err)
	withNullAud := `{"iss":"` + testSpace.Authority().String() + `","sub":"` + testSpace.String() + `","aud":null,"iat":2000000000,"exp":2000007200,"jti":"x","cnf":{"jkt":"` + jkt + `"}}`
	_, err = ParseSpaceCredentialToken(header + "." + b64(withNullAud) + "." + signature)
	require.Error(t, err)
}

func TestVerifyBeforeReplayConsume(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	other, err := crypto.GenerateP256()
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	raw, err := CreateDelegationToken(DelegationTokenParams{
		Issuer: testUser, Subject: testSpace,
		Audience: SpaceHostAudience(testSpace.Authority()), Now: now,
	}, key)
	require.NoError(t, err)
	parsed, err := ParseDelegationToken(raw)
	require.NoError(t, err)
	store := &failingReplayStore{}
	_, err = VerifyDelegationToken(context.Background(), parsed, VerifyDelegationOptions{
		Resolver: keyResolver(testUser, other.PublicKey(), "#atproto"), Issuer: testUser, Subject: testSpace, Audience: SpaceHostAudience(testSpace.Authority()), Now: now, Replay: store,
	})
	require.Error(t, err)
	require.Zero(t, store.calls.Load())
	_, err = VerifyDelegationToken(context.Background(), parsed, VerifyDelegationOptions{
		Resolver: keyResolver(testUser, key.PublicKey(), "#atproto"), Issuer: testUser, Subject: testSpace, Audience: SpaceHostAudience(testSpace.Authority()), Now: now, Replay: store,
	})
	require.ErrorContains(t, err, "backend unavailable")
	require.EqualValues(t, 1, store.calls.Load())
}

func TestVerificationReparsesUnverifiedToken(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	raw, err := CreateDelegationToken(DelegationTokenParams{
		Issuer: testUser, Subject: testSpace, Audience: SpaceHostAudience(testSpace.Authority()), Now: now,
	}, key)
	require.NoError(t, err)
	parsed, err := ParseDelegationToken(raw)
	require.NoError(t, err)
	// Exported parse-only values are attacker-controlled inspection data. A
	// caller mutation must not change the signed claims used for verification.
	parsed.ExpiresAt = now.Add(24 * time.Hour)
	parsed.Subject = "at://did:plc:abcdefghijklmnopqrstuvwx/space/com.example.board/other"
	_, err = VerifyDelegationToken(context.Background(), parsed, VerifyDelegationOptions{
		Resolver: keyResolver(testUser, key.PublicKey(), "#atproto"), Issuer: testUser,
		Subject: testSpace, Audience: SpaceHostAudience(testSpace.Authority()), Now: now.Add(66 * time.Second), Replay: mustReplay(t),
	})
	require.ErrorContains(t, err, "expired")
}

type staticResolver struct{ doc *identity.DIDDocument }

func (r staticResolver) ResolveDID(context.Context, atmos.DID) (*identity.DIDDocument, error) {
	return r.doc, nil
}
func (staticResolver) ResolveHandle(context.Context, atmos.Handle) (atmos.DID, error) { return "", nil }

func keyResolver(did atmos.DID, key crypto.PublicKey, kid string) staticResolver {
	return staticResolver{doc: &identity.DIDDocument{ID: did.String(), VerificationMethod: []identity.VerificationMethod{{
		ID: kid, Type: "Multikey", Controller: did.String(), PublicKeyMultibase: key.Multibase(),
	}}}}
}

func TestResolveCredentialVerificationKeyStrict(t *testing.T) {
	t.Parallel()
	did := atmos.DID("did:plc:abcdefghijklmnopqrstuvwx")
	account, err := crypto.GenerateK256()
	require.NoError(t, err)
	dedicated, err := crypto.GenerateP256()
	require.NoError(t, err)
	doc := &identity.DIDDocument{ID: string(did), VerificationMethod: []identity.VerificationMethod{
		{ID: "#atproto", Type: "Multikey", Controller: string(did), PublicKeyMultibase: account.PublicKey().Multibase()},
		{ID: string(did) + "#atproto_space", Type: "Multikey", Controller: string(did), PublicKeyMultibase: dedicated.PublicKey().Multibase()},
	}}
	for _, kid := range []string{"#atproto", "#atproto_space"} {
		key, err := ResolveCredentialVerificationKey(context.Background(), staticResolver{doc}, did, kid)
		require.NoError(t, err)
		require.NotNil(t, key)
	}
	_, err = ResolveCredentialVerificationKey(context.Background(), staticResolver{doc}, did, "#other")
	require.Error(t, err)
	doc.VerificationMethod = append(doc.VerificationMethod, doc.VerificationMethod[1])
	_, err = ResolveCredentialVerificationKey(context.Background(), staticResolver{doc}, did, "#atproto_space")
	require.Error(t, err)
}

func TestAlgorithmKeyMismatch(t *testing.T) {
	t.Parallel()
	p, err := crypto.GenerateP256()
	require.NoError(t, err)
	k, err := crypto.GenerateK256()
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	raw, err := CreateDelegationToken(DelegationTokenParams{Issuer: testUser, Subject: testSpace, Audience: SpaceHostAudience(testSpace.Authority()), Now: now}, p)
	require.NoError(t, err)
	parsed, err := ParseDelegationToken(raw)
	require.NoError(t, err)
	_, err = VerifyDelegationToken(context.Background(), parsed, VerifyDelegationOptions{Resolver: keyResolver(testUser, k.PublicKey(), "#atproto"), Issuer: testUser, Subject: testSpace, Audience: SpaceHostAudience(testSpace.Authority()), Now: now, Replay: mustReplay(t)})
	require.Error(t, err)
	_, err = CreateDelegationToken(DelegationTokenParams{}, nil)
	require.Error(t, err)
	_, err = CreateClientAttestationToken(ClientAttestationTokenParams{
		ClientID: "https://app.example/metadata.json", KeyID: "key",
		Audience: SpaceHostAudience(testSpace.Authority()), Now: now,
	}, k)
	require.ErrorContains(t, err, "require ES256")
}

func TestSignatureFailureUsesBoundedForcedRefresh(t *testing.T) {
	t.Parallel()
	stale, err := crypto.GenerateP256()
	require.NoError(t, err)
	current, err := crypto.GenerateP256()
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	raw, err := CreateDelegationToken(DelegationTokenParams{
		Issuer: testUser, Subject: testSpace, Audience: SpaceHostAudience(testSpace.Authority()), Now: now,
	}, current)
	require.NoError(t, err)
	parsed, err := ParseDelegationToken(raw)
	require.NoError(t, err)
	var calls atomic.Int64
	refresh, err := NewKeyRefresh(func(context.Context, atmos.DID) (*identity.DIDDocument, error) {
		calls.Add(1)
		return keyResolver(testUser, current.PublicKey(), "#atproto").doc, nil
	}, 1, time.Minute)
	require.NoError(t, err)
	_, err = VerifyDelegationToken(context.Background(), parsed, VerifyDelegationOptions{
		Resolver: keyResolver(testUser, stale.PublicKey(), "#atproto"), Refresh: refresh,
		Issuer: testUser, Subject: testSpace, Audience: SpaceHostAudience(testSpace.Authority()), Now: now, Replay: mustReplay(t),
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, calls.Load())

	parsedAgain, err := ParseDelegationToken(raw)
	require.NoError(t, err)
	_, err = VerifyDelegationToken(context.Background(), parsedAgain, VerifyDelegationOptions{
		Resolver: keyResolver(testUser, stale.PublicKey(), "#atproto"), Refresh: refresh,
		Issuer: testUser, Subject: testSpace, Audience: SpaceHostAudience(testSpace.Authority()), Now: now, Replay: mustReplay(t),
	})
	require.ErrorIs(t, err, ErrRefreshRateLimited)
	require.EqualValues(t, 1, calls.Load())
}

func TestKeyRefreshCoalescesAndBoundsDIDs(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	refresh, err := NewKeyRefresh(func(context.Context, atmos.DID) (*identity.DIDDocument, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return keyResolver(testUser, key.PublicKey(), "#atproto").doc, nil
	}, 1, time.Minute)
	require.NoError(t, err)

	const workers = 16
	start := make(chan struct{})
	results := make(chan error, workers)
	var ready sync.WaitGroup
	ready.Add(workers)
	for range workers {
		go func() {
			ready.Done()
			<-start
			_, resolveErr := refresh.ResolveKey(context.Background(), testUser, "#atproto")
			results <- resolveErr
		}()
	}
	ready.Wait()
	close(start)
	<-entered
	// Keep the underlying resolution in flight while all workers enter the
	// singleflight group; this is synchronization, not a production timeout.
	time.Sleep(10 * time.Millisecond)
	close(release)
	for range workers {
		require.NoError(t, <-results)
	}
	require.EqualValues(t, 1, calls.Load())

	otherDID := atmos.DID("did:plc:0123456789abcdefghijklmn")
	_, err = refresh.ResolveKey(context.Background(), otherDID, "#atproto")
	require.ErrorIs(t, err, ErrRefreshRateLimited)
}

func TestCredentialIssuanceKeyFallbackRules(t *testing.T) {
	t.Parallel()
	did := testSpace.Authority()
	account, err := crypto.GenerateK256()
	require.NoError(t, err)
	dedicated, err := crypto.GenerateP256()
	require.NoError(t, err)
	accountMethod := identity.VerificationMethod{ID: "#atproto", Type: "Multikey", Controller: did.String(), PublicKeyMultibase: account.PublicKey().Multibase()}
	dedicatedMethod := identity.VerificationMethod{ID: "#atproto_space", Type: "Multikey", Controller: did.String(), PublicKeyMultibase: dedicated.PublicKey().Multibase()}

	key, kid, err := ResolveCredentialIssuanceKey(context.Background(), staticResolver{&identity.DIDDocument{ID: did.String(), VerificationMethod: []identity.VerificationMethod{accountMethod}}}, did)
	require.NoError(t, err)
	require.Equal(t, "#atproto", kid)
	require.True(t, key.Equal(account.PublicKey()))

	key, kid, err = ResolveCredentialIssuanceKey(context.Background(), staticResolver{&identity.DIDDocument{ID: did.String(), VerificationMethod: []identity.VerificationMethod{accountMethod, dedicatedMethod}}}, did)
	require.NoError(t, err)
	require.Equal(t, "#atproto_space", kid)
	require.True(t, key.Equal(dedicated.PublicKey()))

	dedicatedMethod.Controller = testUser.String()
	_, _, err = ResolveCredentialIssuanceKey(context.Background(), staticResolver{&identity.DIDDocument{ID: did.String(), VerificationMethod: []identity.VerificationMethod{accountMethod, dedicatedMethod}}}, did)
	require.Error(t, err, "a malformed dedicated key must not fall back")
}

func mustReplay(t *testing.T) ReplayStore {
	t.Helper()
	s, err := NewMemoryReplayStore(4)
	require.NoError(t, err)
	return s
}

func b64(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }

func replacePayload(parts []string, payload string) string {
	return parts[0] + "." + b64(payload) + "." + parts[2]
}
