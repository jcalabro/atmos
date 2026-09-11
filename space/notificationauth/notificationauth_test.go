package notificationauth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/space/credential"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAuthority  = atmos.DID("did:plc:abcdefghijklmnopqrstuvwx")
	testRepo       = atmos.DID("did:plc:zyxwvutsrqponmlkjihgfedc")
	testOtherRepo  = atmos.DID("did:plc:bcdefghijklmnopqrstuvwxy")
	testSubscriber = "did:web:sync.example.com#atproto_space_syncer"
	testSpace      = atmos.SpaceRef("at://did:plc:abcdefghijklmnopqrstuvwx/space/com.example.board/main")
)

type mapResolver struct {
	keys map[atmos.DID]crypto.PublicKey
}

type rawResolver struct {
	doc *identity.DIDDocument
	err error
}

func (r rawResolver) ResolveDID(context.Context, atmos.DID) (*identity.DIDDocument, error) {
	return r.doc, r.err
}

func (rawResolver) ResolveHandle(context.Context, atmos.Handle) (atmos.DID, error) {
	return "", errors.New("not implemented")
}

func (r *mapResolver) ResolveDID(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	key, ok := r.keys[did]
	if !ok {
		return nil, fmt.Errorf("unknown DID %q", did)
	}
	return &identity.DIDDocument{
		ID: string(did),
		VerificationMethod: []identity.VerificationMethod{{
			ID:                 string(did) + "#atproto",
			Type:               "Multikey",
			Controller:         string(did),
			PublicKeyMultibase: key.Multibase(),
		}},
	}, nil
}

func (r *mapResolver) ResolveHandle(context.Context, atmos.Handle) (atmos.DID, error) {
	return "", errors.New("not implemented")
}

type recordingReplayStore struct {
	err   error
	calls atomic.Int64
	mu    sync.Mutex
	args  []replayCall
}

type replayCall struct {
	namespace credential.ReplayNamespace
	id        string
	expiresAt time.Time
}

func (s *recordingReplayStore) Consume(_ context.Context, namespace credential.ReplayNamespace, id string, expiresAt time.Time) error {
	s.calls.Add(1)
	s.mu.Lock()
	s.args = append(s.args, replayCall{namespace: namespace, id: id, expiresAt: expiresAt})
	s.mu.Unlock()
	return s.err
}

type fixture struct {
	authorityKey crypto.PrivateKey
	repoKey      crypto.PrivateKey
	resolver     identity.Resolver
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	authorityKey, err := crypto.GenerateK256()
	require.NoError(t, err)
	repoKey, err := crypto.GenerateP256()
	require.NoError(t, err)
	return fixture{
		authorityKey: authorityKey,
		repoKey:      repoKey,
		resolver: &mapResolver{keys: map[atmos.DID]crypto.PublicKey{
			testAuthority: authorityKey.PublicKey(),
			testRepo:      repoKey.PublicKey(),
		}},
	}
}

func TestWriterNotifyWriteRoundTripAndReplay(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	replay, err := credential.NewMemoryReplayStore(8)
	require.NoError(t, err)
	token, err := CreateWriterNotifyWriteToken(testRepo, testAuthority, time.Now().Add(time.Minute), f.repoKey)
	require.NoError(t, err)

	claims, err := VerifyWriterNotifyWrite(context.Background(), token, testSpace, testRepo, WriterOptions{
		Options:   Options{Resolver: f.resolver, Replay: replay},
		Authority: testAuthority,
	})
	require.NoError(t, err)
	assert.Equal(t, testRepo, claims.Issuer)
	assert.Equal(t, testSpace, claims.Space)
	assert.Equal(t, testRepo, claims.Repo)
	assert.Equal(t, NotifyWriteMethod, claims.LexMethod)

	_, err = VerifyWriterNotifyWrite(context.Background(), token, testSpace, testRepo, WriterOptions{
		Options:   Options{Resolver: f.resolver, Replay: replay},
		Authority: testAuthority,
	})
	require.ErrorIs(t, err, credential.ErrReplay)
}

func TestSubscriberNotificationProfiles(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	tests := []struct {
		name   string
		create func() (string, error)
		verify func(string, ReplayStore) (*Claims, error)
		method atmos.NSID
		repo   atmos.DID
	}{
		{
			name: "write",
			create: func() (string, error) {
				return CreateAuthorityNotifyWriteToken(testAuthority, testSubscriber, time.Now().Add(time.Minute), f.authorityKey)
			},
			verify: func(token string, replay ReplayStore) (*Claims, error) {
				return VerifySubscriberNotifyWrite(context.Background(), token, testSpace, testRepo, SubscriberOptions{
					Options: Options{Resolver: f.resolver, Replay: replay}, Subscriber: testSubscriber,
				})
			},
			method: NotifyWriteMethod,
			repo:   testRepo,
		},
		{
			name: "space deleted",
			create: func() (string, error) {
				return CreateAuthorityNotifySpaceDeletedToken(testAuthority, testSubscriber, time.Now().Add(time.Minute), f.authorityKey)
			},
			verify: func(token string, replay ReplayStore) (*Claims, error) {
				return VerifySubscriberNotifySpaceDeleted(context.Background(), token, testSpace, SubscriberOptions{
					Options: Options{Resolver: f.resolver, Replay: replay}, Subscriber: testSubscriber,
				})
			},
			method: NotifySpaceDeletedMethod,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			replay := &recordingReplayStore{}
			token, err := tt.create()
			require.NoError(t, err)
			claims, err := tt.verify(token, replay)
			require.NoError(t, err)
			assert.Equal(t, testAuthority, claims.Issuer)
			assert.Equal(t, testSubscriber, claims.Audience)
			assert.Equal(t, testSpace, claims.Space)
			assert.Equal(t, tt.repo, claims.Repo)
			assert.Equal(t, tt.method, claims.LexMethod)
			require.EqualValues(t, 1, replay.calls.Load())
			require.Len(t, replay.args, 1)
			assert.Equal(t, credential.ReplayServiceAuth, replay.args[0].namespace)
			assert.Equal(t, claims.JTI, replay.args[0].id)
			assert.Equal(t, claims.ExpiresAt.Add(DefaultMaxAge+DefaultClockSkew), replay.args[0].expiresAt)
		})
	}
}

func TestRoleAndPayloadMismatchesDoNotConsumeReplay(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	validWriter, err := CreateWriterNotifyWriteToken(testRepo, testAuthority, time.Now().Add(time.Minute), f.repoKey)
	require.NoError(t, err)
	wrongIssuer, err := CreateWriterNotifyWriteToken(testOtherRepo, testAuthority, time.Now().Add(time.Minute), f.repoKey)
	require.NoError(t, err)
	wrongAudience, err := CreateWriterNotifyWriteToken(testRepo, testOtherRepo, time.Now().Add(time.Minute), f.repoKey)
	require.NoError(t, err)
	wrongMethod, err := signServiceClaims(f.repoKey, testRepo, []string{string(testAuthority)}, "com.example.other", time.Now(), time.Now().Add(time.Minute), "wrong-method")
	require.NoError(t, err)

	tests := []struct {
		name      string
		token     string
		space     atmos.SpaceRef
		repo      atmos.DID
		authority atmos.DID
	}{
		{"invalid space", validWriter, "not-a-space", testRepo, testAuthority},
		{"wrong hosted authority", validWriter, testSpace, testRepo, testOtherRepo},
		{"invalid repo", validWriter, testSpace, "not-a-did", testAuthority},
		{"issuer does not equal repo", wrongIssuer, testSpace, testRepo, testAuthority},
		{"audience does not equal authority", wrongAudience, testSpace, testRepo, testAuthority},
		{"wrong method", wrongMethod, testSpace, testRepo, testAuthority},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			replay := &recordingReplayStore{}
			_, err := VerifyWriterNotifyWrite(context.Background(), tt.token, tt.space, tt.repo, WriterOptions{
				Options: Options{Resolver: f.resolver, Replay: replay}, Authority: tt.authority,
			})
			require.Error(t, err)
			assert.Zero(t, replay.calls.Load())
		})
	}
}

func TestSubscriberRejectsWrongRoleSpaceAudienceAndRepo(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	valid, err := CreateAuthorityNotifyWriteToken(testAuthority, testSubscriber, time.Now().Add(time.Minute), f.authorityKey)
	require.NoError(t, err)
	writerToken, err := CreateWriterNotifyWriteToken(testRepo, testAuthority, time.Now().Add(time.Minute), f.repoKey)
	require.NoError(t, err)
	otherSpace := atmos.SpaceRef("at://did:plc:bcdefghijklmnopqrstuvwxy/space/com.example.board/main")
	tests := []struct {
		name       string
		token      string
		space      atmos.SpaceRef
		repo       atmos.DID
		subscriber string
	}{
		{"writer token cannot impersonate authority", writerToken, testSpace, testRepo, testSubscriber},
		{"space authority binds issuer", valid, otherSpace, testRepo, testSubscriber},
		{"subscriber audience is exact", valid, testSpace, testRepo, "did:web:other.example.com"},
		{"invalid subscriber", valid, testSpace, testRepo, "https://sync.example.com"},
		{"invalid repo", valid, testSpace, "bad", testSubscriber},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			replay := &recordingReplayStore{}
			_, err := VerifySubscriberNotifyWrite(context.Background(), tt.token, tt.space, tt.repo, SubscriberOptions{
				Options: Options{Resolver: f.resolver, Replay: replay}, Subscriber: tt.subscriber,
			})
			require.Error(t, err)
			assert.Zero(t, replay.calls.Load())
		})
	}
}

func TestStrictAudienceShapeAndIssuerFragment(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	now := time.Now()
	tests := []struct {
		name     string
		issuer   atmos.DID
		audience []string
	}{
		{"multiple audiences", testRepo, []string{string(testAuthority), "did:web:also.example.com"}},
		{"issuer fragment", atmos.DID(string(testRepo) + "#atproto"), []string{string(testAuthority)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			token, err := signServiceClaims(f.repoKey, tt.issuer, tt.audience, string(NotifyWriteMethod), now, now.Add(time.Minute), tt.name)
			require.NoError(t, err)
			replay := &recordingReplayStore{}
			_, err = VerifyWriterNotifyWrite(context.Background(), token, testSpace, testRepo, WriterOptions{
				Options: Options{Resolver: f.resolver, Replay: replay}, Authority: testAuthority,
			})
			require.Error(t, err)
			assert.Zero(t, replay.calls.Load())
		})
	}
}

func TestDuplicateJWTClaimsRejectedBeforeReplay(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	now := time.Now()
	payload := fmt.Sprintf(
		`{"iss":%q,"aud":%q,"aud":%q,"lxm":%q,"iat":%d,"exp":%d,"jti":"duplicate-aud"}`,
		testRepo, testAuthority, testAuthority, NotifyWriteMethod, now.Unix(), now.Add(time.Minute).Unix(),
	)
	token, err := signServicePayload(f.repoKey, []byte(payload))
	require.NoError(t, err)
	replay := &recordingReplayStore{}
	_, err = VerifyWriterNotifyWrite(context.Background(), token, testSpace, testRepo, WriterOptions{
		Options: Options{Resolver: f.resolver, Replay: replay}, Authority: testAuthority,
	})
	require.ErrorContains(t, err, "duplicate")
	assert.Zero(t, replay.calls.Load())
}

func TestServiceJWTEnvelopeBoundsAndTrailingData(t *testing.T) {
	t.Parallel()
	fx := newFixture(t)
	replay := &recordingReplayStore{}
	now := time.Now()

	t.Run("token bytes", func(t *testing.T) {
		_, err := VerifyWriterNotifyWrite(context.Background(), strings.Repeat("a", maxTokenBytes+1), testSpace, testRepo, WriterOptions{
			Options: Options{Resolver: fx.resolver, Replay: replay}, Authority: testAuthority,
		})
		require.ErrorContains(t, err, "1..16384 bytes")
	})

	t.Run("jti bytes", func(t *testing.T) {
		payload := []byte(fmt.Sprintf(
			`{"iss":%q,"aud":%q,"lxm":%q,"iat":%d,"exp":%d,"jti":%q}`,
			testRepo, testAuthority, NotifyWriteMethod, now.Unix(), now.Add(time.Minute).Unix(), strings.Repeat("j", maxJTIBytes+1),
		))
		token, err := signServicePayload(fx.repoKey, payload)
		require.NoError(t, err)
		_, err = VerifyWriterNotifyWrite(context.Background(), token, testSpace, testRepo, WriterOptions{
			Options: Options{Resolver: fx.resolver, Replay: replay}, Authority: testAuthority,
		})
		require.ErrorContains(t, err, "jti must contain")
	})

	t.Run("trailing value", func(t *testing.T) {
		payload := []byte(fmt.Sprintf(
			`{"iss":%q,"aud":%q,"lxm":%q,"iat":%d,"exp":%d,"jti":"trailing"}{}`,
			testRepo, testAuthority, NotifyWriteMethod, now.Unix(), now.Add(time.Minute).Unix(),
		))
		token, err := signServicePayload(fx.repoKey, payload)
		require.NoError(t, err)
		_, err = VerifyWriterNotifyWrite(context.Background(), token, testSpace, testRepo, WriterOptions{
			Options: Options{Resolver: fx.resolver, Replay: replay}, Authority: testAuthority,
		})
		require.ErrorContains(t, err, "unexpected data")
	})
}

func TestJWTAlgorithmMustMatchSelectedKey(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	now := time.Now()
	payload, err := json.Marshal(map[string]any{
		"iss": string(testRepo), "aud": []string{string(testAuthority)}, "lxm": string(NotifyWriteMethod),
		"iat": now.Unix(), "exp": now.Add(time.Minute).Unix(), "jti": "algorithm-confusion",
	})
	require.NoError(t, err)
	// The selected key is P-256, but the protected header falsely labels its
	// otherwise-valid P-256 signature ES256K.
	token, err := signServicePayloadWithAlgorithm(f.repoKey, payload, "ES256K")
	require.NoError(t, err)
	replay := &recordingReplayStore{}
	_, err = VerifyWriterNotifyWrite(context.Background(), token, testSpace, testRepo, WriterOptions{
		Options: Options{Resolver: f.resolver, Replay: replay}, Authority: testAuthority,
	})
	require.ErrorContains(t, err, "algorithm")
	assert.Zero(t, replay.calls.Load())
}

func TestStrictRawSigningKeySelection(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	token, err := CreateWriterNotifyWriteToken(testRepo, testAuthority, time.Now().Add(time.Minute), f.repoKey)
	require.NoError(t, err)
	validMethod := identity.VerificationMethod{
		ID:                 string(testRepo) + "#atproto",
		Type:               "Multikey",
		Controller:         string(testRepo),
		PublicKeyMultibase: f.repoKey.PublicKey().Multibase(),
	}

	tests := []struct {
		name   string
		mutate func(*identity.DIDDocument)
	}{
		{"document id mismatch", func(doc *identity.DIDDocument) { doc.ID = string(testAuthority) }},
		{"missing atproto key", func(doc *identity.DIDDocument) { doc.VerificationMethod = nil }},
		{"duplicate atproto fragment", func(doc *identity.DIDDocument) {
			doc.VerificationMethod = append(doc.VerificationMethod, validMethod)
		}},
		{"foreign full id", func(doc *identity.DIDDocument) {
			doc.VerificationMethod[0].ID = string(testAuthority) + "#atproto"
		}},
		{"wrong controller", func(doc *identity.DIDDocument) {
			doc.VerificationMethod[0].Controller = string(testAuthority)
		}},
		{"legacy key type", func(doc *identity.DIDDocument) {
			doc.VerificationMethod[0].Type = "EcdsaSecp256r1VerificationKey2019"
		}},
		{"invalid multibase", func(doc *identity.DIDDocument) {
			doc.VerificationMethod[0].PublicKeyMultibase = "not-a-key"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			doc := &identity.DIDDocument{ID: string(testRepo), VerificationMethod: []identity.VerificationMethod{validMethod}}
			tt.mutate(doc)
			replay := &recordingReplayStore{}
			_, err := VerifyWriterNotifyWrite(context.Background(), token, testSpace, testRepo, WriterOptions{
				Options: Options{Resolver: rawResolver{doc: doc}, Replay: replay}, Authority: testAuthority,
			})
			require.Error(t, err)
			assert.Zero(t, replay.calls.Load())
		})
	}
}

func TestMethodProfilesAreNotInterchangeable(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	writeToken, err := CreateAuthorityNotifyWriteToken(testAuthority, testSubscriber, time.Now().Add(time.Minute), f.authorityKey)
	require.NoError(t, err)
	deleteToken, err := CreateAuthorityNotifySpaceDeletedToken(testAuthority, testSubscriber, time.Now().Add(time.Minute), f.authorityKey)
	require.NoError(t, err)
	replay := &recordingReplayStore{}

	_, err = VerifySubscriberNotifySpaceDeleted(context.Background(), writeToken, testSpace, SubscriberOptions{
		Options: Options{Resolver: f.resolver, Replay: replay}, Subscriber: testSubscriber,
	})
	require.Error(t, err)
	_, err = VerifySubscriberNotifyWrite(context.Background(), deleteToken, testSpace, testRepo, SubscriberOptions{
		Options: Options{Resolver: f.resolver, Replay: replay}, Subscriber: testSubscriber,
	})
	require.Error(t, err)
	assert.Zero(t, replay.calls.Load())
}

func TestBindingFailureDoesNotBurnOtherwiseValidToken(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	replay, err := credential.NewMemoryReplayStore(4)
	require.NoError(t, err)
	token, err := CreateWriterNotifyWriteToken(testRepo, testAuthority, time.Now().Add(time.Minute), f.repoKey)
	require.NoError(t, err)

	_, err = VerifyWriterNotifyWrite(context.Background(), token, testSpace, testOtherRepo, WriterOptions{
		Options: Options{Resolver: f.resolver, Replay: replay}, Authority: testAuthority,
	})
	require.Error(t, err)
	_, err = VerifyWriterNotifyWrite(context.Background(), token, testSpace, testRepo, WriterOptions{
		Options: Options{Resolver: f.resolver, Replay: replay}, Authority: testAuthority,
	})
	require.NoError(t, err)
}

func TestReplayStoreRequiredAndErrorsPropagate(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	token, err := CreateAuthorityNotifySpaceDeletedToken(testAuthority, testSubscriber, time.Now().Add(time.Minute), f.authorityKey)
	require.NoError(t, err)

	_, err = VerifySubscriberNotifySpaceDeleted(context.Background(), token, testSpace, SubscriberOptions{
		Options: Options{Resolver: f.resolver}, Subscriber: testSubscriber,
	})
	require.ErrorContains(t, err, "replay")

	storeErr := errors.New("replay backend unavailable")
	store := &recordingReplayStore{err: storeErr}
	_, err = VerifySubscriberNotifySpaceDeleted(context.Background(), token, testSpace, SubscriberOptions{
		Options: Options{Resolver: f.resolver, Replay: store}, Subscriber: testSubscriber,
	})
	require.ErrorIs(t, err, storeErr)
}

func TestStrictLifetimeConfiguration(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	now := time.Now()
	valid, err := CreateWriterNotifyWriteToken(testRepo, testAuthority, now.Add(time.Minute), f.repoKey)
	require.NoError(t, err)
	old, err := signServiceClaims(f.repoKey, testRepo, []string{string(testAuthority)}, string(NotifyWriteMethod), now.Add(-DefaultMaxAge-time.Minute), now.Add(time.Minute), "old")
	require.NoError(t, err)
	tests := []struct {
		name string
		tok  string
		opts Options
	}{
		{"max age above profile cap", valid, Options{Resolver: f.resolver, Replay: &recordingReplayStore{}, MaxAge: DefaultMaxAge + time.Nanosecond}},
		{"negative skew", valid, Options{Resolver: f.resolver, Replay: &recordingReplayStore{}, ClockSkew: -time.Nanosecond}},
		{"skew above profile cap", valid, Options{Resolver: f.resolver, Replay: &recordingReplayStore{}, ClockSkew: DefaultClockSkew + time.Nanosecond}},
		{"old token", old, Options{Resolver: f.resolver, Replay: &recordingReplayStore{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := VerifyWriterNotifyWrite(context.Background(), tt.tok, testSpace, testRepo, WriterOptions{Options: tt.opts, Authority: testAuthority})
			require.Error(t, err)
		})
	}
}

func TestReplayConsumptionIsAtomicUnderConcurrency(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	replay, err := credential.NewMemoryReplayStore(32)
	require.NoError(t, err)
	token, err := CreateAuthorityNotifyWriteToken(testAuthority, testSubscriber, time.Now().Add(time.Minute), f.authorityKey)
	require.NoError(t, err)

	const workers = 32
	var successes atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, verifyErr := VerifySubscriberNotifyWrite(context.Background(), token, testSpace, testRepo, SubscriberOptions{
				Options: Options{Resolver: f.resolver, Replay: replay}, Subscriber: testSubscriber,
			})
			if verifyErr == nil {
				successes.Add(1)
				return
			}
			assert.ErrorIs(t, verifyErr, credential.ErrReplay)
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, successes.Load())
}

func TestCreationRejectsInvalidRoleCoordinatesAndLifetime(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	now := time.Now()
	tests := []struct {
		name   string
		create func() (string, error)
	}{
		{"writer issuer fragment", func() (string, error) {
			return CreateWriterNotifyWriteToken(atmos.DID(string(testRepo)+"#atproto"), testAuthority, now.Add(time.Minute), f.repoKey)
		}},
		{"authority fragment", func() (string, error) {
			return CreateAuthorityNotifyWriteToken(atmos.DID(string(testAuthority)+"#atproto"), testSubscriber, now.Add(time.Minute), f.authorityKey)
		}},
		{"invalid subscriber", func() (string, error) {
			return CreateAuthorityNotifyWriteToken(testAuthority, "https://example.com", now.Add(time.Minute), f.authorityKey)
		}},
		{"expired", func() (string, error) {
			return CreateAuthorityNotifySpaceDeletedToken(testAuthority, testSubscriber, now.Add(-time.Second), f.authorityKey)
		}},
		{"lifetime above cap", func() (string, error) {
			return CreateAuthorityNotifySpaceDeletedToken(testAuthority, testSubscriber, now.Add(DefaultMaxAge+time.Minute), f.authorityKey)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := tt.create()
			require.Error(t, err)
		})
	}
}

func FuzzVerifyWriterNotifyWrite(f *testing.F) {
	key, err := crypto.GenerateP256()
	if err != nil {
		f.Fatal(err)
	}
	resolver := &mapResolver{keys: map[atmos.DID]crypto.PublicKey{
		testRepo: key.PublicKey(),
	}}
	valid, err := CreateWriterNotifyWriteToken(testRepo, testAuthority, time.Now().Add(time.Minute), key)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid, string(testSpace), string(testRepo), string(testAuthority))
	f.Add("not-a-token", "not-a-space", "not-a-did", "not-a-did")
	f.Fuzz(func(t *testing.T, token, rawSpace, rawRepo, rawAuthority string) {
		_, _ = VerifyWriterNotifyWrite(context.Background(), token, atmos.SpaceRef(rawSpace), atmos.DID(rawRepo), WriterOptions{
			Options:   Options{Resolver: resolver, Replay: &recordingReplayStore{}},
			Authority: atmos.DID(rawAuthority),
		})
	})
}

func signServiceClaims(key crypto.PrivateKey, issuer atmos.DID, audience []string, method string, issuedAt, expiresAt time.Time, jti string) (string, error) {
	payload, err := json.Marshal(map[string]any{
		"iss": string(issuer), "aud": audience, "lxm": method,
		"iat": issuedAt.Unix(), "exp": expiresAt.Unix(), "jti": jti,
	})
	if err != nil {
		return "", err
	}
	return signServicePayload(key, payload)
}

func signServicePayload(key crypto.PrivateKey, payload []byte) (string, error) {
	alg := "ES256K"
	if _, ok := key.(*crypto.P256PrivateKey); ok {
		alg = "ES256"
	}
	return signServicePayloadWithAlgorithm(key, payload, alg)
}

func signServicePayloadWithAlgorithm(key crypto.PrivateKey, payload []byte, alg string) (string, error) {
	header, err := json.Marshal(map[string]any{"alg": alg, "typ": "JWT"})
	if err != nil {
		return "", err
	}
	encode := base64.RawURLEncoding.EncodeToString
	message := encode(header) + "." + encode(payload)
	signature, err := key.HashAndSign([]byte(message))
	if err != nil {
		return "", err
	}
	return message + "." + encode(signature), nil
}
