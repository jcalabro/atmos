package host

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/space/credential"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeliveryRetryCreatesFreshServiceJWT(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	clock := &mutableClock{now: now}
	store := newTestMemoryStore(t, 4)
	config := testConfig(t)
	state, err := store.CreateSpace(t.Context(), config, now)
	require.NoError(t, err)
	service := "did:plc:cccccccccccccccccccccccc#sync"
	require.NoError(t, store.Register(t.Context(), testRegistration(config.URI, service, "credential", now), RegistrationLimits{1, 1, 1}))
	_, err = store.AdmitWriter(t.Context(), config.URI, state.Generation, testWriter(testMemberDID(), "3jzfcijpj2z2a", 1), now)
	require.NoError(t, err)
	claimed, err := store.ClaimDeliveries(t.Context(), now, 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	authorityKey := mustP256(t)
	subscriberKey := mustP256(t)
	resolver := &integrationResolver{docs: map[atmos.DID]*identity.DIDDocument{
		config.URI.Authority():             didDoc(config.URI.Authority(), authorityKey, nil),
		"did:plc:cccccccccccccccccccccccc": didDoc("did:plc:cccccccccccccccccccccccc", subscriberKey, &identity.Service{ID: "#sync", Type: "AtprotoSpaceSyncer", ServiceEndpoint: "http://subscriber.test"}),
	}}
	transport := &failingOnceDelivery{}
	host := newDeliveryTestHost(t, store, resolver, authorityKey, transport, clock)
	host.processDelivery(t.Context(), claimed[0])
	require.Len(t, transport.tokens, 1)
	assert.True(t, now.Truncate(time.Second).Equal(unverifiedIssuedAt(t, transport.tokens[0])))

	clock.set(now.Add(10 * time.Minute))
	claimed, err = store.ClaimDeliveries(t.Context(), clock.Now(), 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	host.processDelivery(t.Context(), claimed[0])
	require.Len(t, transport.tokens, 2)
	assert.NotEqual(t, transport.tokens[0], transport.tokens[1], "each retry needs a fresh replay-protected JWT")
	assert.True(t, clock.Now().Truncate(time.Second).Equal(unverifiedIssuedAt(t, transport.tokens[1])))

	remaining, err := store.ClaimDeliveries(t.Context(), clock.Now().Add(time.Hour), 1, time.Minute)
	require.NoError(t, err)
	assert.Empty(t, remaining)
}

func unverifiedIssuedAt(t *testing.T, raw string) time.Time {
	t.Helper()
	claims := jwt.MapClaims{}
	_, _, err := jwt.NewParser().ParseUnverified(raw, claims)
	require.NoError(t, err)
	issuedAt, err := claims.GetIssuedAt()
	require.NoError(t, err)
	require.NotNil(t, issuedAt)
	return issuedAt.Time
}

func TestHostShutdownCancelsDeliveryAndLeavesLeaseRecoverable(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	clock := &mutableClock{now: now}
	store := newTestMemoryStore(t, 4)
	config := testConfig(t)
	state, err := store.CreateSpace(t.Context(), config, now)
	require.NoError(t, err)
	service := "did:plc:cccccccccccccccccccccccc#sync"
	require.NoError(t, store.Register(t.Context(), testRegistration(config.URI, service, "credential", now), RegistrationLimits{1, 1, 1}))
	_, err = store.AdmitWriter(t.Context(), config.URI, state.Generation, testWriter(testMemberDID(), "3jzfcijpj2z2a", 1), now)
	require.NoError(t, err)
	authorityKey := mustP256(t)
	resolver := &integrationResolver{docs: map[atmos.DID]*identity.DIDDocument{
		config.URI.Authority():             didDoc(config.URI.Authority(), authorityKey, nil),
		"did:plc:cccccccccccccccccccccccc": didDoc("did:plc:cccccccccccccccccccccccc", mustP256(t), &identity.Service{ID: "#sync", Type: "AtprotoSpaceSyncer", ServiceEndpoint: "http://subscriber.test"}),
	}}
	transport := &blockingDelivery{started: make(chan struct{})}
	host := newDeliveryTestHost(t, store, resolver, authorityKey, transport, clock)
	require.NoError(t, host.Start(t.Context()))
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		require.FailNow(t, "delivery did not start")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, host.Shutdown(shutdownCtx))

	clock.set(now.Add(2 * time.Minute))
	recovered, err := store.ClaimDeliveries(t.Context(), clock.Now(), 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, recovered, 1, "canceled claimed work must survive until lease recovery")
}

func TestDeliveryStopsWhenLeaseExpiresDuringDependencies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		configure func(*Host, *mutableClock, time.Time)
	}{
		{name: "resolver", configure: func(host *Host, clock *mutableClock, expired time.Time) {
			host.resolver = &advancingResolver{Resolver: host.resolver, clock: clock, at: expired}
		}},
		{name: "signer", configure: func(host *Host, clock *mutableClock, expired time.Time) {
			host.signer = &advancingSigner{Signer: host.signer, clock: clock, at: expired}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			now := time.Now().UTC()
			clock := &mutableClock{now: now}
			store := newTestMemoryStore(t, 4)
			config := testConfig(t)
			state, err := store.CreateSpace(t.Context(), config, now)
			require.NoError(t, err)
			service := "did:plc:cccccccccccccccccccccccc#sync"
			require.NoError(t, store.Register(t.Context(), testRegistration(config.URI, service, "credential", now), RegistrationLimits{1, 1, 1}))
			_, err = store.AdmitWriter(t.Context(), config.URI, state.Generation, testWriter(testMemberDID(), "3jzfcijpj2z2a", 1), now)
			require.NoError(t, err)
			claimed, err := store.ClaimDeliveries(t.Context(), now, 1, time.Minute)
			require.NoError(t, err)
			require.Len(t, claimed, 1)
			claimed[0].ExpiresAt = now.Add(30 * time.Second)

			authorityKey := mustP256(t)
			resolver := &integrationResolver{docs: map[atmos.DID]*identity.DIDDocument{
				config.URI.Authority():             didDoc(config.URI.Authority(), authorityKey, nil),
				"did:plc:cccccccccccccccccccccccc": didDoc("did:plc:cccccccccccccccccccccccc", mustP256(t), &identity.Service{ID: "#sync", Type: "AtprotoSpaceSyncer", ServiceEndpoint: "http://subscriber.test"}),
			}}
			transport := &captureDelivery{}
			host := newDeliveryTestHost(t, store, resolver, authorityKey, transport, clock)
			tt.configure(host, clock, claimed[0].ExpiresAt.Add(time.Nanosecond))
			host.processDelivery(t.Context(), claimed[0])
			assert.Zero(t, transport.calls.Load())
			remaining, err := store.ClaimDeliveries(t.Context(), clock.Now(), 1, time.Minute)
			require.NoError(t, err)
			assert.Empty(t, remaining, "expired work must be completed, not retried")
		})
	}
}

func TestDeliveryDoesNotSendAfterLeaseExpires(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		configure func(*Host, *mutableClock, Delivery)
		prepare   func(*mutableClock, *Delivery)
	}{
		// The lease was consumed while the claimed delivery waited in the worker
		// queue: processing must not even start.
		{name: "before processing", prepare: func(clock *mutableClock, delivery *Delivery) {
			clock.set(delivery.LeaseExpires)
		}},
		// The lease expires during service resolution: the send must not start,
		// because another claimer may already own the row.
		{name: "during resolution", configure: func(host *Host, clock *mutableClock, delivery Delivery) {
			host.resolver = &advancingResolver{Resolver: host.resolver, clock: clock, at: delivery.LeaseExpires.Add(time.Nanosecond)}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			now := time.Now().UTC()
			clock := &mutableClock{now: now}
			store := newTestMemoryStore(t, 4)
			config := testConfig(t)
			state, err := store.CreateSpace(t.Context(), config, now)
			require.NoError(t, err)
			service := "did:plc:cccccccccccccccccccccccc#sync"
			require.NoError(t, store.Register(t.Context(), testRegistration(config.URI, service, "credential", now), RegistrationLimits{1, 1, 1}))
			_, err = store.AdmitWriter(t.Context(), config.URI, state.Generation, testWriter(testMemberDID(), "3jzfcijpj2z2a", 1), now)
			require.NoError(t, err)
			claimed, err := store.ClaimDeliveries(t.Context(), now, 1, time.Minute)
			require.NoError(t, err)
			require.Len(t, claimed, 1)

			authorityKey := mustP256(t)
			resolver := &integrationResolver{docs: map[atmos.DID]*identity.DIDDocument{
				config.URI.Authority():             didDoc(config.URI.Authority(), authorityKey, nil),
				"did:plc:cccccccccccccccccccccccc": didDoc("did:plc:cccccccccccccccccccccccc", mustP256(t), &identity.Service{ID: "#sync", Type: "AtprotoSpaceSyncer", ServiceEndpoint: "http://subscriber.test"}),
			}}
			transport := &captureDelivery{}
			host := newDeliveryTestHost(t, store, resolver, authorityKey, transport, clock)
			if tt.configure != nil {
				tt.configure(host, clock, claimed[0])
			}
			if tt.prepare != nil {
				tt.prepare(clock, &claimed[0])
			}
			host.processDelivery(t.Context(), claimed[0])
			assert.Zero(t, transport.calls.Load(), "no notification may be sent on an expired lease")
			clock.set(claimed[0].LeaseExpires.Add(2 * time.Minute))
			recovered, err := store.ClaimDeliveries(t.Context(), clock.Now(), 1, time.Minute)
			require.NoError(t, err)
			assert.Len(t, recovered, 1, "the delivery must remain recoverable through lease reclaim")
		})
	}
}

func TestDeliveryJWTDoesNotOutliveLease(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	clock := &mutableClock{now: now}
	store := newTestMemoryStore(t, 4)
	config := testConfig(t)
	state, err := store.CreateSpace(t.Context(), config, now)
	require.NoError(t, err)
	service := "did:plc:cccccccccccccccccccccccc#sync"
	require.NoError(t, store.Register(t.Context(), testRegistration(config.URI, service, "credential", now), RegistrationLimits{1, 1, 1}))
	_, err = store.AdmitWriter(t.Context(), config.URI, state.Generation, testWriter(testMemberDID(), "3jzfcijpj2z2a", 1), now)
	require.NoError(t, err)
	claimed, err := store.ClaimDeliveries(t.Context(), now, 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	claimed[0].ExpiresAt = now.Add(30 * time.Second).Truncate(time.Second)
	authorityKey := mustP256(t)
	resolver := &integrationResolver{docs: map[atmos.DID]*identity.DIDDocument{
		config.URI.Authority():             didDoc(config.URI.Authority(), authorityKey, nil),
		"did:plc:cccccccccccccccccccccccc": didDoc("did:plc:cccccccccccccccccccccccc", mustP256(t), &identity.Service{ID: "#sync", Type: "AtprotoSpaceSyncer", ServiceEndpoint: "http://subscriber.test"}),
	}}
	transport := &captureDelivery{}
	host := newDeliveryTestHost(t, store, resolver, authorityKey, transport, clock)
	host.processDelivery(t.Context(), claimed[0])
	require.Equal(t, int32(1), transport.calls.Load())
	require.Len(t, transport.tokens, 1)
	assert.True(t, claimed[0].ExpiresAt.Equal(unverifiedExpiresAt(t, transport.tokens[0])))
}

type mutableClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *mutableClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *mutableClock) set(now time.Time) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

type failingOnceDelivery struct {
	mu     sync.Mutex
	tokens []string
}

func (d *failingOnceDelivery) Deliver(_ context.Context, _ *url.URL, _ atmos.NSID, token string, _ any) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.tokens = append(d.tokens, token)
	if len(d.tokens) == 1 {
		return errors.New("temporary delivery failure")
	}
	return nil
}

type blockingDelivery struct{ started chan struct{} }

func (d *blockingDelivery) Deliver(ctx context.Context, _ *url.URL, _ atmos.NSID, _ string, _ any) error {
	select {
	case <-d.started:
	default:
		close(d.started)
	}
	<-ctx.Done()
	return ctx.Err()
}

type captureDelivery struct {
	calls  atomic.Int32
	tokens []string
}

func (d *captureDelivery) Deliver(_ context.Context, _ *url.URL, _ atmos.NSID, token string, _ any) error {
	d.tokens = append(d.tokens, token)
	d.calls.Add(1)
	return nil
}

type advancingResolver struct {
	identity.Resolver
	clock *mutableClock
	at    time.Time
}

func (r *advancingResolver) ResolveDID(ctx context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	doc, err := r.Resolver.ResolveDID(ctx, did)
	r.clock.set(r.at)
	return doc, err
}

type advancingSigner struct {
	Signer
	clock *mutableClock
	at    time.Time
}

func (s *advancingSigner) ServiceKey(ctx context.Context, did atmos.DID) (crypto.PrivateKey, error) {
	key, err := s.Signer.ServiceKey(ctx, did)
	s.clock.set(s.at)
	return key, err
}

func unverifiedExpiresAt(t *testing.T, raw string) time.Time {
	t.Helper()
	claims := jwt.MapClaims{}
	_, _, err := jwt.NewParser().ParseUnverified(raw, claims)
	require.NoError(t, err)
	expiresAt, err := claims.GetExpirationTime()
	require.NoError(t, err)
	require.NotNil(t, expiresAt)
	return expiresAt.Time
}

func newDeliveryTestHost(t *testing.T, store Store, resolver identity.Resolver, key *crypto.P256PrivateKey, delivery DeliveryTransport, clock Clock) *Host {
	t.Helper()
	replay, err := credential.NewMemoryReplayStore(8)
	require.NoError(t, err)
	origin, err := url.Parse("http://authority.test")
	require.NoError(t, err)
	host, err := New(Options{
		Origin: origin, EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true},
		Store: store, Replay: replay,
		AccountAuth: AccountAuthenticatorFunc(func(context.Context, *http.Request) (AccountPrincipal, error) {
			return AccountPrincipal{}, ErrNoAccountCredential
		}),
		Resolver: resolver, Signer: &integrationSigner{authority: testConfig(t).URI.Authority(), key: key},
		Attestations: AttestationVerifierFunc(func(context.Context, string, string, time.Time, credential.ReplayStore) (string, error) {
			return "", nil
		}),
		Policies: PolicyEvaluatorFunc(func(context.Context, PolicyRequest) (bool, error) { return true, nil }),
		Subscribers: SubscriberPolicyFunc(func(context.Context, SubscriberRequest) (SubscriberDecision, error) {
			return SubscriberDecision{Allowed: true, ServiceType: "AtprotoSpaceSyncer"}, nil
		}),
		Delivery: delivery, Clock: clock, Events: DiscardEvents, Limits: integrationLimits(),
	})
	require.NoError(t, err)
	return host
}
