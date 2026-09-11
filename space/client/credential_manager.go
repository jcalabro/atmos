package client

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/space/credential"
	"github.com/jcalabro/gt"
)

// DelegationSession is an eligible account OAuth session that can mint a fresh
// single-use delegation for an exact space.
type DelegationSession interface {
	GetDelegationToken(ctx context.Context, space atmos.SpaceRef) (string, error)
}

// AttestationSource creates a fresh, single-use client attestation for one
// exchange. Implementations should return an empty string when the application
// is intentionally anonymous.
type AttestationSource interface {
	ClientAttestation(ctx context.Context, audience string) (string, error)
}

// CredentialManagerOptions configures an isolated credential cache partition.
type CredentialManagerOptions struct {
	Space atmos.SpaceRef
	// SecurityContext names the caller/viewer partition. It is required to make
	// accidental credential sharing across callers visible at construction.
	SecurityContext string
	Account         DelegationSession
	Attestation     AttestationSource
	Resolver        identity.Resolver
	EndpointPolicy  identity.EndpointPolicy
	HTTPClient      *http.Client
	JSONLimit       int64
	RefreshMargin   time.Duration
	RefreshJitter   time.Duration
	Now             func() time.Time
}

// CredentialManager single-flights exchange and atomically publishes complete
// credential/key pairs. A manager is one cache partition: space, application
// attestation source, eligible account session, and caller security context.
type CredentialManager struct {
	opts      CredentialManagerOptions
	mu        sync.Mutex
	current   CredentialPair
	refreshAt time.Time
	flight    *credentialFlight
}

type credentialFlight struct {
	done chan struct{}
	pair CredentialPair
	err  error
}

// NewCredentialManager validates dependencies without performing network IO.
func NewCredentialManager(opts CredentialManagerOptions) (*CredentialManager, error) {
	if err := validateSpace(opts.Space); err != nil {
		return nil, err
	}
	if opts.SecurityContext == "" || opts.Account == nil || opts.Resolver == nil {
		return nil, errors.New("space client: security context, account session, and identity resolver are required")
	}
	if opts.RefreshMargin < 0 || opts.RefreshJitter < 0 {
		return nil, errors.New("space client: credential refresh durations cannot be negative")
	}
	if opts.RefreshMargin == 0 {
		opts.RefreshMargin = 5 * time.Minute
	}
	if opts.RefreshJitter == 0 {
		opts.RefreshJitter = time.Minute
	}
	if opts.RefreshMargin+opts.RefreshJitter >= credential.MaxSpaceCredentialLifetime {
		return nil, errors.New("space client: credential refresh window must be shorter than credential lifetime")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = NewCorrectnessHTTPClient(NetworkPolicy{})
	}
	return &CredentialManager{opts: opts}, nil
}

// Credential implements [CredentialSource]. Waiters share one refresh, while
// already-started requests retain their copied old pair during atomic rotation.
func (m *CredentialManager) Credential(ctx context.Context, space atmos.SpaceRef) (CredentialPair, error) {
	if space != m.opts.Space {
		return CredentialPair{}, errors.New("space client: credential manager is bound to a different space")
	}
	m.mu.Lock()
	if m.current.Token != "" && m.opts.Now().Before(m.refreshAt) {
		pair := m.current
		m.mu.Unlock()
		return pair, nil
	}
	if m.flight != nil {
		flight := m.flight
		m.mu.Unlock()
		select {
		case <-flight.done:
			return flight.pair, flight.err
		case <-ctx.Done():
			return CredentialPair{}, ctx.Err()
		}
	}
	flight := &credentialFlight{done: make(chan struct{})}
	m.flight = flight
	m.mu.Unlock()

	flight.pair, flight.err = m.exchange(ctx)
	m.mu.Lock()
	if flight.err == nil {
		m.current = flight.pair
		jitter := time.Duration(rand.Int64N(int64(m.opts.RefreshJitter) + 1))
		m.refreshAt = flight.pair.ExpiresAt.Add(-m.opts.RefreshMargin - jitter)
	}
	m.flight = nil
	close(flight.done)
	m.mu.Unlock()
	return flight.pair, flight.err
}

func (m *CredentialManager) exchange(ctx context.Context) (CredentialPair, error) {
	key, err := crypto.GenerateP256()
	if err != nil {
		return CredentialPair{}, fmt.Errorf("space client: generate credential DPoP key: %w", err)
	}
	jwk, err := credential.PublicJWK(key.PublicKey())
	if err != nil {
		return CredentialPair{}, err
	}
	jkt, err := credential.JWKThumbprint(jwk)
	if err != nil {
		return CredentialPair{}, err
	}
	delegation, err := m.opts.Account.GetDelegationToken(ctx, m.opts.Space)
	if err != nil {
		return CredentialPair{}, fmt.Errorf("space client: obtain delegation: %w", err)
	}
	if delegation == "" {
		return CredentialPair{}, errors.New("space client: account returned an empty delegation")
	}
	authority, err := ResolveAuthorityHost(ctx, m.opts.Resolver, m.opts.Space.Authority(), m.opts.EndpointPolicy)
	if err != nil {
		return CredentialPair{}, err
	}
	attestation := ""
	if m.opts.Attestation != nil {
		attestation, err = m.opts.Attestation.ClientAttestation(ctx, authority.Audience)
		if err != nil {
			return CredentialPair{}, fmt.Errorf("space client: create client attestation: %w", err)
		}
	}
	target := xrpcURL(authority.URL, "com.atproto.space.getSpaceCredential", nil)
	signer := RequestSignerFunc(func(context.Context, string, string) (http.Header, error) {
		proof, err := credential.CreateDPoPProof(credential.DPoPProofParams{Key: key, Method: http.MethodPost, TargetURL: target, Now: m.opts.Now()})
		if err != nil {
			return nil, err
		}
		return http.Header{"Authorization": {"Bearer " + delegation}, "DPoP": {proof}}, nil
	})
	eng, err := newEngine(engineOptions{
		HTTPClient: m.opts.HTTPClient, Signer: signer, JSONLimit: m.opts.JSONLimit,
		NetworkPolicy: NetworkPolicy{AllowPrivateNetworks: m.opts.EndpointPolicy.AllowPrivateLiteral},
	})
	if err != nil {
		return CredentialPair{}, err
	}
	input := &comatproto.SpaceGetSpaceCredential_Input{Space: m.opts.Space.String()}
	if attestation != "" {
		input.ClientAttestation = gt.Some(attestation)
	}
	var out comatproto.SpaceGetSpaceCredential_Output
	if err := eng.json(ctx, http.MethodPost, target, input, &out); err != nil {
		return CredentialPair{}, fmt.Errorf("space client: credential exchange: %w", err)
	}
	unverified, err := credential.ParseSpaceCredentialToken(out.Credential)
	if err != nil {
		return CredentialPair{}, fmt.Errorf("space client: parse authority credential: %w", err)
	}
	verified, err := credential.VerifySpaceCredentialToken(ctx, unverified, credential.VerifySpaceCredentialOptions{Resolver: m.opts.Resolver, Subject: m.opts.Space, Now: m.opts.Now()})
	if err != nil {
		return CredentialPair{}, fmt.Errorf("space client: verify authority credential: %w", err)
	}
	if verified.ConfirmationJKT != jkt {
		return CredentialPair{}, errors.New("space client: authority credential is bound to a different DPoP key")
	}
	return CredentialPair{Space: m.opts.Space, Token: out.Credential, Key: key, ExpiresAt: verified.ExpiresAt}, nil
}
