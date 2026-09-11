package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/space/credential"
	"github.com/stretchr/testify/require"
)

type fakeDelegationSession struct {
	calls atomic.Int64
	token string
}

func (s *fakeDelegationSession) GetDelegationToken(context.Context, atmos.SpaceRef) (string, error) {
	s.calls.Add(1)
	return s.token, nil
}

type fullResolver struct {
	docs map[atmos.DID]*identity.DIDDocument
}

func (r fullResolver) ResolveDID(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	return r.docs[did], nil
}
func (r fullResolver) ResolveHandle(context.Context, atmos.Handle) (atmos.DID, error) {
	return "", identity.ErrHandleNotFound
}

func TestCredentialManagerSingleflightExchangeAndBinding(t *testing.T) {
	t.Parallel()
	spaceRef, err := atmos.ParseSpaceRef(testSpace)
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	authorityKey, err := crypto.GenerateP256()
	require.NoError(t, err)
	replay, err := credential.NewMemoryReplayStore(128)
	require.NoError(t, err)
	var exchanges atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		exchanges.Add(1)
		require.Equal(t, "Bearer delegation", r.Header.Get("Authorization"))
		proof, verifyErr := credential.VerifyDPoPProof(r.Context(), r.Header.Get("DPoP"), credential.VerifyDPoPOptions{
			Method: r.Method, TargetURL: "http://" + r.Host + r.URL.RequestURI(), Now: now, Replay: replay,
		})
		require.NoError(t, verifyErr)
		var input struct {
			Space string `json:"space"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&input))
		require.Equal(t, spaceRef.String(), input.Space)
		token, createErr := credential.CreateSpaceCredentialToken(credential.SpaceCredentialTokenParams{
			Issuer: spaceRef.Authority(), Subject: spaceRef, DPoPThumbprint: proof.JKT, KeyID: "#atproto_space", Now: now,
		}, authorityKey)
		require.NoError(t, createErr)
		_, _ = io.WriteString(w, `{"credential":`+quoteJSON(token)+`}`)
	}))
	defer server.Close()

	resolver := fullResolver{docs: map[atmos.DID]*identity.DIDDocument{
		spaceRef.Authority(): {ID: spaceRef.Authority().String(),
			VerificationMethod: []identity.VerificationMethod{{ID: "#atproto_space", Type: "Multikey", Controller: spaceRef.Authority().String(), PublicKeyMultibase: authorityKey.PublicKey().Multibase()}},
			Service:            []identity.Service{{ID: "#atproto_space_host", Type: "AtprotoSpaceHost", ServiceEndpoint: server.URL}}},
	}}
	session := &fakeDelegationSession{token: "delegation"}
	manager, err := NewCredentialManager(CredentialManagerOptions{
		Space: spaceRef, SecurityContext: "viewer-session-1", Account: session, Resolver: resolver,
		EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true}, HTTPClient: server.Client(),
		Now: func() time.Time { return now }, RefreshMargin: time.Minute,
	})
	require.NoError(t, err)

	var wg sync.WaitGroup
	pairs := make(chan CredentialPair, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pair, getErr := manager.Credential(context.Background(), spaceRef)
			require.NoError(t, getErr)
			pairs <- pair
		}()
	}
	wg.Wait()
	close(pairs)
	var first CredentialPair
	for pair := range pairs {
		if first.Token == "" {
			first = pair
		}
		require.Equal(t, first.Token, pair.Token)
		require.Same(t, first.Key, pair.Key)
	}
	require.Equal(t, int64(1), session.calls.Load())
	require.Equal(t, int64(1), exchanges.Load())
}

func TestCredentialManagerNeverReplaysAmbiguousExchangeGrant(t *testing.T) {
	t.Parallel()
	spaceRef, err := atmos.ParseSpaceRef(testSpace)
	require.NoError(t, err)
	now := time.Now().Truncate(time.Second)
	authorityKey, err := crypto.GenerateP256()
	require.NoError(t, err)
	replay, err := credential.NewMemoryReplayStore(16)
	require.NoError(t, err)
	var exchanges atomic.Int64
	var mu sync.Mutex
	var grants, proofs []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := exchanges.Add(1)
		mu.Lock()
		grants = append(grants, r.Header.Get("Authorization"))
		proofs = append(proofs, r.Header.Get("DPoP"))
		mu.Unlock()
		proof, verifyErr := credential.VerifyDPoPProof(r.Context(), r.Header.Get("DPoP"), credential.VerifyDPoPOptions{
			Method: r.Method, TargetURL: "http://" + r.Host + r.URL.RequestURI(), Now: now, Replay: replay,
		})
		if verifyErr != nil {
			t.Errorf("verify exchange proof: %v", verifyErr)
			http.Error(w, "bad proof", http.StatusUnauthorized)
			return
		}
		if attempt == 1 {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("response writer does not support HTTP/1 hijacking")
				return
			}
			connection, _, hijackErr := hijacker.Hijack()
			if hijackErr != nil {
				t.Errorf("hijack first exchange: %v", hijackErr)
				return
			}
			_ = connection.Close()
			return
		}
		token, createErr := credential.CreateSpaceCredentialToken(credential.SpaceCredentialTokenParams{
			Issuer: spaceRef.Authority(), Subject: spaceRef, DPoPThumbprint: proof.JKT, KeyID: "#atproto_space", Now: now,
		}, authorityKey)
		if createErr != nil {
			t.Errorf("create credential: %v", createErr)
			http.Error(w, "issuance failure", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"credential":`+quoteJSON(token)+`}`)
	}))
	defer server.Close()

	resolver := fullResolver{docs: map[atmos.DID]*identity.DIDDocument{
		spaceRef.Authority(): {ID: spaceRef.Authority().String(),
			VerificationMethod: []identity.VerificationMethod{{ID: "#atproto_space", Type: "Multikey", Controller: spaceRef.Authority().String(), PublicKeyMultibase: authorityKey.PublicKey().Multibase()}},
			Service:            []identity.Service{{ID: "#atproto_space_host", Type: "AtprotoSpaceHost", ServiceEndpoint: server.URL}}},
	}}
	session := &sequenceDelegationSession{}
	manager, err := NewCredentialManager(CredentialManagerOptions{
		Space: spaceRef, SecurityContext: "ambiguous-exchange", Account: session, Resolver: resolver,
		EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true}, HTTPClient: server.Client(),
		Now: func() time.Time { return now }, RefreshMargin: time.Minute,
	})
	require.NoError(t, err)

	_, err = manager.Credential(context.Background(), spaceRef)
	require.ErrorIs(t, err, ErrAmbiguousResult)
	require.Equal(t, int64(1), exchanges.Load(), "an ambiguous POST must not be replayed internally")
	_, err = manager.Credential(context.Background(), spaceRef)
	require.NoError(t, err)
	require.Equal(t, int64(2), exchanges.Load())
	require.Equal(t, int64(2), session.calls.Load(), "the retry must mint a new single-use delegation")
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, grants, 2)
	require.NotEqual(t, grants[0], grants[1])
	require.NotEqual(t, proofs[0], proofs[1])
}

type sequenceDelegationSession struct{ calls atomic.Int64 }

func (s *sequenceDelegationSession) GetDelegationToken(context.Context, atmos.SpaceRef) (string, error) {
	return fmt.Sprintf("delegation-%d", s.calls.Add(1)), nil
}

func quoteJSON(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
