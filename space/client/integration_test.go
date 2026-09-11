package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestRealCredentialAndDPoPAcrossAuthorityAndTwoRepoHosts(t *testing.T) {
	t.Parallel()
	account := atmos.DID(testAccount)
	authority := atmos.DID("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb")
	author1 := atmos.DID("did:plc:cccccccccccccccccccccccc")
	author2 := atmos.DID("did:plc:dddddddddddddddddddddddd")
	spaceRef, err := atmos.ParseSpaceRef(testSpace)
	require.NoError(t, err)
	now := time.Now().Truncate(time.Second)

	accountKey, err := crypto.GenerateP256()
	require.NoError(t, err)
	authorityKey, err := crypto.GenerateK256()
	require.NoError(t, err)
	replay, err := credential.NewMemoryReplayStore(128)
	require.NoError(t, err)

	resolver := &topologyResolver{documents: make(map[atmos.DID]*identity.DIDDocument)}
	resolver.documents[account] = didDocument(account, accountKey, "#atproto", "", "")

	var credentialMu sync.Mutex
	var issuedCredential string
	var capturedProofs []capturedRequest
	recordCID := mustRecordCID(t, `{"$type":"com.example.post","text":"hello"}`)

	newRepoHost := func(author atmos.DID) *httptest.Server {
		var server *httptest.Server
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			credentialMu.Lock()
			issued := issuedCredential
			credentialMu.Unlock()
			if err := verifySpaceRequest(r.Context(), r, server.URL+r.URL.RequestURI(), issued, spaceRef, resolver, replay); err != nil {
				t.Errorf("verify request at %s: %v", author, err)
				http.Error(w, "invalid auth", http.StatusUnauthorized)
				return
			}
			credentialMu.Lock()
			capturedProofs = append(capturedProofs, capturedRequest{host: server.URL, proof: r.Header.Get("DPoP")})
			credentialMu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"uri":"`+testSpace+`/`+author.String()+`/com.example.post/one","cid":"`+recordCID+`","value":{"$type":"com.example.post","text":"hello"}}`)
		}))
		return server
	}
	repo1 := newRepoHost(author1)
	defer repo1.Close()
	repo2 := newRepoHost(author2)
	defer repo2.Close()
	resolver.documents[author1] = didDocument(author1, nil, "", "#atproto_pds", repo1.URL)
	resolver.documents[author2] = didDocument(author2, nil, "", "#atproto_pds", repo2.URL)

	var authorityServer *httptest.Server
	authorityServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target := authorityServer.URL + r.URL.RequestURI()
		switch r.URL.Path {
		case "/xrpc/com.atproto.space.getSpaceCredential":
			delegation := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if delegation == r.Header.Get("Authorization") || delegation == "" {
				http.Error(w, "missing delegation", http.StatusUnauthorized)
				return
			}
			proof, err := credential.VerifyDPoPProof(r.Context(), r.Header.Get("DPoP"), credential.VerifyDPoPOptions{
				Method: r.Method, TargetURL: target, Now: now, Replay: replay,
			})
			if err != nil {
				t.Errorf("verify exchange DPoP: %v", err)
				http.Error(w, "bad DPoP", http.StatusUnauthorized)
				return
			}
			parsed, err := credential.ParseDelegationToken(delegation)
			if err == nil {
				_, err = credential.VerifyDelegationToken(r.Context(), parsed, credential.VerifyDelegationOptions{
					Resolver: resolver, Issuer: account, Subject: spaceRef,
					Audience: credential.SpaceHostAudience(authority), Now: now, Replay: replay,
				})
			}
			if err != nil {
				t.Errorf("verify delegation: %v", err)
				http.Error(w, "bad delegation", http.StatusUnauthorized)
				return
			}
			issued, err := credential.CreateSpaceCredentialToken(credential.SpaceCredentialTokenParams{
				Issuer: authority, Subject: spaceRef, DPoPThumbprint: proof.JKT,
				KeyID: "#atproto_space", Now: now,
			}, authorityKey)
			if err != nil {
				t.Errorf("create credential: %v", err)
				http.Error(w, "issuance failure", http.StatusInternalServerError)
				return
			}
			credentialMu.Lock()
			issuedCredential = issued
			credentialMu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"credential": issued})
		case "/xrpc/com.atproto.space.listRepos":
			credentialMu.Lock()
			issued := issuedCredential
			credentialMu.Unlock()
			if err := verifySpaceRequest(r.Context(), r, target, issued, spaceRef, resolver, replay); err != nil {
				t.Errorf("verify authority read: %v", err)
				http.Error(w, "invalid auth", http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"repos":[]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer authorityServer.Close()
	resolver.documents[authority] = didDocument(authority, authorityKey, "#atproto_space", "#atproto_space_host", authorityServer.URL)

	delegations := &realDelegationSession{issuer: account, subject: spaceRef, audience: credential.SpaceHostAudience(authority), key: accountKey, now: now}
	manager, err := NewCredentialManager(CredentialManagerOptions{
		Space: spaceRef, SecurityContext: "integration-viewer", Account: delegations,
		Resolver: resolver, EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true},
		HTTPClient: authorityServer.Client(), Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	reader, err := NewReaderClient(context.Background(), ReaderOptions{
		Space: spaceRef, Resolver: resolver,
		EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true},
		HTTPClient:     authorityServer.Client(), Source: manager,
	})
	require.NoError(t, err)

	_, err = reader.ListRepos(context.Background(), 0, "")
	require.NoError(t, err)
	_, err = reader.GetRecord(context.Background(), author1, "com.example.post", "one")
	require.NoError(t, err)
	_, err = reader.GetRecord(context.Background(), author2, "com.example.post", "one")
	require.NoError(t, err)
	require.Equal(t, int64(1), delegations.calls.Load(), "one credential exchange must be reused across hosts")

	credentialMu.Lock()
	proofs := append([]capturedRequest(nil), capturedProofs...)
	issued := issuedCredential
	credentialMu.Unlock()
	require.Len(t, proofs, 2)
	require.NotEqual(t, proofs[0].proof, proofs[1].proof)
	parsed, err := credential.ParseSpaceCredentialToken(issued)
	require.NoError(t, err)
	wrongHostReplay, err := credential.NewMemoryReplayStore(4)
	require.NoError(t, err)
	_, err = credential.VerifyDPoPProof(context.Background(), proofs[0].proof, credential.VerifyDPoPOptions{
		Method: http.MethodGet, TargetURL: proofs[1].host + "/xrpc/com.atproto.space.getRecord",
		Credential: issued, ExpectedJKT: parsed.ConfirmationJKT, Replay: wrongHostReplay,
	})
	require.ErrorContains(t, err, "method or target", "a proof captured by one repo host must fail at another")
}

type capturedRequest struct {
	host  string
	proof string
}

type topologyResolver struct {
	documents map[atmos.DID]*identity.DIDDocument
}

func (r *topologyResolver) ResolveDID(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	document := r.documents[did]
	if document == nil {
		return nil, errors.New("unknown DID")
	}
	return document, nil
}

func (*topologyResolver) ResolveHandle(context.Context, atmos.Handle) (atmos.DID, error) {
	return "", errors.New("handles are not supported")
}

type realDelegationSession struct {
	issuer   atmos.DID
	subject  atmos.SpaceRef
	audience string
	key      *crypto.P256PrivateKey
	now      time.Time
	calls    atomic.Int64
}

func (s *realDelegationSession) GetDelegationToken(context.Context, atmos.SpaceRef) (string, error) {
	s.calls.Add(1)
	return credential.CreateDelegationToken(credential.DelegationTokenParams{
		Issuer: s.issuer, Subject: s.subject, Audience: s.audience, Now: s.now,
	}, s.key)
}

func didDocument(did atmos.DID, key crypto.PrivateKey, keyID, serviceID, serviceURL string) *identity.DIDDocument {
	document := &identity.DIDDocument{ID: did.String()}
	if key != nil {
		document.VerificationMethod = []identity.VerificationMethod{{
			ID: keyID, Type: "Multikey", Controller: did.String(), PublicKeyMultibase: key.PublicKey().Multibase(),
		}}
	}
	if serviceID != "" {
		serviceType := "AtprotoPersonalDataServer"
		if serviceID == "#atproto_space_host" {
			serviceType = "AtprotoSpaceHost"
		}
		document.Service = []identity.Service{{ID: serviceID, Type: serviceType, ServiceEndpoint: serviceURL}}
	}
	return document
}

func verifySpaceRequest(ctx context.Context, request *http.Request, target, expectedCredential string, space atmos.SpaceRef, resolver identity.Resolver, replay credential.ReplayStore) error {
	if expectedCredential == "" || request.Header.Get("Authorization") != "DPoP "+expectedCredential {
		return errors.New("wrong authorization role")
	}
	parsed, err := credential.ParseSpaceCredentialToken(expectedCredential)
	if err != nil {
		return err
	}
	verified, err := credential.VerifySpaceCredentialToken(ctx, parsed, credential.VerifySpaceCredentialOptions{
		Resolver: resolver, Subject: space,
	})
	if err != nil {
		return err
	}
	_, err = credential.VerifyDPoPProof(ctx, request.Header.Get("DPoP"), credential.VerifyDPoPOptions{
		Method: request.Method, TargetURL: target, Credential: expectedCredential,
		ExpectedJKT: verified.ConfirmationJKT, Replay: replay,
	})
	return err
}
