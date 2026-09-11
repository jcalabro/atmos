package client

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/stretchr/testify/require"
)

type staticCredentialSource struct{ pair CredentialPair }

func (s staticCredentialSource) Credential(context.Context, atmos.SpaceRef) (CredentialPair, error) {
	return s.pair, nil
}

func TestReaderClientRoutesTwoReposAndCreatesFreshProofs(t *testing.T) {
	t.Parallel()
	spaceRef, err := atmos.ParseSpaceRef(testSpace)
	require.NoError(t, err)
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	author1 := atmos.DID("did:plc:cccccccccccccccccccccccc")
	author2 := atmos.DID("did:plc:dddddddddddddddddddddddd")
	cid := mustRecordCID(t, `{"$type":"com.example.post"}`)

	var mu sync.Mutex
	jtis := map[string]struct{}{}
	newRepo := func(author atmos.DID) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "DPoP credential", r.Header.Get("Authorization"))
			proof := r.Header.Get("DPoP")
			parsed, _, parseErr := jwt.NewParser().ParseUnverified(proof, jwt.MapClaims{})
			require.NoError(t, parseErr)
			jti, ok := parsed.Claims.(jwt.MapClaims)["jti"].(string)
			require.True(t, ok)
			mu.Lock()
			_, duplicate := jtis[jti]
			jtis[jti] = struct{}{}
			mu.Unlock()
			require.False(t, duplicate)
			_, _ = io.WriteString(w, `{"uri":"`+testSpace+`/`+string(author)+`/com.example.post/one","cid":"`+cid+`","value":{"$type":"com.example.post"}}`)
		}))
	}
	repo1 := newRepo(author1)
	defer repo1.Close()
	repo2 := newRepo(author2)
	defer repo2.Close()
	authority := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{"repos":[]}`) }))
	defer authority.Close()

	resolver := resolverFunc(func(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
		endpoint := authority.URL
		fragment, typ := "atproto_space_host", "AtprotoSpaceHost"
		switch did {
		case author1:
			endpoint, fragment, typ = repo1.URL, "atproto_pds", "AtprotoPersonalDataServer"
		case author2:
			endpoint, fragment, typ = repo2.URL, "atproto_pds", "AtprotoPersonalDataServer"
		}
		return &identity.DIDDocument{ID: string(did), Service: []identity.Service{{ID: "#" + fragment, Type: typ, ServiceEndpoint: endpoint}}}, nil
	})
	reader, err := NewReaderClient(context.Background(), ReaderOptions{
		Space: spaceRef, Resolver: resolver, EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true},
		HTTPClient: authority.Client(), Source: staticCredentialSource{pair: CredentialPair{Space: spaceRef, Token: "credential", Key: key, ExpiresAt: time.Now().Add(time.Hour)}},
	})
	require.NoError(t, err)
	_, err = reader.GetRecord(context.Background(), author1, "com.example.post", "one")
	require.NoError(t, err)
	_, err = reader.GetRecord(context.Background(), author2, "com.example.post", "one")
	require.NoError(t, err)
	require.Len(t, jtis, 2)
}

func TestReaderClientRejectsMismatchedCredentialWithoutRequest(t *testing.T) {
	t.Parallel()
	spaceRef, err := atmos.ParseSpaceRef(testSpace)
	require.NoError(t, err)
	other, err := atmos.ParseSpaceRef("at://did:plc:bbbbbbbbbbbbbbbbbbbbbbbb/space/com.example.forum/other")
	require.NoError(t, err)
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	authority := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("request must not be sent") }))
	defer authority.Close()
	resolver := resolverFunc(func(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
		return &identity.DIDDocument{ID: string(did), Service: []identity.Service{{ID: "#atproto_space_host", Type: "AtprotoSpaceHost", ServiceEndpoint: authority.URL}}}, nil
	})
	reader, err := NewReaderClient(context.Background(), ReaderOptions{
		Space: spaceRef, Resolver: resolver, EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true}, HTTPClient: authority.Client(),
		Source: staticCredentialSource{pair: CredentialPair{Space: other, Token: "credential", Key: key, ExpiresAt: time.Now().Add(time.Hour)}},
	})
	require.NoError(t, err)
	_, err = reader.ListRepos(context.Background(), 0, "")
	require.Error(t, err)
}

func TestBindRepoPinsEndpointAndKeyFromOneIdentityResolution(t *testing.T) {
	t.Parallel()
	spaceRef, err := atmos.ParseSpaceRef(testSpace)
	require.NoError(t, err)
	author := atmos.DID("did:plc:cccccccccccccccccccccccc")
	oldKey, err := crypto.GenerateP256()
	require.NoError(t, err)
	newKey, err := crypto.GenerateP256()
	require.NoError(t, err)
	authorDoc := &identity.DIDDocument{ID: author.String(), VerificationMethod: []identity.VerificationMethod{{ID: "#atproto", Type: "Multikey", Controller: author.String(), PublicKeyMultibase: oldKey.PublicKey().Multibase()}}, Service: []identity.Service{{ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: "http://127.0.0.1:3001"}}}
	resolver := resolverFunc(func(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
		if did == author {
			return authorDoc, nil
		}
		return &identity.DIDDocument{ID: did.String(), Service: []identity.Service{{ID: "#atproto_space_host", Type: "AtprotoSpaceHost", ServiceEndpoint: "http://127.0.0.1:3000"}}}, nil
	})
	proofKey, err := crypto.GenerateP256()
	require.NoError(t, err)
	reader, err := NewReaderClient(context.Background(), ReaderOptions{Space: spaceRef, Resolver: resolver, EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true}, Source: staticCredentialSource{pair: CredentialPair{Space: spaceRef, Token: "credential", Key: proofKey, ExpiresAt: time.Now().Add(time.Hour)}}})
	require.NoError(t, err)
	bound, err := reader.BindRepo(context.Background(), author)
	require.NoError(t, err)
	authorDoc = &identity.DIDDocument{ID: author.String(), VerificationMethod: []identity.VerificationMethod{{ID: "#atproto", Type: "Multikey", Controller: author.String(), PublicKeyMultibase: newKey.PublicKey().Multibase()}}, Service: []identity.Service{{ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: "http://127.0.0.1:3002"}}}
	require.Equal(t, "http://127.0.0.1:3001", bound.EndpointURL())
	require.True(t, bound.VerificationKey().Equal(oldKey.PublicKey()))
	rebound, err := reader.BindRepo(context.Background(), author)
	require.NoError(t, err)
	require.Equal(t, "http://127.0.0.1:3002", rebound.EndpointURL())
	require.True(t, rebound.VerificationKey().Equal(newKey.PublicKey()))
}

func TestReaderClientListReposRejectsDuplicateDIDs(t *testing.T) {
	t.Parallel()
	spaceRef, err := atmos.ParseSpaceRef(testSpace)
	require.NoError(t, err)
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	did := "did:plc:cccccccccccccccccccccccc"
	authority := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"repos":[{"did":"`+did+`","rev":"3l7xqs6y45k2j","hash":{"$bytes":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}},{"did":"`+did+`","rev":"3l7xqs6y45k2j","hash":{"$bytes":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}}]}`)
	}))
	defer authority.Close()
	resolver := resolverFunc(func(_ context.Context, resolved atmos.DID) (*identity.DIDDocument, error) {
		return &identity.DIDDocument{ID: string(resolved), Service: []identity.Service{{ID: "#atproto_space_host", Type: "AtprotoSpaceHost", ServiceEndpoint: authority.URL}}}, nil
	})
	reader, err := NewReaderClient(context.Background(), ReaderOptions{
		Space: spaceRef, Resolver: resolver, EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true},
		HTTPClient: authority.Client(), Source: staticCredentialSource{pair: CredentialPair{Space: spaceRef, Token: "credential", Key: key, ExpiresAt: time.Now().Add(time.Hour)}},
	})
	require.NoError(t, err)
	_, err = reader.ListRepos(context.Background(), 0, "")
	require.ErrorContains(t, err, "duplicate repo DID")
}
