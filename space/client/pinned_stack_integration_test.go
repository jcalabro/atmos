package client_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	atmoscrypto "github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	spaceclient "github.com/jcalabro/atmos/space/client"
	"github.com/jcalabro/atmos/space/notificationauth"
	"github.com/jcalabro/atmos/space/simplespace"
	"github.com/stretchr/testify/require"
)

// TestPinnedReferenceStack is an opt-in wire gate against the immutable
// atproto and Bulletin revisions recorded in SPACES.md. The harness supplies
// only synthetic accounts and local endpoints; it never contacts a public PDS.
func TestPinnedReferenceStack(t *testing.T) {
	introspectURL := os.Getenv("ATMOS_ATPROTO_INTROSPECT_URL")
	if introspectURL == "" {
		t.Skip("set ATMOS_ATPROTO_INTROSPECT_URL to the pinned multi-PDS introspection endpoint")
	}
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	var topology struct {
		PDSes []struct {
			URL string `json:"url"`
		} `json:"pdses"`
		PLC struct {
			URL string `json:"url"`
		} `json:"plc"`
	}
	getJSON(t, ctx, introspectURL, &topology)
	require.GreaterOrEqual(t, len(topology.PDSes), 2)
	alice := loginSynthetic(t, ctx, topology.PDSes[0].URL, topology.PLC.URL, "alice.test", "alice-pass")
	bob := loginSynthetic(t, ctx, topology.PDSes[1].URL, topology.PLC.URL, "bob.test2", "bob-pass")
	resolver := syntheticResolver{alice.DID: alice.Doc, bob.DID: bob.Doc}
	aliceClient := newSyntheticAccount(t, ctx, alice, resolver)
	bobClient := newSyntheticAccount(t, ctx, bob, resolver)
	manager, err := simplespace.NewClient(aliceClient)
	require.NoError(t, err)

	space, err := manager.Create(ctx, "com.example.group", atmos.RecordKey(fmt.Sprintf("atmos-%d", time.Now().UnixNano())),
		simplespace.Policy{Kind: simplespace.PolicyPublic}, simplespace.Policy{Kind: simplespace.PolicyPublic},
		simplespace.AppAccess{Kind: simplespace.AppAccessOpen})
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Delete(context.Background(), space) })
	require.NoError(t, manager.PutMember(ctx, space, simplespace.Member{DID: bob.DID, Read: true, Write: true}))

	for i := range 3 {
		rkey := atmos.RecordKey(fmt.Sprintf("record-%d", i))
		for _, item := range []struct {
			client *spaceclient.AccountClient
			text   string
		}{{aliceClient, "alice"}, {bobClient, "bob"}} {
			record := json.RawMessage(fmt.Sprintf(`{"$type":"com.example.groupPost","text":"%s-%d"}`, item.text, i))
			_, err := item.client.CreateRecord(ctx, space, "com.example.groupPost", rkey, record, spaceclient.ValidateKnown)
			require.NoError(t, err)
		}
	}

	credentials, err := spaceclient.NewCredentialManager(spaceclient.CredentialManagerOptions{
		Space: space, SecurityContext: bob.DID.String(), Account: bobClient, Resolver: resolver,
		EndpointPolicy: localEndpointPolicy(), HTTPClient: localHTTPClient(),
	})
	require.NoError(t, err)
	_, err = credentials.Credential(ctx, space)
	require.NoError(t, err)
	reader, err := spaceclient.NewReaderClient(ctx, spaceclient.ReaderOptions{
		Space: space, Resolver: resolver, Source: credentials, EndpointPolicy: localEndpointPolicy(), HTTPClient: localHTTPClient(),
	})
	require.NoError(t, err)

	repos, err := reader.ListRepos(ctx, 1, "")
	require.NoError(t, err)
	require.Len(t, repos.Repos, 1)
	require.True(t, repos.Cursor.HasVal(), "cross-PDS writer notification must make directory pagination observable")
	page, err := reader.ListRecords(ctx, bob.DID, "com.example.groupPost", 1, "", false, false)
	require.NoError(t, err)
	require.Len(t, page.Records, 1)
	require.True(t, page.Cursor.HasVal())
	next, err := reader.ListRecords(ctx, bob.DID, "com.example.groupPost", 1, page.Cursor.Val(), false, false)
	require.NoError(t, err)
	require.Len(t, next.Records, 1)
	require.NotEqual(t, page.Records[0].Rkey, next.Records[0].Rkey)

	require.NoError(t, manager.Delete(ctx, space))
	_, err = reader.GetSpace(ctx)
	require.Error(t, err, "a deleted space credential must no longer authorize management reads")
}

// TestPinnedBulletinNotificationAuth verifies that the pinned Bulletin stack
// independently resolves and accepts atmos-signed write and deletion service
// JWTs. Health alone is not cross-implementation protocol evidence.
func TestPinnedBulletinNotificationAuth(t *testing.T) {
	bulletin := os.Getenv("ATMOS_BULLETIN_URL")
	if bulletin == "" {
		t.Skip("set ATMOS_BULLETIN_URL to the pinned Bulletin internal endpoint")
	}
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	var health struct {
		OK bool `json:"ok"`
	}
	getJSON(t, ctx, bulletin+"/health", &health)
	require.True(t, health.OK)
	testBulletinNotificationAuth(t, ctx, bulletin, "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
}

func testBulletinNotificationAuth(t *testing.T, ctx context.Context, bulletin string, repo atmos.DID) {
	t.Helper()
	key, err := atmoscrypto.GenerateP256()
	require.NoError(t, err)

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err)
	address, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok)
	port := address.Port
	authority := atmos.DID(fmt.Sprintf("did:web:localhost%%3A%d", port))
	document := &identity.DIDDocument{
		ID: authority.String(), AlsoKnownAs: []string{}, Service: []identity.Service{},
		VerificationMethod: []identity.VerificationMethod{{
			ID: authority.String() + "#atproto", Type: "Multikey", Controller: authority.String(),
			PublicKeyMultibase: key.PublicKey().Multibase(),
		}},
	}
	didServer := &http.Server{ReadHeaderTimeout: 5 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/.well-known/did.json" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/did+ld+json")
		if err := json.NewEncoder(w).Encode(document); err != nil {
			t.Errorf("serve synthetic authority DID document: %v", err)
		}
	})}
	go func() {
		if err := didServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			t.Errorf("synthetic authority DID server: %v", err)
		}
	}()
	t.Cleanup(func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, didServer.Shutdown(shutdownContext))
	})

	space := atmos.SpaceRef(fmt.Sprintf("at://%s/space/my.bulletin.board/self", authority))
	require.NoError(t, space.Validate())
	subscriber := "did:web:localhost%3A3000#bulletin"
	writeToken, err := notificationauth.CreateAuthorityNotifyWriteToken(authority, subscriber, time.Now().Add(time.Minute), key)
	require.NoError(t, err)
	deleteToken, err := notificationauth.CreateAuthorityNotifySpaceDeletedToken(authority, subscriber, time.Now().Add(time.Minute), key)
	require.NoError(t, err)
	status, _ := requestBulletinNotification(t, ctx, bulletin+"/xrpc/com.atproto.space.notifyWrite", deleteToken, map[string]string{
		"space": space.String(), "repo": repo.String(), "rev": "interop-rev",
	})
	require.NotEqual(t, http.StatusOK, status, "Bulletin must reject a deletion token at the notifyWrite method")
	postBulletinNotification(t, ctx, bulletin+"/xrpc/com.atproto.space.notifyWrite", writeToken, map[string]string{
		"space": space.String(), "repo": repo.String(), "rev": "interop-rev",
	})
	postBulletinNotification(t, ctx, bulletin+"/xrpc/com.atproto.space.notifySpaceDeleted", deleteToken, map[string]string{
		"space": space.String(),
	})
}

func postBulletinNotification(t *testing.T, ctx context.Context, target, token string, input any) {
	t.Helper()
	status, body := requestBulletinNotification(t, ctx, target, token, input)
	require.Equal(t, http.StatusOK, status, string(body))
}

func requestBulletinNotification(t *testing.T, ctx context.Context, target, token string, input any) (int, []byte) {
	t.Helper()
	encoded, err := json.Marshal(input)
	require.NoError(t, err)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(encoded))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer func() { require.NoError(t, response.Body.Close()) }()
	body, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	require.NoError(t, err)
	return response.StatusCode, body
}

type syntheticSession struct {
	Access string                `json:"accessJwt"`
	DID    atmos.DID             `json:"did"`
	Doc    *identity.DIDDocument `json:"didDoc"`
}

type syntheticResolver map[atmos.DID]*identity.DIDDocument

func (r syntheticResolver) ResolveDID(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	if r[did] == nil {
		return nil, fmt.Errorf("synthetic DID %s is absent", did)
	}
	return r[did], nil
}

func (r syntheticResolver) ResolveHandle(context.Context, atmos.Handle) (atmos.DID, error) {
	return "", identity.ErrHandleNotFound
}

func loginSynthetic(t *testing.T, ctx context.Context, pds, plc, handle, password string) syntheticSession {
	t.Helper()
	encoded, err := json.Marshal(map[string]string{"identifier": handle, "password": password})
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, pds+"/xrpc/com.atproto.server.createSession", bytes.NewReader(encoded))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, response.Body.Close()) }()
	require.Equal(t, http.StatusOK, response.StatusCode)
	var session syntheticSession
	require.NoError(t, json.NewDecoder(response.Body).Decode(&session))
	if session.Doc == nil {
		getJSON(t, ctx, plc+"/"+session.DID.String(), &session.Doc)
	}
	return session
}

func newSyntheticAccount(t *testing.T, ctx context.Context, session syntheticSession, resolver syntheticResolver) *spaceclient.AccountClient {
	t.Helper()
	client, err := spaceclient.NewAccountClient(ctx, spaceclient.AccountOptions{
		DID: session.DID, Resolver: resolver, EndpointPolicy: localEndpointPolicy(), HTTPClient: localHTTPClient(),
		Signer: spaceclient.RequestSignerFunc(func(context.Context, string, string) (http.Header, error) {
			return http.Header{"Authorization": {"Bearer " + session.Access}, "DPoP": {"synthetic-account-proof"}}, nil
		}),
	})
	require.NoError(t, err)
	return client
}

func localEndpointPolicy() identity.EndpointPolicy {
	return identity.EndpointPolicy{AllowHTTP: true, AllowPrivateNetworks: true}
}

func localHTTPClient() *http.Client {
	return spaceclient.NewPooledHTTPClient(spaceclient.NetworkPolicy{AllowPrivateNetworks: true})
}

func getJSON(t *testing.T, ctx context.Context, target string, output any) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	require.NoError(t, err)
	response, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, response.Body.Close()) }()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode, string(data))
	require.NoError(t, json.Unmarshal(data, output))
}
