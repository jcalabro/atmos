package host

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	spaces "github.com/jcalabro/atmos/space"
	spaceclient "github.com/jcalabro/atmos/space/client"
	"github.com/jcalabro/atmos/space/credential"
	"github.com/jcalabro/atmos/space/notificationauth"
	"github.com/jcalabro/atmos/space/simplespace"
	spacesync "github.com/jcalabro/atmos/space/sync"
	"github.com/jcalabro/atmos/xrpcserver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	integrationAuthority  = atmos.DID("did:plc:abcdefghijklmnopqrstuvwx")
	integrationAuthorOne  = atmos.DID("did:plc:bcdefghijklmnopqrstuvwxy")
	integrationAuthorTwo  = atmos.DID("did:plc:cdefghijklmnopqrstuvwxyz")
	integrationSubscriber = "did:plc:defghijklmnopqrstuvwxyza#space_syncer"
)

func TestHost_TwoAuthorCredentialDirectoryFanoutAndDeletion(t *testing.T) {
	t.Parallel()
	var allowedAccountAction atomic.Uint32
	var deletionEvents atomic.Int32
	allowedAccountAction.Store(uint32(AccountCreate))
	authorityKey := mustP256(t)
	authorOneKey := mustP256(t)
	authorTwoKey := mustP256(t)
	dpopKey := mustP256(t)
	resolver := &integrationResolver{docs: map[atmos.DID]*identity.DIDDocument{
		integrationAuthority:               didDoc(integrationAuthority, authorityKey, nil),
		integrationAuthorOne:               didDoc(integrationAuthorOne, authorOneKey, &identity.Service{ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: "http://author-one.test"}),
		integrationAuthorTwo:               didDoc(integrationAuthorTwo, authorTwoKey, &identity.Service{ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: "http://author-two.test"}),
		"did:plc:defghijklmnopqrstuvwxyza": didDoc("did:plc:defghijklmnopqrstuvwxyza", mustP256(t), &identity.Service{ID: "#space_syncer", Type: "AtprotoSpaceSyncer", ServiceEndpoint: "http://subscriber.test"}),
	}}
	store := newTestMemoryStore(t, 32)
	replay, err := credential.NewMemoryReplayStore(256)
	require.NoError(t, err)
	deliveries := &recordingDelivery{notify: make(chan DeliveryKind, 8)}
	origin, err := url.Parse("http://authority.test")
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Second)
	h, err := New(Options{
		Origin: origin, EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true},
		Store: store, Replay: replay,
		AccountAuth: AccountAuthenticatorFunc(func(_ context.Context, req *http.Request) (AccountPrincipal, error) {
			if req.Header.Get("Authorization") != "Account "+integrationAuthority.String() {
				return AccountPrincipal{}, ErrNoAccountCredential
			}
			return AccountPrincipal{DID: integrationAuthority, Permissions: AccountPermissionsFunc(func(_ atmos.SpaceRef, action AccountAction) bool {
				return uint32(action) == allowedAccountAction.Load()
			})}, nil
		}),
		Resolver: resolver,
		Signer:   &integrationSigner{authority: integrationAuthority, key: authorityKey},
		Attestations: AttestationVerifierFunc(func(context.Context, string, string, time.Time, credential.ReplayStore) (string, error) {
			return "", errors.New("attestations are not expected for open app access")
		}),
		Policies: SimplePolicyEvaluator{},
		Subscribers: SubscriberPolicyFunc(func(_ context.Context, req SubscriberRequest) (SubscriberDecision, error) {
			return SubscriberDecision{Allowed: req.Service == integrationSubscriber, ServiceType: "AtprotoSpaceSyncer"}, nil
		}),
		Delivery: deliveries,
		Clock:    ClockFunc(func() time.Time { return now }), Events: EventSinkFunc(func(_ context.Context, event Event) {
			if event.Kind == EventSpaceDeleted {
				deletionEvents.Add(1)
			}
		}),
		Limits: integrationLimits(),
	})
	require.NoError(t, err)
	server := &xrpcserver.Server{}
	require.NoError(t, h.Mount(server))
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	resolver.docs[integrationAuthority].Service = append(resolver.docs[integrationAuthority].Service, identity.Service{
		ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: httpServer.URL,
	})
	accountClient, err := spaceclient.NewAccountClient(t.Context(), spaceclient.AccountOptions{
		DID: integrationAuthority, Resolver: resolver,
		EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true},
		HTTPClient:     httpServer.Client(),
		Signer: spaceclient.RequestSignerFunc(func(context.Context, string, string) (http.Header, error) {
			return http.Header{"Authorization": {"Account " + integrationAuthority.String()}}, nil
		}),
	})
	require.NoError(t, err)
	manager, err := simplespace.NewClient(accountClient)
	require.NoError(t, err)

	space, err := manager.Create(t.Context(), "com.example.board", "main",
		simplespace.Policy{Kind: simplespace.PolicyMemberList},
		simplespace.Policy{Kind: simplespace.PolicyMemberList},
		simplespace.AppAccess{Kind: simplespace.AppAccessOpen})
	require.NoError(t, err)
	require.Equal(t, integrationAuthority, space.Authority())
	allowedAccountAction.Store(uint32(AccountReadSelf))
	config, err := manager.Get(t.Context(), space)
	require.NoError(t, err)
	require.Equal(t, simplespace.PolicyMemberList, config.ReadPolicy.Kind)

	allowedAccountAction.Store(uint32(AccountUpdate))
	for _, author := range []atmos.DID{integrationAuthorOne, integrationAuthorTwo} {
		require.NoError(t, manager.PutMember(t.Context(), space, simplespace.Member{DID: author, Read: true, Write: true}))
	}
	allowedAccountAction.Store(uint32(AccountReadSelf))
	members, cursor, err := manager.Members(t.Context(), space, 10, "")
	require.NoError(t, err)
	require.Empty(t, cursor)
	require.Len(t, members, 2)
	allowedAccountAction.Store(uint32(AccountUpdate))
	require.NoError(t, manager.RemoveMember(t.Context(), space, integrationAuthorTwo))
	require.NoError(t, manager.PutMember(t.Context(), space, simplespace.Member{DID: integrationAuthorTwo, Read: true, Write: true}))
	writePolicy := simplespace.Policy{Kind: simplespace.PolicyMemberList}
	require.NoError(t, manager.Update(t.Context(), space, simplespace.Patch{WritePolicy: &writePolicy}))

	delegation, err := credential.CreateDelegationToken(credential.DelegationTokenParams{
		Issuer: integrationAuthorOne, Subject: space, Audience: credential.SpaceHostAudience(integrationAuthority), Now: now,
	}, authorOneKey)
	require.NoError(t, err)
	exchangeProof, err := credential.CreateDPoPProof(credential.DPoPProofParams{
		Key: dpopKey, Method: http.MethodPost, TargetURL: origin.String() + "/xrpc/" + methodGetCredential, Now: now,
	})
	require.NoError(t, err)
	var credentialOutput comatproto.SpaceGetSpaceCredential_Output
	response := requestJSON(t, httpServer.URL, methodGetCredential, &comatproto.SpaceGetSpaceCredential_Input{Space: space.String()}, "Bearer "+delegation, exchangeProof, &credentialOutput)
	require.Equal(t, http.StatusOK, response)
	require.NotEmpty(t, credentialOutput.Credential)
	parsedCredential, err := credential.ParseSpaceCredentialToken(credentialOutput.Credential)
	require.NoError(t, err)
	verifiedCredential, err := credential.VerifySpaceCredentialToken(t.Context(), parsedCredential, credential.VerifySpaceCredentialOptions{Resolver: resolver, Subject: space, Now: now})
	require.NoError(t, err)
	thumbprint, err := credential.JWKThumbprint(mustPublicJWK(t, dpopKey))
	require.NoError(t, err)
	assert.Equal(t, thumbprint, verifiedCredential.ConfirmationJKT)

	registerProof := mustDPoP(t, dpopKey, http.MethodPost, origin.String()+"/xrpc/"+methodRegisterNotify, credentialOutput.Credential, now)
	var registered comatproto.SpaceRegisterNotify_Output
	response = requestJSON(t, httpServer.URL, methodRegisterNotify, &comatproto.SpaceRegisterNotify_Input{Space: space.String(), Service: integrationSubscriber}, "DPoP "+credentialOutput.Credential, registerProof, &registered)
	require.Equal(t, http.StatusOK, response)
	_, err = atmos.ParseDatetime(registered.ExpiresAt)
	require.NoError(t, err)
	unregisterProof := mustDPoP(t, dpopKey, http.MethodPost, origin.String()+"/xrpc/"+methodUnregisterNotify, credentialOutput.Credential, now)
	response = requestJSON(t, httpServer.URL, methodUnregisterNotify, &comatproto.SpaceUnregisterNotify_Input{Space: space.String(), Service: integrationSubscriber}, "DPoP "+credentialOutput.Credential, unregisterProof, nil)
	require.Equal(t, http.StatusOK, response)
	reregisterProof := mustDPoP(t, dpopKey, http.MethodPost, origin.String()+"/xrpc/"+methodRegisterNotify, credentialOutput.Credential, now)
	response = requestJSON(t, httpServer.URL, methodRegisterNotify, &comatproto.SpaceRegisterNotify_Input{Space: space.String(), Service: integrationSubscriber}, "DPoP "+credentialOutput.Credential, reregisterProof, &registered)
	require.Equal(t, http.StatusOK, response)

	for i, author := range []struct {
		did atmos.DID
		key *crypto.P256PrivateKey
	}{{integrationAuthorOne, authorOneKey}, {integrationAuthorTwo, authorTwoKey}} {
		rev := atmos.NewTID(now.Add(time.Duration(i)*time.Microsecond).UnixMicro(), uint(i))
		var hash [32]byte
		hash[0] = byte(i + 1)
		token, err := notificationauth.CreateWriterNotifyWriteToken(author.did, integrationAuthority, time.Now().Add(time.Minute), author.key)
		require.NoError(t, err)
		response = requestJSON(t, httpServer.URL, methodNotifyWrite, &comatproto.SpaceNotifyWrite_Input{
			Space: space.String(), Repo: author.did.String(), Rev: rev.String(), Hash: hash[:],
		}, "Bearer "+token, "", nil)
		require.Equal(t, http.StatusOK, response)
	}

	listProof := mustDPoP(t, dpopKey, http.MethodGet, origin.String()+"/xrpc/"+methodListRepos, credentialOutput.Credential, now)
	var repos comatproto.SpaceListRepos_Output
	response = requestQuery(t, httpServer.URL, methodListRepos, url.Values{"space": {space.String()}}, "DPoP "+credentialOutput.Credential, listProof, &repos)
	require.Equal(t, http.StatusOK, response)
	require.Len(t, repos.Repos, 2)
	assert.Equal(t, integrationAuthorOne.String(), repos.Repos[0].DID)
	assert.Equal(t, integrationAuthorTwo.String(), repos.Repos[1].DID)

	syncSource := newIntegrationSyncSource(t, store, space, map[atmos.DID]*crypto.P256PrivateKey{
		integrationAuthorOne: authorOneKey, integrationAuthorTwo: authorTwoKey,
	}, now)
	syncStore, err := spacesync.NewMemoryStore(8, 8, 16, 8<<20)
	require.NoError(t, err)
	purger := &recordingCredentialPurger{}
	syncer, err := spacesync.New(spacesync.Options{
		Space: space, Store: syncStore, Source: syncSource, Limits: integrationSyncLimits(),
		Recovery: spacesync.RecoveryFull, Credential: purger,
	})
	require.NoError(t, err)
	directory, err := syncSource.ListRepos(t.Context(), 10, "")
	require.NoError(t, err)
	require.Len(t, directory.Repos, 2)
	for _, hint := range directory.Repos {
		repo, err := syncer.SyncRepo(t.Context(), hint.Author)
		require.NoError(t, err)
		require.True(t, repo.Checkpoint.ValuesComplete)
		require.Len(t, repo.Records, 1)
	}
	deliveries.onDelete = func(ctx context.Context) error {
		return syncer.ApplyVerifiedSpaceDeletion(ctx, "authority notification")
	}

	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, h.Shutdown(context.Background())) })
	require.Equal(t, DeliveryWrite, awaitDelivery(t, deliveries.notify))
	require.Equal(t, DeliveryWrite, awaitDelivery(t, deliveries.notify))

	allowedAccountAction.Store(uint32(AccountDelete))
	missing := atmos.SpaceRef("at://did:plc:abcdefghijklmnopqrstuvwx/space/com.example.board/missing")
	require.Error(t, manager.Delete(t.Context(), missing))
	require.Zero(t, deletionEvents.Load(), "a missing space has no committed tombstone to announce")
	require.NoError(t, manager.Delete(t.Context(), space))
	require.Equal(t, int32(1), deletionEvents.Load())
	require.Equal(t, DeliverySpaceDeleted, awaitDelivery(t, deliveries.notify))
	for _, author := range []atmos.DID{integrationAuthorOne, integrationAuthorTwo} {
		_, err := syncStore.LoadRepo(t.Context(), spacesync.RepoKey{Space: space, Author: author})
		require.ErrorIs(t, err, spacesync.ErrNotFound)
	}
	assert.Equal(t, int32(1), purger.calls.Load())
	_, err = store.CreateSpace(t.Context(), testConfigWithSpace(space), now.Add(time.Hour))
	require.ErrorIs(t, err, ErrTombstoned)
}

func TestHost_RejectsAccountReaderRoleConfusionAndReplays(t *testing.T) {
	// The end-to-end test above consumes a delegation and DPoP proof. Repeating
	// either compact token must fail before policy or signer work is reached.
	t.Parallel()
	store := newTestMemoryStore(t, 8)
	config := testConfig(t)
	now := time.Now().UTC().Truncate(time.Second)
	_, err := store.CreateSpace(t.Context(), config, now)
	require.NoError(t, err)
	key := mustP256(t)
	resolver := &integrationResolver{docs: map[atmos.DID]*identity.DIDDocument{
		config.URI.Authority(): didDoc(config.URI.Authority(), key, nil),
		integrationAuthorOne:   didDoc(integrationAuthorOne, key, nil),
	}}
	replay, err := credential.NewMemoryReplayStore(32)
	require.NoError(t, err)
	origin, _ := url.Parse("http://authority.test")
	h, err := New(Options{
		Origin: origin, EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true}, Store: store, Replay: replay,
		AccountAuth: AccountAuthenticatorFunc(func(context.Context, *http.Request) (AccountPrincipal, error) {
			return AccountPrincipal{}, ErrNoAccountCredential
		}),
		Resolver: resolver, Signer: &integrationSigner{authority: config.URI.Authority(), key: key},
		Attestations: AttestationVerifierFunc(func(context.Context, string, string, time.Time, credential.ReplayStore) (string, error) {
			return "", nil
		}),
		Policies: SimplePolicyEvaluator{}, Subscribers: SubscriberPolicyFunc(func(context.Context, SubscriberRequest) (SubscriberDecision, error) { return SubscriberDecision{}, nil }),
		Delivery: DeliveryTransportFunc(func(context.Context, *url.URL, atmos.NSID, string, any) error { return nil }), Clock: ClockFunc(func() time.Time { return now }), Events: DiscardEvents, Limits: integrationLimits(),
	})
	require.NoError(t, err)
	server := &xrpcserver.Server{}
	require.NoError(t, h.Mount(server))
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()

	delegation, err := credential.CreateDelegationToken(credential.DelegationTokenParams{Issuer: integrationAuthorOne, Subject: config.URI, Audience: credential.SpaceHostAudience(config.URI.Authority()), Now: now}, key)
	require.NoError(t, err)
	dpopKey := mustP256(t)
	proof := mustDPoP(t, dpopKey, http.MethodPost, origin.String()+"/xrpc/"+methodGetCredential, "", now)
	input := &comatproto.SpaceGetSpaceCredential_Input{Space: config.URI.String()}
	first := requestJSON(t, httpServer.URL, methodGetCredential, input, "Bearer "+delegation, proof, nil)
	require.Equal(t, http.StatusOK, first)
	second := requestJSON(t, httpServer.URL, methodGetCredential, input, "Bearer "+delegation, proof, nil)
	assert.Equal(t, http.StatusBadRequest, second)

	readerAsAccount := requestQuery(t, httpServer.URL, methodListMembers, url.Values{"space": {config.URI.String()}}, "Bearer not-an-account-token", "", nil)
	assert.Equal(t, http.StatusUnauthorized, readerAsAccount)
}

func TestMountedHostInteroperatesWithTypedReaderClient(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	config := testConfig(t)
	authorityKey := mustP256(t)
	dpopKey := mustP256(t)
	store := newTestMemoryStore(t, 8)
	_, err := store.CreateSpace(t.Context(), config, now)
	require.NoError(t, err)
	replay, err := credential.NewMemoryReplayStore(64)
	require.NoError(t, err)
	xserver := &xrpcserver.Server{}
	httpServer := httptest.NewUnstartedServer(xserver)
	origin, err := url.Parse("http://" + httpServer.Listener.Addr().String())
	require.NoError(t, err)
	resolver := &integrationResolver{docs: map[atmos.DID]*identity.DIDDocument{
		config.URI.Authority(): {
			ID:                 config.URI.Authority().String(),
			VerificationMethod: []identity.VerificationMethod{{ID: "#atproto", Type: "Multikey", Controller: config.URI.Authority().String(), PublicKeyMultibase: authorityKey.PublicKey().Multibase()}},
			Service:            []identity.Service{{ID: "#atproto_space_host", Type: "AtprotoSpaceHost", ServiceEndpoint: origin.String()}},
		},
	}}
	h, err := New(Options{
		Origin: origin, EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true},
		Store: store, Replay: replay,
		AccountAuth: AccountAuthenticatorFunc(func(context.Context, *http.Request) (AccountPrincipal, error) {
			return AccountPrincipal{}, ErrNoAccountCredential
		}),
		Resolver: resolver, Signer: &integrationSigner{authority: config.URI.Authority(), key: authorityKey},
		Attestations: AttestationVerifierFunc(func(context.Context, string, string, time.Time, credential.ReplayStore) (string, error) {
			return "", nil
		}),
		Policies:    SimplePolicyEvaluator{},
		Subscribers: SubscriberPolicyFunc(func(context.Context, SubscriberRequest) (SubscriberDecision, error) { return SubscriberDecision{}, nil }),
		Delivery:    DeliveryTransportFunc(func(context.Context, *url.URL, atmos.NSID, string, any) error { return nil }),
		Clock:       ClockFunc(time.Now), Events: DiscardEvents, Limits: integrationLimits(),
	})
	require.NoError(t, err)
	require.NoError(t, h.Mount(xserver))
	httpServer.Start()
	defer httpServer.Close()
	jwk, err := credential.PublicJWK(dpopKey.PublicKey())
	require.NoError(t, err)
	thumbprint, err := credential.JWKThumbprint(jwk)
	require.NoError(t, err)
	rawCredential, err := credential.CreateSpaceCredentialToken(credential.SpaceCredentialTokenParams{
		Issuer: config.URI.Authority(), Subject: config.URI, DPoPThumbprint: thumbprint, KeyID: "#atproto", Now: now,
	}, authorityKey)
	require.NoError(t, err)
	reader, err := spaceclient.NewReaderClient(t.Context(), spaceclient.ReaderOptions{
		Space: config.URI, Resolver: resolver,
		EndpointPolicy: identity.EndpointPolicy{AllowHTTP: true, AllowPrivateLiteral: true},
		HTTPClient:     httpServer.Client(),
		Source:         staticCredentialSource{pair: spaceclient.CredentialPair{Space: config.URI, Token: rawCredential, Key: dpopKey, ExpiresAt: now.Add(time.Hour)}},
	})
	require.NoError(t, err)
	repos, err := reader.ListRepos(t.Context(), 10, "")
	require.NoError(t, err)
	assert.Empty(t, repos.Repos)
	spaceOutput, err := reader.GetSpace(t.Context())
	require.NoError(t, err)
	assert.Equal(t, config.URI.String(), spaceOutput.URI)
}

type staticCredentialSource struct{ pair spaceclient.CredentialPair }

func (s staticCredentialSource) Credential(context.Context, atmos.SpaceRef) (spaceclient.CredentialPair, error) {
	return s.pair, nil
}

type integrationResolver struct {
	docs map[atmos.DID]*identity.DIDDocument
}

func (r *integrationResolver) ResolveDID(_ context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	doc := r.docs[did]
	if doc == nil {
		return nil, fmt.Errorf("unknown DID %s", did)
	}
	copy := *doc
	copy.VerificationMethod = append([]identity.VerificationMethod(nil), doc.VerificationMethod...)
	copy.Service = append([]identity.Service(nil), doc.Service...)
	return &copy, nil
}

func (*integrationResolver) ResolveHandle(context.Context, atmos.Handle) (atmos.DID, error) {
	return "", errors.New("not implemented")
}

type integrationSigner struct {
	authority atmos.DID
	key       *crypto.P256PrivateKey
}

func (s *integrationSigner) CredentialKey(context.Context, atmos.DID) (crypto.PrivateKey, string, error) {
	return s.key, "#atproto", nil
}

func (s *integrationSigner) ServiceKey(context.Context, atmos.DID) (crypto.PrivateKey, error) {
	return s.key, nil
}

type recordingDelivery struct {
	mu       sync.Mutex
	kinds    []DeliveryKind
	notify   chan DeliveryKind
	onDelete func(context.Context) error
}

func (d *recordingDelivery) Deliver(ctx context.Context, _ *url.URL, method atmos.NSID, _ string, _ any) error {
	kind := DeliveryWrite
	if method == notificationauth.NotifySpaceDeletedMethod {
		kind = DeliverySpaceDeleted
	}
	d.mu.Lock()
	d.kinds = append(d.kinds, kind)
	onDelete := d.onDelete
	d.mu.Unlock()
	if kind == DeliverySpaceDeleted && onDelete != nil {
		if err := onDelete(ctx); err != nil {
			return err
		}
	}
	d.notify <- kind
	return nil
}

type recordingCredentialPurger struct{ calls atomic.Int32 }

func (p *recordingCredentialPurger) PurgeCredential(context.Context, atmos.SpaceRef) error {
	p.calls.Add(1)
	return nil
}

type integrationSyncSource struct {
	store   Store
	space   atmos.SpaceRef
	authors map[atmos.DID]*integrationAuthorSource
}

type integrationAuthorSource struct {
	key     *crypto.P256PrivateKey
	commit  spaces.SignedCommit
	car     []byte
	records map[spaces.RecordPath]spacesync.Record
	hostURL string
}

func newIntegrationSyncSource(t *testing.T, store Store, space atmos.SpaceRef, keys map[atmos.DID]*crypto.P256PrivateKey, now time.Time) *integrationSyncSource {
	t.Helper()
	result := &integrationSyncSource{store: store, space: space, authors: make(map[atmos.DID]*integrationAuthorSource, len(keys))}
	i := 0
	for author, key := range keys {
		path, err := spaces.ParseRecordPath(fmt.Sprintf("com.example.post/%d", i))
		require.NoError(t, err)
		blobCID := cbor.ComputeCID(cbor.CodecRaw, []byte("private blob for "+author.String()))
		data, err := cbor.Marshal(map[string]any{
			"$type": "com.example.post", "text": author.String(),
			"blob": map[string]any{"$type": "blob", "ref": blobCID, "mimeType": "text/plain", "size": int64(16)},
		})
		require.NoError(t, err)
		record := spacesync.Record{CID: cbor.ComputeCID(cbor.CodecDagCBOR, data), Data: data}
		records := map[spaces.RecordPath]spacesync.Record{path: record}
		index := spaces.RepoIndex{path: record.CID}
		repoCommit, err := spaces.NewRepoCommitFromIndex(index)
		require.NoError(t, err)
		rev := atmos.NewTID(now.Add(time.Duration(i)*time.Microsecond).UnixMicro(), uint(i))
		commit, err := repoCommit.Sign(spaces.CommitContext{Space: space, Author: author, Rev: rev}, key)
		require.NoError(t, err)
		var car bytes.Buffer
		require.NoError(t, spaces.SerializeRepoCAR(t.Context(), &car, commit, integrationSnapshot(records), spaces.CARFull, integrationSyncLimits().CAR))
		result.authors[author] = &integrationAuthorSource{key: key, commit: commit, car: car.Bytes(), records: records, hostURL: fmt.Sprintf("http://author-%d.test", i)}
		i++
	}
	return result
}

func (s *integrationSyncSource) ListRepos(ctx context.Context, limit int, _ string) (spacesync.RepoPage, error) {
	writers, err := s.store.ListWriters(ctx, s.space, "", limit)
	if err != nil {
		return spacesync.RepoPage{}, err
	}
	page := spacesync.RepoPage{Repos: make([]spacesync.RepoHint, len(writers))}
	for i, writer := range writers {
		page.Repos[i] = spacesync.RepoHint{Author: writer.DID, Revision: writer.Revision, Hash: writer.Hash}
	}
	return page, nil
}

func (s *integrationSyncSource) OpenAuthor(_ context.Context, author atmos.DID) (spacesync.AuthorSource, error) {
	source := s.authors[author]
	if source == nil {
		return nil, errors.New("unknown author")
	}
	return source, nil
}

func (s *integrationAuthorSource) ListRepoOps(context.Context, atmos.TID, int, string, bool) (spacesync.OperationPage, error) {
	return spacesync.OperationPage{}, errors.New("bootstrap must use CAR recovery")
}

func (s *integrationAuthorSource) GetLatestCommit(context.Context) (spaces.SignedCommit, error) {
	return s.commit, nil
}

func (s *integrationAuthorSource) GetRecord(_ context.Context, path spaces.RecordPath) (spacesync.Record, error) {
	record, ok := s.records[path]
	if !ok {
		return spacesync.Record{}, spacesync.ErrNotFound
	}
	return record.Clone(), nil
}

func (s *integrationAuthorSource) GetRepo(context.Context, bool, int64) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.car)), nil
}

func (s *integrationAuthorSource) Resolved() spacesync.ResolvedAuthor {
	return spacesync.ResolvedAuthor{Key: s.key.PublicKey(), Provenance: spacesync.Provenance{HostURL: s.hostURL, KeyMultibase: s.key.PublicKey().Multibase(), ResolvedAt: time.Now()}}
}

type integrationSnapshot map[spaces.RecordPath]spacesync.Record

func (s integrationSnapshot) ForEach(ctx context.Context, yield func(spaces.RecordRef) error) error {
	index := make(spaces.RepoIndex, len(s))
	for path, record := range s {
		index[path] = record.CID
	}
	for _, path := range index.Paths() {
		if err := yield(spaces.RecordRef{Path: path, CID: s[path].CID}); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (s integrationSnapshot) OpenRecord(_ context.Context, path spaces.RecordPath) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s[path].Data)), nil
}

func integrationSyncLimits() spacesync.Limits {
	return spacesync.Limits{
		PageSize: 10, DirectoryPageSize: 10, MaxPages: 10, MaxDirectoryPages: 10,
		MaxAuthors: 10, MaxOperations: 100, MaxIncrementalBytes: 1 << 20,
		MaxFinalFetches: 100, MaxRecoveryAttempts: 2, MaxRepoBytes: 1 << 20,
		PassTimeout: 5 * time.Second, CleanupTimeout: time.Second,
		CAR: spaces.CARLimits{MaxHeaderSize: 4096, MaxCommitSize: 4096, MaxIndexSize: 1 << 16, MaxRecordSize: 1 << 16, MaxTotalSize: 1 << 20, MaxRecords: 100},
	}
}

func didDoc(did atmos.DID, key *crypto.P256PrivateKey, service *identity.Service) *identity.DIDDocument {
	doc := &identity.DIDDocument{ID: did.String(), VerificationMethod: []identity.VerificationMethod{{
		ID: "#atproto", Type: "Multikey", Controller: did.String(), PublicKeyMultibase: key.PublicKey().Multibase(),
	}}}
	if service != nil {
		doc.Service = []identity.Service{*service}
	}
	return doc
}

func integrationLimits() Limits {
	return Limits{
		MaxRequestBody: 1 << 20, MaxListMembers: 100, MaxListWriters: 100,
		RegistrationTTL: time.Hour, Registration: RegistrationLimits{PerSpace: 8, PerCredential: 8, PerService: 8},
		DeliveryWorkers: 2, DeliveryBatch: 2, DeliveryLease: time.Second,
		DeliveryPoll: 5 * time.Millisecond, DeliveryTimeout: 500 * time.Millisecond,
		DeliveryRetention: time.Hour, DeliveryMaxAttempts: 3, CredentialLifetime: time.Hour,
	}
}

func requestJSON(t *testing.T, baseURL, method string, input any, authorization, dpop string, output any) int {
	t.Helper()
	body, err := json.Marshal(input)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, baseURL+"/xrpc/"+method, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	return executeRequest(t, req, authorization, dpop, output)
}

func requestQuery(t *testing.T, baseURL, method string, query url.Values, authorization, dpop string, output any) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, baseURL+"/xrpc/"+method+"?"+query.Encode(), nil)
	require.NoError(t, err)
	return executeRequest(t, req, authorization, dpop, output)
}

func executeRequest(t *testing.T, req *http.Request, authorization, dpop string, output any) int {
	t.Helper()
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	if dpop != "" {
		req.Header.Set("DPoP", dpop)
	}
	response, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	if output != nil && response.StatusCode >= 200 && response.StatusCode < 300 {
		require.NoError(t, json.NewDecoder(response.Body).Decode(output))
	} else {
		_, err = io.Copy(io.Discard, response.Body)
		require.NoError(t, err)
	}
	require.NoError(t, response.Body.Close())
	return response.StatusCode
}

func mustP256(t *testing.T) *crypto.P256PrivateKey {
	t.Helper()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	return key
}

func mustPublicJWK(t *testing.T, key *crypto.P256PrivateKey) credential.ECPublicJWK {
	t.Helper()
	jwk, err := credential.PublicJWK(key.PublicKey())
	require.NoError(t, err)
	return jwk
}

func mustDPoP(t *testing.T, key *crypto.P256PrivateKey, method, target, rawCredential string, now time.Time) string {
	t.Helper()
	proof, err := credential.CreateDPoPProof(credential.DPoPProofParams{Key: key, Method: method, TargetURL: target, Credential: rawCredential, Now: now})
	require.NoError(t, err)
	return proof
}

func awaitDelivery(t *testing.T, channel <-chan DeliveryKind) DeliveryKind {
	t.Helper()
	select {
	case kind := <-channel:
		return kind
	case <-time.After(3 * time.Second):
		require.FailNow(t, "timed out waiting for durable delivery")
		return 0
	}
}

func testConfigWithSpace(space atmos.SpaceRef) simplespace.Config {
	return simplespace.Config{URI: space, ReadPolicy: simplespace.Policy{Kind: simplespace.PolicyPublic}, WritePolicy: simplespace.Policy{Kind: simplespace.PolicyPublic}, AppAccess: simplespace.AppAccess{Kind: simplespace.AppAccessOpen}}
}
