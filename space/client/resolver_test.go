package client

import (
	"context"
	"errors"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/stretchr/testify/require"
)

type resolverFunc func(context.Context, atmos.DID) (*identity.DIDDocument, error)

func (f resolverFunc) ResolveDID(ctx context.Context, did atmos.DID) (*identity.DIDDocument, error) {
	return f(ctx, did)
}

func TestResolveAuthorityHost(t *testing.T) {
	t.Parallel()
	authority := atmos.DID("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")
	base := &identity.DIDDocument{ID: string(authority), Service: []identity.Service{
		{ID: "#atproto_space_host", Type: "AtprotoSpaceHost", ServiceEndpoint: "https://space.example"},
		{ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: "https://pds.example"},
	}}
	resolve := resolverFunc(func(context.Context, atmos.DID) (*identity.DIDDocument, error) { return base, nil })

	got, err := ResolveAuthorityHost(context.Background(), resolve, authority, identity.EndpointPolicy{})
	require.NoError(t, err)
	require.Equal(t, "https://space.example", got.URL.String())
	require.Equal(t, string(authority)+"#atproto_space_host", got.Audience)

	t.Run("absent dedicated falls back", func(t *testing.T) {
		doc := *base
		doc.Service = base.Service[1:]
		got, err := ResolveAuthorityHost(context.Background(), resolverFunc(func(context.Context, atmos.DID) (*identity.DIDDocument, error) {
			return &doc, nil
		}), authority, identity.EndpointPolicy{})
		require.NoError(t, err)
		require.Equal(t, "https://pds.example", got.URL.String())
		// JWT audience remains the space-host identifier even when HTTP falls back.
		require.Equal(t, string(authority)+"#atproto_space_host", got.Audience)
	})

	t.Run("malformed dedicated never falls back", func(t *testing.T) {
		doc := *base
		doc.Service = append([]identity.Service(nil), base.Service...)
		doc.Service[0].Type = "WrongType"
		_, err := ResolveAuthorityHost(context.Background(), resolverFunc(func(context.Context, atmos.DID) (*identity.DIDDocument, error) {
			return &doc, nil
		}), authority, identity.EndpointPolicy{})
		require.ErrorIs(t, err, identity.ErrMalformedSelectedEntry)
	})
}

func TestResolveRepoAndCallbackHosts(t *testing.T) {
	t.Parallel()
	did := atmos.DID("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb")
	doc := &identity.DIDDocument{ID: string(did), Service: []identity.Service{
		{ID: "#atproto_pds", Type: "AtprotoPersonalDataServer", ServiceEndpoint: "https://repo.example"},
		{ID: string(did) + "#sync", Type: "AtprotoSpaceSyncer", ServiceEndpoint: "https://sync.example"},
	}}
	resolve := resolverFunc(func(_ context.Context, requested atmos.DID) (*identity.DIDDocument, error) {
		require.Equal(t, did, requested)
		return doc, nil
	})

	repo, err := ResolveRepoHost(context.Background(), resolve, did, identity.EndpointPolicy{})
	require.NoError(t, err)
	require.Equal(t, "https://repo.example", repo.URL.String())
	require.Equal(t, string(did), repo.Audience)

	callback, err := ResolveService(context.Background(), resolve, string(did)+"#sync", "AtprotoSpaceSyncer", identity.EndpointPolicy{})
	require.NoError(t, err)
	require.Equal(t, "https://sync.example", callback.URL.String())
	require.Equal(t, string(did)+"#sync", callback.Audience)

	bare, err := ResolveService(context.Background(), resolve, string(did), "", identity.EndpointPolicy{})
	require.NoError(t, err)
	require.Equal(t, "https://repo.example", bare.URL.String())
	require.Equal(t, string(did), bare.Audience)
}

func TestResolveHostRejectsResolverFailureAndInvalidServiceID(t *testing.T) {
	t.Parallel()
	did := atmos.DID("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb")
	boom := errors.New("resolver unavailable")
	resolve := resolverFunc(func(context.Context, atmos.DID) (*identity.DIDDocument, error) { return nil, boom })
	_, err := ResolveRepoHost(context.Background(), resolve, did, identity.EndpointPolicy{})
	require.ErrorIs(t, err, boom)

	_, err = ResolveService(context.Background(), resolve, "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb#bad#fragment", "Anything", identity.EndpointPolicy{})
	require.Error(t, err)
}
