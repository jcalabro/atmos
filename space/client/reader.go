package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	spaces "github.com/jcalabro/atmos/space"
	"github.com/jcalabro/atmos/space/credential"
)

// CredentialPair is an atomically published reusable space credential and its
// distinct proof-of-possession key. Callers must treat Token as sensitive.
type CredentialPair struct {
	Space     atmos.SpaceRef
	Token     string
	Key       *crypto.P256PrivateKey
	ExpiresAt time.Time
}

// CredentialSource supplies a credential/key pair for one bound space. It may
// single-flight an early refresh before returning.
type CredentialSource interface {
	Credential(ctx context.Context, space atmos.SpaceRef) (CredentialPair, error)
}

// ReaderOptions configures a credential-authenticated reader bound to one space.
type ReaderOptions struct {
	Space           atmos.SpaceRef
	Resolver        DIDResolver
	EndpointPolicy  identity.EndpointPolicy
	HTTPClient      *http.Client
	Source          CredentialSource
	JSONLimit       int64
	MaxReadAttempts int
}

// ReaderClient is bound to exactly one space. It resolves authority and author
// destinations itself and offers no arbitrary NSID, URL, token, or underlying
// authenticated-client escape hatch.
type ReaderClient struct {
	space          atmos.SpaceRef
	resolver       DIDResolver
	endpointPolicy identity.EndpointPolicy
	authority      Endpoint
	source         CredentialSource
	eng            *engine
}

// RepoReader is an immutable direct-host binding for one author. Endpoint and
// #atproto verification key are selected from the same raw DID document, so a
// logical sync pass cannot mix request routing with a later identity version.
// It exposes no credential, signer, arbitrary URL, or arbitrary XRPC method.
type RepoReader struct {
	owner      *ReaderClient
	author     atmos.DID
	endpoint   Endpoint
	key        crypto.PublicKey
	resolvedAt time.Time
}

// BindRepo resolves and binds one author's direct repo host and signing key.
func (c *ReaderClient) BindRepo(ctx context.Context, author atmos.DID) (*RepoReader, error) {
	doc, err := resolveDocument(ctx, c.resolver, author)
	if err != nil {
		return nil, err
	}
	_, destination, err := identity.SelectService(doc, author, pdsFragment, pdsServiceType, c.endpointPolicy)
	if err != nil {
		return nil, fmt.Errorf("space client: resolve repo host: %w", err)
	}
	_, key, err := identity.SelectVerificationMethod(doc, author, "atproto")
	if err != nil {
		return nil, fmt.Errorf("space client: resolve author key: %w", err)
	}
	endpoint := Endpoint{DID: author, Fragment: pdsFragment, Audience: string(author), URL: destination}
	return &RepoReader{owner: c, author: author, endpoint: endpoint, key: key, resolvedAt: time.Now()}, nil
}

// Author returns the exact author bound to this direct reader.
func (r *RepoReader) Author() atmos.DID { return r.author }

// EndpointURL returns the strictly resolved direct-host URL used by this binding.
func (r *RepoReader) EndpointURL() string { return r.endpoint.URL.String() }

// VerificationKey returns the author key selected alongside EndpointURL.
func (r *RepoReader) VerificationKey() crypto.PublicKey { return r.key }

// ResolvedAt returns when the immutable identity binding was created.
func (r *RepoReader) ResolvedAt() time.Time { return r.resolvedAt }

// GetLatestCommit gets the current structurally validated commit through this binding.
func (r *RepoReader) GetLatestCommit(ctx context.Context) (*comatproto.SpaceGetLatestCommit_Output, error) {
	eng, err := r.owner.engine()
	if err != nil {
		return nil, err
	}
	var out comatproto.SpaceGetLatestCommit_Output
	if err := eng.json(ctx, http.MethodGet, xrpcURL(r.endpoint.URL, "com.atproto.space.getLatestCommit", repoParams(r.owner.space, r.author)), nil, &out); err != nil {
		return nil, err
	}
	if _, err := rawCommit(out.Commit).Validate(); err != nil {
		return nil, fmt.Errorf("space client: invalid latest commit: %w", err)
	}
	return &out, nil
}

// ListRepoOps gets one validated operation page through this binding.
func (r *RepoReader) ListRepoOps(ctx context.Context, since atmos.TID, limit int, cursor string, excludeValues bool) (*comatproto.SpaceListRepoOps_Output, error) {
	params := repoParams(r.owner.space, r.author)
	if since != "" {
		if err := since.Validate(); err != nil {
			return nil, err
		}
		params.Set("since", since.String())
	}
	if err := setLimit(params, limit, 1000); err != nil {
		return nil, err
	}
	setOptional(params, "cursor", cursor)
	setBool(params, "excludeValues", excludeValues)
	eng, err := r.owner.engine()
	if err != nil {
		return nil, err
	}
	var out comatproto.SpaceListRepoOps_Output
	if err := eng.json(ctx, http.MethodGet, xrpcURL(r.endpoint.URL, "com.atproto.space.listRepoOps", params), nil, &out); err != nil {
		return nil, err
	}
	if err := validateRepoOps(&out, excludeValues); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetRecord gets one exact final record through this binding.
func (r *RepoReader) GetRecord(ctx context.Context, path spaces.RecordPath) (*comatproto.SpaceGetRecord_Output, error) {
	if err := path.Validate(); err != nil {
		return nil, err
	}
	params := repoParams(r.owner.space, r.author)
	params.Set("collection", path.Collection().String())
	params.Set("rkey", path.RecordKey().String())
	eng, err := r.owner.engine()
	if err != nil {
		return nil, err
	}
	var out comatproto.SpaceGetRecord_Output
	if err := eng.json(ctx, http.MethodGet, xrpcURL(r.endpoint.URL, "com.atproto.space.getRecord", params), nil, &out); err != nil {
		return nil, err
	}
	if err := validateRecordOutput(out.URI, out.CID, r.owner.space, r.author, path.Collection(), path.RecordKey()); err != nil {
		return nil, err
	}
	if err := validateRecordCID(out.Value, path.Collection(), out.CID); err != nil {
		return nil, fmt.Errorf("space client: invalid record value response: %w", err)
	}
	return &out, nil
}

// GetRepo streams a CAR through this binding.
func (r *RepoReader) GetRepo(ctx context.Context, excludeValues bool, maxBytes int64) (io.ReadCloser, error) {
	params := repoParams(r.owner.space, r.author)
	setBool(params, "excludeValues", excludeValues)
	eng, err := r.owner.engine()
	if err != nil {
		return nil, err
	}
	return eng.stream(ctx, xrpcURL(r.endpoint.URL, "com.atproto.space.getRepo", params), maxBytes, nil)
}

// NewReaderClient constructs a strictly endpoint-bound space reader.
func NewReaderClient(ctx context.Context, opts ReaderOptions) (*ReaderClient, error) {
	if err := validateSpace(opts.Space); err != nil {
		return nil, err
	}
	if opts.Source == nil {
		return nil, fmt.Errorf("space client: credential source is required")
	}
	authority, err := ResolveAuthorityHost(ctx, opts.Resolver, opts.Space.Authority(), opts.EndpointPolicy)
	if err != nil {
		return nil, err
	}
	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = NewCorrectnessHTTPClient(NetworkPolicy{})
	}
	reader := &ReaderClient{space: opts.Space, resolver: opts.Resolver, endpointPolicy: opts.EndpointPolicy, authority: authority, source: opts.Source}
	eng, err := newEngine(engineOptions{
		HTTPClient: httpClient, Signer: RequestSignerFunc(reader.signRequest),
		JSONLimit: opts.JSONLimit, MaxReadAttempts: opts.MaxReadAttempts,
		NetworkPolicy: NetworkPolicy{AllowPrivateNetworks: opts.EndpointPolicy.AllowPrivateLiteral},
	})
	if err != nil {
		return nil, err
	}
	reader.eng = eng
	return reader, nil
}

// Space returns the immutable space identity to which this reader is bound.
func (c *ReaderClient) Space() atmos.SpaceRef { return c.space }

// ListRepos lists the authority's mutable writer directory.
func (c *ReaderClient) ListRepos(ctx context.Context, limit int, cursor string) (*comatproto.SpaceListRepos_Output, error) {
	params := url.Values{"space": {c.space.String()}}
	if err := setLimit(params, limit, 1000); err != nil {
		return nil, err
	}
	setOptional(params, "cursor", cursor)
	eng, err := c.engine()
	if err != nil {
		return nil, err
	}
	var out comatproto.SpaceListRepos_Output
	if err := eng.json(ctx, http.MethodGet, xrpcURL(c.authority.URL, "com.atproto.space.listRepos", params), nil, &out); err != nil {
		return nil, err
	}
	seen := make(map[atmos.DID]struct{}, len(out.Repos))
	for i := range out.Repos {
		did := atmos.DID(out.Repos[i].DID)
		if err := did.Validate(); err != nil {
			return nil, fmt.Errorf("space client: invalid repos[%d].did: %w", i, err)
		}
		if _, duplicate := seen[did]; duplicate {
			return nil, fmt.Errorf("space client: duplicate repo DID %q", did)
		}
		seen[did] = struct{}{}
		if err := atmos.TID(out.Repos[i].Rev).Validate(); err != nil {
			return nil, fmt.Errorf("space client: invalid repos[%d].rev: %w", i, err)
		}
		if len(out.Repos[i].Hash) != 32 {
			return nil, fmt.Errorf("space client: repos[%d].hash must be 32 bytes", i)
		}
	}
	return &out, nil
}

// GetSpace gets policy for the reader's bound simple space.
func (c *ReaderClient) GetSpace(ctx context.Context) (*comatproto.SimplespaceGetSpace_Output, error) {
	eng, err := c.engine()
	if err != nil {
		return nil, err
	}
	var out comatproto.SimplespaceGetSpace_Output
	params := url.Values{"space": {c.space.String()}}
	if err := eng.json(ctx, http.MethodGet, xrpcURL(c.authority.URL, "com.atproto.simplespace.getSpace", params), nil, &out); err != nil {
		return nil, err
	}
	if out.URI != c.space.String() {
		return nil, fmt.Errorf("space client: getSpace response URI does not match bound space")
	}
	return &out, nil
}

// RegisterNotify registers a strictly resolved callback service with the authority.
func (c *ReaderClient) RegisterNotify(ctx context.Context, serviceID, serviceType string) (*comatproto.SpaceRegisterNotify_Output, error) {
	if _, err := ResolveService(ctx, c.resolver, serviceID, serviceType, c.endpointPolicy); err != nil {
		return nil, err
	}
	eng, err := c.engine()
	if err != nil {
		return nil, err
	}
	input := &comatproto.SpaceRegisterNotify_Input{Space: c.space.String(), Service: serviceID}
	var out comatproto.SpaceRegisterNotify_Output
	if err := eng.json(ctx, http.MethodPost, xrpcURL(c.authority.URL, "com.atproto.space.registerNotify", nil), input, &out); err != nil {
		return nil, err
	}
	if _, err := atmos.ParseDatetime(out.ExpiresAt); err != nil {
		return nil, fmt.Errorf("space client: invalid registration expiry: %w", err)
	}
	return &out, nil
}

// UnregisterNotify removes a strictly resolved callback registration.
func (c *ReaderClient) UnregisterNotify(ctx context.Context, serviceID, serviceType string) error {
	if _, err := ResolveService(ctx, c.resolver, serviceID, serviceType, c.endpointPolicy); err != nil {
		return err
	}
	eng, err := c.engine()
	if err != nil {
		return err
	}
	input := &comatproto.SpaceUnregisterNotify_Input{Space: c.space.String(), Service: serviceID}
	return eng.json(ctx, http.MethodPost, xrpcURL(c.authority.URL, "com.atproto.space.unregisterNotify", nil), input, nil)
}

// GetRecord gets one record directly from its author's resolved repo host.
func (c *ReaderClient) GetRecord(ctx context.Context, author atmos.DID, collection atmos.NSID, rkey atmos.RecordKey) (*comatproto.SpaceGetRecord_Output, error) {
	if err := validateRecordCoordinates(c.space, collection, rkey, false); err != nil {
		return nil, err
	}
	endpoint, eng, err := c.repoEngine(ctx, author)
	if err != nil {
		return nil, err
	}
	params := repoParams(c.space, author)
	params.Set("collection", string(collection))
	params.Set("rkey", string(rkey))
	var out comatproto.SpaceGetRecord_Output
	if err := eng.json(ctx, http.MethodGet, xrpcURL(endpoint.URL, "com.atproto.space.getRecord", params), nil, &out); err != nil {
		return nil, err
	}
	if err := validateRecordOutput(out.URI, out.CID, c.space, author, collection, rkey); err != nil {
		return nil, err
	}
	if err := validateRecordCID(out.Value, collection, out.CID); err != nil {
		return nil, fmt.Errorf("space client: invalid record value response: %w", err)
	}
	return &out, nil
}

// GetLatestCommit gets one structurally validated signed commit.
func (c *ReaderClient) GetLatestCommit(ctx context.Context, author atmos.DID) (*comatproto.SpaceGetLatestCommit_Output, error) {
	endpoint, eng, err := c.repoEngine(ctx, author)
	if err != nil {
		return nil, err
	}
	var out comatproto.SpaceGetLatestCommit_Output
	if err := eng.json(ctx, http.MethodGet, xrpcURL(endpoint.URL, "com.atproto.space.getLatestCommit", repoParams(c.space, author)), nil, &out); err != nil {
		return nil, err
	}
	if _, err := rawCommit(out.Commit).Validate(); err != nil {
		return nil, fmt.Errorf("space client: invalid latest commit: %w", err)
	}
	return &out, nil
}

// ListRecords lists one author's permissioned records.
func (c *ReaderClient) ListRecords(ctx context.Context, author atmos.DID, collection atmos.NSID, limit int, cursor string, reverse, excludeValues bool) (*comatproto.SpaceListRecords_Output, error) {
	endpoint, eng, err := c.repoEngine(ctx, author)
	if err != nil {
		return nil, err
	}
	params := repoParams(c.space, author)
	if collection != "" {
		if err := collection.Validate(); err != nil {
			return nil, err
		}
		params.Set("collection", string(collection))
	}
	if err := setLimit(params, limit, 1000); err != nil {
		return nil, err
	}
	setOptional(params, "cursor", cursor)
	setBool(params, "reverse", reverse)
	setBool(params, "excludeValues", excludeValues)
	var out comatproto.SpaceListRecords_Output
	if err := eng.json(ctx, http.MethodGet, xrpcURL(endpoint.URL, "com.atproto.space.listRecords", params), nil, &out); err != nil {
		return nil, err
	}
	if err := validateListedRecords(out.Records, collection, excludeValues); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListRepoOps lists one author's incremental operation page.
func (c *ReaderClient) ListRepoOps(ctx context.Context, author atmos.DID, since atmos.TID, limit int, cursor string, excludeValues bool) (*comatproto.SpaceListRepoOps_Output, error) {
	endpoint, eng, err := c.repoEngine(ctx, author)
	if err != nil {
		return nil, err
	}
	params := repoParams(c.space, author)
	if since != "" {
		if err := since.Validate(); err != nil {
			return nil, err
		}
		params.Set("since", since.String())
	}
	if err := setLimit(params, limit, 1000); err != nil {
		return nil, err
	}
	setOptional(params, "cursor", cursor)
	setBool(params, "excludeValues", excludeValues)
	var out comatproto.SpaceListRepoOps_Output
	if err := eng.json(ctx, http.MethodGet, xrpcURL(endpoint.URL, "com.atproto.space.listRepoOps", params), nil, &out); err != nil {
		return nil, err
	}
	if err := validateRepoOps(&out, excludeValues); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListBlobs lists one author's raw blob CIDs.
func (c *ReaderClient) ListBlobs(ctx context.Context, author atmos.DID, since atmos.TID, limit int, cursor string) (*comatproto.SpaceListBlobs_Output, error) {
	endpoint, eng, err := c.repoEngine(ctx, author)
	if err != nil {
		return nil, err
	}
	params := repoParams(c.space, author)
	if since != "" {
		if err := since.Validate(); err != nil {
			return nil, err
		}
		params.Set("since", since.String())
	}
	if err := setLimit(params, limit, 1000); err != nil {
		return nil, err
	}
	setOptional(params, "cursor", cursor)
	var out comatproto.SpaceListBlobs_Output
	if err := eng.json(ctx, http.MethodGet, xrpcURL(endpoint.URL, "com.atproto.space.listBlobs", params), nil, &out); err != nil {
		return nil, err
	}
	for i, value := range out.Cids {
		cid, err := cbor.ParseCIDString(value)
		if err != nil || cid.Codec() != cbor.CodecRaw {
			return nil, fmt.Errorf("space client: invalid blob CID at index %d", i)
		}
	}
	return &out, nil
}

// GetRepo streams one author's CAR directly from its resolved repo host.
func (c *ReaderClient) GetRepo(ctx context.Context, author atmos.DID, excludeValues bool, maxBytes int64) (io.ReadCloser, error) {
	endpoint, eng, err := c.repoEngine(ctx, author)
	if err != nil {
		return nil, err
	}
	params := repoParams(c.space, author)
	setBool(params, "excludeValues", excludeValues)
	return eng.stream(ctx, xrpcURL(endpoint.URL, "com.atproto.space.getRepo", params), maxBytes, nil)
}

// GetBlob streams and verifies one author's blob directly from its repo host.
func (c *ReaderClient) GetBlob(ctx context.Context, author atmos.DID, rawCID string, maxBytes int64) (io.ReadCloser, error) {
	cid, err := cbor.ParseCIDString(rawCID)
	if err != nil || cid.Codec() != cbor.CodecRaw {
		return nil, fmt.Errorf("space client: invalid raw blob CID")
	}
	endpoint, eng, err := c.repoEngine(ctx, author)
	if err != nil {
		return nil, err
	}
	params := repoParams(c.space, author)
	params.Set("cid", cid.String())
	hash := cid.Hash()
	return eng.stream(ctx, xrpcURL(endpoint.URL, "com.atproto.space.getBlob", params), maxBytes, &hash)
}

func (c *ReaderClient) repoEngine(ctx context.Context, author atmos.DID) (Endpoint, *engine, error) {
	endpoint, err := ResolveRepoHost(ctx, c.resolver, author, c.endpointPolicy)
	if err != nil {
		return Endpoint{}, nil, err
	}
	eng, err := c.engine()
	return endpoint, eng, err
}

func (c *ReaderClient) engine() (*engine, error) {
	if c.eng == nil {
		return nil, fmt.Errorf("space client: reader engine is not initialized")
	}
	return c.eng, nil
}

func (c *ReaderClient) signRequest(ctx context.Context, method, targetURL string) (http.Header, error) {
	pair, err := c.source.Credential(ctx, c.space)
	if err != nil {
		return nil, err
	}
	if pair.Space != c.space || pair.Token == "" || pair.Key == nil {
		return nil, fmt.Errorf("space client: credential source returned an invalid pair")
	}
	proof, err := credential.CreateDPoPProof(credential.DPoPProofParams{Key: pair.Key, Method: method, TargetURL: targetURL, Credential: pair.Token})
	if err != nil {
		return nil, err
	}
	return http.Header{"Authorization": {"DPoP " + pair.Token}, "DPoP": {proof}}, nil
}
