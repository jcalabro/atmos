package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/identity"
	spaces "github.com/jcalabro/atmos/space"
	"github.com/jcalabro/gt"
)

// MaxBatchWrites is the pinned protocol's atomic applyWrites limit. An
// oversized batch is rejected rather than split into non-atomic requests.
const MaxBatchWrites = 200

// ValidationMode preserves the three wire states of the optional validate
// flag: omitted, explicitly required, or explicitly skipped.
type ValidationMode uint8

const (
	// ValidateKnown omits the flag, validating records whose Lexicons are known.
	ValidateKnown ValidationMode = iota
	// ValidateRequired explicitly requires Lexicon validation.
	ValidateRequired
	// ValidateSkipped explicitly disables Lexicon validation.
	ValidateSkipped
)

// WriteAction identifies a closed applyWrites operation variant.
type WriteAction uint8

const (
	WriteCreate WriteAction = iota + 1
	WriteUpdate
	WriteDelete
)

// Write is one ordered operation in an atomic permissioned-repo batch. RKey
// may be empty only for creates, where the server generates it.
type Write struct {
	Action     WriteAction
	Collection atmos.NSID
	RKey       atmos.RecordKey
	Value      json.RawMessage
}

// AccountOptions configures an account-bound spaces client.
type AccountOptions struct {
	DID             atmos.DID
	Resolver        DIDResolver
	EndpointPolicy  identity.EndpointPolicy
	HTTPClient      *http.Client
	Signer          RequestSigner
	JSONLimit       int64
	MaxReadAttempts int
}

// AccountClient exposes only operations that may use account OAuth. Repo
// methods structurally target the configured account and cannot name another
// account's permissioned repo.
type AccountClient struct {
	did      atmos.DID
	endpoint Endpoint
	engine   *engine
}

// NewAccountClient strictly resolves the account's PDS and constructs an
// endpoint-bound client. Signer is required and should provide the account's
// OAuth Authorization and DPoP headers for each request.
func NewAccountClient(ctx context.Context, opts AccountOptions) (*AccountClient, error) {
	if opts.Signer == nil {
		return nil, fmt.Errorf("space client: account request signer is required")
	}
	endpoint, err := ResolveRepoHost(ctx, opts.Resolver, opts.DID, opts.EndpointPolicy)
	if err != nil {
		return nil, err
	}
	eng, err := newEngine(engineOptions{
		HTTPClient: opts.HTTPClient, Signer: opts.Signer, JSONLimit: opts.JSONLimit,
		MaxReadAttempts: opts.MaxReadAttempts,
		NetworkPolicy:   NetworkPolicy{AllowPrivateNetworks: opts.EndpointPolicy.AllowPrivateLiteral},
	})
	if err != nil {
		return nil, err
	}
	return &AccountClient{did: opts.DID, endpoint: endpoint, engine: eng}, nil
}

// DID returns the account DID. It reveals no credential or underlying client.
func (c *AccountClient) DID() atmos.DID { return c.did }

// GetDelegationToken obtains a fresh single-use delegation from the account's
// PDS. The caller's OAuth grant must cover whole-space read; read_self is not
// sufficient for credential exchange.
func (c *AccountClient) GetDelegationToken(ctx context.Context, space atmos.SpaceRef) (string, error) {
	if err := validateSpace(space); err != nil {
		return "", err
	}
	var out comatproto.SpaceGetDelegationToken_Output
	err := c.engine.jsonOnce(ctx, http.MethodGet, c.query("com.atproto.space.getDelegationToken", url.Values{"space": {space.String()}}), nil, &out)
	if err != nil {
		return "", err
	}
	if out.Token == "" {
		return "", fmt.Errorf("space client: delegation response omitted token")
	}
	return out.Token, nil
}

// ListSpaces lists locally known spaces for this account.
func (c *AccountClient) ListSpaces(ctx context.Context, typ atmos.NSID, authority atmos.DID, limit int, cursor string) (*comatproto.SpaceListSpaces_Output, error) {
	params := url.Values{}
	if typ != "" {
		if err := typ.Validate(); err != nil {
			return nil, fmt.Errorf("space client: invalid type filter: %w", err)
		}
		params.Set("type", string(typ))
	}
	if authority != "" {
		if err := authority.Validate(); err != nil {
			return nil, fmt.Errorf("space client: invalid authority filter: %w", err)
		}
		params.Set("did", string(authority))
	}
	if err := setLimit(params, limit, 100); err != nil {
		return nil, err
	}
	setOptional(params, "cursor", cursor)
	var out comatproto.SpaceListSpaces_Output
	if err := c.engine.json(ctx, http.MethodGet, c.query("com.atproto.space.listSpaces", params), nil, &out); err != nil {
		return nil, err
	}
	seen := make(map[atmos.SpaceRef]struct{}, len(out.Spaces))
	for i := range out.Spaces {
		space, err := atmos.ParseSpaceRef(out.Spaces[i].URI)
		if err != nil {
			return nil, fmt.Errorf("space client: invalid spaces[%d].uri: %w", i, err)
		}
		if typ != "" && space.Type() != typ {
			return nil, fmt.Errorf("space client: spaces[%d].uri does not match requested type", i)
		}
		if authority != "" && space.Authority() != authority {
			return nil, fmt.Errorf("space client: spaces[%d].uri does not match requested authority", i)
		}
		if _, duplicate := seen[space]; duplicate {
			return nil, fmt.Errorf("space client: duplicate space URI %q", space)
		}
		seen[space] = struct{}{}
	}
	return &out, nil
}

// GetSpace gets simple-space policy for a space owned by this account. Account
// OAuth is never sent to an unrelated authority.
func (c *AccountClient) GetSpace(ctx context.Context, space atmos.SpaceRef) (*comatproto.SimplespaceGetSpace_Output, error) {
	if err := c.requireOwnedSpace(space); err != nil {
		return nil, err
	}
	var out comatproto.SimplespaceGetSpace_Output
	if err := c.engine.json(ctx, http.MethodGet, c.query("com.atproto.simplespace.getSpace", url.Values{"space": {space.String()}}), nil, &out); err != nil {
		return nil, err
	}
	if out.URI != space.String() {
		return nil, fmt.Errorf("space client: getSpace response URI does not match request")
	}
	return &out, nil
}

// ListMembers lists a simple-space member policy. Only the authority's account
// OAuth is eligible; a reader credential is intentionally insufficient.
func (c *AccountClient) ListMembers(ctx context.Context, space atmos.SpaceRef, limit int, cursor string) (*comatproto.SimplespaceListMembers_Output, error) {
	if err := c.requireOwnedSpace(space); err != nil {
		return nil, err
	}
	params := url.Values{"space": {space.String()}}
	if err := setLimit(params, limit, 1000); err != nil {
		return nil, err
	}
	setOptional(params, "cursor", cursor)
	var out comatproto.SimplespaceListMembers_Output
	if err := c.engine.json(ctx, http.MethodGet, c.query("com.atproto.simplespace.listMembers", params), nil, &out); err != nil {
		return nil, err
	}
	for i := range out.Members {
		if err := atmos.DID(out.Members[i].DID).Validate(); err != nil {
			return nil, fmt.Errorf("space client: invalid members[%d].did: %w", i, err)
		}
	}
	return &out, nil
}

// GetLatestCommit returns a structurally validated commit for this account's repo.
func (c *AccountClient) GetLatestCommit(ctx context.Context, space atmos.SpaceRef) (*comatproto.SpaceGetLatestCommit_Output, error) {
	if err := validateSpace(space); err != nil {
		return nil, err
	}
	var out comatproto.SpaceGetLatestCommit_Output
	params := repoParams(space, c.did)
	if err := c.engine.json(ctx, http.MethodGet, c.query("com.atproto.space.getLatestCommit", params), nil, &out); err != nil {
		return nil, err
	}
	if _, err := rawCommit(out.Commit).Validate(); err != nil {
		return nil, fmt.Errorf("space client: invalid latest commit: %w", err)
	}
	return &out, nil
}

// GetRecord gets one record from this account's repo.
func (c *AccountClient) GetRecord(ctx context.Context, space atmos.SpaceRef, collection atmos.NSID, rkey atmos.RecordKey) (*comatproto.SpaceGetRecord_Output, error) {
	if err := validateRecordCoordinates(space, collection, rkey, false); err != nil {
		return nil, err
	}
	params := repoParams(space, c.did)
	params.Set("collection", string(collection))
	params.Set("rkey", string(rkey))
	var out comatproto.SpaceGetRecord_Output
	if err := c.engine.json(ctx, http.MethodGet, c.query("com.atproto.space.getRecord", params), nil, &out); err != nil {
		return nil, err
	}
	if err := validateRecordOutput(out.URI, out.CID, space, c.did, collection, rkey); err != nil {
		return nil, err
	}
	if err := validateRecordCID(out.Value, collection, out.CID); err != nil {
		return nil, fmt.Errorf("space client: invalid record value response: %w", err)
	}
	return &out, nil
}

// ListRecords lists records in this account's permissioned repo.
func (c *AccountClient) ListRecords(ctx context.Context, space atmos.SpaceRef, collection atmos.NSID, limit int, cursor string, reverse, excludeValues bool) (*comatproto.SpaceListRecords_Output, error) {
	if err := validateSpace(space); err != nil {
		return nil, err
	}
	params := repoParams(space, c.did)
	if collection != "" {
		if err := collection.Validate(); err != nil {
			return nil, fmt.Errorf("space client: invalid collection: %w", err)
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
	if err := c.engine.json(ctx, http.MethodGet, c.query("com.atproto.space.listRecords", params), nil, &out); err != nil {
		return nil, err
	}
	if err := validateListedRecords(out.Records, collection, excludeValues); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListRepoOps lists an incremental operation page for this account's repo.
func (c *AccountClient) ListRepoOps(ctx context.Context, space atmos.SpaceRef, since atmos.TID, limit int, cursor string, excludeValues bool) (*comatproto.SpaceListRepoOps_Output, error) {
	if err := validateSpace(space); err != nil {
		return nil, err
	}
	params := repoParams(space, c.did)
	if since != "" {
		if err := since.Validate(); err != nil {
			return nil, fmt.Errorf("space client: invalid since revision: %w", err)
		}
		params.Set("since", since.String())
	}
	if err := setLimit(params, limit, 1000); err != nil {
		return nil, err
	}
	setOptional(params, "cursor", cursor)
	setBool(params, "excludeValues", excludeValues)
	var out comatproto.SpaceListRepoOps_Output
	if err := c.engine.json(ctx, http.MethodGet, c.query("com.atproto.space.listRepoOps", params), nil, &out); err != nil {
		return nil, err
	}
	if err := validateRepoOps(&out, excludeValues); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListBlobs lists blob CIDs referenced by this account's permissioned repo.
func (c *AccountClient) ListBlobs(ctx context.Context, space atmos.SpaceRef, since atmos.TID, limit int, cursor string) (*comatproto.SpaceListBlobs_Output, error) {
	if err := validateSpace(space); err != nil {
		return nil, err
	}
	params := repoParams(space, c.did)
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
	if err := c.engine.json(ctx, http.MethodGet, c.query("com.atproto.space.listBlobs", params), nil, &out); err != nil {
		return nil, err
	}
	for i, raw := range out.Cids {
		cid, err := cbor.ParseCIDString(raw)
		if err != nil || cid.Codec() != cbor.CodecRaw {
			return nil, fmt.Errorf("space client: invalid blob CID at index %d", i)
		}
	}
	return &out, nil
}

// GetRepo streams a full or index-only CAR. Verification remains provisional
// until the caller passes the complete stream to space.VerifyRepoCAR.
func (c *AccountClient) GetRepo(ctx context.Context, space atmos.SpaceRef, excludeValues bool, maxBytes int64) (io.ReadCloser, error) {
	if err := validateSpace(space); err != nil {
		return nil, err
	}
	params := repoParams(space, c.did)
	setBool(params, "excludeValues", excludeValues)
	return c.engine.stream(ctx, c.query("com.atproto.space.getRepo", params), maxBytes, nil)
}

// GetBlob streams and verifies one raw blob CID from this account's space repo.
func (c *AccountClient) GetBlob(ctx context.Context, space atmos.SpaceRef, rawCID string, maxBytes int64) (io.ReadCloser, error) {
	if err := validateSpace(space); err != nil {
		return nil, err
	}
	cid, err := cbor.ParseCIDString(rawCID)
	if err != nil || cid.Codec() != cbor.CodecRaw {
		return nil, fmt.Errorf("space client: invalid raw blob CID")
	}
	params := repoParams(space, c.did)
	params.Set("cid", cid.String())
	hash := cid.Hash()
	return c.engine.stream(ctx, c.query("com.atproto.space.getBlob", params), maxBytes, &hash)
}

// UploadBlob uploads bytes to the account's ordinary repo blob store. Upload
// and the later permissioned-record write are separate, non-atomic operations;
// this method never retries an ambiguous POST.
func (c *AccountClient) UploadBlob(ctx context.Context, contentType string, body io.Reader) (*comatproto.RepoUploadBlob_Output, error) {
	var out comatproto.RepoUploadBlob_Output
	if err := c.engine.upload(ctx, c.query("com.atproto.repo.uploadBlob", nil), contentType, body, &out); err != nil {
		return nil, err
	}
	if out.Blob.Type != "blob" || out.Blob.Size < 0 || out.Blob.MimeType == "" {
		return nil, fmt.Errorf("space client: invalid upload blob descriptor")
	}
	cid, err := cbor.ParseCIDString(out.Blob.Ref.Link)
	if err != nil || cid.Codec() != cbor.CodecRaw {
		return nil, fmt.Errorf("space client: invalid uploaded blob CID")
	}
	return &out, nil
}

// CreateRecord creates a record in this account's permissioned repo.
func (c *AccountClient) CreateRecord(ctx context.Context, space atmos.SpaceRef, collection atmos.NSID, rkey atmos.RecordKey, record json.RawMessage, mode ValidationMode) (*comatproto.SpaceCreateRecord_Output, error) {
	if err := validateRecordInput(space, collection, rkey, record, true); err != nil {
		return nil, err
	}
	validate, err := validationOption(mode)
	if err != nil {
		return nil, err
	}
	input := &comatproto.SpaceCreateRecord_Input{Repo: string(c.did), Space: space.String(), Collection: string(collection), Record: record, Validate: validate}
	if rkey != "" {
		input.Rkey = gt.Some(string(rkey))
	}
	var out comatproto.SpaceCreateRecord_Output
	if err := c.engine.json(ctx, http.MethodPost, c.query("com.atproto.space.createRecord", nil), input, &out); err != nil {
		return nil, err
	}
	if err := validateWriteOutput(out.URI, out.CID, out.ValidationStatus, space, c.did, collection, rkey); err != nil {
		return nil, err
	}
	if err := validateRecordCID(record, collection, out.CID); err != nil {
		return nil, err
	}
	return &out, nil
}

// PutRecord creates or updates a record in this account's permissioned repo.
func (c *AccountClient) PutRecord(ctx context.Context, space atmos.SpaceRef, collection atmos.NSID, rkey atmos.RecordKey, record json.RawMessage, mode ValidationMode) (*comatproto.SpacePutRecord_Output, error) {
	if err := validateRecordInput(space, collection, rkey, record, false); err != nil {
		return nil, err
	}
	validate, err := validationOption(mode)
	if err != nil {
		return nil, err
	}
	input := &comatproto.SpacePutRecord_Input{Repo: string(c.did), Space: space.String(), Collection: string(collection), Rkey: string(rkey), Record: record, Validate: validate}
	var out comatproto.SpacePutRecord_Output
	if err := c.engine.json(ctx, http.MethodPost, c.query("com.atproto.space.putRecord", nil), input, &out); err != nil {
		return nil, err
	}
	if err := validateWriteOutput(out.URI, out.CID, out.ValidationStatus, space, c.did, collection, rkey); err != nil {
		return nil, err
	}
	if err := validateRecordCID(record, collection, out.CID); err != nil {
		return nil, err
	}
	return &out, nil
}

// DeleteRecord deletes one record from this account's permissioned repo.
func (c *AccountClient) DeleteRecord(ctx context.Context, space atmos.SpaceRef, collection atmos.NSID, rkey atmos.RecordKey) error {
	if err := validateRecordCoordinates(space, collection, rkey, false); err != nil {
		return err
	}
	input := &comatproto.SpaceDeleteRecord_Input{Repo: string(c.did), Space: space.String(), Collection: string(collection), Rkey: string(rkey)}
	var out comatproto.SpaceDeleteRecord_Output
	return c.engine.json(ctx, http.MethodPost, c.query("com.atproto.space.deleteRecord", nil), input, &out)
}

// ApplyWrites sends one atomic, ordered batch without splitting or retrying it.
func (c *AccountClient) ApplyWrites(ctx context.Context, space atmos.SpaceRef, writes []Write, mode ValidationMode) (*comatproto.SpaceApplyWrites_Output, error) {
	if err := validateSpace(space); err != nil {
		return nil, err
	}
	if len(writes) > MaxBatchWrites {
		return nil, fmt.Errorf("space client: batch has %d writes, maximum is %d", len(writes), MaxBatchWrites)
	}
	validate, err := validationOption(mode)
	if err != nil {
		return nil, err
	}
	wire := make([]comatproto.SpaceApplyWrites_Input_Writes, len(writes))
	for i := range writes {
		converted, err := convertWrite(space, writes[i])
		if err != nil {
			return nil, fmt.Errorf("space client: writes[%d]: %w", i, err)
		}
		wire[i] = converted
	}
	input := &comatproto.SpaceApplyWrites_Input{Repo: string(c.did), Space: space.String(), Writes: wire, Validate: validate}
	var out comatproto.SpaceApplyWrites_Output
	if err := c.engine.json(ctx, http.MethodPost, c.query("com.atproto.space.applyWrites", nil), input, &out); err != nil {
		return nil, err
	}
	if len(out.Results) != len(writes) {
		return nil, fmt.Errorf("space client: batch returned %d results for %d writes", len(out.Results), len(writes))
	}
	if err := validateBatchResults(out.Results, writes, space, c.did); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *AccountClient) query(nsid string, params url.Values) string {
	return xrpcURL(c.endpoint.URL, nsid, params)
}

func (c *AccountClient) requireOwnedSpace(space atmos.SpaceRef) error {
	if err := validateSpace(space); err != nil {
		return err
	}
	if space.Authority() != c.did {
		return fmt.Errorf("space client: account OAuth cannot target another authority")
	}
	return nil
}

func repoParams(space atmos.SpaceRef, repo atmos.DID) url.Values {
	return url.Values{"space": {space.String()}, "repo": {string(repo)}}
}

func setOptional(values url.Values, key, value string) {
	if value != "" {
		values.Set(key, value)
	}
}

func setBool(values url.Values, key string, value bool) {
	if value {
		values.Set(key, strconv.FormatBool(value))
	}
}

func setLimit(values url.Values, limit, maximum int) error {
	if limit < 0 || limit > maximum {
		return fmt.Errorf("space client: limit %d outside 0..%d", limit, maximum)
	}
	if limit != 0 {
		values.Set("limit", strconv.Itoa(limit))
	}
	return nil
}

func rawCommit(c comatproto.SpaceDefs_SignedCommit) spaces.RawSignedCommit {
	return spaces.RawSignedCommit{Version: c.Ver, Hash: c.Hash, IKM: c.Ikm, MAC: c.Mac, Sig: c.Sig, Rev: c.Rev}
}
