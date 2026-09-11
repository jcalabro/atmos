package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	spaces "github.com/jcalabro/atmos/space"
	"github.com/jcalabro/atmos/space/client"
	"github.com/jcalabro/atmos/xrpc"
)

// RepoHint is mutable authority-directory discovery state, never a verified
// checkpoint.
type RepoHint struct {
	Author   atmos.DID
	Revision atmos.TID
	Hash     [spaces.CommitHashSize]byte
}

// RepoPage is one authority-directory page.
type RepoPage struct {
	Repos  []RepoHint
	Cursor string
}

// Operation is one ordered oplog transition. A nil CID is a delete and a nil
// Prev is a create. Body may be omitted for an intermediate or excluded value.
type Operation struct {
	Revision atmos.TID
	Path     spaces.RecordPath
	CID      *cbor.CID
	Prev     *cbor.CID
	Body     []byte
}

// OperationPage is one opaque-cursor oplog page. Commit is present only at the
// terminal head reached by this pagination pass.
type OperationPage struct {
	Operations []Operation
	Cursor     string
	Commit     *spaces.SignedCommit
}

// ResolvedAuthor is the exact direct-host/key context for verification.
type ResolvedAuthor struct {
	Key        crypto.PublicKey
	Provenance Provenance
}

// Source supplies strictly routed direct-host reads. Implementations must not
// redirect repo requests to mirrors or reuse a DPoP proof for another wire send.
type Source interface {
	ListRepos(context.Context, int, string) (RepoPage, error)
	OpenAuthor(context.Context, atmos.DID) (AuthorSource, error)
}

// AuthorSource pins every direct-host request and verification operation in one
// logical pass to a single raw identity resolution.
type AuthorSource interface {
	ListRepoOps(context.Context, atmos.TID, int, string, bool) (OperationPage, error)
	GetLatestCommit(context.Context) (spaces.SignedCommit, error)
	GetRecord(context.Context, spaces.RecordPath) (Record, error)
	GetRepo(context.Context, bool, int64) (io.ReadCloser, error)
	Resolved() ResolvedAuthor
}

// CallbackRegistrar is an optional Source capability.
type CallbackRegistrar interface {
	RegisterNotify(context.Context, string, string) (time.Time, error)
}

// CredentialPurger removes cached credential/key material after confirmed
// deletion. It is intentionally separate from ordinary access denial.
type CredentialPurger interface {
	PurgeCredential(context.Context, atmos.SpaceRef) error
}

// ReaderSource adapts the endpoint-bound Phase Two ReaderClient to Source.
type ReaderSource struct {
	Reader *client.ReaderClient
	Now    func() time.Time
	Purger CredentialPurger
}

// ListRepos fetches one writer-directory page.
func (s ReaderSource) ListRepos(ctx context.Context, limit int, cursor string) (RepoPage, error) {
	if s.Reader == nil {
		return RepoPage{}, errors.New("space sync: reader source is nil")
	}
	out, err := s.Reader.ListRepos(ctx, limit, cursor)
	if err != nil {
		return RepoPage{}, classifySourceError(err)
	}
	page := RepoPage{Repos: make([]RepoHint, len(out.Repos))}
	if out.Cursor.HasVal() {
		page.Cursor = out.Cursor.Val()
	}
	for i := range out.Repos {
		page.Repos[i].Author = atmos.DID(out.Repos[i].DID)
		page.Repos[i].Revision = atmos.TID(out.Repos[i].Rev)
		copy(page.Repos[i].Hash[:], out.Repos[i].Hash)
	}
	return page, nil
}

// OpenAuthor creates one immutable host/key binding for a logical pass.
func (s ReaderSource) OpenAuthor(ctx context.Context, author atmos.DID) (AuthorSource, error) {
	if s.Reader == nil {
		return nil, errors.New("space sync: reader source is nil")
	}
	bound, err := s.Reader.BindRepo(ctx, author)
	if err != nil {
		return nil, err
	}
	now := bound.ResolvedAt()
	if s.Now != nil {
		now = s.Now()
	}
	return &readerAuthorSource{bound: bound, resolved: ResolvedAuthor{Key: bound.VerificationKey(), Provenance: Provenance{HostURL: bound.EndpointURL(), KeyMultibase: bound.VerificationKey().Multibase(), ResolvedAt: now}}}, nil
}

type readerAuthorSource struct {
	bound    *client.RepoReader
	resolved ResolvedAuthor
}

func (s *readerAuthorSource) Resolved() ResolvedAuthor { return s.resolved }

func (s *readerAuthorSource) GetLatestCommit(ctx context.Context) (spaces.SignedCommit, error) {
	out, err := s.bound.GetLatestCommit(ctx)
	if err != nil {
		return spaces.SignedCommit{}, classifySourceError(err)
	}
	return rawCommit(out.Commit).Validate()
}

func (s *readerAuthorSource) ListRepoOps(ctx context.Context, since atmos.TID, limit int, cursor string, excludeValues bool) (OperationPage, error) {
	out, err := s.bound.ListRepoOps(ctx, since, limit, cursor, excludeValues)
	if err != nil {
		return OperationPage{}, classifySourceError(err)
	}
	page := OperationPage{Operations: make([]Operation, len(out.Ops))}
	if out.Cursor.HasVal() {
		page.Cursor = out.Cursor.Val()
	}
	if out.Commit.HasVal() {
		commit, err := rawCommit(out.Commit.Val()).Validate()
		if err != nil {
			return OperationPage{}, err
		}
		page.Commit = &commit
	}
	for i := range out.Ops {
		op := &out.Ops[i]
		path, err := spaces.ParseRecordPath(op.Collection + "/" + op.Rkey)
		if err != nil {
			return OperationPage{}, err
		}
		page.Operations[i] = Operation{Revision: atmos.TID(op.Rev), Path: path}
		if op.CID.HasVal() {
			cid, err := cbor.ParseCIDString(op.CID.Val())
			if err != nil {
				return OperationPage{}, err
			}
			page.Operations[i].CID = &cid
		}
		if op.Prev.HasVal() {
			prev, err := cbor.ParseCIDString(op.Prev.Val())
			if err != nil {
				return OperationPage{}, err
			}
			page.Operations[i].Prev = &prev
		}
		if len(op.Value) != 0 {
			body, err := canonicalRecord(op.Value)
			if err != nil {
				return OperationPage{}, err
			}
			page.Operations[i].Body = body
		}
	}
	return page, nil
}

// GetRecord obtains and canonically encodes one exact final record.
func (s *readerAuthorSource) GetRecord(ctx context.Context, path spaces.RecordPath) (Record, error) {
	out, err := s.bound.GetRecord(ctx, path)
	if err != nil {
		return Record{}, classifySourceError(err)
	}
	cid, err := cbor.ParseCIDString(out.CID)
	if err != nil {
		return Record{}, err
	}
	body, err := canonicalRecord(out.Value)
	if err != nil {
		return Record{}, err
	}
	return Record{CID: cid, Data: body}, nil
}

// GetRepo obtains a bounded CAR stream from the direct author host.
func (s *readerAuthorSource) GetRepo(ctx context.Context, indexOnly bool, maxBytes int64) (io.ReadCloser, error) {
	body, err := s.bound.GetRepo(ctx, indexOnly, maxBytes)
	if err != nil {
		return nil, classifySourceError(err)
	}
	return body, nil
}

// RegisterNotify registers the optional callback through the typed reader.
func (s ReaderSource) RegisterNotify(ctx context.Context, serviceID, serviceType string) (time.Time, error) {
	if s.Reader == nil {
		return time.Time{}, errors.New("space sync: reader source is nil")
	}
	out, err := s.Reader.RegisterNotify(ctx, serviceID, serviceType)
	if err != nil {
		return time.Time{}, classifySourceError(err)
	}
	datetime, err := atmos.ParseDatetime(out.ExpiresAt)
	if err != nil {
		return time.Time{}, err
	}
	return datetime.Time(), nil
}

// PurgeCredential delegates deletion cleanup to the configured credential
// owner. ReaderSource never silently assumes its request source is stateless.
func (s ReaderSource) PurgeCredential(ctx context.Context, space atmos.SpaceRef) error {
	if s.Purger == nil {
		return errors.New("space sync: reader source credential purger is required")
	}
	return s.Purger.PurgeCredential(ctx, space)
}

func rawCommit(c comatproto.SpaceDefs_SignedCommit) spaces.RawSignedCommit {
	return spaces.RawSignedCommit{Version: c.Ver, Hash: c.Hash, IKM: c.Ikm, MAC: c.Mac, Sig: c.Sig, Rev: c.Rev}
}

func canonicalRecord(raw json.RawMessage) ([]byte, error) {
	value, err := cbor.FromJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("space sync: decode record JSON: %w", err)
	}
	data, err := cbor.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("space sync: encode canonical record: %w", err)
	}
	return data, nil
}

func classifySourceError(err error) error {
	var xerr *xrpc.Error
	if !errors.As(err, &xerr) {
		return err
	}
	switch xerr.Name {
	case "RepoTakendown", "RepoSuspended", "RepoDeactivated":
		return fmt.Errorf("%w: %s", ErrAccountSuspended, xerr.Name)
	case "RepoNotFound":
		return ErrRepoAbsent
	}
	if xerr.StatusCode == 401 || xerr.StatusCode == 403 {
		return fmt.Errorf("%w: HTTP %d", ErrCredentialDenied, xerr.StatusCode)
	}
	return err
}
