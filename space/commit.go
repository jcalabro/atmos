package space

import (
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	atmoscrypto "github.com/jcalabro/atmos/crypto"
)

const (
	// CommitVersion is the only supported permissioned-repo commit version.
	CommitVersion int64 = 1
	// CommitHashSize is the required repository digest size.
	CommitHashSize = sha256.Size
	// CommitIKMSize is the required per-reader commit key size.
	CommitIKMSize = 32
	// CommitMACSize is the required HMAC-SHA256 size.
	CommitMACSize = sha256.Size
	// CommitSignatureSize is the compact R||S signature size for P-256 and K-256.
	CommitSignatureSize = 64
)

const commitDomain = "atproto-space-v1"

// CommitContext binds a signed commit to its space, author, and revision. The
// space and author are external authenticated context and are not encoded in a
// SignedCommit.
type CommitContext struct {
	Space  atmos.SpaceRef
	Author atmos.DID
	Rev    atmos.TID
}

// NewCommitContext parses and validates a commit context.
func NewCommitContext(space, author, rev string) (CommitContext, error) {
	spaceRef, err := atmos.ParseSpaceRef(space)
	if err != nil {
		return CommitContext{}, fmt.Errorf("space: invalid commit space: %w", err)
	}
	authorDID, err := atmos.ParseDID(author)
	if err != nil {
		return CommitContext{}, fmt.Errorf("space: invalid commit author: %w", err)
	}
	revision, err := atmos.ParseTID(rev)
	if err != nil {
		return CommitContext{}, fmt.Errorf("space: invalid commit revision: %w", err)
	}
	return CommitContext{Space: spaceRef, Author: authorDID, Rev: revision}, nil
}

// Validate verifies every coordinate of the context.
func (c CommitContext) Validate() error {
	if err := c.Space.Validate(); err != nil {
		return fmt.Errorf("space: invalid commit space: %w", err)
	}
	if err := c.Author.Validate(); err != nil {
		return fmt.Errorf("space: invalid commit author: %w", err)
	}
	if err := c.Rev.Validate(); err != nil {
		return fmt.Errorf("space: invalid commit revision: %w", err)
	}
	return nil
}

// Bytes returns the exact authenticated context encoding for ikm. Length
// prefixes are uint16 big-endian and count UTF-8 bytes.
func (c CommitContext) Bytes(ikm [CommitIKMSize]byte) ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	fields := [][]byte{[]byte(c.Space), []byte(c.Author), []byte(c.Rev), ikm[:]}
	size := len(commitDomain)
	for _, field := range fields {
		if len(field) > 0xffff {
			return nil, errors.New("space: commit context field exceeds uint16 length prefix")
		}
		size += 2 + len(field)
	}
	out := make([]byte, 0, size)
	out = append(out, commitDomain...)
	for _, field := range fields {
		out = append(out, byte(len(field)>>8), byte(len(field)))
		out = append(out, field...)
	}
	return out, nil
}

// RawSignedCommit is the untrusted, variable-length representation returned by
// the CBOR decoder. Call Validate to obtain a SignedCommit.
type RawSignedCommit struct {
	Version int64
	Hash    []byte
	IKM     []byte
	MAC     []byte
	Sig     []byte
	Rev     string
}

// Clone returns a deep copy.
func (c RawSignedCommit) Clone() RawSignedCommit {
	c.Hash = append([]byte(nil), c.Hash...)
	c.IKM = append([]byte(nil), c.IKM...)
	c.MAC = append([]byte(nil), c.MAC...)
	c.Sig = append([]byte(nil), c.Sig...)
	return c
}

// SignedCommit is a structurally validated permissioned-repo commit. It is not
// authentic until VerifyCommit succeeds.
type SignedCommit struct {
	Version   int64
	Hash      [CommitHashSize]byte
	IKM       [CommitIKMSize]byte
	MAC       [CommitMACSize]byte
	Signature [CommitSignatureSize]byte
	Rev       atmos.TID
}

// Validate verifies fields that can be checked without external context or a
// public key.
func (c SignedCommit) Validate() error {
	if c.Version != CommitVersion {
		return fmt.Errorf("space: unsupported commit version %d", c.Version)
	}
	if err := c.Rev.Validate(); err != nil {
		return fmt.Errorf("space: invalid signed commit revision: %w", err)
	}
	return nil
}

// Validate checks raw field lengths and syntax and returns a fixed-size commit.
func (c RawSignedCommit) Validate() (SignedCommit, error) {
	if c.Version != CommitVersion {
		return SignedCommit{}, fmt.Errorf("space: unsupported commit version %d", c.Version)
	}
	if len(c.Hash) != CommitHashSize {
		return SignedCommit{}, fmt.Errorf("space: commit hash must be %d bytes, got %d", CommitHashSize, len(c.Hash))
	}
	if len(c.IKM) != CommitIKMSize {
		return SignedCommit{}, fmt.Errorf("space: commit ikm must be %d bytes, got %d", CommitIKMSize, len(c.IKM))
	}
	if len(c.MAC) != CommitMACSize {
		return SignedCommit{}, fmt.Errorf("space: commit mac must be %d bytes, got %d", CommitMACSize, len(c.MAC))
	}
	if len(c.Sig) != CommitSignatureSize {
		return SignedCommit{}, fmt.Errorf("space: commit signature must be %d bytes, got %d", CommitSignatureSize, len(c.Sig))
	}
	rev, err := atmos.ParseTID(c.Rev)
	if err != nil {
		return SignedCommit{}, fmt.Errorf("space: invalid signed commit revision: %w", err)
	}
	out := SignedCommit{Version: c.Version, Rev: rev}
	copy(out.Hash[:], c.Hash)
	copy(out.IKM[:], c.IKM)
	copy(out.MAC[:], c.MAC)
	copy(out.Signature[:], c.Sig)
	return out, nil
}

// EncodeCBOR returns the canonical DAG-CBOR encoding of c.
func (c SignedCommit) EncodeCBOR() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 256)
	buf = cbor.AppendMapHeader(buf, 6)
	// DAG-CBOR key order: length first, then bytewise.
	buf = cbor.AppendText(buf, "ikm")
	buf = cbor.AppendBytes(buf, c.IKM[:])
	buf = cbor.AppendText(buf, "mac")
	buf = cbor.AppendBytes(buf, c.MAC[:])
	buf = cbor.AppendText(buf, "rev")
	buf = cbor.AppendText(buf, c.Rev.String())
	buf = cbor.AppendText(buf, "sig")
	buf = cbor.AppendBytes(buf, c.Signature[:])
	buf = cbor.AppendText(buf, "ver")
	buf = cbor.AppendInt(buf, c.Version)
	buf = cbor.AppendText(buf, "hash")
	buf = cbor.AppendBytes(buf, c.Hash[:])
	return buf, nil
}

// DecodeSignedCommit decodes canonical DAG-CBOR into an untrusted raw commit.
// Unknown fields are rejected rather than discarded.
func DecodeSignedCommit(data []byte) (RawSignedCommit, error) {
	value, err := cbor.Unmarshal(data)
	if err != nil {
		return RawSignedCommit{}, fmt.Errorf("space: decode signed commit: %w", err)
	}
	m, ok := value.(map[string]any)
	if !ok {
		return RawSignedCommit{}, errors.New("space: signed commit must be a map")
	}
	allowed := map[string]struct{}{"ver": {}, "hash": {}, "ikm": {}, "mac": {}, "sig": {}, "rev": {}}
	for key := range m {
		if _, ok := allowed[key]; !ok {
			return RawSignedCommit{}, fmt.Errorf("space: signed commit has unknown field %q", key)
		}
	}
	if len(m) != len(allowed) {
		return RawSignedCommit{}, errors.New("space: signed commit is missing required fields")
	}
	version, ok := m["ver"].(int64)
	if !ok {
		return RawSignedCommit{}, errors.New("space: signed commit ver must be an integer")
	}
	hash, ok := m["hash"].([]byte)
	if !ok {
		return RawSignedCommit{}, errors.New("space: signed commit hash must be bytes")
	}
	ikm, ok := m["ikm"].([]byte)
	if !ok {
		return RawSignedCommit{}, errors.New("space: signed commit ikm must be bytes")
	}
	mac, ok := m["mac"].([]byte)
	if !ok {
		return RawSignedCommit{}, errors.New("space: signed commit mac must be bytes")
	}
	sig, ok := m["sig"].([]byte)
	if !ok {
		return RawSignedCommit{}, errors.New("space: signed commit sig must be bytes")
	}
	rev, ok := m["rev"].(string)
	if !ok {
		return RawSignedCommit{}, errors.New("space: signed commit rev must be text")
	}
	return RawSignedCommit{Version: version, Hash: hash, IKM: ikm, MAC: mac, Sig: sig, Rev: rev}, nil
}

// VerifiedCommit is a commit whose MAC and signature have been verified in its
// external context.
type VerifiedCommit struct {
	SignedCommit SignedCommit
	Context      CommitContext
}

// VerifyCommit verifies structural validity, revision binding, the symmetric
// MAC, and the author's compact EC signature.
func VerifyCommit(commit SignedCommit, ctx CommitContext, key atmoscrypto.PublicKey) (VerifiedCommit, error) {
	verified, err := verifyCommit(commit, ctx, key)
	if err != nil {
		return VerifiedCommit{}, wrapVerification("commit", err)
	}
	return verified, nil
}

func verifyCommit(commit SignedCommit, ctx CommitContext, key atmoscrypto.PublicKey) (VerifiedCommit, error) {
	if err := commit.Validate(); err != nil {
		return VerifiedCommit{}, err
	}
	if err := ctx.Validate(); err != nil {
		return VerifiedCommit{}, err
	}
	if nilInterface(key) {
		return VerifiedCommit{}, errors.New("space: commit verification key is nil")
	}
	if commit.Rev != ctx.Rev {
		return VerifiedCommit{}, errors.New("space: signed commit revision does not match context")
	}
	ctxBytes, err := ctx.Bytes(commit.IKM)
	if err != nil {
		return VerifiedCommit{}, err
	}
	wantMAC, err := computeCommitMAC(commit.IKM, ctxBytes, commit.Hash)
	if err != nil {
		return VerifiedCommit{}, fmt.Errorf("space: compute commit MAC: %w", err)
	}
	if subtle.ConstantTimeCompare(wantMAC[:], commit.MAC[:]) != 1 {
		return VerifiedCommit{}, errors.New("space: commit MAC verification failed")
	}
	if err := key.HashAndVerify(ctxBytes, commit.Signature[:]); err != nil {
		return VerifiedCommit{}, fmt.Errorf("space: commit signature verification failed: %w", err)
	}
	return VerifiedCommit{SignedCommit: commit, Context: ctx}, nil
}

func computeCommitMAC(ikm [CommitIKMSize]byte, ctxBytes []byte, hash [CommitHashSize]byte) ([CommitMACSize]byte, error) {
	key, err := hkdf.Expand(sha256.New, ikm[:], string(ctxBytes), sha256.Size)
	if err != nil {
		return [CommitMACSize]byte{}, err
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(hash[:])
	var out [CommitMACSize]byte
	copy(out[:], mac.Sum(nil))
	return out, nil
}

// RepoCommit incrementally maintains a permissioned repository LtHash.
type RepoCommit struct {
	setHash LtHash
}

// NewRepoCommit returns an empty repository commitment.
func NewRepoCommit() *RepoCommit { return &RepoCommit{} }

// NewRepoCommitFromState restores a repository commitment from a full LtHash
// state. A nil state means an empty repository; a non-nil state must be exact.
func NewRepoCommitFromState(state []byte) (*RepoCommit, error) {
	if state == nil {
		return NewRepoCommit(), nil
	}
	hash, err := NewLtHashFromState(state)
	if err != nil {
		return nil, err
	}
	return &RepoCommit{setHash: *hash}, nil
}

// State returns the full resumable LtHash state.
func (r *RepoCommit) State() [LtHashStateSize]byte { return r.setHash.State() }

// Digest returns the current repository digest.
func (r *RepoCommit) Digest() [CommitHashSize]byte { return r.setHash.Digest() }

// Add adds one path/CID tuple after validating all components.
func (r *RepoCommit) Add(collection, rkey string, cid cbor.CID) error {
	element, err := formatSetHashElement(collection, rkey, cid)
	if err != nil {
		return err
	}
	r.setHash.Add(element)
	return nil
}

// Remove subtracts one path/CID tuple after validating all components.
func (r *RepoCommit) Remove(collection, rkey string, cid cbor.CID) error {
	element, err := formatSetHashElement(collection, rkey, cid)
	if err != nil {
		return err
	}
	r.setHash.Remove(element)
	return nil
}

func formatSetHashElement(collection, rkey string, cid cbor.CID) ([]byte, error) {
	if _, err := atmos.ParseNSID(collection); err != nil {
		return nil, fmt.Errorf("space: invalid record collection: %w", err)
	}
	if _, err := atmos.ParseRecordKey(rkey); err != nil {
		return nil, fmt.Errorf("space: invalid record key: %w", err)
	}
	if !cid.Defined() || cid.Codec() != cbor.CodecDagCBOR {
		return nil, errors.New("space: record CID must be a defined dag-cbor CID")
	}
	return []byte(collection + "/" + rkey + "/" + cid.String()), nil
}

// RepoOp describes one repository transition. Prev is absent for a create;
// CID is absent for a delete; both are present for an update.
type RepoOp struct {
	Collection string
	RKey       string
	CID        *cbor.CID
	Prev       *cbor.CID
}

// Apply applies one operation. The durable store remains responsible for
// enforcing that Prev is the one current CID and that an operation is not
// replayed.
func (r *RepoCommit) Apply(op RepoOp) error {
	if op.Prev == nil && op.CID == nil {
		return errors.New("space: repo operation must have prev or cid")
	}
	var (
		prevElement []byte
		cidElement  []byte
		err         error
	)
	if op.Prev != nil {
		prevElement, err = formatSetHashElement(op.Collection, op.RKey, *op.Prev)
		if err != nil {
			return err
		}
	}
	if op.CID != nil {
		cidElement, err = formatSetHashElement(op.Collection, op.RKey, *op.CID)
		if err != nil {
			return err
		}
	}
	// Both halves are validated before either mutation, so failures are atomic.
	if prevElement != nil {
		r.setHash.Remove(prevElement)
	}
	if cidElement != nil {
		r.setHash.Add(cidElement)
	}
	return nil
}

// Matches reports whether the current digest equals the commit's claim.
func (r *RepoCommit) Matches(commit SignedCommit) bool {
	digest := r.Digest()
	return subtle.ConstantTimeCompare(digest[:], commit.Hash[:]) == 1
}

// Sign creates a commit with a fresh random IKM.
func (r *RepoCommit) Sign(ctx CommitContext, key atmoscrypto.PrivateKey) (SignedCommit, error) {
	return r.SignWithRandom(ctx, key, rand.Reader)
}

// SignWithRandom is like Sign but obtains the IKM from random. It exists for
// deterministic interoperability fixtures and injectable entropy sources.
func (r *RepoCommit) SignWithRandom(ctx CommitContext, key atmoscrypto.PrivateKey, random io.Reader) (SignedCommit, error) {
	if err := ctx.Validate(); err != nil {
		return SignedCommit{}, err
	}
	if nilInterface(key) {
		return SignedCommit{}, errors.New("space: commit signing key is nil")
	}
	if random == nil {
		return SignedCommit{}, errors.New("space: commit randomness source is nil")
	}
	commit := SignedCommit{Version: CommitVersion, Hash: r.Digest(), Rev: ctx.Rev}
	if _, err := io.ReadFull(random, commit.IKM[:]); err != nil {
		return SignedCommit{}, fmt.Errorf("space: read commit randomness: %w", err)
	}
	ctxBytes, err := ctx.Bytes(commit.IKM)
	if err != nil {
		return SignedCommit{}, err
	}
	commit.MAC, err = computeCommitMAC(commit.IKM, ctxBytes, commit.Hash)
	if err != nil {
		return SignedCommit{}, fmt.Errorf("space: compute commit MAC: %w", err)
	}
	sig, err := key.HashAndSign(ctxBytes)
	if err != nil {
		return SignedCommit{}, fmt.Errorf("space: sign commit: %w", err)
	}
	if len(sig) != CommitSignatureSize {
		return SignedCommit{}, fmt.Errorf("space: signer returned %d-byte signature, want %d", len(sig), CommitSignatureSize)
	}
	copy(commit.Signature[:], sig)
	return commit, nil
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
