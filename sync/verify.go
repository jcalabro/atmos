package sync

import (
	"context"
	"fmt"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/repo"
)

// VerifyCommit verifies a commit's signature by resolving the DID to get
// the signing key. Returns error if verification fails or DID can't be resolved.
func (c *Client) VerifyCommit(ctx context.Context, commit *repo.Commit) error {
	if !c.opts.Directory.HasVal() {
		return fmt.Errorf("sync: no directory configured for signature verification")
	}
	return VerifyCommitWithDirectory(ctx, c.opts.Directory.Val(), commit)
}

// VerifyCommitWithDirectory verifies a commit's signature against the
// signing key resolved from dir. It is the directory-parameterized form
// of [Client.VerifyCommit], exposed so callers that hold their own
// [*identity.Directory] (e.g. the backfill engine, whose routing is
// decoupled from verification) can verify without constructing a Client
// solely to carry the directory. dir must be non-nil.
func VerifyCommitWithDirectory(ctx context.Context, dir *identity.Directory, commit *repo.Commit) error {
	if dir == nil {
		return fmt.Errorf("sync: no directory configured for signature verification")
	}

	did, err := atmos.ParseDID(commit.DID)
	if err != nil {
		return fmt.Errorf("sync: invalid DID in commit: %w", err)
	}

	id, err := dir.LookupDID(ctx, did)
	if err != nil {
		return fmt.Errorf("sync: resolving DID %s: %w", did, err)
	}

	pubkey, err := id.PublicKey()
	if err != nil {
		return fmt.Errorf("sync: getting public key for %s: %w", did, err)
	}

	if err := commit.VerifySignature(pubkey); err != nil {
		return fmt.Errorf("sync: signature verification failed for %s: %w", did, err)
	}

	return nil
}
