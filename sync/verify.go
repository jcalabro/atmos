package sync

import (
	"context"
	"fmt"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/repo"
)

// VerifyCommit verifies a commit's signature by resolving the DID to get
// the signing key. Returns error if verification fails or DID can't be resolved.
func (c *Client) VerifyCommit(ctx context.Context, commit *repo.Commit) error {
	if !c.opts.Directory.HasVal() {
		return fmt.Errorf("sync: no directory configured for signature verification")
	}

	did, err := atmos.ParseDID(commit.DID)
	if err != nil {
		return fmt.Errorf("sync: invalid DID in commit: %w", err)
	}

	dir := c.opts.Directory.Val()
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
