package sync

import (
	"context"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/repo"
)

// LockDIDForTest exposes the per-DID lock for testing. Returns the
// unlock function. NOT for production use.
func LockDIDForTest(v *Verifier, did atmos.DID) func() {
	return v.lockDID(did)
}

// AllowResyncForTest exposes the per-DID rate limiter for testing.
func AllowResyncForTest(v *Verifier, did atmos.DID) bool {
	return v.allowResync(did)
}

// VerifyCommitSignatureForTest exposes the verifier's signature path
// for unit testing.
func VerifyCommitSignatureForTest(v *Verifier, ctx context.Context, did atmos.DID, c *repo.Commit) error {
	return v.verifyCommitSignature(ctx, did, c)
}
