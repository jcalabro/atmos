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

// MutexCacheLen returns the live size of the per-DID mutex cache.
// Test-only.
func MutexCacheLen(v *Verifier) int { return v.didMu.Len() }

// LimiterCacheLen returns the live size of the per-DID limiter cache.
// Test-only.
func LimiterCacheLen(v *Verifier) int { return v.limiters.Len() }

// SendAsyncErrorForTest exposes sendAsyncError to external tests.
func SendAsyncErrorForTest(v *Verifier, err error) {
	v.sendAsyncError(err)
}
