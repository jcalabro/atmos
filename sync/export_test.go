package sync

import "github.com/jcalabro/atmos"

// LockDIDForTest exposes the per-DID lock for testing. Returns the
// unlock function. NOT for production use.
func LockDIDForTest(v *Verifier, did atmos.DID) func() {
	return v.lockDID(did)
}
