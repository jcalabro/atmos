package sync_test

import (
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
)

// ExampleNewVerifier wires a Verifier with the lenient defaults
// suitable for most consumer applications: PolicyResync recovers
// from chain breaks transparently, LegacyAccept lets non-upgraded
// upstreams flow through, and the 5-minute future-rev tolerance
// matches indigo's relay.
//
// The returned *Verifier is passed in streaming.Options.Verifier to
// enable per-event verification on the firehose.
func ExampleNewVerifier() {
	dir := &identity.Directory{Resolver: &identity.DefaultResolver{}}
	xc := &xrpc.Client{Host: "https://bsky.network"}
	sc := sync.NewClient(sync.Options{Client: xc, Directory: gt.Some(dir)})

	v, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:  dir,
		StateStore: sync.NewMemStateStore(), // production: durable store
		SyncClient: gt.Some(sc),
	})
	if err != nil {
		panic(err)
	}
	_ = v
}

// ExampleNewVerifier_strict configures the verifier in strict mode:
// non-recoverable failures surface a typed error to the consumer
// rather than being repaired by an automatic resync, and
// Sync-1.0-shape commits are rejected rather than passed through.
func ExampleNewVerifier_strict() {
	dir := &identity.Directory{Resolver: &identity.DefaultResolver{}}

	v, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:          dir,
		StateStore:         sync.NewMemStateStore(),
		Policy:             gt.Some(sync.PolicyError),
		LegacyCommitPolicy: gt.Some(sync.LegacyReject),
		OnVerificationFailure: gt.Some(func(_ atmos.DID, _ error) {
			// Log + emit metric. err is one of the typed errors in
			// this package (ChainBreakError, SignatureError, etc.).
		}),
	})
	if err != nil {
		panic(err)
	}
	_ = v
}
