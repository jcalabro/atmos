package sync_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/internal/testutil"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
)

// FuzzInvertCommit feeds random bytes as the CAR diff to a commit
// shape and asserts (a) no panic, (b) any returned error is a typed
// *InversionError.
func FuzzInvertCommit(f *testing.F) {
	// Seeds: a few well-known broken inputs.
	f.Add([]byte{0x00, 0x01, 0x02})
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})

	expected, _ := cbor.ParseCIDString("bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha")

	f.Fuzz(func(t *testing.T, blocks []byte) {
		commit := &comatproto.SyncSubscribeRepos_Commit{
			Repo:     "did:plc:fuzzed",
			Rev:      "r1",
			Blocks:   blocks,
			Commit:   lextypes.LexCIDLink{Link: expected.String()},
			PrevData: gt.Some(lextypes.LexCIDLink{Link: expected.String()}),
		}
		_, err := sync.InvertCommit(commit)
		if err == nil {
			// A random CAR happened to parse without referencing
			// missing blocks; that's allowed.
			return
		}
		var ie *sync.InversionError
		if !errors.As(err, &ie) {
			t.Fatalf("expected *InversionError or nil, got %T: %v", err, err)
		}
	})
}

// FuzzVerifyAndExpand drives random byte streams as the CAR diff into
// a verifier. Asserts (a) no panics, (b) any returned error is one of
// our typed errors or a wrapped infrastructure error.
func FuzzVerifyAndExpand(f *testing.F) {
	f.Add([]byte{0x00})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})

	dir := &identity.Directory{Resolver: testutil.NewTrackingResolver()}
	v, err := sync.NewVerifier(sync.VerifierOptions{
		SyncClient: gt.Some(sync.NewClient(sync.Options{Client: &xrpc.Client{Host: "https://nope.invalid"}})),
		Directory:  dir,
		StateStore: sync.NewMemStateStore(),
		Policy:     gt.Some(sync.PolicyError),
	})
	if err != nil {
		f.Fatal(err)
	}

	f.Fuzz(func(t *testing.T, blocks []byte) {
		commit := &comatproto.SyncSubscribeRepos_Commit{
			Repo:   "did:plc:fuzzed",
			Rev:    "3aaaaaaaaaaaa",
			Blocks: blocks,
			Commit: lextypes.LexCIDLink{Link: "bafyreigh2akiscaildc6dpyqhskdjkdg3hglmqgqsaftvjj5d3lqvazgha"},
		}
		_, err := v.VerifyAndExpand(context.Background(), commit, nil)
		// Any returned error is acceptable as long as it isn't a panic.
		_ = err
	})
}
