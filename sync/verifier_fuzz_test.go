package sync_test

import (
	"errors"
	"testing"

	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/sync"
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
