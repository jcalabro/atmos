package streaming

import (
	"testing"

	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

// These helpers are socket-free, so they live here without the
// !js && !wasip1 build constraint and are usable by tests that drive the
// client over an injected in-memory transport (e.g. dial_inject_test.go).

// buildFrame constructs an ATProto event stream frame: CBOR header + CBOR body.
func buildFrame(t string, body []byte) []byte {
	hdr := cbor.AppendMapHeader(nil, 2)
	hdr = cbor.AppendTextKey(hdr, "op")
	hdr = cbor.AppendInt(hdr, 1)
	hdr = cbor.AppendTextKey(hdr, "t")
	hdr = cbor.AppendText(hdr, t)
	return append(hdr, body...)
}

func buildIdentityBody(seq int64, did string) []byte {
	evt := &comatproto.SyncSubscribeRepos_Identity{
		LexiconTypeID: "com.atproto.sync.subscribeRepos#identity",
		DID:           did,
		Seq:           seq,
		Time:          "2024-01-01T00:00:00Z",
	}
	data, err := evt.MarshalCBOR()
	if err != nil {
		panic(err)
	}
	return data
}

func buildAccountBody(seq int64, did string, active bool) []byte {
	evt := &comatproto.SyncSubscribeRepos_Account{
		LexiconTypeID: "com.atproto.sync.subscribeRepos#account",
		DID:           did,
		Seq:           seq,
		Active:        active,
		Time:          "2024-01-01T00:00:00Z",
	}
	data, err := evt.MarshalCBOR()
	if err != nil {
		panic(err)
	}
	return data
}

func mustNewClient(t *testing.T, opts Options) *Client {
	t.Helper()
	c, err := NewClient(opts)
	require.NoError(t, err)
	return c
}
