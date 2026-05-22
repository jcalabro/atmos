package streaming

import (
	"context"
	"testing"

	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

func TestVerifyOne_NoVerifier(t *testing.T) {
	t.Parallel()
	c := &Client{} // no verifier
	evt := Event{
		Seq:    7,
		Commit: &comatproto.SyncSubscribeRepos_Commit{Repo: "did:plc:a"},
	}
	res := c.verifyOne(context.Background(), evt)
	require.Equal(t, int64(7), res.evt.Seq)
	require.False(t, res.evt.verifierRan)
	require.False(t, res.silentDrop)
	require.NoError(t, res.hookErr)
	require.NoError(t, res.accountErr)
}

func TestVerifyOne_NilVerifierOption(t *testing.T) {
	t.Parallel()
	// Verifier set but value is nil — the "disable" sentinel.
	c := &Client{opts: Options{Verifier: gt.Some[*sync.Verifier](nil)}}
	evt := Event{
		Seq:    7,
		Commit: &comatproto.SyncSubscribeRepos_Commit{Repo: "did:plc:a"},
	}
	res := c.verifyOne(context.Background(), evt)
	require.NoError(t, res.hookErr)
	require.NoError(t, res.accountErr)
	require.False(t, res.evt.verifierRan)
	require.False(t, res.silentDrop)
}

func TestVerifyOne_LabelInfo(t *testing.T) {
	t.Parallel()
	c := &Client{}
	evt := Event{LabelInfo: &comatproto.LabelSubscribeLabels_Info{}}
	res := c.verifyOne(context.Background(), evt)
	require.NoError(t, res.hookErr)
	require.NoError(t, res.accountErr)
	require.False(t, res.evt.verifierRan)
	require.False(t, res.silentDrop)
}
