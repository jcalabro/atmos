package labeling

import (
	"sync"
	"testing"
	"time"

	atmos "github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/crypto"

	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testTime = time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

func TestSignVerify_P256(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	label := NewAt(atmos.DID("did:plc:test"), "at://did:plc:test/app.bsky.feed.post/abc", "spam", testTime)
	require.NoError(t, Sign(label, key))
	assert.NotEmpty(t, label.Sig)
	require.NoError(t, Verify(label, key.PublicKey()))
}

func TestSignVerify_K256(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateK256()
	require.NoError(t, err)

	label := NewAt(atmos.DID("did:plc:test"), "at://did:plc:test/app.bsky.feed.post/abc", "spam", testTime)
	require.NoError(t, Sign(label, key))
	require.NoError(t, Verify(label, key.PublicKey()))
}

func TestVerify_TamperedSignature(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	label := NewAt(atmos.DID("did:plc:test"), "at://did:plc:test/app.bsky.feed.post/abc", "spam", testTime)
	require.NoError(t, Sign(label, key))

	label.Sig[0] ^= 0xff
	assert.Error(t, Verify(label, key.PublicKey()))
}

func TestVerify_WrongKey(t *testing.T) {
	t.Parallel()
	key1, err := crypto.GenerateP256()
	require.NoError(t, err)
	key2, err := crypto.GenerateP256()
	require.NoError(t, err)

	label := NewAt(atmos.DID("did:plc:test"), "at://did:plc:test/app.bsky.feed.post/abc", "spam", testTime)
	require.NoError(t, Sign(label, key1))
	assert.Error(t, Verify(label, key2.PublicKey()))
}

func TestVerify_WrongCurve(t *testing.T) {
	t.Parallel()
	p256Key, err := crypto.GenerateP256()
	require.NoError(t, err)
	k256Key, err := crypto.GenerateK256()
	require.NoError(t, err)

	label := NewAt(atmos.DID("did:plc:test"), "at://did:plc:test/app.bsky.feed.post/abc", "spam", testTime)
	require.NoError(t, Sign(label, p256Key))
	assert.Error(t, Verify(label, k256Key.PublicKey()))
}

func TestVerify_Unsigned(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	label := NewAt(atmos.DID("did:plc:test"), "at://did:plc:test/app.bsky.feed.post/abc", "spam", testTime)
	err = Verify(label, key.PublicKey())
	require.ErrorIs(t, err, ErrNotSigned)
}

func TestNegateSignVerify(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	label := NegateAt(atmos.DID("did:plc:test"), "at://did:plc:test/app.bsky.feed.post/abc", "spam", testTime)
	assert.True(t, label.Neg.HasVal())
	assert.True(t, label.Neg.Val())
	assert.Equal(t, gt.Some(int64(1)), label.Ver)

	require.NoError(t, Sign(label, key))
	require.NoError(t, Verify(label, key.PublicKey()))
}

func TestNewAt_Fields(t *testing.T) {
	t.Parallel()
	label := NewAt(atmos.DID("did:plc:test"), "at://did:plc:test/app.bsky.feed.post/abc", "spam", testTime)
	assert.Equal(t, "did:plc:test", label.Src)
	assert.Equal(t, "at://did:plc:test/app.bsky.feed.post/abc", label.URI)
	assert.Equal(t, "spam", label.Val)
	assert.Equal(t, "2024-06-15T12:00:00Z", label.Cts)
	assert.Equal(t, gt.Some(int64(1)), label.Ver)
	assert.Nil(t, label.Sig)
	assert.False(t, label.Neg.HasVal())
	assert.False(t, label.CID.HasVal())
	assert.False(t, label.Exp.HasVal())
}

func TestNew_UsesCurrentTime(t *testing.T) {
	t.Parallel()
	before := time.Now().UTC()
	label := New(atmos.DID("did:plc:test"), "at://x", "spam")
	after := time.Now().UTC()

	cts, err := time.Parse(time.RFC3339, label.Cts)
	require.NoError(t, err)
	assert.False(t, cts.Before(before.Truncate(time.Second)))
	assert.False(t, cts.After(after.Add(time.Second)))
}

func TestUnsignedBytes_Deterministic(t *testing.T) {
	t.Parallel()
	label := NewAt(atmos.DID("did:plc:test"), "at://did:plc:test/app.bsky.feed.post/abc", "spam", testTime)
	b1, err := UnsignedBytes(label)
	require.NoError(t, err)
	b2, err := UnsignedBytes(label)
	require.NoError(t, err)
	assert.Equal(t, b1, b2)
}

func TestUnsignedBytes_ExcludesSig(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	label := NewAt(atmos.DID("did:plc:test"), "at://did:plc:test/app.bsky.feed.post/abc", "spam", testTime)
	before, err := UnsignedBytes(label)
	require.NoError(t, err)

	require.NoError(t, Sign(label, key))
	assert.NotEmpty(t, label.Sig)

	after, err := UnsignedBytes(label)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestUnsignedBytes_DoesNotMutateLabel(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	label := NewAt(atmos.DID("did:plc:test"), "at://did:plc:test/app.bsky.feed.post/abc", "spam", testTime)
	require.NoError(t, Sign(label, key))
	sigCopy := make([]byte, len(label.Sig))
	copy(sigCopy, label.Sig)

	_, err = UnsignedBytes(label)
	require.NoError(t, err)
	assert.Equal(t, sigCopy, label.Sig)
}

func TestUnsignedBytes_Concurrent(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	label := NewAt(atmos.DID("did:plc:test"), "at://did:plc:test/app.bsky.feed.post/abc", "spam", testTime)
	require.NoError(t, Sign(label, key))

	// Get expected bytes for comparison.
	expected, err := UnsignedBytes(label)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := UnsignedBytes(label)
			assert.NoError(t, err)
			assert.Equal(t, expected, got)
		}()
	}
	wg.Wait()
}

func TestSignVerify_WithOptionalFields(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	label := NewAt(atmos.DID("did:plc:test"), "at://did:plc:test/app.bsky.feed.post/abc", "spam", testTime)
	label.CID = gt.Some("bafyreig6fjkullxfe3x3veqpkp2d7p3rw6kzuzjg2ikxnch62jqnb2oei")
	label.Exp = gt.Some("2025-01-01T00:00:00Z")

	require.NoError(t, Sign(label, key))
	require.NoError(t, Verify(label, key.PublicKey()))
}

func TestNegate_UsesCurrentTime(t *testing.T) {
	t.Parallel()
	before := time.Now().UTC()
	label := Negate(atmos.DID("did:plc:test"), "at://did:plc:test/app.bsky.feed.post/abc", "spam")
	after := time.Now().UTC()

	cts, err := time.Parse(time.RFC3339, label.Cts)
	require.NoError(t, err)
	require.False(t, cts.Before(before.Add(-time.Second)))
	require.False(t, cts.After(after.Add(time.Second)))
}

func TestNegateAt_Fields(t *testing.T) {
	t.Parallel()
	cts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	label := NegateAt(atmos.DID("did:plc:test"), "at://did:plc:test/app.bsky.feed.post/abc", "spam", cts)

	assert.Equal(t, "did:plc:test", label.Src)
	assert.Equal(t, "at://did:plc:test/app.bsky.feed.post/abc", label.URI)
	assert.Equal(t, "spam", label.Val)
	assert.Equal(t, "2024-06-15T12:00:00Z", label.Cts)
	assert.True(t, label.Ver.HasVal())
	assert.Equal(t, int64(1), label.Ver.Val())
	assert.True(t, label.Neg.HasVal())
	assert.True(t, label.Neg.Val())
	assert.Nil(t, label.Sig)
}

func TestMarshalUnmarshalRoundtrip(t *testing.T) {
	t.Parallel()
	key, err := crypto.GenerateP256()
	require.NoError(t, err)

	label := NewAt(atmos.DID("did:plc:test"), "at://did:plc:test/app.bsky.feed.post/abc", "spam", testTime)
	label.CID = gt.Some("bafyreig6fjkullxfe3x3veqpkp2d7p3rw6kzuzjg2ikxnch62jqnb2oei")
	require.NoError(t, Sign(label, key))

	data, err := label.MarshalCBOR()
	require.NoError(t, err)

	var decoded comatproto.LabelDefs_Label
	require.NoError(t, decoded.UnmarshalCBOR(data))

	assert.Equal(t, label.Src, decoded.Src)
	assert.Equal(t, label.URI, decoded.URI)
	assert.Equal(t, label.Val, decoded.Val)
	assert.Equal(t, label.Cts, decoded.Cts)
	assert.Equal(t, label.Ver, decoded.Ver)
	assert.Equal(t, label.CID, decoded.CID)
	assert.Equal(t, label.Sig, decoded.Sig)

	require.NoError(t, Verify(&decoded, key.PublicKey()))
}
