package space

import (
	"encoding/hex"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLtHashPinnedCrossLanguageVectors(t *testing.T) {
	t.Parallel()

	hash := NewLtHash()
	digest := hash.Digest()
	require.Equal(t, "e5a00aa9991ac8a5ee3109844d84a55583bd20572ad3ffcd42792f3c36b183ad", hex.EncodeToString(digest[:]))

	hash.Add([]byte("alpha"))
	digest = hash.Digest()
	require.Equal(t, "b67bd2b422af744abac9389b45c079be960de496e16c643360eb6ba1921b51be", hex.EncodeToString(digest[:]))

	hash.Add([]byte("snowman-☃"))
	digest = hash.Digest()
	require.Equal(t, "531de20334717eced2e5581b291646c5cb54269587b259ba3f03d80148ca6872", hex.EncodeToString(digest[:]))

	hash.Add([]byte("alpha"))
	digest = hash.Digest()
	require.Equal(t, "26e0c491c8a6589650ab900cd71a52e8ec0b840c2197ac6e1f619fa8fe1cd49f", hex.EncodeToString(digest[:]))
	hash.Remove([]byte("alpha"))
	digest = hash.Digest()
	require.Equal(t, "531de20334717eced2e5581b291646c5cb54269587b259ba3f03d80148ca6872", hex.EncodeToString(digest[:]))
	hash.Remove([]byte("snowman-☃"))
	hash.Remove([]byte("alpha"))
	require.True(t, hash.IsEmpty())
}

func TestLtHashOrderIndependenceStateRoundTripAndCopyIsolation(t *testing.T) {
	t.Parallel()

	a := NewLtHash()
	a.Add([]byte("one"))
	a.Add([]byte("two"))
	b := NewLtHash()
	b.Add([]byte("two"))
	b.Add([]byte("one"))
	require.True(t, a.Equal(b))

	state := a.State()
	restored, err := NewLtHashFromState(state[:])
	require.NoError(t, err)
	require.True(t, a.Equal(restored))
	state[0] ^= 0xff
	require.True(t, a.Equal(restored), "constructor and State must not alias caller memory")

	_, err = NewLtHashFromState(make([]byte, LtHashStateSize-1))
	require.Error(t, err)
}

func TestLtHashUint16Wraparound(t *testing.T) {
	t.Parallel()

	state := make([]byte, LtHashStateSize)
	for i := 0; i < LtHashStateSize; i += 2 {
		state[i] = 0xff
		state[i+1] = 0xff
	}
	hash, err := NewLtHashFromState(state)
	require.NoError(t, err)
	hash.Add([]byte("wrap"))
	hash.Remove([]byte("wrap"))
	got := hash.State()
	require.Equal(t, state, got[:])
}

func FuzzLtHashRoundTrip(f *testing.F) {
	f.Add([]byte("com.example.post/key/bafyreia"))
	f.Add([]byte(""))
	f.Fuzz(func(t *testing.T, element []byte) {
		h := NewLtHash()
		before := h.State()
		h.Add(element)
		h.Remove(element)
		require.Equal(t, before, h.State())
	})
}

func BenchmarkLtHashElementSizes(b *testing.B) {
	for _, size := range []int{16, 256, 4096} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			element := make([]byte, size)
			h := NewLtHash()
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				h.Add(element)
			}
		})
	}
}
