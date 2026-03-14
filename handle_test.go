package atmos

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseHandle_Valid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "handle_syntax_valid.txt") {
		t.Run(v, func(t *testing.T) {
			h, err := ParseHandle(v)
			require.NoError(t, err)
			require.Equal(t, v, h.String())
		})
	}
}

func TestParseHandle_Invalid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "handle_syntax_invalid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseHandle(v)
			require.Error(t, err)
		})
	}
}

func TestHandle_Normalize(t *testing.T) {
	t.Parallel()
	h, err := ParseHandle("Alice.Bsky.Social")
	require.NoError(t, err)
	require.Equal(t, Handle("alice.bsky.social"), h.Normalize())
}

func TestHandle_TLD(t *testing.T) {
	t.Parallel()
	h, err := ParseHandle("alice.bsky.social")
	require.NoError(t, err)
	require.Equal(t, "social", h.TLD())
}

func TestHandle_AllowedTLD(t *testing.T) {
	t.Parallel()
	cases := []struct {
		handle  string
		allowed bool
	}{
		{"alice.bsky.social", true},
		{"laptop.local", false},
		{"test.onion", false},
		{"example.test", true},
	}
	for _, tc := range cases {
		h, err := ParseHandle(tc.handle)
		require.NoError(t, err, tc.handle)
		require.Equal(t, tc.allowed, h.AllowedTLD(), tc.handle)
	}
}

func TestHandle_IsInvalidHandle(t *testing.T) {
	t.Parallel()
	require.True(t, HandleInvalid.IsInvalidHandle())
	h, _ := ParseHandle("alice.bsky.social")
	require.False(t, h.IsInvalidHandle())
}

func TestHandle_ZeroValue(t *testing.T) {
	t.Parallel()
	var h Handle
	require.Equal(t, "", h.String())
	require.Equal(t, "", h.TLD())
}

func TestHandle_MarshalRoundTrip(t *testing.T) {
	t.Parallel()
	h, err := ParseHandle("alice.bsky.social")
	require.NoError(t, err)
	b, err := h.MarshalText()
	require.NoError(t, err)
	var h2 Handle
	require.NoError(t, h2.UnmarshalText(b))
	require.Equal(t, h, h2)
}

func TestHandle_ATIdentifier(t *testing.T) {
	t.Parallel()
	h, err := ParseHandle("test.example.com")
	require.NoError(t, err)
	id := h.ATIdentifier()
	require.Equal(t, ATIdentifier("test.example.com"), id)
	require.True(t, id.IsHandle())
}
