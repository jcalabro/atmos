package atmos

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCID_Valid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "cid_syntax_valid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseCID(v)
			require.NoError(t, err)
		})
	}
}

func TestParseCID_Invalid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "cid_syntax_invalid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseCID(v)
			require.Error(t, err)
		})
	}
}

func TestParseCID_RejectsCIDv0(t *testing.T) {
	t.Parallel()
	_, err := ParseCID("QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n")
	require.Error(t, err)
	_, err = ParseCID("QmcRD4wkPPi6dig81r5sLj9Zm1gDCL4zgpEj9CfuRrGbzF")
	require.Error(t, err)
}

func TestCID_MarshalRoundTrip(t *testing.T) {
	t.Parallel()
	raw := "bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454"
	c, err := ParseCID(raw)
	require.NoError(t, err)
	require.Equal(t, raw, c.String())
	b, err := c.MarshalText()
	require.NoError(t, err)
	require.Equal(t, raw, string(b))
	var c2 CID
	require.NoError(t, c2.UnmarshalText(b))
	require.Equal(t, c, c2)

	var bad CID
	require.Error(t, bad.UnmarshalText([]byte("notacid")))
}
