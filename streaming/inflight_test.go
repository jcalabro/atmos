package streaming

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInflightSeqs_AddRemoveMin(t *testing.T) {
	var s inflightSeqs
	require.Equal(t, int64(0), s.Min())
	s.Add(5)
	s.Add(2)
	s.Add(7)
	require.Equal(t, int64(2), s.Min())
	s.Remove(2)
	require.Equal(t, int64(5), s.Min())
	s.Remove(7)
	require.Equal(t, int64(5), s.Min())
	s.Remove(5)
	require.Equal(t, int64(0), s.Min())
}

func TestInflightSeqs_RemoveMissing(t *testing.T) {
	var s inflightSeqs
	s.Add(10)
	s.Remove(99) // no-op
	require.Equal(t, int64(10), s.Min())
}
