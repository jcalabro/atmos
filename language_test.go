package atmos

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseLanguage_Valid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "language_syntax_valid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseLanguage(v)
			require.NoError(t, err)
		})
	}
}

func TestParseLanguage_Invalid(t *testing.T) {
	t.Parallel()
	for _, v := range loadTestVectors(t, "language_syntax_invalid.txt") {
		t.Run(v, func(t *testing.T) {
			_, err := ParseLanguage(v)
			require.Error(t, err)
		})
	}
}
