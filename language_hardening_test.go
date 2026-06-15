package atmos

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// M3: align ParseLanguage with the reference BCP-47 well-formedness regex used
// by @atproto/syntax. Each case below is verified against that regex.
func TestParseLanguage_ReferenceConformance(t *testing.T) {
	t.Parallel()

	valid := []string{
		"x-private",    // private-use only
		"x-fr-CH",      // private-use only (reference asserts valid)
		"de-x-private", // language + private use
		"abcd",         // 4-char primary subtag (script-length language)
		"abcde",        // 5-8 char primary subtag
		"en-GB-boont-r-extended-sequence-x-private",
		"zh-min-nan",         // grandfathered (irregular)
		"i-klingon",          // grandfathered (registered i- tag)
		"sgn-BE-NL",          // grandfathered
		"hy-Latn-IT-arevela", // language-script-region-variant
	}
	for _, v := range valid {
		t.Run("valid/"+v, func(t *testing.T) {
			_, err := ParseLanguage(v)
			require.NoError(t, err, "%q should be well-formed", v)
		})
	}

	invalid := []string{
		"i-zzz9",            // arbitrary i- tag not in grandfathered list
		"en-" + "aaaaaaaaa", // 9-char subtag exceeds the 8-char cap
		"ja-",               // trailing hyphen
		"a-DE",              // 1-char primary subtag
		"-en",               // leading hyphen
		"en--US",            // empty subtag
	}
	for _, v := range invalid {
		t.Run("invalid/"+v, func(t *testing.T) {
			_, err := ParseLanguage(v)
			require.Error(t, err, "%q should be rejected", v)
		})
	}
}
