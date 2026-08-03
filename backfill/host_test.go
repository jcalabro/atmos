package backfill_test

import (
	"strings"
	"testing"

	"github.com/jcalabro/atmos/backfill"
	"github.com/stretchr/testify/require"
)

func TestValidateHostname(t *testing.T) {
	t.Parallel()
	for _, valid := range []string{"pds.example.com", "a-b.host.bsky.network", "PDS.EXAMPLE.COM"} {
		require.NoError(t, backfill.ValidateHostname(valid), valid)
	}
	for _, invalid := range []string{"", "localhost", "pds", "127.0.0.1", "[::1]", "https://pds.example.com", "pds.example.com:443", "pds.local", "x.internal", "x.lan", ".example.com", "x..example.com", "-x.example.com", "x-.example.com", "x_example.com", "pds.example.com."} {
		require.Error(t, backfill.ValidateHostname(invalid), invalid)
	}
}

func FuzzValidateHostname(f *testing.F) {
	for _, seed := range []string{"pds.example.com", "localhost", "127.0.0.1", "https://evil.example", strings.Repeat("a", 254)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		err := backfill.ValidateHostname(value)
		if err == nil {
			require.NotContains(t, value, ":")
			require.Contains(t, value, ".")
			require.Equal(t, value, strings.TrimSpace(value))
		}
	})
}
