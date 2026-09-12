package host

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAlphaLimitsSatisfyHostInvariants(t *testing.T) {
	t.Parallel()
	limits := AlphaLimits()
	require.Positive(t, limits.DeliveryWorkers)
	require.Less(t, limits.DeliveryTimeout, limits.DeliveryLease)
	require.LessOrEqual(t, limits.RegistrationTTL, maxRegistrationTTL)
	require.LessOrEqual(t, limits.CredentialLifetime, defaultCredentialLifetime)
}
