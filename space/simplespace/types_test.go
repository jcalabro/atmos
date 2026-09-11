package simplespace

import (
	"encoding/json"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPolicyWireRoundTrip(t *testing.T) {
	t.Parallel()
	space, err := atmos.ParseSpaceRef("at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/space/com.example.space/room")
	require.NoError(t, err)
	cases := []Policy{
		{Kind: PolicyPublic},
		{Kind: PolicyMemberList},
		{Kind: PolicyManagingApp, ManagingApp: "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb#manager"},
	}
	for _, policy := range cases {
		policy := policy
		t.Run(policyName(policy), func(t *testing.T) {
			t.Parallel()
			input, err := CreateInput("com.example.space", "room", policy, policy, AppAccess{Kind: AppAccessOpen})
			require.NoError(t, err)
			config, err := DecodeCreate(space, input)
			require.NoError(t, err)
			assert.Equal(t, policy, config.ReadPolicy)
			assert.Equal(t, policy, config.WritePolicy)
			output, err := Output(config)
			require.NoError(t, err)
			decoded, err := DecodeOutput(output)
			require.NoError(t, err)
			assert.Equal(t, config, decoded)
		})
	}
}

func TestDecodeCreate_UnknownPoliciesFailClosed(t *testing.T) {
	t.Parallel()
	space, err := atmos.ParseSpaceRef("at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/space/com.example.space/room")
	require.NoError(t, err)
	var input comatproto.SimplespaceCreateSpace_Input
	err = json.Unmarshal([]byte(`{
		"type":"com.example.space",
		"readPolicy":{"$type":"com.example.unknown#public"},
		"writePolicy":{"$type":"com.example.unknown#public"},
		"appAccess":{"$type":"com.example.unknown#open"}
	}`), &input)
	require.NoError(t, err)
	_, err = DecodeCreate(space, &input)
	require.ErrorContains(t, err, "unknown")
}

func TestAppAccessValidationAndCopyIsolation(t *testing.T) {
	t.Parallel()
	allowed := []string{"https://client.example/metadata.json"}
	apps := AppAccess{Kind: AppAccessAllowList, Allowed: allowed}
	require.NoError(t, apps.Validate())
	input, err := CreateInput("com.example.space", "room", Policy{Kind: PolicyPublic}, Policy{Kind: PolicyMemberList}, apps)
	require.NoError(t, err)
	allowed[0] = "mutated"
	assert.Equal(t, "https://client.example/metadata.json", input.AppAccess.SimplespaceDefs_AllowList.Val().Allowed[0])

	assert.Error(t, (AppAccess{Kind: AppAccessAllowList, Allowed: []string{"x", "x"}}).Validate())
	assert.Error(t, (AppAccess{Kind: AppAccessOpen, Allowed: []string{"x"}}).Validate())
}

func TestPatchRequiresChangeAndPreservesClosedValues(t *testing.T) {
	t.Parallel()
	assert.Error(t, (Patch{}).Validate())
	policy := Policy{Kind: PolicyMemberList}
	patch := Patch{WritePolicy: &policy}
	input, err := UpdateInput("at://did:plc:aaaaaaaaaaaaaaaaaaaaaaaa/space/com.example.space/room", patch)
	require.NoError(t, err)
	decoded, err := DecodeUpdate(input)
	require.NoError(t, err)
	require.NotNil(t, decoded.WritePolicy)
	assert.Equal(t, policy, *decoded.WritePolicy)
	assert.Nil(t, decoded.ReadPolicy)
}

func policyName(policy Policy) string {
	switch policy.Kind {
	case PolicyPublic:
		return "public"
	case PolicyMemberList:
		return "member-list"
	default:
		return "managing-app"
	}
}
