package host

import (
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/oauth"
	"github.com/stretchr/testify/require"
)

func TestOAuthAccountPermissions_ExactActions(t *testing.T) {
	t.Parallel()

	space := atmos.SpaceRef("at://did:plc:authority/space/com.example.group/main")
	otherSpace := atmos.SpaceRef("at://did:plc:authority/space/com.example.group/other")
	read, err := oauth.ParseSpacePermission("space:com.example.group?authority=did%3Aplc%3Aauthority&skey=main&action=read_self")
	require.NoError(t, err)
	create, err := oauth.ParseSpacePermission("space:com.example.group?authority=did%3Aplc%3Aauthority&skey=main&action=read_self&manage=create")
	require.NoError(t, err)
	update, err := oauth.ParseSpacePermission("space:com.example.group?authority=did%3Aplc%3Aauthority&skey=main&action=read_self&manage=update")
	require.NoError(t, err)
	deletePermission, err := oauth.ParseSpacePermission("space:com.example.group?authority=did%3Aplc%3Aauthority&skey=main&action=read_self&manage=delete")
	require.NoError(t, err)

	tests := []struct {
		name       string
		permission oauth.SpacePermission
		action     AccountAction
	}{
		{name: "read self", permission: read, action: AccountReadSelf},
		{name: "create", permission: create, action: AccountCreate},
		{name: "update", permission: update, action: AccountUpdate},
		{name: "delete", permission: deletePermission, action: AccountDelete},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			permissions := OAuthAccountPermissions{tt.permission}
			for _, action := range []AccountAction{AccountReadSelf, AccountCreate, AccountUpdate, AccountDelete} {
				require.Equal(t, action == tt.action || (tt.action != AccountReadSelf && action == AccountReadSelf), permissions.Allows(space, action), "action %d", action)
			}
			require.False(t, permissions.Allows(otherSpace, tt.action))
			require.False(t, permissions.Allows(space, AccountAction(255)))
		})
	}
}

func TestOAuthAccountPermissions_MultipleWildcardAndUnexpanded(t *testing.T) {
	t.Parallel()

	space := atmos.SpaceRef("at://did:plc:authority/space/com.example.group/main")
	unrelated, err := oauth.ParseSpacePermission("space:com.example.other?authority=*&action=read_self&manage=update")
	require.NoError(t, err)
	wildcard, err := oauth.ParseSpacePermission("space:*?authority=*&skey=*&action=read_self&manage=update")
	require.NoError(t, err)
	unexpanded, err := oauth.ParseSpacePermission("space:com.example.group?action=read_self&manage=update")
	require.NoError(t, err)

	require.True(t, OAuthAccountPermissions{unrelated, wildcard}.Allows(space, AccountUpdate))
	require.False(t, OAuthAccountPermissions{unrelated}.Allows(space, AccountUpdate))
	require.False(t, OAuthAccountPermissions{unexpanded}.Allows(space, AccountUpdate))
	require.False(t, OAuthAccountPermissions(nil).Allows(space, AccountUpdate))
	require.False(t, OAuthAccountPermissions{wildcard}.Allows(atmos.SpaceRef("invalid"), AccountUpdate))
}
