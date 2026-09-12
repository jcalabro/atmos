package simplespace

import (
	"context"
	"errors"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientManagementRoundTripAndReplaceSemantics(t *testing.T) {
	t.Parallel()
	api := &fakeManagementAPI{did: "did:plc:abcdefghijklmnopqrstuvwx"}
	client, err := NewClient(api)
	require.NoError(t, err)
	space, err := client.Create(t.Context(), "com.example.board", "main",
		Policy{Kind: PolicyMemberList}, Policy{Kind: PolicyManagingApp, ManagingApp: "did:plc:bcdefghijklmnopqrstuvwxy#manager"},
		AppAccess{Kind: AppAccessAllowList, Allowed: []string{"https://client.example/metadata.json"}})
	require.NoError(t, err)
	require.Equal(t, atmos.SpaceRef("at://did:plc:abcdefghijklmnopqrstuvwx/space/com.example.board/main"), space)

	member := Member{DID: "did:plc:cdefghijklmnopqrstuvwxyz", Read: true, Write: false}
	require.NoError(t, client.PutMember(t.Context(), space, member))
	member.Read, member.Write = false, true
	require.NoError(t, client.PutMember(t.Context(), space, member))
	assert.Equal(t, member, api.member, "both booleans must be sent on every replacement")

	read := Policy{Kind: PolicyPublic}
	require.NoError(t, client.Update(t.Context(), space, Patch{ReadPolicy: &read}))
	require.NotNil(t, api.patch.ReadPolicy)
	assert.Equal(t, read, *api.patch.ReadPolicy)
	assert.Nil(t, api.patch.WritePolicy)

	members, cursor, err := client.Members(t.Context(), space, 10, "")
	require.NoError(t, err)
	assert.Equal(t, []Member{member}, members)
	assert.Equal(t, "next", cursor)
	require.NoError(t, client.RemoveMember(t.Context(), space, member.DID))
	assert.True(t, api.removed)
	require.NoError(t, client.Delete(t.Context(), space))
	assert.True(t, api.deleted)
}

func TestClientRejectsCrossAuthorityResponsesAndMutations(t *testing.T) {
	t.Parallel()
	api := &fakeManagementAPI{did: "did:plc:abcdefghijklmnopqrstuvwx", createURI: "at://did:plc:bcdefghijklmnopqrstuvwxy/space/com.example.board/main"}
	client, err := NewClient(api)
	require.NoError(t, err)
	_, err = client.Create(t.Context(), "com.example.board", "main", Policy{Kind: PolicyPublic}, Policy{Kind: PolicyPublic}, AppAccess{Kind: AppAccessOpen})
	require.ErrorContains(t, err, "does not match")
	foreign := atmos.SpaceRef("at://did:plc:bcdefghijklmnopqrstuvwxy/space/com.example.board/main")
	require.Error(t, client.Delete(t.Context(), foreign))
	require.Error(t, client.PutMember(t.Context(), foreign, Member{DID: "did:plc:cdefghijklmnopqrstuvwxyz"}))
	_, err = client.Get(t.Context(), foreign)
	require.ErrorContains(t, err, "not owned", "Get must enforce the same local ownership boundary as mutations")
	_, _, err = client.Members(t.Context(), foreign, 10, "")
	require.ErrorContains(t, err, "not owned")
}

type fakeManagementAPI struct {
	did       atmos.DID
	createURI string
	member    Member
	patch     Patch
	removed   bool
	deleted   bool
}

func (f *fakeManagementAPI) DID() atmos.DID { return f.did }

func (f *fakeManagementAPI) CreateSimpleSpace(_ context.Context, input *comatproto.SimplespaceCreateSpace_Input) (*comatproto.SimplespaceCreateSpace_Output, error) {
	skey := "generated"
	if input.Skey.HasVal() {
		skey = input.Skey.Val()
	}
	uri := f.createURI
	if uri == "" {
		uri = "at://" + f.did.String() + "/space/" + input.Type + "/" + skey
	}
	return &comatproto.SimplespaceCreateSpace_Output{URI: uri}, nil
}

func (f *fakeManagementAPI) UpdateSimpleSpace(_ context.Context, input *comatproto.SimplespaceUpdateSpace_Input) error {
	patch, err := DecodeUpdate(input)
	f.patch = patch
	return err
}

func (f *fakeManagementAPI) DeleteSimpleSpace(context.Context, *comatproto.SimplespaceDeleteSpace_Input) error {
	f.deleted = true
	return nil
}

func (f *fakeManagementAPI) PutSimpleSpaceMember(_ context.Context, input *comatproto.SimplespacePutMember_Input) error {
	did, err := atmos.ParseDID(input.DID)
	if err != nil {
		return err
	}
	f.member = Member{DID: did, Read: input.Read, Write: input.Write}
	return nil
}

func (f *fakeManagementAPI) RemoveSimpleSpaceMember(context.Context, *comatproto.SimplespaceRemoveMember_Input) error {
	f.removed = true
	return nil
}

func (f *fakeManagementAPI) GetSpace(context.Context, atmos.SpaceRef) (*comatproto.SimplespaceGetSpace_Output, error) {
	return nil, errors.New("unused")
}

func (f *fakeManagementAPI) ListMembers(context.Context, atmos.SpaceRef, int, string) (*comatproto.SimplespaceListMembers_Output, error) {
	return &comatproto.SimplespaceListMembers_Output{
		Members: []comatproto.SimplespaceListMembers_Member{{DID: f.member.DID.String(), Read: f.member.Read, Write: f.member.Write}},
		Cursor:  gt.Some("next"),
	}, nil
}
