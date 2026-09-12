package simplespace

import (
	"context"
	"errors"
	"fmt"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
)

// ManagementAPI is the narrow account-authenticated transport needed by
// Client. It deliberately contains no reader-credential methods.
type ManagementAPI interface {
	CreateSimpleSpace(context.Context, *comatproto.SimplespaceCreateSpace_Input) (*comatproto.SimplespaceCreateSpace_Output, error)
	UpdateSimpleSpace(context.Context, *comatproto.SimplespaceUpdateSpace_Input) error
	DeleteSimpleSpace(context.Context, *comatproto.SimplespaceDeleteSpace_Input) error
	PutSimpleSpaceMember(context.Context, *comatproto.SimplespacePutMember_Input) error
	RemoveSimpleSpaceMember(context.Context, *comatproto.SimplespaceRemoveMember_Input) error
	GetSpace(context.Context, atmos.SpaceRef) (*comatproto.SimplespaceGetSpace_Output, error)
	ListMembers(context.Context, atmos.SpaceRef, int, string) (*comatproto.SimplespaceListMembers_Output, error)
	DID() atmos.DID
}

// Client exposes validated simple-space management operations through an
// authority account client.
type Client struct {
	api ManagementAPI
}

// NewClient constructs a management client.
func NewClient(api ManagementAPI) (*Client, error) {
	if api == nil {
		return nil, errors.New("simplespace: management API is required")
	}
	if err := api.DID().Validate(); err != nil {
		return nil, fmt.Errorf("simplespace: invalid authority account: %w", err)
	}
	return &Client{api: api}, nil
}

// Create creates a space and verifies the returned identity.
func (c *Client) Create(ctx context.Context, typ atmos.NSID, skey atmos.RecordKey, read, write Policy, apps AppAccess) (atmos.SpaceRef, error) {
	input, err := CreateInput(typ, skey, read, write, apps)
	if err != nil {
		return "", err
	}
	out, err := c.api.CreateSimpleSpace(ctx, input)
	if err != nil {
		return "", err
	}
	if out == nil {
		return "", errors.New("simplespace: create response is absent")
	}
	space, err := atmos.ParseSpaceRef(out.URI)
	if err != nil {
		return "", fmt.Errorf("simplespace: invalid create response URI: %w", err)
	}
	if space.Authority() != c.api.DID() || space.Type() != typ || (skey != "" && space.Key() != skey) {
		return "", errors.New("simplespace: create response does not match request")
	}
	return space, nil
}

// Get returns a closed, validated policy configuration. Like every other
// management operation, it accepts only spaces owned by the configured
// account; foreign spaces are read through reader credentials, not this client.
func (c *Client) Get(ctx context.Context, space atmos.SpaceRef) (Config, error) {
	if err := requireOwned(c.api.DID(), space); err != nil {
		return Config{}, err
	}
	out, err := c.api.GetSpace(ctx, space)
	if err != nil {
		return Config{}, err
	}
	config, err := DecodeOutput(out)
	if err != nil {
		return Config{}, err
	}
	if config.URI != space {
		return Config{}, errors.New("simplespace: get response does not match request")
	}
	return config, nil
}

// Update atomically replaces every supplied policy value.
func (c *Client) Update(ctx context.Context, space atmos.SpaceRef, patch Patch) error {
	if err := requireOwned(c.api.DID(), space); err != nil {
		return err
	}
	input, err := UpdateInput(space, patch)
	if err != nil {
		return err
	}
	return c.api.UpdateSimpleSpace(ctx, input)
}

// Delete permanently tombstones a space on an atmos authority host.
func (c *Client) Delete(ctx context.Context, space atmos.SpaceRef) error {
	if err := requireOwned(c.api.DID(), space); err != nil {
		return err
	}
	return c.api.DeleteSimpleSpace(ctx, &comatproto.SimplespaceDeleteSpace_Input{Space: space.String()})
}

// PutMember replaces both access booleans for a member.
func (c *Client) PutMember(ctx context.Context, space atmos.SpaceRef, member Member) error {
	if err := requireOwned(c.api.DID(), space); err != nil {
		return err
	}
	if err := member.Validate(); err != nil {
		return err
	}
	return c.api.PutSimpleSpaceMember(ctx, &comatproto.SimplespacePutMember_Input{
		Space: space.String(), DID: member.DID.String(), Read: member.Read, Write: member.Write,
	})
}

// RemoveMember removes one member-list entry.
func (c *Client) RemoveMember(ctx context.Context, space atmos.SpaceRef, did atmos.DID) error {
	if err := requireOwned(c.api.DID(), space); err != nil {
		return err
	}
	if err := did.Validate(); err != nil {
		return err
	}
	return c.api.RemoveSimpleSpaceMember(ctx, &comatproto.SimplespaceRemoveMember_Input{Space: space.String(), DID: did.String()})
}

// Members returns one validated member page.
func (c *Client) Members(ctx context.Context, space atmos.SpaceRef, limit int, cursor string) ([]Member, string, error) {
	if err := requireOwned(c.api.DID(), space); err != nil {
		return nil, "", err
	}
	out, err := c.api.ListMembers(ctx, space, limit, cursor)
	if err != nil {
		return nil, "", err
	}
	members := make([]Member, len(out.Members))
	for i, wire := range out.Members {
		did, err := atmos.ParseDID(wire.DID)
		if err != nil {
			return nil, "", fmt.Errorf("simplespace: invalid member %d: %w", i, err)
		}
		members[i] = Member{DID: did, Read: wire.Read, Write: wire.Write}
	}
	next := ""
	if out.Cursor.HasVal() {
		next = out.Cursor.Val()
	}
	return members, next, nil
}

func requireOwned(authority atmos.DID, space atmos.SpaceRef) error {
	if err := space.Validate(); err != nil {
		return err
	}
	if space.Authority() != authority {
		return errors.New("simplespace: space is not owned by the configured account")
	}
	return nil
}
