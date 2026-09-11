package oauth

import (
	"errors"
	"strings"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSpacePermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		raw         string
		wantType    string
		wantAuth    string
		wantSKey    string
		wantCols    []string
		wantActions []SpaceAction
		wantManage  []SpaceManageOp
	}{
		{
			name:        "exact defaults",
			raw:         "space:com.example.group",
			wantType:    "com.example.group",
			wantAuth:    "self",
			wantSKey:    "*",
			wantActions: []SpaceAction{SpaceActionRead, SpaceActionCreate, SpaceActionUpdate, SpaceActionDelete},
		},
		{
			name:        "wildcards",
			raw:         "space:*?authority=*&skey=*&collection=*",
			wantType:    "*",
			wantAuth:    "*",
			wantSKey:    "*",
			wantCols:    []string{"*"},
			wantActions: []SpaceAction{SpaceActionRead, SpaceActionCreate, SpaceActionUpdate, SpaceActionDelete},
		},
		{
			name:        "all explicit and repeated",
			raw:         "space:com.example.group?authority=did:plc:alice&skey=main&collection=com.example.zed&action=delete&collection=com.example.alpha&action=read_self&manage=delete&manage=create&action=create",
			wantType:    "com.example.group",
			wantAuth:    "did:plc:alice",
			wantSKey:    "main",
			wantCols:    []string{"com.example.alpha", "com.example.zed"},
			wantActions: []SpaceAction{SpaceActionReadSelf, SpaceActionCreate, SpaceActionDelete},
			wantManage:  []SpaceManageOp{SpaceManageCreate, SpaceManageDelete},
		},
		{
			name:        "duplicates and collection wildcard normalize",
			raw:         "space:com.example.group?collection=com.example.one&collection=*&collection=com.example.one&action=read&action=read&manage=update&manage=update",
			wantType:    "com.example.group",
			wantAuth:    "self",
			wantSKey:    "*",
			wantCols:    []string{"*"},
			wantActions: []SpaceAction{SpaceActionRead},
			wantManage:  []SpaceManageOp{SpaceManageUpdate},
		},
		{
			name:        "encoded positional type",
			raw:         "space:com%2Eexample%2Egroup",
			wantType:    "com.example.group",
			wantAuth:    "self",
			wantSKey:    "*",
			wantActions: []SpaceAction{SpaceActionRead, SpaceActionCreate, SpaceActionUpdate, SpaceActionDelete},
		},
		{
			name:        "named type and maximum record key",
			raw:         "space?type=com.example.group&skey=" + strings.Repeat("x", 512),
			wantType:    "com.example.group",
			wantAuth:    "self",
			wantSKey:    strings.Repeat("x", 512),
			wantActions: []SpaceAction{SpaceActionRead, SpaceActionCreate, SpaceActionUpdate, SpaceActionDelete},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			permission, err := ParseSpacePermission(tt.raw)
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, permission.Type())
			assert.Equal(t, tt.wantAuth, permission.Authority())
			assert.Equal(t, tt.wantSKey, permission.SKey())
			assert.Equal(t, tt.wantCols, permission.Collections())
			assert.Equal(t, tt.wantActions, permission.Actions())
			assert.Equal(t, tt.wantManage, permission.ManageOps())
		})
	}
}

func TestParseSpacePermissionRejectsMalformed(t *testing.T) {
	t.Parallel()

	tests := []string{
		"",
		"space",
		"repo:com.example.group",
		"space:",
		"space:short",
		"space:foo bar",
		"space:com.example.group?type=com.example.other",
		"space?type=com.example.group&type=com.example.other",
		"space:com.example.group?authority=did:plc:a&authority=did:plc:b",
		"space:com.example.group?skey=one&skey=two",
		"space:com.example.group?unknown=value",
		"space:com.example.group?authority=not-a-did",
		"space:com.example.group?authority=did:",
		"space:com.example.group?skey=",
		"space:com.example.group?skey=.",
		"space:com.example.group?skey=a%2Fb",
		"space:com.example.group?skey=" + strings.Repeat("x", 513),
		"space:com.example.group?collection=not_an_nsid",
		"space:com.example.group?collection=",
		"space:com.example.group?action=bogus",
		"space:com.example.group?action=",
		"space:com.example.group?manage=read",
		"space:com.example.group?manage=",
		"space:com.example.group?collection=com.example.one;action=create",
		"space:com.example.group?collection=%zz",
		"space:com.example%zz",
		"space:com.example.group?=create",
	}

	for _, raw := range tests {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()

			_, err := ParseSpacePermission(raw)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidSpacePermission)
		})
	}
}

func TestSpacePermissionCanonicalString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{
			input: "space:com.example.group?authority=self&skey=*&action=delete&action=read&action=create&action=update&collection=com.example.zed&collection=com.example.alpha&manage=delete&manage=create",
			want:  "space:com.example.group?collection=com.example.alpha&collection=com.example.zed&manage=create&manage=delete",
		},
		{
			input: "space:com.example.group?action=update&action=read_self&action=update&collection=*",
			want:  "space:com.example.group?collection=*&action=read_self&action=update",
		},
		{
			input: "space:*?authority=did%3Aplc%3Aalice&skey=a.b-c_d~e%3Af&action=read",
			want:  "space:*?authority=did:plc:alice&skey=a.b-c_d~e:f&action=read",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			permission, err := ParseSpacePermission(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.want, permission.String())

			reparsed, err := ParseSpacePermission(permission.String())
			require.NoError(t, err)
			assert.Equal(t, permission, reparsed)
		})
	}
}

func TestSpacePermissionGettersReturnCopies(t *testing.T) {
	t.Parallel()

	permission, err := ParseSpacePermission("space:com.example.group?collection=com.example.record&action=create&manage=update")
	require.NoError(t, err)

	collections := permission.Collections()
	actions := permission.Actions()
	manage := permission.ManageOps()
	collections[0] = "*"
	actions[0] = SpaceActionDelete
	manage[0] = SpaceManageDelete

	assert.Equal(t, []string{"com.example.record"}, permission.Collections())
	assert.Equal(t, []SpaceAction{SpaceActionCreate}, permission.Actions())
	assert.Equal(t, []SpaceManageOp{SpaceManageUpdate}, permission.ManageOps())
}

func TestSpacePermissionMatches(t *testing.T) {
	t.Parallel()

	base := SpacePermissionMatch{
		Type:      atmos.NSID("com.example.group"),
		Authority: atmos.DID("did:plc:alice"),
		SKey:      atmos.RecordKey("main"),
	}
	target := func(action SpaceAction, collection atmos.NSID) SpacePermissionMatch {
		match := base
		match.Action = action
		match.Collection = collection
		return match
	}

	t.Run("unresolved self fails closed", func(t *testing.T) {
		permission, err := ParseSpacePermission("space:com.example.group")
		require.NoError(t, err)
		assert.False(t, permission.Matches(target(SpaceActionRead, "")))
	})

	t.Run("tuple and action semantics", func(t *testing.T) {
		permission, err := ParseSpacePermission("space:com.example.group?authority=*&skey=main&collection=com.example.record&action=read&action=create&manage=update")
		require.NoError(t, err)

		assert.True(t, permission.Matches(target(SpaceActionRead, "")))
		assert.True(t, permission.Matches(target(SpaceActionReadSelf, "")), "read implies read_self")
		assert.True(t, permission.Matches(target(SpaceActionCreate, "com.example.record")))
		assert.False(t, permission.Matches(target(SpaceActionUpdate, "com.example.record")))
		assert.False(t, permission.Matches(target(SpaceActionCreate, "com.example.other")))

		manage := base
		manage.Manage = SpaceManageUpdate
		assert.True(t, permission.Matches(manage))
		manage.Manage = SpaceManageDelete
		assert.False(t, permission.Matches(manage))
	})

	t.Run("read self does not imply read", func(t *testing.T) {
		permission, err := ParseSpacePermission("space:*?authority=*&action=read_self")
		require.NoError(t, err)
		assert.True(t, permission.Matches(target(SpaceActionReadSelf, "")))
		assert.False(t, permission.Matches(target(SpaceActionRead, "")))
	})

	t.Run("omitted collection blocks default writes", func(t *testing.T) {
		permission, err := ParseSpacePermission("space:*?authority=*")
		require.NoError(t, err)
		assert.False(t, permission.Matches(target(SpaceActionCreate, "com.example.record")))
	})

	t.Run("wildcards match concrete tuple", func(t *testing.T) {
		permission, err := ParseSpacePermission("space:*?authority=*&skey=*&collection=*")
		require.NoError(t, err)
		assert.True(t, permission.Matches(target(SpaceActionDelete, "com.example.anything")))
	})

	t.Run("invalid targets fail closed", func(t *testing.T) {
		permission, err := ParseSpacePermission("space:*?authority=*&collection=*")
		require.NoError(t, err)

		invalid := []SpacePermissionMatch{
			{},
			{Type: "bad", Authority: base.Authority, SKey: base.SKey, Action: SpaceActionRead},
			{Type: base.Type, Authority: "not-a-did", SKey: base.SKey, Action: SpaceActionRead},
			{Type: base.Type, Authority: base.Authority, SKey: ".", Action: SpaceActionRead},
			{Type: base.Type, Authority: base.Authority, SKey: base.SKey, Action: "bogus"},
			{Type: base.Type, Authority: base.Authority, SKey: base.SKey, Action: SpaceActionRead, Manage: SpaceManageUpdate},
			{Type: base.Type, Authority: base.Authority, SKey: base.SKey, Action: SpaceActionCreate},
			{Type: base.Type, Authority: base.Authority, SKey: base.SKey, Action: SpaceActionRead, Collection: "com.example.record"},
		}
		for _, match := range invalid {
			assert.False(t, permission.Matches(match), "%+v", match)
		}
	})
}

func TestSpacePermissionExpandDefaults(t *testing.T) {
	t.Parallel()

	permission, err := ParseSpacePermission("space:com.example.group")
	require.NoError(t, err)
	expanded, err := permission.ExpandDefaults(SpacePermissionExpansion{
		UserDID:     "did:plc:alice",
		SpaceType:   "com.example.group",
		Collections: []atmos.NSID{"com.example.thread", "com.example.reply", "com.example.thread"},
	})
	require.NoError(t, err)

	assert.Equal(t, "did:plc:alice", expanded.Authority())
	assert.Equal(t, []string{"com.example.reply", "com.example.thread"}, expanded.Collections())
	assert.True(t, expanded.Matches(SpacePermissionMatch{
		Type:       "com.example.group",
		Authority:  "did:plc:alice",
		SKey:       "any",
		Collection: "com.example.thread",
		Action:     SpaceActionCreate,
	}))
	assert.Equal(t, "self", permission.Authority(), "expansion must not mutate the source")
	assert.Empty(t, permission.Collections())
}

func TestSpacePermissionExpandDefaultsPreservesExplicitCollections(t *testing.T) {
	t.Parallel()

	permission, err := ParseSpacePermission("space:com.example.group?authority=did:plc:authority&collection=com.example.explicit")
	require.NoError(t, err)
	expanded, err := permission.ExpandDefaults(SpacePermissionExpansion{
		Collections: []atmos.NSID{"com.example.default"},
	})
	require.NoError(t, err)
	assert.Equal(t, permission, expanded)
}

func TestSpacePermissionExpandDefaultsIgnoresDeclarationForExplicitCollections(t *testing.T) {
	t.Parallel()

	// An explicit-collection grant must not depend on declaration inputs, so a
	// malformed declaration (e.g. from an unrelated corrupt record) cannot
	// reject it.
	permission, err := ParseSpacePermission("space:com.example.group?authority=did:plc:authority&collection=com.example.explicit")
	require.NoError(t, err)
	expanded, err := permission.ExpandDefaults(SpacePermissionExpansion{
		SpaceType:   "not a valid nsid",
		Collections: []atmos.NSID{"*", "also invalid"},
	})
	require.NoError(t, err)
	assert.Equal(t, permission, expanded)

	permission, err = ParseSpacePermission("space:com.example.group?collection=com.example.explicit")
	require.NoError(t, err)
	expanded, err = permission.ExpandDefaults(SpacePermissionExpansion{
		UserDID:     "did:plc:alice",
		SpaceType:   "not a valid nsid",
		Collections: []atmos.NSID{"*"},
	})
	require.NoError(t, err)
	assert.Equal(t, "did:plc:alice", expanded.Authority(), "self authority must still resolve on the explicit path")
	assert.Equal(t, []string{"com.example.explicit"}, expanded.Collections())
}

func TestSpacePermissionExpandDefaultsRejectsUnsafeInputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		scope     string
		expansion SpacePermissionExpansion
		wantErr   error
	}{
		{
			name:      "self requires user DID",
			scope:     "space:com.example.group?collection=com.example.record",
			expansion: SpacePermissionExpansion{},
			wantErr:   ErrInvalidSpacePermissionExpansion,
		},
		{
			name:  "invalid user DID",
			scope: "space:com.example.group?collection=com.example.record",
			expansion: SpacePermissionExpansion{
				UserDID: "not-a-did",
			},
			wantErr: ErrInvalidSpacePermissionExpansion,
		},
		{
			name:  "collections require declaration type",
			scope: "space:com.example.group?authority=*",
			expansion: SpacePermissionExpansion{
				Collections: []atmos.NSID{"com.example.record"},
			},
			wantErr: ErrInvalidSpacePermissionExpansion,
		},
		{
			name:  "declaration type must match grant",
			scope: "space:com.example.group?authority=*",
			expansion: SpacePermissionExpansion{
				SpaceType:   "com.example.other",
				Collections: []atmos.NSID{"com.example.record"},
			},
			wantErr: ErrInvalidSpacePermissionExpansion,
		},
		{
			name:  "wildcard type cannot use one declaration",
			scope: "space:*?authority=*",
			expansion: SpacePermissionExpansion{
				SpaceType:   "com.example.group",
				Collections: []atmos.NSID{"com.example.record"},
			},
			wantErr: ErrInvalidSpacePermissionExpansion,
		},
		{
			name:  "invalid declaration collection",
			scope: "space:com.example.group?authority=*",
			expansion: SpacePermissionExpansion{
				SpaceType:   "com.example.group",
				Collections: []atmos.NSID{"*"},
			},
			wantErr: ErrInvalidSpacePermissionExpansion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			permission, err := ParseSpacePermission(tt.scope)
			require.NoError(t, err)
			_, err = permission.ExpandDefaults(tt.expansion)
			require.Error(t, err)
			assert.True(t, errors.Is(err, tt.wantErr))
		})
	}
}

func TestSpaceScopeNeededFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		match SpacePermissionMatch
		want  string
	}{
		{
			name:  "read",
			match: SpacePermissionMatch{Type: "com.example.group", Authority: "did:plc:alice", SKey: "main", Action: SpaceActionRead},
			want:  "space:com.example.group?authority=did:plc:alice&skey=main&action=read",
		},
		{
			name:  "write",
			match: SpacePermissionMatch{Type: "com.example.group", Authority: "did:plc:alice", SKey: "main", Collection: "com.example.record", Action: SpaceActionCreate},
			want:  "space:com.example.group?authority=did:plc:alice&skey=main&collection=com.example.record&action=create",
		},
		{
			name:  "manage does not imply record writes",
			match: SpacePermissionMatch{Type: "com.example.group", Authority: "did:plc:alice", SKey: "main", Manage: SpaceManageUpdate},
			want:  "space:com.example.group?authority=did:plc:alice&skey=main&action=read_self&manage=update",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			scope, err := SpaceScopeNeededFor(tt.match)
			require.NoError(t, err)
			assert.Equal(t, tt.want, scope)
			permission, err := ParseSpacePermission(scope)
			require.NoError(t, err)
			assert.True(t, permission.Matches(tt.match))

			write := tt.match
			write.Action = SpaceActionCreate
			write.Manage = ""
			write.Collection = "com.example.unrequested"
			if tt.match.Action != SpaceActionCreate {
				assert.False(t, permission.Matches(write), "suggestion widened to an unrequested write")
			}
		})
	}
}

func TestSpaceScopeNeededForRejectsInvalidTargets(t *testing.T) {
	t.Parallel()

	for _, target := range []SpacePermissionMatch{
		{},
		{Type: "*", Authority: "did:plc:alice", SKey: "main", Action: SpaceActionRead},
		{Type: "com.example.group", Authority: "*", SKey: "main", Action: SpaceActionRead},
		{Type: "com.example.group", Authority: "did:plc:alice", SKey: "*", Action: SpaceActionRead},
		{Type: "com.example.group", Authority: "did:plc:alice", SKey: "main", Action: SpaceActionCreate, Collection: "*"},
		{Type: "com.example.group", Authority: "did:plc:alice", SKey: "main", Manage: "bogus"},
	} {
		_, err := SpaceScopeNeededFor(target)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidSpacePermission)
	}
}

func FuzzParseSpacePermission(f *testing.F) {
	for _, seed := range []string{
		"space:com.example.group",
		"space:*?authority=*&collection=*&action=create",
		"space:com.example.group?authority=did:plc:alice&skey=main&collection=com.example.record&action=read&action=update",
		"space:com.example.group?authority=did%3Aplc%3Aalice",
		"space:bad?collection=%zz",
		"",
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		permission, err := ParseSpacePermission(raw)
		if err != nil {
			if !errors.Is(err, ErrInvalidSpacePermission) {
				t.Fatalf("unexpected error type: %v", err)
			}
			return
		}

		canonical := permission.String()
		reparsed, err := ParseSpacePermission(canonical)
		if err != nil {
			t.Fatalf("canonical scope %q did not parse: %v", canonical, err)
		}
		if !assert.ObjectsAreEqual(permission, reparsed) {
			t.Fatalf("round trip mismatch: %#v != %#v", permission, reparsed)
		}
		if reparsed.String() != canonical {
			t.Fatalf("format not idempotent: %q != %q", reparsed.String(), canonical)
		}
	})
}

func BenchmarkParseSpacePermission(b *testing.B) {
	const scope = "space:com.example.group?authority=did:plc:alice&skey=main&collection=com.example.thread&collection=com.example.reply&action=read&action=create&action=update&manage=update"

	b.ReportAllocs()
	for b.Loop() {
		permission, err := ParseSpacePermission(scope)
		if err != nil {
			b.Fatal(err)
		}
		if permission.spaceType == "" {
			b.Fatal("empty parsed permission")
		}
	}
}

func BenchmarkSpacePermissionMatches(b *testing.B) {
	permission, err := ParseSpacePermission("space:com.example.group?authority=did:plc:alice&skey=main&collection=com.example.thread&action=create")
	if err != nil {
		b.Fatal(err)
	}
	target := SpacePermissionMatch{
		Type:       "com.example.group",
		Authority:  "did:plc:alice",
		SKey:       "main",
		Collection: "com.example.thread",
		Action:     SpaceActionCreate,
	}

	b.ReportAllocs()
	for b.Loop() {
		if !permission.Matches(target) {
			b.Fatal("permission did not match")
		}
	}
}
