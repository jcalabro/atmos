package oauth

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strings"

	"github.com/jcalabro/atmos"
)

var (
	// ErrInvalidSpacePermission identifies a malformed OAuth space permission.
	ErrInvalidSpacePermission = errors.New("oauth: invalid space permission")
	// ErrInvalidSpacePermissionExpansion identifies invalid or inapplicable
	// inputs used to materialize issuance-time space permission defaults.
	ErrInvalidSpacePermissionExpansion = errors.New("oauth: invalid space permission expansion")
)

// SpaceAction is a record-level action in an OAuth space permission.
type SpaceAction string

const (
	// SpaceActionReadSelf permits reading the granting user's repo in a space.
	SpaceActionReadSelf SpaceAction = "read_self"
	// SpaceActionRead permits reading every admitted repo in a space.
	SpaceActionRead SpaceAction = "read"
	// SpaceActionCreate permits creating records in allowed collections.
	SpaceActionCreate SpaceAction = "create"
	// SpaceActionUpdate permits updating records in allowed collections.
	SpaceActionUpdate SpaceAction = "update"
	// SpaceActionDelete permits deleting records in allowed collections.
	SpaceActionDelete SpaceAction = "delete"
)

// SpaceManageOp is a space-level management operation in an OAuth permission.
type SpaceManageOp string

const (
	// SpaceManageCreate permits creating the selected space.
	SpaceManageCreate SpaceManageOp = "create"
	// SpaceManageUpdate permits updating the selected space.
	SpaceManageUpdate SpaceManageOp = "update"
	// SpaceManageDelete permits deleting the selected space.
	SpaceManageDelete SpaceManageOp = "delete"
)

var (
	spaceActionOrder = []SpaceAction{
		SpaceActionReadSelf,
		SpaceActionRead,
		SpaceActionCreate,
		SpaceActionUpdate,
		SpaceActionDelete,
	}
	spaceDefaultActions = []SpaceAction{
		SpaceActionRead,
		SpaceActionCreate,
		SpaceActionUpdate,
		SpaceActionDelete,
	}
	spaceManageOrder = []SpaceManageOp{
		SpaceManageCreate,
		SpaceManageUpdate,
		SpaceManageDelete,
	}
)

// SpacePermission is a parsed, canonical OAuth permission for a space. It is
// immutable; slice accessors return copies.
type SpacePermission struct {
	spaceType  string
	authority  string
	skey       string
	collection []string
	action     []SpaceAction
	manage     []SpaceManageOp
}

// ParseSpacePermission parses one complete space permission scope. Unknown,
// duplicate single-valued, malformed, and non-space scopes return
// ErrInvalidSpacePermission; callers must not silently discard that error when
// constructing an authorization grant.
func ParseSpacePermission(raw string) (SpacePermission, error) {
	head, rawQuery, hasQuery := strings.Cut(raw, "?")

	var positional string
	switch {
	case head == "space":
	case strings.HasPrefix(head, "space:"):
		positional = strings.TrimPrefix(head, "space:")
		if positional == "" {
			return SpacePermission{}, invalidSpacePermission("empty type")
		}
		var err error
		positional, err = url.PathUnescape(positional)
		if err != nil {
			return SpacePermission{}, invalidSpacePermission("invalid positional type encoding: %v", err)
		}
	default:
		return SpacePermission{}, invalidSpacePermission("scope prefix is not space")
	}

	values := make(url.Values)
	if hasQuery && rawQuery != "" {
		var err error
		values, err = url.ParseQuery(rawQuery)
		if err != nil {
			return SpacePermission{}, invalidSpacePermission("invalid query encoding: %v", err)
		}
	}

	for key := range values {
		switch key {
		case "type", "authority", "skey", "collection", "action", "manage":
		default:
			return SpacePermission{}, invalidSpacePermission("unknown parameter %q", key)
		}
	}

	typeValues := values["type"]
	if positional != "" && len(typeValues) != 0 {
		return SpacePermission{}, invalidSpacePermission("type cannot be both positional and named")
	}
	if len(typeValues) > 1 {
		return SpacePermission{}, invalidSpacePermission("type occurs more than once")
	}
	spaceType := positional
	if len(typeValues) == 1 {
		spaceType = typeValues[0]
	}
	if err := validateSpaceType(spaceType); err != nil {
		return SpacePermission{}, invalidSpacePermission("type: %v", err)
	}

	authority, err := singleSpaceParam(values, "authority", "self")
	if err != nil {
		return SpacePermission{}, err
	}
	if err := validateSpaceAuthority(authority); err != nil {
		return SpacePermission{}, invalidSpacePermission("authority: %v", err)
	}

	skey, err := singleSpaceParam(values, "skey", "*")
	if err != nil {
		return SpacePermission{}, err
	}
	if skey != "*" {
		if _, parseErr := atmos.ParseRecordKey(skey); parseErr != nil {
			return SpacePermission{}, invalidSpacePermission("skey: %v", parseErr)
		}
	}

	collections, err := parseSpaceCollections(values["collection"])
	if err != nil {
		return SpacePermission{}, err
	}
	actions, err := parseSpaceActions(values["action"])
	if err != nil {
		return SpacePermission{}, err
	}
	manage, err := parseSpaceManageOps(values["manage"])
	if err != nil {
		return SpacePermission{}, err
	}

	return SpacePermission{
		spaceType:  spaceType,
		authority:  authority,
		skey:       skey,
		collection: collections,
		action:     actions,
		manage:     manage,
	}, nil
}

// Type returns the concrete space-type NSID or "*" wildcard.
func (p SpacePermission) Type() string { return p.spaceType }

// Authority returns a concrete authority DID, "self", or "*" wildcard.
func (p SpacePermission) Authority() string { return p.authority }

// SKey returns the concrete space key or "*" wildcard.
func (p SpacePermission) SKey() string { return p.skey }

// Collections returns the canonical allowed write collections. An empty result
// permits no writes until declaration defaults are explicitly expanded.
func (p SpacePermission) Collections() []string { return slices.Clone(p.collection) }

// Actions returns the canonical record-action list.
func (p SpacePermission) Actions() []SpaceAction { return slices.Clone(p.action) }

// ManageOps returns the canonical space-management operation list.
func (p SpacePermission) ManageOps() []SpaceManageOp { return slices.Clone(p.manage) }

// IsSelfAuthority reports whether the authority still needs issuance-time
// resolution to the granting user's DID.
func (p SpacePermission) IsSelfAuthority() bool { return p.authority == "self" }

// Matches reports whether the permission grants target. Invalid targets and
// permissions with an unresolved "self" authority fail closed.
func (p SpacePermission) Matches(target SpacePermissionMatch) bool {
	if validateSpacePermissionMatch(target) != nil || p.authority == "self" {
		return false
	}
	if p.spaceType != "*" && p.spaceType != target.Type.String() {
		return false
	}
	if p.authority != "*" && p.authority != target.Authority.String() {
		return false
	}
	if p.skey != "*" && p.skey != target.SKey.String() {
		return false
	}

	if target.Manage != "" {
		return slices.Contains(p.manage, target.Manage)
	}
	switch target.Action {
	case SpaceActionRead:
		return slices.Contains(p.action, SpaceActionRead)
	case SpaceActionReadSelf:
		return slices.Contains(p.action, SpaceActionRead) || slices.Contains(p.action, SpaceActionReadSelf)
	default:
		return slices.Contains(p.action, target.Action) &&
			(slices.Contains(p.collection, "*") || slices.Contains(p.collection, target.Collection.String()))
	}
}

// SpacePermissionExpansion supplies issuance-time facts that scope parsing
// deliberately cannot infer. SpaceType and Collections must come from the
// declaration selected by the issuer; UserDID is the granting user's DID.
type SpacePermissionExpansion struct {
	UserDID     atmos.DID
	SpaceType   atmos.NSID
	Collections []atmos.NSID
}

// ExpandDefaults resolves an authority of "self" and, only when the scope
// omitted collections, freezes the selected declaration's collection list into
// the returned permission. It never mutates p. Declaration defaults cannot be
// applied to a wildcard-type permission or a different concrete type. When the
// scope carries explicit collections, the declaration inputs are ignored
// entirely so an unrelated malformed declaration cannot reject a valid grant.
func (p SpacePermission) ExpandDefaults(expansion SpacePermissionExpansion) (SpacePermission, error) {
	if expansion.UserDID != "" {
		if err := expansion.UserDID.Validate(); err != nil {
			return SpacePermission{}, invalidSpacePermissionExpansion("user DID: %v", err)
		}
	}

	result := p.clone()
	if result.authority == "self" {
		if expansion.UserDID == "" {
			return SpacePermission{}, invalidSpacePermissionExpansion("self authority requires a user DID")
		}
		result.authority = expansion.UserDID.String()
	}
	if len(result.collection) != 0 {
		return result, nil
	}

	if result.spaceType == "*" {
		return SpacePermission{}, invalidSpacePermissionExpansion("one declaration cannot expand a wildcard space type")
	}
	if expansion.SpaceType == "" {
		return SpacePermission{}, invalidSpacePermissionExpansion("collection defaults require a declaration space type")
	}
	if err := expansion.SpaceType.Validate(); err != nil {
		return SpacePermission{}, invalidSpacePermissionExpansion("space type: %v", err)
	}
	if result.spaceType != expansion.SpaceType.String() {
		return SpacePermission{}, invalidSpacePermissionExpansion(
			"declaration type %q does not match permission type %q",
			expansion.SpaceType,
			result.spaceType,
		)
	}
	collections := make([]string, len(expansion.Collections))
	for i, collection := range expansion.Collections {
		if err := collection.Validate(); err != nil {
			return SpacePermission{}, invalidSpacePermissionExpansion("collection %d: %v", i, err)
		}
		collections[i] = collection.String()
	}
	result.collection = canonicalStrings(collections)

	return result, nil
}

// SpacePermissionMatch identifies one concrete record or management operation.
// Exactly one of Action and Manage must be set. Collection must be set only for
// create, update, and delete actions.
type SpacePermissionMatch struct {
	Type       atmos.NSID
	Authority  atmos.DID
	SKey       atmos.RecordKey
	Collection atmos.NSID
	Action     SpaceAction
	Manage     SpaceManageOp
}

// SpaceScopeNeededFor returns the narrowest space scope that grants target.
// Management scopes explicitly carry read_self because omitting action would
// request all default record actions, including writes.
func SpaceScopeNeededFor(target SpacePermissionMatch) (string, error) {
	if err := validateSpacePermissionMatch(target); err != nil {
		return "", invalidSpacePermission("target: %v", err)
	}

	permission := SpacePermission{
		spaceType: target.Type.String(),
		authority: target.Authority.String(),
		skey:      target.SKey.String(),
	}
	if target.Manage != "" {
		permission.action = []SpaceAction{SpaceActionReadSelf}
		permission.manage = []SpaceManageOp{target.Manage}
	} else {
		permission.action = []SpaceAction{target.Action}
		if target.Collection != "" {
			permission.collection = []string{target.Collection.String()}
		}
	}
	return permission.String(), nil
}

// String returns the canonical OAuth scope representation.
func (p SpacePermission) String() string {
	var builder strings.Builder
	builder.Grow(64)
	builder.WriteString("space:")
	builder.WriteString(escapeSpaceScopeComponent(p.spaceType))

	first := true
	appendParam := func(key, value string) {
		if first {
			builder.WriteByte('?')
			first = false
		} else {
			builder.WriteByte('&')
		}
		builder.WriteString(key)
		builder.WriteByte('=')
		builder.WriteString(escapeSpaceScopeComponent(value))
	}
	if p.authority != "self" {
		appendParam("authority", p.authority)
	}
	if p.skey != "*" {
		appendParam("skey", p.skey)
	}
	for _, collection := range p.collection {
		appendParam("collection", collection)
	}
	if !slices.Equal(p.action, spaceDefaultActions) {
		for _, action := range p.action {
			appendParam("action", string(action))
		}
	}
	for _, manage := range p.manage {
		appendParam("manage", string(manage))
	}
	return builder.String()
}

func (p SpacePermission) clone() SpacePermission {
	p.collection = slices.Clone(p.collection)
	p.action = slices.Clone(p.action)
	p.manage = slices.Clone(p.manage)
	return p
}

func singleSpaceParam(values url.Values, key, defaultValue string) (string, error) {
	params := values[key]
	if len(params) > 1 {
		return "", invalidSpacePermission("%s occurs more than once", key)
	}
	if len(params) == 0 {
		return defaultValue, nil
	}
	return params[0], nil
}

func parseSpaceCollections(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	for _, value := range values {
		if value == "*" {
			continue
		}
		if _, err := atmos.ParseNSID(value); err != nil {
			return nil, invalidSpacePermission("collection %q: %v", value, err)
		}
	}
	if slices.Contains(values, "*") {
		return []string{"*"}, nil
	}
	return canonicalStrings(values), nil
}

func parseSpaceActions(values []string) ([]SpaceAction, error) {
	if len(values) == 0 {
		return slices.Clone(spaceDefaultActions), nil
	}
	seen := make(map[SpaceAction]struct{}, len(values))
	for _, value := range values {
		action := SpaceAction(value)
		if !slices.Contains(spaceActionOrder, action) {
			return nil, invalidSpacePermission("unknown action %q", value)
		}
		seen[action] = struct{}{}
	}
	result := make([]SpaceAction, 0, len(seen))
	for _, action := range spaceActionOrder {
		if _, ok := seen[action]; ok {
			result = append(result, action)
		}
	}
	return result, nil
}

func parseSpaceManageOps(values []string) ([]SpaceManageOp, error) {
	if len(values) == 0 {
		return nil, nil
	}
	seen := make(map[SpaceManageOp]struct{}, len(values))
	for _, value := range values {
		op := SpaceManageOp(value)
		if !slices.Contains(spaceManageOrder, op) {
			return nil, invalidSpacePermission("unknown manage operation %q", value)
		}
		seen[op] = struct{}{}
	}
	result := make([]SpaceManageOp, 0, len(seen))
	for _, op := range spaceManageOrder {
		if _, ok := seen[op]; ok {
			result = append(result, op)
		}
	}
	return result, nil
}

func validateSpacePermissionMatch(target SpacePermissionMatch) error {
	if err := target.Type.Validate(); err != nil {
		return fmt.Errorf("type: %w", err)
	}
	if err := target.Authority.Validate(); err != nil {
		return fmt.Errorf("authority: %w", err)
	}
	if err := target.SKey.Validate(); err != nil {
		return fmt.Errorf("skey: %w", err)
	}
	if (target.Action == "") == (target.Manage == "") {
		return errors.New("exactly one of action and manage must be set")
	}
	if target.Manage != "" {
		if !slices.Contains(spaceManageOrder, target.Manage) {
			return fmt.Errorf("unknown manage operation %q", target.Manage)
		}
		if target.Collection != "" {
			return errors.New("management operation cannot name a collection")
		}
		return nil
	}
	if !slices.Contains(spaceActionOrder, target.Action) {
		return fmt.Errorf("unknown action %q", target.Action)
	}
	switch target.Action {
	case SpaceActionRead, SpaceActionReadSelf:
		if target.Collection != "" {
			return errors.New("read action cannot name a collection")
		}
	default:
		if err := target.Collection.Validate(); err != nil {
			return fmt.Errorf("collection: %w", err)
		}
	}
	return nil
}

func validateSpaceType(value string) error {
	if value == "*" {
		return nil
	}
	_, err := atmos.ParseNSID(value)
	return err
}

func validateSpaceAuthority(value string) error {
	if value == "*" || value == "self" {
		return nil
	}
	_, err := atmos.ParseDID(value)
	return err
}

func canonicalStrings(values []string) []string {
	result := slices.Clone(values)
	sort.Strings(result)
	return slices.Compact(result)
}

func escapeSpaceScopeComponent(value string) string {
	const hex = "0123456789ABCDEF"
	var builder strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		if isUnescapedSpaceScopeByte(c) {
			builder.WriteByte(c)
			continue
		}
		builder.WriteByte('%')
		builder.WriteByte(hex[c>>4])
		builder.WriteByte(hex[c&0x0f])
	}
	return builder.String()
}

func isUnescapedSpaceScopeByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
		strings.ContainsRune("-_.!~*'():/+,@", rune(c))
}

func invalidSpacePermission(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidSpacePermission, fmt.Sprintf(format, args...))
}

func invalidSpacePermissionExpansion(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidSpacePermissionExpansion, fmt.Sprintf(format, args...))
}
