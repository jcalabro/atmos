package simplespace

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/gt"
)

// PolicyKind is a closed simple-space user-policy variant.
type PolicyKind uint8

const (
	// PolicyPublic permits every valid DID.
	PolicyPublic PolicyKind = iota + 1
	// PolicyMemberList consults the authority's member list.
	PolicyMemberList
	// PolicyManagingApp delegates the decision to a configured service.
	PolicyManagingApp
)

// AppAccessKind is a closed application-access variant.
type AppAccessKind uint8

const (
	// AppAccessOpen permits applications without an attestation.
	AppAccessOpen AppAccessKind = iota + 1
	// AppAccessAllowList requires a verified client attestation whose client_id
	// appears in Allowed.
	AppAccessAllowList
)

// Access identifies the policy perimeter being evaluated.
type Access uint8

const (
	// AccessRead controls credential issuance and policy reads.
	AccessRead Access = iota + 1
	// AccessWrite controls writer-directory admission and forwarding.
	AccessWrite
)

// Policy is one closed read or write policy.
type Policy struct {
	Kind        PolicyKind
	ManagingApp string
}

// Validate validates a policy without applying it.
func (p Policy) Validate() error {
	switch p.Kind {
	case PolicyPublic, PolicyMemberList:
		if p.ManagingApp != "" {
			return errors.New("simplespace: managing app is only valid for a managing-app policy")
		}
	case PolicyManagingApp:
		if err := ValidateServiceIdentifier(p.ManagingApp); err != nil {
			return fmt.Errorf("simplespace: invalid managing app: %w", err)
		}
	default:
		return fmt.Errorf("simplespace: unsupported policy kind %d", p.Kind)
	}
	return nil
}

// AppAccess is one closed application-access policy.
type AppAccess struct {
	Kind    AppAccessKind
	Allowed []string
}

// Validate validates an application policy and rejects ambiguous or duplicate
// allowlists.
func (a AppAccess) Validate() error {
	switch a.Kind {
	case AppAccessOpen:
		if len(a.Allowed) != 0 {
			return errors.New("simplespace: open app access cannot contain an allowlist")
		}
	case AppAccessAllowList:
		seen := make(map[string]struct{}, len(a.Allowed))
		for i, clientID := range a.Allowed {
			if clientID == "" || len(clientID) > 2048 || strings.TrimSpace(clientID) != clientID {
				return fmt.Errorf("simplespace: invalid allowed client_id at index %d", i)
			}
			if _, ok := seen[clientID]; ok {
				return fmt.Errorf("simplespace: duplicate allowed client_id %q", clientID)
			}
			seen[clientID] = struct{}{}
		}
	default:
		return fmt.Errorf("simplespace: unsupported app-access kind %d", a.Kind)
	}
	return nil
}

// Config is the complete policy configuration for one space.
type Config struct {
	URI         atmos.SpaceRef
	ReadPolicy  Policy
	WritePolicy Policy
	AppAccess   AppAccess
}

// Validate validates the complete policy configuration.
func (c Config) Validate() error {
	if err := c.URI.Validate(); err != nil {
		return fmt.Errorf("simplespace: invalid space: %w", err)
	}
	if err := c.ReadPolicy.Validate(); err != nil {
		return fmt.Errorf("simplespace: invalid read policy: %w", err)
	}
	if err := c.WritePolicy.Validate(); err != nil {
		return fmt.Errorf("simplespace: invalid write policy: %w", err)
	}
	if err := c.AppAccess.Validate(); err != nil {
		return fmt.Errorf("simplespace: invalid app access: %w", err)
	}
	return nil
}

// Clone returns a deep copy safe to retain across store boundaries.
func (c Config) Clone() Config {
	c.AppAccess.Allowed = slices.Clone(c.AppAccess.Allowed)
	return c
}

// Patch contains atomic replacement values for updateSpace. Nil fields are
// unchanged; non-nil policy values replace the complete previous value.
type Patch struct {
	ReadPolicy  *Policy
	WritePolicy *Policy
	AppAccess   *AppAccess
}

// Validate validates all supplied replacements and rejects an empty patch.
func (p Patch) Validate() error {
	if p.ReadPolicy == nil && p.WritePolicy == nil && p.AppAccess == nil {
		return errors.New("simplespace: update contains no changes")
	}
	if p.ReadPolicy != nil {
		if err := p.ReadPolicy.Validate(); err != nil {
			return fmt.Errorf("simplespace: invalid read policy: %w", err)
		}
	}
	if p.WritePolicy != nil {
		if err := p.WritePolicy.Validate(); err != nil {
			return fmt.Errorf("simplespace: invalid write policy: %w", err)
		}
	}
	if p.AppAccess != nil {
		if err := p.AppAccess.Validate(); err != nil {
			return fmt.Errorf("simplespace: invalid app access: %w", err)
		}
	}
	return nil
}

// Member is the complete replaceable member-list entry.
type Member struct {
	DID   atmos.DID
	Read  bool
	Write bool
}

// Validate validates a member entry.
func (m Member) Validate() error {
	if err := m.DID.Validate(); err != nil {
		return fmt.Errorf("simplespace: invalid member DID: %w", err)
	}
	return nil
}

// ValidateServiceIdentifier validates a DID with an optional single fragment.
func ValidateServiceIdentifier(value string) error {
	didText, fragment, hasFragment := strings.Cut(value, "#")
	if _, err := atmos.ParseDID(didText); err != nil {
		return err
	}
	if hasFragment && (fragment == "" || strings.Contains(fragment, "#")) {
		return errors.New("service identifier has an invalid fragment")
	}
	return nil
}

func decodePolicy(value any) (Policy, error) {
	switch value := value.(type) {
	case comatproto.SimplespaceCreateSpace_Input_ReadPolicy:
		return policyFromRefs(value.SimplespaceDefs_PublicPolicy, value.SimplespaceDefs_MemberListPolicy, value.SimplespaceDefs_ManagingAppPolicy, value.Unknown.HasVal())
	case comatproto.SimplespaceCreateSpace_Input_WritePolicy:
		return policyFromRefs(value.SimplespaceDefs_PublicPolicy, value.SimplespaceDefs_MemberListPolicy, value.SimplespaceDefs_ManagingAppPolicy, value.Unknown.HasVal())
	case comatproto.SimplespaceUpdateSpace_Input_ReadPolicy:
		return policyFromRefs(value.SimplespaceDefs_PublicPolicy, value.SimplespaceDefs_MemberListPolicy, value.SimplespaceDefs_ManagingAppPolicy, value.Unknown.HasVal())
	case comatproto.SimplespaceUpdateSpace_Input_WritePolicy:
		return policyFromRefs(value.SimplespaceDefs_PublicPolicy, value.SimplespaceDefs_MemberListPolicy, value.SimplespaceDefs_ManagingAppPolicy, value.Unknown.HasVal())
	case comatproto.SimplespaceGetSpace_Output_ReadPolicy:
		return policyFromRefs(value.SimplespaceDefs_PublicPolicy, value.SimplespaceDefs_MemberListPolicy, value.SimplespaceDefs_ManagingAppPolicy, value.Unknown.HasVal())
	case comatproto.SimplespaceGetSpace_Output_WritePolicy:
		return policyFromRefs(value.SimplespaceDefs_PublicPolicy, value.SimplespaceDefs_MemberListPolicy, value.SimplespaceDefs_ManagingAppPolicy, value.Unknown.HasVal())
	default:
		return Policy{}, fmt.Errorf("simplespace: unsupported generated policy %T", value)
	}
}

func policyFromRefs(public gt.Ref[comatproto.SimplespaceDefs_PublicPolicy], member gt.Ref[comatproto.SimplespaceDefs_MemberListPolicy], managing gt.Ref[comatproto.SimplespaceDefs_ManagingAppPolicy], unknown bool) (Policy, error) {
	count := 0
	if public.HasVal() {
		count++
	}
	if member.HasVal() {
		count++
	}
	if managing.HasVal() {
		count++
	}
	if unknown || count != 1 {
		return Policy{}, errors.New("simplespace: unknown or ambiguous policy variant")
	}
	if public.HasVal() {
		return Policy{Kind: PolicyPublic}, nil
	}
	if member.HasVal() {
		return Policy{Kind: PolicyMemberList}, nil
	}
	policy := Policy{Kind: PolicyManagingApp, ManagingApp: managing.Val().ManagingApp}
	return policy, policy.Validate()
}

func decodeAppAccess(value any) (AppAccess, error) {
	var open gt.Ref[comatproto.SimplespaceDefs_Open]
	var allow gt.Ref[comatproto.SimplespaceDefs_AllowList]
	var unknown bool
	switch value := value.(type) {
	case comatproto.SimplespaceCreateSpace_Input_AppAccess:
		open, allow, unknown = value.SimplespaceDefs_Open, value.SimplespaceDefs_AllowList, value.Unknown.HasVal()
	case comatproto.SimplespaceUpdateSpace_Input_AppAccess:
		open, allow, unknown = value.SimplespaceDefs_Open, value.SimplespaceDefs_AllowList, value.Unknown.HasVal()
	case comatproto.SimplespaceGetSpace_Output_AppAccess:
		open, allow, unknown = value.SimplespaceDefs_Open, value.SimplespaceDefs_AllowList, value.Unknown.HasVal()
	default:
		return AppAccess{}, fmt.Errorf("simplespace: unsupported generated app policy %T", value)
	}
	if unknown || open.HasVal() == allow.HasVal() {
		return AppAccess{}, errors.New("simplespace: unknown or ambiguous app-access variant")
	}
	if open.HasVal() {
		return AppAccess{Kind: AppAccessOpen}, nil
	}
	result := AppAccess{Kind: AppAccessAllowList, Allowed: slices.Clone(allow.Val().Allowed)}
	return result, result.Validate()
}

// DecodeCreate validates and converts the generated createSpace input.
func DecodeCreate(uri atmos.SpaceRef, input *comatproto.SimplespaceCreateSpace_Input) (Config, error) {
	if input == nil {
		return Config{}, errors.New("simplespace: create input is required")
	}
	read, err := decodePolicy(input.ReadPolicy)
	if err != nil {
		return Config{}, fmt.Errorf("simplespace: read policy: %w", err)
	}
	write, err := decodePolicy(input.WritePolicy)
	if err != nil {
		return Config{}, fmt.Errorf("simplespace: write policy: %w", err)
	}
	apps, err := decodeAppAccess(input.AppAccess)
	if err != nil {
		return Config{}, fmt.Errorf("simplespace: app access: %w", err)
	}
	config := Config{URI: uri, ReadPolicy: read, WritePolicy: write, AppAccess: apps}
	return config, config.Validate()
}

// DecodeUpdate validates and converts the generated updateSpace input.
func DecodeUpdate(input *comatproto.SimplespaceUpdateSpace_Input) (Patch, error) {
	if input == nil {
		return Patch{}, errors.New("simplespace: update input is required")
	}
	var patch Patch
	if input.ReadPolicy.HasVal() {
		value, err := decodePolicy(input.ReadPolicy.Val())
		if err != nil {
			return Patch{}, err
		}
		patch.ReadPolicy = &value
	}
	if input.WritePolicy.HasVal() {
		value, err := decodePolicy(input.WritePolicy.Val())
		if err != nil {
			return Patch{}, err
		}
		patch.WritePolicy = &value
	}
	if input.AppAccess.HasVal() {
		value, err := decodeAppAccess(input.AppAccess.Val())
		if err != nil {
			return Patch{}, err
		}
		patch.AppAccess = &value
	}
	return patch, patch.Validate()
}

// DecodeOutput validates and converts a getSpace response.
func DecodeOutput(output *comatproto.SimplespaceGetSpace_Output) (Config, error) {
	if output == nil {
		return Config{}, errors.New("simplespace: getSpace output is required")
	}
	uri, err := atmos.ParseSpaceRef(output.URI)
	if err != nil {
		return Config{}, fmt.Errorf("simplespace: invalid output URI: %w", err)
	}
	read, err := decodePolicy(output.ReadPolicy)
	if err != nil {
		return Config{}, err
	}
	write, err := decodePolicy(output.WritePolicy)
	if err != nil {
		return Config{}, err
	}
	apps, err := decodeAppAccess(output.AppAccess)
	if err != nil {
		return Config{}, err
	}
	config := Config{URI: uri, ReadPolicy: read, WritePolicy: write, AppAccess: apps}
	return config, config.Validate()
}

// CreateInput constructs a generated createSpace input from closed policy
// types. SKey may be empty to request server generation.
func CreateInput(typ atmos.NSID, skey atmos.RecordKey, read, write Policy, apps AppAccess) (*comatproto.SimplespaceCreateSpace_Input, error) {
	if err := typ.Validate(); err != nil {
		return nil, fmt.Errorf("simplespace: invalid space type: %w", err)
	}
	if skey != "" {
		if err := skey.Validate(); err != nil {
			return nil, fmt.Errorf("simplespace: invalid space key: %w", err)
		}
	}
	if err := read.Validate(); err != nil {
		return nil, err
	}
	if err := write.Validate(); err != nil {
		return nil, err
	}
	if err := apps.Validate(); err != nil {
		return nil, err
	}
	input := &comatproto.SimplespaceCreateSpace_Input{
		Type: string(typ), ReadPolicy: createReadPolicy(read),
		WritePolicy: createWritePolicy(write), AppAccess: createAppAccess(apps),
	}
	if skey != "" {
		input.Skey = gt.Some(string(skey))
	}
	return input, nil
}

func createReadPolicy(policy Policy) comatproto.SimplespaceCreateSpace_Input_ReadPolicy {
	var result comatproto.SimplespaceCreateSpace_Input_ReadPolicy
	setPolicy(policy, &result.SimplespaceDefs_PublicPolicy, &result.SimplespaceDefs_MemberListPolicy, &result.SimplespaceDefs_ManagingAppPolicy)
	return result
}

func createWritePolicy(policy Policy) comatproto.SimplespaceCreateSpace_Input_WritePolicy {
	var result comatproto.SimplespaceCreateSpace_Input_WritePolicy
	setPolicy(policy, &result.SimplespaceDefs_PublicPolicy, &result.SimplespaceDefs_MemberListPolicy, &result.SimplespaceDefs_ManagingAppPolicy)
	return result
}

func createAppAccess(apps AppAccess) comatproto.SimplespaceCreateSpace_Input_AppAccess {
	var result comatproto.SimplespaceCreateSpace_Input_AppAccess
	setAppAccess(apps, &result.SimplespaceDefs_Open, &result.SimplespaceDefs_AllowList)
	return result
}

// UpdateInput constructs a generated updateSpace input from an atomic patch.
func UpdateInput(space atmos.SpaceRef, patch Patch) (*comatproto.SimplespaceUpdateSpace_Input, error) {
	if err := space.Validate(); err != nil {
		return nil, err
	}
	if err := patch.Validate(); err != nil {
		return nil, err
	}
	input := &comatproto.SimplespaceUpdateSpace_Input{Space: space.String()}
	if patch.ReadPolicy != nil {
		var value comatproto.SimplespaceUpdateSpace_Input_ReadPolicy
		setPolicy(*patch.ReadPolicy, &value.SimplespaceDefs_PublicPolicy, &value.SimplespaceDefs_MemberListPolicy, &value.SimplespaceDefs_ManagingAppPolicy)
		input.ReadPolicy = gt.Some(value)
	}
	if patch.WritePolicy != nil {
		var value comatproto.SimplespaceUpdateSpace_Input_WritePolicy
		setPolicy(*patch.WritePolicy, &value.SimplespaceDefs_PublicPolicy, &value.SimplespaceDefs_MemberListPolicy, &value.SimplespaceDefs_ManagingAppPolicy)
		input.WritePolicy = gt.Some(value)
	}
	if patch.AppAccess != nil {
		var value comatproto.SimplespaceUpdateSpace_Input_AppAccess
		setAppAccess(*patch.AppAccess, &value.SimplespaceDefs_Open, &value.SimplespaceDefs_AllowList)
		input.AppAccess = gt.Some(value)
	}
	return input, nil
}

func setPolicy(policy Policy, public *gt.Ref[comatproto.SimplespaceDefs_PublicPolicy], member *gt.Ref[comatproto.SimplespaceDefs_MemberListPolicy], managing *gt.Ref[comatproto.SimplespaceDefs_ManagingAppPolicy]) {
	switch policy.Kind {
	case PolicyPublic:
		*public = gt.SomeRef(comatproto.SimplespaceDefs_PublicPolicy{})
	case PolicyMemberList:
		*member = gt.SomeRef(comatproto.SimplespaceDefs_MemberListPolicy{})
	case PolicyManagingApp:
		*managing = gt.SomeRef(comatproto.SimplespaceDefs_ManagingAppPolicy{ManagingApp: policy.ManagingApp})
	}
}

func setAppAccess(apps AppAccess, open *gt.Ref[comatproto.SimplespaceDefs_Open], allow *gt.Ref[comatproto.SimplespaceDefs_AllowList]) {
	if apps.Kind == AppAccessOpen {
		*open = gt.SomeRef(comatproto.SimplespaceDefs_Open{})
	} else {
		*allow = gt.SomeRef(comatproto.SimplespaceDefs_AllowList{Allowed: slices.Clone(apps.Allowed)})
	}
}

// Output converts a validated configuration to the generated wire response.
func Output(config Config) (*comatproto.SimplespaceGetSpace_Output, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	var output comatproto.SimplespaceGetSpace_Output
	output.URI = config.URI.String()
	setPolicy(config.ReadPolicy, &output.ReadPolicy.SimplespaceDefs_PublicPolicy, &output.ReadPolicy.SimplespaceDefs_MemberListPolicy, &output.ReadPolicy.SimplespaceDefs_ManagingAppPolicy)
	setPolicy(config.WritePolicy, &output.WritePolicy.SimplespaceDefs_PublicPolicy, &output.WritePolicy.SimplespaceDefs_MemberListPolicy, &output.WritePolicy.SimplespaceDefs_ManagingAppPolicy)
	setAppAccess(config.AppAccess, &output.AppAccess.SimplespaceDefs_Open, &output.AppAccess.SimplespaceDefs_AllowList)
	return &output, nil
}
