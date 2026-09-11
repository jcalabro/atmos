package host

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/oauth"
	"github.com/jcalabro/atmos/space/credential"
	"github.com/jcalabro/atmos/space/simplespace"
)

var (
	// ErrNoAccountCredential tells mixed-auth handlers that the request did not
	// present an account credential. Other authenticator errors are terminal.
	ErrNoAccountCredential = errors.New("space host: no account credential")
	// ErrDenied is a policy denial, distinct from evaluator or dependency failure.
	ErrDenied = errors.New("space host: access denied")
	// ErrAppDenied attributes a credential refusal to application policy.
	ErrAppDenied = fmt.Errorf("%w: application", ErrDenied)
	// ErrUserDenied attributes a credential or writer refusal to user policy.
	ErrUserDenied = fmt.Errorf("%w: user", ErrDenied)
	// ErrInvalidDelegation identifies delegation/DPoP exchange authentication.
	ErrInvalidDelegation = errors.New("space host: invalid delegation authentication")
	// ErrInvalidClientAttestation identifies metadata/JWKS attestation failure.
	ErrInvalidClientAttestation = errors.New("space host: invalid client attestation")
	// ErrManagingAppUnavailable identifies fail-closed managing-app errors.
	ErrManagingAppUnavailable = errors.New("space host: managing app unavailable")
)

// AccountAction identifies an exact management permission check.
type AccountAction uint8

const (
	// AccountReadSelf is required for owner getSpace/listMembers calls.
	AccountReadSelf AccountAction = iota + 1
	// AccountCreate is required to create a space.
	AccountCreate
	// AccountUpdate is required to update a space or its membership.
	AccountUpdate
	// AccountDelete is required to permanently delete a space.
	AccountDelete
)

// AccountPermissions evaluates the account OAuth grant for one concrete space.
type AccountPermissions interface {
	Allows(space atmos.SpaceRef, action AccountAction) bool
}

// AccountPermissionsFunc adapts a function to AccountPermissions.
type AccountPermissionsFunc func(atmos.SpaceRef, AccountAction) bool

// Allows implements AccountPermissions.
func (f AccountPermissionsFunc) Allows(space atmos.SpaceRef, action AccountAction) bool {
	return f != nil && f(space, action)
}

// OAuthAccountPermissions adapts expanded OAuth space permissions to the
// exact authority-host permission checks. Unexpanded self-authority grants and
// invalid spaces or actions fail closed through oauth.SpacePermission.Matches.
type OAuthAccountPermissions []oauth.SpacePermission

// Allows implements AccountPermissions.
func (p OAuthAccountPermissions) Allows(space atmos.SpaceRef, action AccountAction) bool {
	target := oauth.SpacePermissionMatch{
		Type:      space.Type(),
		Authority: space.Authority(),
		SKey:      space.Key(),
	}
	switch action {
	case AccountReadSelf:
		target.Action = oauth.SpaceActionReadSelf
	case AccountCreate:
		target.Manage = oauth.SpaceManageCreate
	case AccountUpdate:
		target.Manage = oauth.SpaceManageUpdate
	case AccountDelete:
		target.Manage = oauth.SpaceManageDelete
	default:
		return false
	}
	for _, permission := range p {
		if permission.Matches(target) {
			return true
		}
	}
	return false
}

// AccountPrincipal is returned only after the embedding resource server has
// validated issuer, audience, expiry, account binding, and DPoP as applicable.
type AccountPrincipal struct {
	DID         atmos.DID
	Permissions AccountPermissions
}

// AccountAuthenticator authenticates account OAuth for the current authority
// deployment. It must not accept generic service-auth or space credentials.
type AccountAuthenticator interface {
	AuthenticateAccount(context.Context, *http.Request) (AccountPrincipal, error)
}

// AccountAuthenticatorFunc adapts a function to AccountAuthenticator.
type AccountAuthenticatorFunc func(context.Context, *http.Request) (AccountPrincipal, error)

// AuthenticateAccount implements AccountAuthenticator.
func (f AccountAuthenticatorFunc) AuthenticateAccount(ctx context.Context, req *http.Request) (AccountPrincipal, error) {
	if f == nil {
		return AccountPrincipal{}, ErrNoAccountCredential
	}
	return f(ctx, req)
}

// Signer separates reusable credential signing from service-auth signing.
type Signer interface {
	CredentialKey(context.Context, atmos.DID) (crypto.PrivateKey, string, error)
	// ServiceKey returns the authority's #atproto key: notification peers verify
	// a bare authority issuer through that exact DID verification method.
	ServiceKey(context.Context, atmos.DID) (crypto.PrivateKey, error)
}

// AttestationVerifier validates metadata/JWKS, signature, bindings, lifetime,
// and replay before returning the attested client_id.
type AttestationVerifier interface {
	VerifyAttestation(context.Context, string, string, time.Time, credential.ReplayStore) (string, error)
}

// AttestationVerifierFunc adapts a function to AttestationVerifier.
type AttestationVerifierFunc func(context.Context, string, string, time.Time, credential.ReplayStore) (string, error)

// VerifyAttestation implements AttestationVerifier.
func (f AttestationVerifierFunc) VerifyAttestation(ctx context.Context, raw, audience string, now time.Time, replay credential.ReplayStore) (string, error) {
	return f(ctx, raw, audience, now, replay)
}

// MetadataAttestationVerifier uses the hardened credential metadata/JWKS
// resolver implemented by space/credential.
type MetadataAttestationVerifier struct {
	Resolver *credential.ClientMetadataResolver
}

// VerifyAttestation implements AttestationVerifier.
func (v MetadataAttestationVerifier) VerifyAttestation(ctx context.Context, raw, audience string, now time.Time, replay credential.ReplayStore) (string, error) {
	verified, err := credential.VerifyResolvedClientAttestation(ctx, raw, credential.VerifyResolvedClientAttestationOptions{
		Resolver: v.Resolver, Audience: audience, Now: now, Replay: replay,
	})
	if err != nil {
		return "", err
	}
	return verified.Issuer, nil
}

// PolicyRequest is evaluated against one immutable policy generation.
type PolicyRequest struct {
	Config   simplespace.Config
	Member   *simplespace.Member
	User     atmos.DID
	Access   simplespace.Access
	ClientID string
}

// PolicyEvaluator evaluates public, member-list, and managing-app variants.
// Errors fail closed and remain distinguishable from a negative decision.
type PolicyEvaluator interface {
	Authorize(context.Context, PolicyRequest) (bool, error)
}

// PolicyEvaluatorFunc adapts a function to PolicyEvaluator.
type PolicyEvaluatorFunc func(context.Context, PolicyRequest) (bool, error)

// Authorize implements PolicyEvaluator.
func (f PolicyEvaluatorFunc) Authorize(ctx context.Context, req PolicyRequest) (bool, error) {
	return f(ctx, req)
}

// ManagingAppChecker calls one configured managing-app service with authority
// service authentication.
type ManagingAppChecker interface {
	CheckUserAccess(context.Context, string, atmos.SpaceRef, atmos.DID, simplespace.Access, string) (bool, error)
}

// SimplePolicyEvaluator implements the closed policy variants.
type SimplePolicyEvaluator struct {
	ManagingApp ManagingAppChecker
}

// Authorize implements PolicyEvaluator.
func (e SimplePolicyEvaluator) Authorize(ctx context.Context, req PolicyRequest) (bool, error) {
	if req.User == req.Config.URI.Authority() {
		return true, nil
	}
	policy := req.Config.ReadPolicy
	if req.Access == simplespace.AccessWrite {
		policy = req.Config.WritePolicy
	} else if req.Access != simplespace.AccessRead {
		return false, errors.New("space host: invalid policy access")
	}
	switch policy.Kind {
	case simplespace.PolicyPublic:
		return true, nil
	case simplespace.PolicyMemberList:
		if req.Member == nil {
			return false, nil
		}
		if req.Access == simplespace.AccessRead {
			return req.Member.Read, nil
		}
		return req.Member.Write, nil
	case simplespace.PolicyManagingApp:
		if e.ManagingApp == nil {
			return false, ErrManagingAppUnavailable
		}
		allowed, err := e.ManagingApp.CheckUserAccess(ctx, policy.ManagingApp, req.Config.URI, req.User, req.Access, req.ClientID)
		if err != nil {
			return false, errors.Join(ErrManagingAppUnavailable, err)
		}
		return allowed, nil
	default:
		return false, errors.New("space host: unsupported policy variant")
	}
}

// SubscriberOperation distinguishes admission from withdrawal. Both require an
// explicit policy decision because the wire protocol proves neither ownership.
type SubscriberOperation uint8

const (
	// SubscriberRegister requests delivery admission.
	SubscriberRegister SubscriberOperation = iota + 1
	// SubscriberWithdraw requests removal of an existing service.
	SubscriberWithdraw
)

// SubscriberRequest is an explicit callback-policy decision input.
type SubscriberRequest struct {
	Operation    SubscriberOperation
	Space        atmos.SpaceRef
	Service      string
	CredentialID string
}

// SubscriberDecision names the exact DID service type to resolve. Allowed must
// be true; the zero value denies.
type SubscriberDecision struct {
	Allowed     bool
	ServiceType string
}

// SubscriberPolicy explicitly admits or withdraws callback targets.
type SubscriberPolicy interface {
	AuthorizeSubscriber(context.Context, SubscriberRequest) (SubscriberDecision, error)
}

// SubscriberPolicyFunc adapts a function to SubscriberPolicy.
type SubscriberPolicyFunc func(context.Context, SubscriberRequest) (SubscriberDecision, error)

// AuthorizeSubscriber implements SubscriberPolicy.
func (f SubscriberPolicyFunc) AuthorizeSubscriber(ctx context.Context, req SubscriberRequest) (SubscriberDecision, error) {
	return f(ctx, req)
}

// DeliveryTransport performs exactly one notification HTTP attempt. The Host
// creates a fresh service JWT and strictly resolves endpoint for every attempt.
type DeliveryTransport interface {
	Deliver(context.Context, *url.URL, atmos.NSID, string, any) error
}

// DeliveryTransportFunc adapts a function to DeliveryTransport.
type DeliveryTransportFunc func(context.Context, *url.URL, atmos.NSID, string, any) error

// Deliver implements DeliveryTransport.
func (f DeliveryTransportFunc) Deliver(ctx context.Context, endpoint *url.URL, method atmos.NSID, token string, body any) error {
	return f(ctx, endpoint, method, token, body)
}

// Clock supplies wall time for auth, leases, and durable scheduling.
type Clock interface{ Now() time.Time }

// ClockFunc adapts a function to Clock.
type ClockFunc func() time.Time

// Now implements Clock.
func (f ClockFunc) Now() time.Time { return f() }

// EventKind identifies structured operational feedback.
type EventKind string

const (
	// EventCredentialIssued reports successful issuance without token material.
	EventCredentialIssued EventKind = "credential-issued"
	// EventWriterAccepted reports durable directory advancement.
	EventWriterAccepted EventKind = "writer-accepted"
	// EventDeliverySucceeded reports durable outbox acknowledgement.
	EventDeliverySucceeded EventKind = "delivery-succeeded"
	// EventDeliveryFailed reports durable retry scheduling.
	EventDeliveryFailed EventKind = "delivery-failed"
	// EventQueueSaturated reports backpressure.
	EventQueueSaturated EventKind = "queue-saturated"
	// EventSpaceDeleted reports a committed lifecycle tombstone.
	EventSpaceDeleted EventKind = "space-deleted"
)

// Event contains bounded operational fields. It never contains credentials,
// proofs, keys, or record bodies.
type Event struct {
	Kind      EventKind
	Space     atmos.SpaceRef
	DID       atmos.DID
	Service   string
	Attempt   uint32
	Err       error
	Timestamp time.Time
}

// EventSink receives structured operational events.
type EventSink interface{ Emit(context.Context, Event) }

// EventSinkFunc adapts a function to EventSink.
type EventSinkFunc func(context.Context, Event)

// Emit implements EventSink.
func (f EventSinkFunc) Emit(ctx context.Context, event Event) { f(ctx, event) }

// DiscardEvents is an explicit opt-out from operational events.
var DiscardEvents EventSink = EventSinkFunc(func(context.Context, Event) {})
