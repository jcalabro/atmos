package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/serviceauth"
	spaceclient "github.com/jcalabro/atmos/space/client"
	"github.com/jcalabro/atmos/space/simplespace"
)

// ManagingAppServiceType returns the exact expected DID service type for one
// configured managing-app identifier.
type ManagingAppServiceType func(context.Context, string) (string, error)

// HTTPManagingAppOptions configures fail-closed managing-app callbacks.
type HTTPManagingAppOptions struct {
	Resolver       identity.Resolver
	Signer         Signer
	Clock          Clock
	EndpointPolicy identity.EndpointPolicy
	ServiceType    ManagingAppServiceType
	Timeout        time.Duration
	MaxResponse    int64
}

// HTTPManagingAppChecker strictly resolves and calls checkUserAccess with a
// fresh authority service JWT on every logical attempt.
type HTTPManagingAppChecker struct {
	resolver       identity.Resolver
	signer         Signer
	clock          Clock
	endpointPolicy identity.EndpointPolicy
	serviceType    ManagingAppServiceType
	client         *http.Client
	timeout        time.Duration
	maxResponse    int64
}

// NewHTTPManagingAppChecker constructs a bounded callback client using the
// explicit no-reuse HTTP/1 correctness baseline. This prevents Go's transport
// from transparently replaying one service JWT on another wire attempt.
func NewHTTPManagingAppChecker(opts HTTPManagingAppOptions) (*HTTPManagingAppChecker, error) {
	if opts.Resolver == nil || opts.Signer == nil || opts.Clock == nil || opts.ServiceType == nil || opts.Timeout <= 0 || opts.MaxResponse <= 0 || opts.MaxResponse == math.MaxInt64 {
		return nil, errors.New("space host: managing-app resolver, signer, clock, service-type policy, timeout, and response limit below MaxInt64 are required")
	}
	client := spaceclient.NewCorrectnessHTTPClient(spaceclient.NetworkPolicy{AllowPrivateNetworks: opts.EndpointPolicy.AllowPrivateLiteral})
	client.Timeout = opts.Timeout
	return &HTTPManagingAppChecker{
		resolver: opts.Resolver, signer: opts.Signer, clock: opts.Clock, endpointPolicy: opts.EndpointPolicy,
		serviceType: opts.ServiceType, client: client, timeout: opts.Timeout, maxResponse: opts.MaxResponse,
	}, nil
}

// CheckUserAccess implements ManagingAppChecker.
func (c *HTTPManagingAppChecker) CheckUserAccess(parent context.Context, managingApp string, space atmos.SpaceRef, user atmos.DID, access simplespace.Access, clientID string) (bool, error) {
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	serviceType, err := c.serviceType(ctx, managingApp)
	if err != nil || serviceType == "" {
		return false, errors.Join(ErrManagingAppUnavailable, err)
	}
	endpoint, err := resolveServiceWith(ctx, c.resolver, c.endpointPolicy, managingApp, serviceType)
	if err != nil {
		return false, errors.Join(ErrManagingAppUnavailable, err)
	}
	key, err := c.signer.ServiceKey(ctx, space.Authority())
	if err != nil {
		return false, errors.Join(ErrManagingAppUnavailable, err)
	}
	now := c.clock.Now()
	token, err := serviceauth.CreateToken(serviceauth.TokenParams{
		Issuer: space.Authority(), Audience: managingApp,
		IssuedAt: now, LexMethod: "com.atproto.simplespace.checkUserAccess", Exp: now.Add(time.Minute),
	}, key)
	if err != nil {
		return false, errors.Join(ErrManagingAppUnavailable, err)
	}
	target := *endpoint
	target.Path = strings.TrimRight(target.Path, "/") + "/xrpc/com.atproto.simplespace.checkUserAccess"
	query := url.Values{
		"space": {space.String()}, "user": {user.String()},
	}
	switch access {
	case simplespace.AccessRead:
		query.Set("access", "read")
	case simplespace.AccessWrite:
		query.Set("access", "write")
	default:
		return false, errors.New("space host: invalid managing-app access")
	}
	if clientID != "" {
		query.Set("clientId", clientID)
	}
	target.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	response, err := c.client.Do(req)
	if err != nil {
		return false, errors.Join(ErrManagingAppUnavailable, err)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, c.maxResponse+1))
	closeErr := response.Body.Close()
	if err != nil || int64(len(body)) > c.maxResponse {
		return false, errors.Join(ErrManagingAppUnavailable, err)
	}
	if closeErr != nil {
		return false, errors.Join(ErrManagingAppUnavailable, closeErr)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return false, fmt.Errorf("%w: HTTP %d", ErrManagingAppUnavailable, response.StatusCode)
	}
	var output comatproto.SimplespaceCheckUserAccess_Output
	if err := json.Unmarshal(body, &output); err != nil {
		return false, errors.Join(ErrManagingAppUnavailable, err)
	}
	return output.Authorized, nil
}

func resolveServiceWith(ctx context.Context, resolver identity.Resolver, policy identity.EndpointPolicy, identifier, serviceType string) (*url.URL, error) {
	didText, fragment, hasFragment := strings.Cut(identifier, "#")
	did, err := atmos.ParseDID(didText)
	if err != nil {
		return nil, err
	}
	if !hasFragment {
		fragment = "atproto_pds"
	}
	doc, err := resolver.ResolveDID(ctx, did)
	if err != nil {
		return nil, err
	}
	_, endpoint, err := identity.SelectService(doc, did, fragment, serviceType, policy)
	return endpoint, err
}

var _ ManagingAppChecker = (*HTTPManagingAppChecker)(nil)
