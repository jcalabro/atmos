package client

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
)

const (
	pdsFragment       = "atproto_pds"
	pdsServiceType    = "AtprotoPersonalDataServer"
	spaceHostFragment = "atproto_space_host"
	spaceHostType     = "AtprotoSpaceHost"
)

// DIDResolver resolves raw DID documents. Raw entries are required here so
// duplicate fragments, complete IDs, controllers, and malformed selected
// entries cannot be hidden by a lossy map projection.
type DIDResolver interface {
	ResolveDID(ctx context.Context, did atmos.DID) (*identity.DIDDocument, error)
}

// Endpoint is a strictly resolved HTTP destination and its separate JWT
// audience identifier.
type Endpoint struct {
	DID      atmos.DID
	Fragment string
	Audience string
	URL      *url.URL
}

// ResolveAuthorityHost resolves the dedicated space-host service. It falls
// back to the authority's PDS only when the dedicated entry is absent; a
// present malformed entry fails closed. The JWT audience remains the dedicated
// space-host identifier in either case.
func ResolveAuthorityHost(ctx context.Context, resolver DIDResolver, authority atmos.DID, policy identity.EndpointPolicy) (Endpoint, error) {
	doc, err := resolveDocument(ctx, resolver, authority)
	if err != nil {
		return Endpoint{}, err
	}
	_, destination, err := identity.SelectService(doc, authority, spaceHostFragment, spaceHostType, policy)
	if err == nil {
		return Endpoint{DID: authority, Fragment: spaceHostFragment, Audience: string(authority) + "#" + spaceHostFragment, URL: destination}, nil
	}
	if !errors.Is(err, identity.ErrSelectedEntryNotFound) {
		return Endpoint{}, fmt.Errorf("space client: resolve authority space host: %w", err)
	}
	_, destination, err = identity.SelectService(doc, authority, pdsFragment, pdsServiceType, policy)
	if err != nil {
		return Endpoint{}, fmt.Errorf("space client: resolve authority PDS fallback: %w", err)
	}
	return Endpoint{DID: authority, Fragment: pdsFragment, Audience: string(authority) + "#" + spaceHostFragment, URL: destination}, nil
}

// ResolveRepoHost resolves an author's PDS. Permissioned repo hosts do not use
// a distinct service fragment in the pinned protocol.
func ResolveRepoHost(ctx context.Context, resolver DIDResolver, author atmos.DID, policy identity.EndpointPolicy) (Endpoint, error) {
	doc, err := resolveDocument(ctx, resolver, author)
	if err != nil {
		return Endpoint{}, err
	}
	_, destination, err := identity.SelectService(doc, author, pdsFragment, pdsServiceType, policy)
	if err != nil {
		return Endpoint{}, fmt.Errorf("space client: resolve repo host: %w", err)
	}
	return Endpoint{DID: author, Fragment: pdsFragment, Audience: string(author), URL: destination}, nil
}

// ResolveService resolves a DID with an optional service fragment. A bare DID
// selects its PDS. For an explicit fragment, expectedType is required so a
// caller cannot silently authorize an entry with an unintended role.
func ResolveService(ctx context.Context, resolver DIDResolver, serviceID, expectedType string, policy identity.EndpointPolicy) (Endpoint, error) {
	bare, fragment, present := strings.Cut(serviceID, "#")
	if present && (fragment == "" || strings.Contains(fragment, "#")) {
		return Endpoint{}, fmt.Errorf("space client: invalid service identifier %q", serviceID)
	}
	did, err := atmos.ParseDID(bare)
	if err != nil {
		return Endpoint{}, fmt.Errorf("space client: invalid service DID: %w", err)
	}
	if !present {
		if expectedType != "" && expectedType != pdsServiceType {
			return Endpoint{}, fmt.Errorf("space client: bare service DID can only select a PDS")
		}
		return ResolveRepoHost(ctx, resolver, did, policy)
	}
	if expectedType == "" {
		return Endpoint{}, fmt.Errorf("space client: expected service type is required for #%s", fragment)
	}
	doc, err := resolveDocument(ctx, resolver, did)
	if err != nil {
		return Endpoint{}, err
	}
	_, destination, err := identity.SelectService(doc, did, fragment, expectedType, policy)
	if err != nil {
		return Endpoint{}, fmt.Errorf("space client: resolve service %s: %w", serviceID, err)
	}
	return Endpoint{DID: did, Fragment: fragment, Audience: serviceID, URL: destination}, nil
}

func resolveDocument(ctx context.Context, resolver DIDResolver, did atmos.DID) (*identity.DIDDocument, error) {
	if resolver == nil {
		return nil, fmt.Errorf("space client: DID resolver is required")
	}
	if err := did.Validate(); err != nil {
		return nil, fmt.Errorf("space client: invalid DID: %w", err)
	}
	doc, err := resolver.ResolveDID(ctx, did)
	if err != nil {
		return nil, fmt.Errorf("space client: resolve %s: %w", did, err)
	}
	return doc, nil
}
