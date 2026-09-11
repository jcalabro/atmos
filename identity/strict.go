package identity

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/crypto"
)

var (
	// ErrSelectedEntryNotFound indicates that a requested DID-document entry is absent.
	// Callers may use this error, and only this error, to select a specified fallback.
	ErrSelectedEntryNotFound = errors.New("identity: selected DID document entry not found")
	// ErrMalformedSelectedEntry indicates that a selected entry exists but is invalid.
	ErrMalformedSelectedEntry = errors.New("identity: malformed selected DID document entry")
)

// EndpointPolicy controls explicit test/development exceptions to HTTPS endpoint validation.
type EndpointPolicy struct {
	AllowHTTP           bool
	AllowPrivateLiteral bool
}

// SelectVerificationMethod strictly selects one public Multikey entry without
// collapsing duplicate fragments or losing its full ID and controller.
func SelectVerificationMethod(doc *DIDDocument, expected atmos.DID, fragment string) (*VerificationMethod, crypto.PublicKey, error) {
	if err := validateExpectedDocument(doc, expected); err != nil {
		return nil, nil, err
	}
	fragment = strings.TrimPrefix(fragment, "#")
	if fragment == "" || strings.Contains(fragment, "#") {
		return nil, nil, fmt.Errorf("%w: invalid verification fragment %q", ErrMalformedSelectedEntry, fragment)
	}
	matches := make([]VerificationMethod, 0, 1)
	for _, method := range doc.VerificationMethod {
		if fragmentFromID(method.ID) == fragment {
			matches = append(matches, method)
		}
	}
	if len(matches) == 0 {
		return nil, nil, fmt.Errorf("%w: #%s", ErrSelectedEntryNotFound, fragment)
	}
	if len(matches) != 1 {
		return nil, nil, fmt.Errorf("%w: duplicate verification fragment #%s", ErrMalformedSelectedEntry, fragment)
	}
	selected := matches[0]
	if !validEntryID(selected.ID, expected, fragment) {
		return nil, nil, fmt.Errorf("%w: verification id %q does not belong to %s", ErrMalformedSelectedEntry, selected.ID, expected)
	}
	if selected.Controller != string(expected) {
		return nil, nil, fmt.Errorf("%w: verification controller %q does not match %s", ErrMalformedSelectedEntry, selected.Controller, expected)
	}
	if selected.Type != "Multikey" {
		return nil, nil, fmt.Errorf("%w: verification type %q is not Multikey", ErrMalformedSelectedEntry, selected.Type)
	}
	key, err := crypto.ParsePublicMultibase(selected.PublicKeyMultibase)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: invalid public key: %w", ErrMalformedSelectedEntry, err)
	}
	copy := selected
	return &copy, key, nil
}

// SelectService strictly selects one service entry and validates its endpoint.
func SelectService(doc *DIDDocument, expected atmos.DID, fragment, serviceType string, policy EndpointPolicy) (*Service, *url.URL, error) {
	if err := validateExpectedDocument(doc, expected); err != nil {
		return nil, nil, err
	}
	fragment = strings.TrimPrefix(fragment, "#")
	if fragment == "" || serviceType == "" || strings.Contains(fragment, "#") {
		return nil, nil, fmt.Errorf("%w: invalid service selector", ErrMalformedSelectedEntry)
	}
	matches := make([]Service, 0, 1)
	for _, service := range doc.Service {
		if fragmentFromID(service.ID) == fragment {
			matches = append(matches, service)
		}
	}
	if len(matches) == 0 {
		return nil, nil, fmt.Errorf("%w: #%s", ErrSelectedEntryNotFound, fragment)
	}
	if len(matches) != 1 {
		return nil, nil, fmt.Errorf("%w: duplicate service fragment #%s", ErrMalformedSelectedEntry, fragment)
	}
	selected := matches[0]
	if !validEntryID(selected.ID, expected, fragment) {
		return nil, nil, fmt.Errorf("%w: service id %q does not belong to %s", ErrMalformedSelectedEntry, selected.ID, expected)
	}
	if selected.Type != serviceType {
		return nil, nil, fmt.Errorf("%w: service type %q, want %q", ErrMalformedSelectedEntry, selected.Type, serviceType)
	}
	endpoint, err := validateServiceEndpoint(selected.ServiceEndpoint, policy)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrMalformedSelectedEntry, err)
	}
	copy := selected
	return &copy, endpoint, nil
}

func validateExpectedDocument(doc *DIDDocument, expected atmos.DID) error {
	if doc == nil {
		return fmt.Errorf("%w: nil DID document", ErrMalformedSelectedEntry)
	}
	if err := expected.Validate(); err != nil {
		return fmt.Errorf("%w: invalid expected DID: %w", ErrMalformedSelectedEntry, err)
	}
	if expected.Method() != "plc" && expected.Method() != "web" {
		return fmt.Errorf("%w: unsupported DID method %q", ErrMalformedSelectedEntry, expected.Method())
	}
	if expected.Method() == "plc" {
		if err := expected.ValidatePLC(); err != nil {
			return fmt.Errorf("%w: invalid did:plc: %w", ErrMalformedSelectedEntry, err)
		}
	}
	if doc.ID != string(expected) {
		return fmt.Errorf("%w: document id %q does not match %s", ErrMalformedSelectedEntry, doc.ID, expected)
	}
	return nil
}

func validEntryID(id string, expected atmos.DID, fragment string) bool {
	return id == "#"+fragment || id == string(expected)+"#"+fragment
}

func validateServiceEndpoint(raw string, policy EndpointPolicy) (*url.URL, error) {
	if raw == "" {
		return nil, fmt.Errorf("service endpoint is absent or not a string")
	}
	endpoint, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid service endpoint: %w", err)
	}
	if !endpoint.IsAbs() || endpoint.Host == "" || endpoint.Opaque != "" || endpoint.User != nil ||
		endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.RawFragment != "" {
		return nil, fmt.Errorf("service endpoint must be an absolute URL without userinfo, query, or fragment")
	}
	if endpoint.Scheme != "https" && (!policy.AllowHTTP || endpoint.Scheme != "http") {
		return nil, fmt.Errorf("service endpoint must use HTTPS")
	}
	if !policy.AllowPrivateLiteral {
		host := endpoint.Hostname()
		if strings.EqualFold(host, "localhost") {
			return nil, fmt.Errorf("service endpoint must not use localhost")
		}
		if strings.Contains(host, "%") {
			return nil, fmt.Errorf("service endpoint must not use an IPv6 zone identifier")
		}
		if ip := net.ParseIP(host); ip != nil && (ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified()) {
			return nil, fmt.Errorf("service endpoint must not use a private IP literal")
		}
	}
	return endpoint, nil
}
