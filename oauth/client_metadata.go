package oauth

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// ClientMetadata is the OAuth client metadata document.
// For a discoverable client, client_id is the URL where this document is hosted.
// Loopback clients instead encode their redirect URI and scope in client_id.
type ClientMetadata struct {
	ClientID                    string   `json:"client_id"`
	ApplicationType             string   `json:"application_type,omitempty"`
	GrantTypes                  []string `json:"grant_types"`
	Scope                       string   `json:"scope"`
	ResponseTypes               []string `json:"response_types"`
	RedirectURIs                []string `json:"redirect_uris"`
	DPoPBoundAccessTokens       bool     `json:"dpop_bound_access_tokens"`
	TokenEndpointAuthMethod     string   `json:"token_endpoint_auth_method"`
	TokenEndpointAuthSigningAlg string   `json:"token_endpoint_auth_signing_alg,omitempty"`
	JWKS                        *JWKSet  `json:"jwks,omitempty"`
	// JWKSURI points to the public key set for private_key_jwt clients.
	// It must not be set together with JWKS.
	JWKSURI    *string `json:"jwks_uri,omitempty"`
	ClientName string  `json:"client_name,omitempty"`
	ClientURI  string  `json:"client_uri,omitempty"`
	LogoURI    string  `json:"logo_uri,omitempty"`
	TOSURI     string  `json:"tos_uri,omitempty"`
	PolicyURI  string  `json:"policy_uri,omitempty"`
}

// JWKSet is a JSON Web Key Set containing public keys for confidential clients.
type JWKSet struct {
	Keys []ECPublicJWK `json:"keys"`
}

// ValidateClientAuth checks the client authentication metadata required by
// ATProto OAuth. A confidential client must advertise exactly one of jwks and
// jwks_uri, and its signing algorithm must match atmos's ES256 client auth.
func (m ClientMetadata) ValidateClientAuth() error {
	if m.JWKS != nil && m.JWKSURI != nil {
		return fmt.Errorf("oauth: jwks and jwks_uri are mutually exclusive")
	}
	if m.JWKSURI != nil {
		if err := validateJWKSURI(*m.JWKSURI); err != nil {
			return err
		}
	}

	switch m.TokenEndpointAuthMethod {
	case "none":
		if m.TokenEndpointAuthSigningAlg != "" {
			return fmt.Errorf("oauth: public client must not declare token_endpoint_auth_signing_alg")
		}
	case "private_key_jwt":
		if m.TokenEndpointAuthSigningAlg != "ES256" {
			return fmt.Errorf("oauth: private_key_jwt requires token_endpoint_auth_signing_alg ES256")
		}
		if m.JWKS == nil && m.JWKSURI == nil {
			return fmt.Errorf("oauth: private_key_jwt requires jwks or jwks_uri")
		}
		if m.JWKS != nil {
			if len(m.JWKS.Keys) == 0 {
				return fmt.Errorf("oauth: private_key_jwt requires at least one public key in jwks")
			}
			for _, key := range m.JWKS.Keys {
				if err := validateClientPublicJWK(key); err != nil {
					return err
				}
			}
		}
	default:
		return fmt.Errorf("oauth: unsupported token_endpoint_auth_method %q", m.TokenEndpointAuthMethod)
	}
	return nil
}

func validateJWKSURI(raw string) error {
	if strings.Contains(raw, "\\") {
		return fmt.Errorf("oauth: invalid jwks_uri %q: backslashes are not allowed", raw)
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.Opaque != "" || u.User != nil || u.Fragment != "" {
		return fmt.Errorf("oauth: invalid jwks_uri %q: expected an absolute web URL", raw)
	}
	host := strings.ToLower(u.Hostname())
	isLoopback := host == "localhost" || host == "127.0.0.1" || host == "::1"
	if u.Scheme != "https" && (u.Scheme != "http" || !isLoopback) {
		return fmt.Errorf("oauth: invalid jwks_uri %q: expected HTTPS or HTTP on loopback", raw)
	}
	if u.Scheme == "https" && isLoopback {
		return fmt.Errorf("oauth: invalid jwks_uri %q: HTTPS loopback URLs are not supported", raw)
	}
	if u.Scheme == "https" && net.ParseIP(host) == nil &&
		(!strings.Contains(host, ".") || strings.HasSuffix(host, ".local") || numericIPv4Host(host)) {
		return fmt.Errorf("oauth: invalid jwks_uri %q: HTTPS host must be a public domain or IP address", raw)
	}
	return nil
}

// Numeric IPv4 shorthand (for example, 127.1 or 0x7f.1) is normalized to a
// loopback IP by URL parsers used by authorization servers. Reject it rather
// than treating it as an ordinary DNS name.
func numericIPv4Host(host string) bool {
	parts := strings.Split(strings.TrimSuffix(host, "."), ".")
	if len(parts) > 4 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		if strings.HasPrefix(part, "0x") || strings.HasPrefix(part, "0X") {
			part = part[2:]
			if part == "" {
				return false
			}
			for _, r := range part {
				switch {
				case '0' <= r && r <= '9', 'a' <= r && r <= 'f', 'A' <= r && r <= 'F':
				default:
					return false
				}
			}
			continue
		}
		for _, r := range part {
			if r < '0' || r > '9' {
				return false
			}
		}
	}
	return true
}

// NewLoopbackClientMetadata configures a public ATProto OAuth client that
// receives callbacks on a loopback IP address. The special http://localhost
// client ID encodes the redirect URI and scope, so no metadata endpoint is
// needed. Use a nil [Client.Key]; this does not change outbound SSRF protection.
func NewLoopbackClientMetadata(redirectURI, scope string) (ClientMetadata, error) {
	if err := validateLoopbackRedirectURI(redirectURI); err != nil {
		return ClientMetadata{}, err
	}
	if !validATProtoOAuthScope(scope) {
		return ClientMetadata{}, fmt.Errorf("oauth: invalid loopback client scope %q: expected space-separated OAuth scope including atproto", scope)
	}

	params := url.Values{
		"redirect_uri": {redirectURI},
		"scope":        {scope},
	}
	return ClientMetadata{
		ClientID:                "http://localhost?" + params.Encode(),
		ApplicationType:         "native",
		GrantTypes:              []string{"authorization_code", "refresh_token"},
		Scope:                   scope,
		ResponseTypes:           []string{"code"},
		RedirectURIs:            []string{redirectURI},
		DPoPBoundAccessTokens:   true,
		TokenEndpointAuthMethod: "none",
	}, nil
}

func validateLoopbackRedirectURI(raw string) error {
	if strings.Contains(raw, "\\") {
		return fmt.Errorf("oauth: invalid loopback redirect URI %q: backslashes are not allowed", raw)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("oauth: invalid loopback redirect URI %q: %w", raw, err)
	}
	if u.Scheme != "http" || u.User != nil || u.Opaque != "" || u.Fragment != "" {
		return fmt.Errorf("oauth: invalid loopback redirect URI %q: expected an http URL without userinfo or fragment", raw)
	}
	if u.Hostname() != "127.0.0.1" && u.Hostname() != "::1" {
		return fmt.Errorf("oauth: invalid loopback redirect URI %q: host must be 127.0.0.1 or [::1]", raw)
	}

	expectedHost := u.Hostname()
	if expectedHost == "::1" {
		expectedHost = "[::1]"
	}
	if port := u.Port(); port != "" {
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil || n == 0 {
			return fmt.Errorf("oauth: invalid loopback redirect URI %q: port must be between 1 and 65535", raw)
		}
		expectedHost += ":" + port
	}
	if u.Host != expectedHost {
		return fmt.Errorf("oauth: invalid loopback redirect URI %q: malformed host or port", raw)
	}
	if _, err := url.ParseQuery(u.RawQuery); err != nil {
		return fmt.Errorf("oauth: invalid loopback redirect URI %q: %w", raw, err)
	}
	return nil
}

func validATProtoOAuthScope(scope string) bool {
	if scope == "" {
		return false
	}
	hasATProto := false
	for _, token := range strings.Split(scope, " ") {
		if token == "" {
			return false
		}
		if token == "atproto" {
			hasATProto = true
		}
		for i := range len(token) {
			b := token[i]
			if b != '!' && (b < '#' || b > '[') && (b < ']' || b > '~') {
				return false
			}
		}
	}
	return hasATProto
}
