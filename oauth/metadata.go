package oauth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// AuthServerMetadata is the OAuth 2.0 Authorization Server Metadata
// per RFC 8414, with ATProto-specific requirements.
type AuthServerMetadata struct {
	Issuer                             string   `json:"issuer"`
	AuthorizationEndpoint              string   `json:"authorization_endpoint"`
	TokenEndpoint                      string   `json:"token_endpoint"`
	PushedAuthorizationRequestEndpoint string   `json:"pushed_authorization_request_endpoint"`
	RevocationEndpoint                 string   `json:"revocation_endpoint,omitempty"`
	DPoPSigningAlgValuesSupported      []string `json:"dpop_signing_alg_values_supported"`
	ScopesSupported                    []string `json:"scopes_supported"`
	ResponseTypesSupported             []string `json:"response_types_supported"`
	GrantTypesSupported                []string `json:"grant_types_supported"`
	CodeChallengeMethodsSupported      []string `json:"code_challenge_methods_supported"`
	TokenEndpointAuthMethodsSupported  []string `json:"token_endpoint_auth_methods_supported"`

	AuthorizationResponseIssParameterSupported bool     `json:"authorization_response_iss_parameter_supported"`
	RequirePushedAuthorizationRequests         bool     `json:"require_pushed_authorization_requests"`
	ClientIDMetadataDocumentSupported          bool     `json:"client_id_metadata_document_supported"`
	ProtectedResources                         []string `json:"protected_resources,omitempty"` // RFC 9728 §4
}

// ProtectedResourceMetadata is the OAuth 2.0 Protected Resource Metadata
// per RFC 9728, used to discover the authorization server for a PDS.
type ProtectedResourceMetadata struct {
	Resource             string   `json:"resource"`
	AuthorizationServers []string `json:"authorization_servers"`
}

// FetchProtectedResourceMetadata fetches the protected resource metadata from a PDS.
// The pdsURL should be the PDS origin (e.g., "https://bsky.social").
// No HTTP redirects are followed (SSRF prevention).
func FetchProtectedResourceMetadata(ctx context.Context, client *http.Client, pdsURL string) (*ProtectedResourceMetadata, error) {
	url := strings.TrimRight(pdsURL, "/") + "/.well-known/oauth-protected-resource"

	body, err := fetchMetadataJSON(ctx, client, url)
	if err != nil {
		return nil, fmt.Errorf("oauth: fetch protected resource metadata from %s: %w", pdsURL, err)
	}

	var meta ProtectedResourceMetadata
	if err := json.Unmarshal(body, &meta); err != nil {
		return nil, fmt.Errorf("oauth: parse protected resource metadata: %w", err)
	}

	// Validate resource matches the PDS origin.
	pdsOrigin := originFromURL(pdsURL)
	if originFromURL(meta.Resource) != pdsOrigin {
		return nil, fmt.Errorf("oauth: protected resource %q does not match PDS origin %q", meta.Resource, pdsOrigin)
	}

	if len(meta.AuthorizationServers) == 0 {
		return nil, fmt.Errorf("oauth: protected resource metadata has no authorization_servers")
	}

	return &meta, nil
}

// FetchAuthServerMetadata fetches the authorization server metadata.
// The issuer should be the AS origin (e.g., "https://bsky.social").
// No HTTP redirects are followed (SSRF prevention).
func FetchAuthServerMetadata(ctx context.Context, client *http.Client, issuer string) (*AuthServerMetadata, error) {
	url := strings.TrimRight(issuer, "/") + "/.well-known/oauth-authorization-server"

	body, err := fetchMetadataJSON(ctx, client, url)
	if err != nil {
		return nil, fmt.Errorf("oauth: fetch AS metadata from %s: %w", issuer, err)
	}

	var meta AuthServerMetadata
	if err := json.Unmarshal(body, &meta); err != nil {
		return nil, fmt.Errorf("oauth: parse AS metadata: %w", err)
	}

	// Validate issuer matches (mix-up prevention per RFC 8414 §3.3).
	if meta.Issuer != issuer {
		return nil, fmt.Errorf("oauth: AS issuer %q does not match expected %q", meta.Issuer, issuer)
	}

	// ATProto requirements.
	if !meta.ClientIDMetadataDocumentSupported {
		return nil, fmt.Errorf("oauth: AS %q does not support client_id_metadata_document", issuer)
	}
	if !meta.RequirePushedAuthorizationRequests {
		return nil, fmt.Errorf("oauth: AS %q does not require PAR", issuer)
	}
	if meta.PushedAuthorizationRequestEndpoint == "" {
		return nil, fmt.Errorf("oauth: AS %q missing pushed_authorization_request_endpoint", issuer)
	}
	if meta.TokenEndpoint == "" {
		return nil, fmt.Errorf("oauth: AS %q missing token_endpoint", issuer)
	}
	if meta.AuthorizationEndpoint == "" {
		return nil, fmt.Errorf("oauth: AS %q missing authorization_endpoint", issuer)
	}

	return &meta, nil
}

// fetchMetadataJSON fetches a JSON document without following redirects.
// Uses a separate http.Client to avoid mutating the caller's client state.
func fetchMetadataJSON(ctx context.Context, client *http.Client, rawURL string) ([]byte, error) {
	// Create a new client that shares the transport but doesn't follow redirects.
	// We don't shallow-copy the caller's client to avoid sharing cookie jars
	// or overriding their CheckRedirect.
	noRedirectClient := &http.Client{
		Transport: client.Transport,
		Timeout:   client.Timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := noRedirectClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d from %s", resp.StatusCode, rawURL)
	}

	// Validate Content-Type is JSON.
	ct := resp.Header.Get("Content-Type")
	if ct != "" && !strings.HasPrefix(ct, "application/json") {
		return nil, fmt.Errorf("unexpected Content-Type %q from %s, expected application/json", ct, rawURL)
	}

	// Limit response size to 1MB.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	return body, nil
}
