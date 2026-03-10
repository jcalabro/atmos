package oauth

// ClientMetadata is the OAuth client metadata document.
// The client_id is the URL where this document is hosted.
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
	ClientName                  string   `json:"client_name,omitempty"`
	ClientURI                   string   `json:"client_uri,omitempty"`
	LogoURI                     string   `json:"logo_uri,omitempty"`
	TOSURI                      string   `json:"tos_uri,omitempty"`
	PolicyURI                   string   `json:"policy_uri,omitempty"`
}

// JWKSet is a JSON Web Key Set containing public keys for confidential clients.
type JWKSet struct {
	Keys []ECPublicJWK `json:"keys"`
}
