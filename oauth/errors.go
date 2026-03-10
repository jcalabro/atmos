// Package oauth implements the ATProto OAuth 2.0 client with mandatory
// PKCE, PAR (Pushed Authorization Requests), and DPoP (Demonstration of
// Proof-of-Possession).
package oauth

import "errors"

var (
	// ErrInvalidState is returned when the callback state parameter doesn't
	// match a pending authorization request.
	ErrInvalidState = errors.New("oauth: invalid or expired state parameter")

	// ErrIssuerMismatch is returned when the callback iss parameter doesn't
	// match the expected authorization server.
	ErrIssuerMismatch = errors.New("oauth: issuer mismatch")

	// ErrMissingIssuer is returned when the callback is missing the required
	// iss parameter and the AS declared support for it.
	ErrMissingIssuer = errors.New("oauth: missing iss parameter in callback")

	// ErrIssuerVerification is returned when the sub DID's resolved PDS does
	// not point to the expected authorization server.
	ErrIssuerVerification = errors.New("oauth: issuer verification failed — DID does not resolve to expected AS")

	// ErrMissingScope is returned when the token response is missing the
	// required "atproto" scope.
	ErrMissingScope = errors.New("oauth: token response missing required atproto scope")

	// ErrNoSession is returned when no session exists for the requested DID.
	ErrNoSession = errors.New("oauth: no session for DID")

	// ErrTokenExpired is returned when a token has expired and cannot be refreshed.
	ErrTokenExpired = errors.New("oauth: token expired and no refresh token available")

	// ErrNoRefreshToken is returned when refresh is attempted without a refresh token.
	ErrNoRefreshToken = errors.New("oauth: no refresh token available")

	// ErrUseDPoPNonce is the error code returned by servers requiring a DPoP nonce.
	ErrUseDPoPNonce = errors.New("oauth: use_dpop_nonce")
)

// OAuthError represents an error response from an OAuth endpoint.
type OAuthError struct {
	Code        string `json:"error"`
	Description string `json:"error_description,omitempty"`
}

func (e *OAuthError) Error() string {
	if e.Description != "" {
		return "oauth: " + e.Code + ": " + e.Description
	}
	return "oauth: " + e.Code
}
