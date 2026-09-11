package credential

import "errors"

var (
	// ErrInvalidToken indicates malformed or profile-invalid JWT input.
	ErrInvalidToken = errors.New("credential: invalid token")
	// ErrTokenExpired indicates a token or proof outside its acceptance window.
	ErrTokenExpired = errors.New("credential: token expired")
	// ErrInvalidSignature indicates that a JWT signature did not verify against
	// the required key and algorithm.
	ErrInvalidSignature = errors.New("credential: invalid signature")
	// ErrTokenBinding indicates a valid token presented for the wrong issuer,
	// subject, audience, key, HTTP method, or target.
	ErrTokenBinding = errors.New("credential: token binding mismatch")
)
