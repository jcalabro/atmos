// Package xrpcserver implements an XRPC HTTP server that routes
// /xrpc/{nsid} requests to handler functions with JSON serialization
// and standard XRPC error envelopes.
//
// Server implements http.Handler and composes with any standard middleware
// (including serviceauth.Middleware). Handlers are registered before serving;
// registration methods are not safe for concurrent use with ServeHTTP.
//
// Typed generic helpers (Query, Procedure, RawQuery, etc.) let users write
// plain Go functions while the framework handles JSON encode/decode and
// error envelope serialization.
package xrpcserver

import (
	"net/url"
	"strconv"

	"github.com/jcalabro/gt"
	"github.com/jcalabro/atmos/xrpc"
)

// Params provides typed access to XRPC query parameters.
type Params struct{ vals url.Values }

// String returns the required string parameter for key.
// Returns a 400 InvalidRequest error if the key is absent.
// Use Has to distinguish between absent and empty-string values.
func (p Params) String(key string) (string, error) {
	if !p.Has(key) {
		return "", InvalidRequest("missing required parameter: " + key)
	}
	return p.vals.Get(key), nil
}

// Int64 returns the required int64 parameter for key.
// Returns a 400 InvalidRequest error if missing or not a valid integer.
func (p Params) Int64(key string) (int64, error) {
	v := p.vals.Get(key)
	if v == "" && !p.Has(key) {
		return 0, InvalidRequest("missing required parameter: " + key)
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, InvalidRequest("invalid integer parameter: " + key)
	}
	return n, nil
}

// Bool returns the required bool parameter for key.
// Returns a 400 InvalidRequest error if missing or not "true"/"false".
func (p Params) Bool(key string) (bool, error) {
	v := p.vals.Get(key)
	if v == "" && !p.Has(key) {
		return false, InvalidRequest("missing required parameter: " + key)
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, InvalidRequest("invalid boolean parameter: " + key)
	}
	return b, nil
}

// StringOr returns the string parameter for key, or fallback if absent.
func (p Params) StringOr(key, fallback string) string {
	if !p.Has(key) {
		return fallback
	}
	return p.vals.Get(key)
}

// Int64Or returns the int64 parameter for key, or fallback if absent or invalid.
func (p Params) Int64Or(key string, fallback int64) int64 {
	v := p.vals.Get(key)
	if v == "" && !p.Has(key) {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fallback
	}
	return n
}

// BoolOr returns the bool parameter for key, or fallback if absent or invalid.
func (p Params) BoolOr(key string, fallback bool) bool {
	v := p.vals.Get(key)
	if v == "" && !p.Has(key) {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

// StringOptional returns the string parameter for key as an Option.
// Returns None if the key is absent, Some(value) if present (even if empty).
func (p Params) StringOptional(key string) gt.Option[string] {
	if !p.Has(key) {
		return gt.None[string]()
	}
	return gt.Some(p.vals.Get(key))
}

// Strings returns all values for a repeated parameter key.
func (p Params) Strings(key string) []string {
	return p.vals[key]
}

// Has reports whether key is present in the query parameters.
func (p Params) Has(key string) bool {
	_, ok := p.vals[key]
	return ok
}

// Raw returns the underlying url.Values.
func (p Params) Raw() url.Values {
	return p.vals
}

// InvalidRequest returns a 400 InvalidRequest XRPC error.
func InvalidRequest(msg string) *xrpc.Error {
	return &xrpc.Error{StatusCode: 400, Name: "InvalidRequest", Message: msg}
}
