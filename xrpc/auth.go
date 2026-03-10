package xrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// AuthInfo holds session credentials.
type AuthInfo struct {
	AccessJwt  string `json:"accessJwt"`
	RefreshJwt string `json:"refreshJwt"`
	Handle     string `json:"handle"`
	DID        string `json:"did"`
}

// sessionState guards concurrent access to auth info.
type sessionState struct {
	mu   sync.RWMutex
	auth *AuthInfo
}

func (s *sessionState) get() *AuthInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.auth == nil {
		return nil
	}
	cp := *s.auth
	return &cp
}

func (s *sessionState) set(a *AuthInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a != nil {
		cp := *a
		s.auth = &cp
	} else {
		s.auth = nil
	}
}

// Auth returns a copy of the current session info, or nil.
func (c *Client) Auth() *AuthInfo {
	return c.session.get()
}

// SetAuth sets the session info.
func (c *Client) SetAuth(a *AuthInfo) {
	c.session.set(a)
}

// createSessionReq is the request body for com.atproto.server.createSession.
type createSessionReq struct {
	Identifier string `json:"identifier"`
	Password   string `json:"password"`
}

// CreateSession authenticates with the server and stores the session.
func (c *Client) CreateSession(ctx context.Context, identifier, password string) (*AuthInfo, error) {
	bodyBytes, err := json.Marshal(&createSessionReq{
		Identifier: identifier,
		Password:   password,
	})
	if err != nil {
		return nil, fmt.Errorf("xrpc: marshal createSession: %w", err)
	}

	var out AuthInfo
	// No auth header for createSession — pass empty bearerOverride and ensure
	// no stored session leaks through by using doInternal with explicit empty override.
	err = c.doInternal(ctx, "POST", "com.atproto.server.createSession", "application/json", nil, marshalBody(bodyBytes), &out, noAuth)
	if err != nil {
		return nil, err
	}
	c.SetAuth(&out)
	return c.Auth(), nil
}

// RefreshSession refreshes the session using the refresh JWT.
// Does not mutate shared state until the refresh succeeds, so concurrent
// requests continue using the current access JWT during the refresh call.
func (c *Client) RefreshSession(ctx context.Context) (*AuthInfo, error) {
	auth := c.Auth()
	if auth == nil {
		return nil, &Error{StatusCode: 0, Name: "NoSession", Message: "no session to refresh"}
	}

	var out AuthInfo
	err := c.doInternal(ctx, "POST", "com.atproto.server.refreshSession", "", nil, nil, &out, auth.RefreshJwt)
	if err != nil {
		return nil, err
	}
	c.SetAuth(&out)
	return c.Auth(), nil
}

// DeleteSession deletes the current session using the refresh JWT.
// Does not mutate shared state until the delete succeeds.
func (c *Client) DeleteSession(ctx context.Context) error {
	auth := c.Auth()
	if auth == nil {
		return &Error{StatusCode: 0, Name: "NoSession", Message: "no session to delete"}
	}

	err := c.doInternal(ctx, "POST", "com.atproto.server.deleteSession", "", nil, nil, nil, auth.RefreshJwt)
	if err != nil {
		return err
	}
	c.SetAuth(nil)
	return nil
}

// noAuth is a sentinel value for doInternal to suppress the Authorization header
// entirely (rather than falling through to the stored session).
const noAuth = "\x00"

// marshalBody wraps pre-marshaled bytes in a *bytes.Reader for Do's retry logic.
func marshalBody(b []byte) *bytes.Reader {
	return bytes.NewReader(b)
}
