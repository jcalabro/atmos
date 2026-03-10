package oauth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/jcalabro/atmos/crypto"
)

// Session represents an authenticated OAuth session for a user.
type Session struct {
	DPoPKey  *crypto.P256PrivateKey
	TokenSet TokenSet
}

// sessionJSON is the JSON-serializable form of Session.
type sessionJSON struct {
	DPoPKey  string   `json:"dpop_key"` // base64url-encoded 32-byte P-256 scalar
	TokenSet TokenSet `json:"token_set"`
}

func (s *Session) MarshalJSON() ([]byte, error) {
	var keyStr string
	if s.DPoPKey != nil {
		keyStr = base64.RawURLEncoding.EncodeToString(s.DPoPKey.Bytes())
	}
	return json.Marshal(sessionJSON{
		DPoPKey:  keyStr,
		TokenSet: s.TokenSet,
	})
}

func (s *Session) UnmarshalJSON(data []byte) error {
	var j sessionJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}

	raw, err := base64.RawURLEncoding.DecodeString(j.DPoPKey)
	if err != nil {
		return fmt.Errorf("oauth: decode DPoP key: %w", err)
	}
	key, err := crypto.ParsePrivateP256(raw)
	if err != nil {
		return fmt.Errorf("oauth: parse DPoP key: %w", err)
	}

	s.DPoPKey = key
	s.TokenSet = j.TokenSet
	return nil
}

// AuthState stores the pending authorization flow state.
type AuthState struct {
	Issuer             string
	DPoPKey            *crypto.P256PrivateKey
	AuthMethod         string // "none" or "private_key_jwt"
	Verifier           string // PKCE verifier
	RedirectURI        string
	AppState           string // Opaque application state
	TokenEndpoint      string
	RevocationEndpoint string
}

// authStateJSON is the JSON-serializable form of AuthState.
type authStateJSON struct {
	Issuer             string `json:"issuer"`
	DPoPKey            string `json:"dpop_key"`
	AuthMethod         string `json:"auth_method"`
	Verifier           string `json:"verifier"`
	RedirectURI        string `json:"redirect_uri"`
	AppState           string `json:"app_state,omitempty"`
	TokenEndpoint      string `json:"token_endpoint"`
	RevocationEndpoint string `json:"revocation_endpoint,omitempty"`
}

func (s *AuthState) MarshalJSON() ([]byte, error) {
	var keyStr string
	if s.DPoPKey != nil {
		keyStr = base64.RawURLEncoding.EncodeToString(s.DPoPKey.Bytes())
	}
	return json.Marshal(authStateJSON{
		Issuer:             s.Issuer,
		DPoPKey:            keyStr,
		AuthMethod:         s.AuthMethod,
		Verifier:           s.Verifier,
		RedirectURI:        s.RedirectURI,
		AppState:           s.AppState,
		TokenEndpoint:      s.TokenEndpoint,
		RevocationEndpoint: s.RevocationEndpoint,
	})
}

func (s *AuthState) UnmarshalJSON(data []byte) error {
	var j authStateJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}

	raw, err := base64.RawURLEncoding.DecodeString(j.DPoPKey)
	if err != nil {
		return fmt.Errorf("oauth: decode DPoP key: %w", err)
	}
	key, err := crypto.ParsePrivateP256(raw)
	if err != nil {
		return fmt.Errorf("oauth: parse DPoP key: %w", err)
	}

	s.Issuer = j.Issuer
	s.DPoPKey = key
	s.AuthMethod = j.AuthMethod
	s.Verifier = j.Verifier
	s.RedirectURI = j.RedirectURI
	s.AppState = j.AppState
	s.TokenEndpoint = j.TokenEndpoint
	s.RevocationEndpoint = j.RevocationEndpoint
	return nil
}

// SessionStore persists OAuth sessions. Keyed by user DID.
type SessionStore interface {
	// GetSession retrieves a session by DID. Returns [ErrNoSession] if not found.
	GetSession(ctx context.Context, did string) (*Session, error)
	// SetSession stores or replaces a session for the given DID.
	SetSession(ctx context.Context, did string, session *Session) error
	// DeleteSession removes the session for the given DID.
	DeleteSession(ctx context.Context, did string) error
}

// StateStore stores pending authorization flow state. Keyed by state parameter.
// Entries should be short-lived (auto-expire after ~10 minutes).
type StateStore interface {
	// GetState retrieves pending auth state. Returns [ErrInvalidState] if not found.
	GetState(ctx context.Context, state string) (*AuthState, error)
	// SetState stores pending auth state for the given state parameter.
	SetState(ctx context.Context, state string, data *AuthState) error
	// DeleteState removes the pending auth state.
	DeleteState(ctx context.Context, state string) error
}

// MemorySessionStore is an in-memory [SessionStore] for testing. Not safe for concurrent use.
type MemorySessionStore struct {
	sessions map[string]*Session
}

// NewMemorySessionStore creates an empty in-memory session store.
func NewMemorySessionStore() *MemorySessionStore {
	return &MemorySessionStore{sessions: make(map[string]*Session)}
}

func (s *MemorySessionStore) GetSession(_ context.Context, did string) (*Session, error) {
	sess, ok := s.sessions[did]
	if !ok {
		return nil, ErrNoSession
	}
	return sess, nil
}

func (s *MemorySessionStore) SetSession(_ context.Context, did string, session *Session) error {
	s.sessions[did] = session
	return nil
}

func (s *MemorySessionStore) DeleteSession(_ context.Context, did string) error {
	delete(s.sessions, did)
	return nil
}

// MemoryStateStore is an in-memory [StateStore] for testing. Not safe for concurrent use.
type MemoryStateStore struct {
	states map[string]*AuthState
}

// NewMemoryStateStore creates an empty in-memory state store.
func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{states: make(map[string]*AuthState)}
}

func (s *MemoryStateStore) GetState(_ context.Context, state string) (*AuthState, error) {
	data, ok := s.states[state]
	if !ok {
		return nil, ErrInvalidState
	}
	return data, nil
}

func (s *MemoryStateStore) SetState(_ context.Context, state string, data *AuthState) error {
	s.states[state] = data
	return nil
}

func (s *MemoryStateStore) DeleteState(_ context.Context, state string) error {
	delete(s.states, state)
	return nil
}
