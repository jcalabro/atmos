package identity

import "context"

// Cache is a bring-your-own cache interface for identity resolution.
type Cache interface {
	// Get retrieves a cached identity by key. Returns false if not found or expired.
	Get(ctx context.Context, key string) (*Identity, bool)
	// Set stores an identity in the cache.
	Set(ctx context.Context, key string, val *Identity)
	// Delete removes an identity from the cache.
	Delete(ctx context.Context, key string)
}
