package serviceauth

import (
	"context"
	"net/http"
	"strings"

	"github.com/jcalabro/atmos"
)

type didContextKey struct{}

// DIDFromContext returns the verified issuer DID set by the service auth
// middleware, or an empty DID if not present.
func DIDFromContext(ctx context.Context) atmos.DID {
	v, _ := ctx.Value(didContextKey{}).(atmos.DID)
	return v
}

// Middleware returns HTTP middleware that verifies service auth JWTs from
// the Authorization header. The verified issuer DID is stored in the
// request context and can be retrieved with [DIDFromContext].
//
// If required is true, requests without valid auth receive a 401 response.
// If required is false, requests without auth proceed with an empty DID in
// the context.
//
// The XRPC method NSID is extracted from the request path (the last
// component of /xrpc/<nsid>).
func Middleware(opts VerifyOptions, required bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token := extractBearer(r)
			if token == "" {
				if required {
					http.Error(w, "authorization required", http.StatusUnauthorized)
					return
				}
				next.ServeHTTP(w, r)
				return
			}

			// Extract NSID from path if not set in options.
			vopts := opts
			if vopts.LexMethod == "" {
				if nsid := extractNSID(r.URL.Path); nsid != "" {
					vopts.LexMethod = atmos.NSID(nsid)
				}
			}

			claims, err := VerifyToken(r.Context(), token, vopts)
			if err != nil {
				if required {
					http.Error(w, "invalid authorization", http.StatusUnauthorized)
					return
				}
				next.ServeHTTP(w, r)
				return
			}

			ctx := context.WithValue(r.Context(), didContextKey{}, claims.Issuer)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// extractBearer extracts the bearer token from the Authorization header.
func extractBearer(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return ""
	}
	return auth[len("Bearer "):]
}

// extractNSID extracts the XRPC method NSID from a path like /xrpc/com.atproto.sync.getBlob.
func extractNSID(path string) string {
	const prefix = "/xrpc/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	return path[len(prefix):]
}
