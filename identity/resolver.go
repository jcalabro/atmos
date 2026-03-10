package identity

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jcalabro/gt"
	"github.com/jcalabro/atmos"
)

// Resolver performs low-level network resolution without verification or caching.
type Resolver interface {
	// ResolveDID fetches the DID document for the given DID.
	ResolveDID(ctx context.Context, did atmos.DID) (*DIDDocument, error)
	// ResolveHandle resolves a handle to its DID via DNS or HTTP.
	ResolveHandle(ctx context.Context, handle atmos.Handle) (atmos.DID, error)
}

// DefaultResolver resolves DIDs and handles via network requests.
type DefaultResolver struct {
	HTTPClient            gt.Option[*http.Client]
	PLCURL                gt.Option[string]
	SkipDNSDomainSuffixes []string // e.g. [".bsky.social"] — HTTP-only for these

	clientOnce sync.Once
	httpClient *http.Client
}

func (r *DefaultResolver) client() *http.Client {
	if r.HTTPClient.HasVal() {
		return r.HTTPClient.Val()
	}
	r.clientOnce.Do(func() {
		r.httpClient = &http.Client{Timeout: 10 * time.Second}
	})
	return r.httpClient
}

func (r *DefaultResolver) plcURL() string {
	return r.PLCURL.ValOr("https://plc.directory")
}

// ResolveDID fetches the DID document for the given DID.
func (r *DefaultResolver) ResolveDID(ctx context.Context, did atmos.DID) (*DIDDocument, error) {
	switch did.Method() {
	case "plc":
		return r.resolvePLC(ctx, did)
	case "web":
		return r.resolveWeb(ctx, did)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedDIDMethod, did.Method())
	}
}

func (r *DefaultResolver) resolvePLC(ctx context.Context, did atmos.DID) (*DIDDocument, error) {
	url := r.plcURL() + "/" + string(did)
	body, err := r.httpGet(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDIDNotFound, err)
	}
	doc, err := ParseDIDDocument(body)
	if err != nil {
		return nil, err
	}
	if doc.ID != string(did) {
		return nil, fmt.Errorf("%w: document ID %q does not match %q", ErrDIDNotFound, doc.ID, did)
	}
	return doc, nil
}

func (r *DefaultResolver) resolveWeb(ctx context.Context, did atmos.DID) (*DIDDocument, error) {
	host := did.Identifier()
	url := "https://" + host + "/.well-known/did.json"
	body, err := r.httpGet(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDIDNotFound, err)
	}
	doc, err := ParseDIDDocument(body)
	if err != nil {
		return nil, err
	}
	// Verify the document ID matches the DID.
	if doc.ID != string(did) {
		return nil, fmt.Errorf("%w: document ID %q does not match %q", ErrDIDNotFound, doc.ID, did)
	}
	return doc, nil
}

// ResolveHandle resolves a handle to a DID. If the handle matches a
// SkipDNSDomainSuffixes entry, only HTTP is used. Otherwise DNS and HTTP
// are raced in parallel, returning whichever succeeds first.
func (r *DefaultResolver) ResolveHandle(ctx context.Context, handle atmos.Handle) (atmos.DID, error) {
	if r.shouldSkipDNS(handle) {
		return r.resolveHandleHTTP(ctx, handle)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		did atmos.DID
		err error
	}

	ch := make(chan result, 2)

	go func() {
		did, err := r.resolveHandleDNS(ctx, handle)
		ch <- result{did, err}
	}()

	go func() {
		did, err := r.resolveHandleHTTP(ctx, handle)
		ch <- result{did, err}
	}()

	// Wait for both results; return the first success or both errors.
	var firstErr error
	for range 2 {
		res := <-ch
		if res.err == nil {
			return res.did, nil
		}
		if firstErr == nil {
			firstErr = res.err
		}
	}

	return "", firstErr
}

func (r *DefaultResolver) shouldSkipDNS(handle atmos.Handle) bool {
	h := strings.ToLower(string(handle))
	for _, suffix := range r.SkipDNSDomainSuffixes {
		if strings.HasSuffix(h, strings.ToLower(suffix)) {
			return true
		}
	}
	return false
}

func (r *DefaultResolver) resolveHandleHTTP(ctx context.Context, handle atmos.Handle) (atmos.DID, error) {
	url := "https://" + string(handle) + "/.well-known/atproto-did"
	body, err := r.httpGetAccept(ctx, url, "text/plain")
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrHandleNotFound, err)
	}
	raw := strings.TrimSpace(string(body))
	did, err := atmos.ParseDID(raw)
	if err != nil {
		return "", fmt.Errorf("%w: invalid DID in response: %w", ErrHandleNotFound, err)
	}
	return did, nil
}

func (r *DefaultResolver) resolveHandleDNS(ctx context.Context, handle atmos.Handle) (atmos.DID, error) {
	name := "_atproto." + string(handle)
	records, err := net.DefaultResolver.LookupTXT(ctx, name)
	if err != nil {
		return "", err
	}
	for _, txt := range records {
		if strings.HasPrefix(txt, "did=") {
			raw := txt[4:]
			did, err := atmos.ParseDID(raw)
			if err != nil {
				continue
			}
			return did, nil
		}
	}
	return "", fmt.Errorf("%w: no valid did= TXT record for %s", ErrHandleNotFound, name)
}

func (r *DefaultResolver) httpGet(ctx context.Context, url string) ([]byte, error) {
	return r.httpGetAccept(ctx, url, "application/json")
}

func (r *DefaultResolver) httpGetAccept(ctx context.Context, url, accept string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", accept)
	resp, err := r.client().Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("HTTP 404: %s", url)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, url)
	}
	// Limit body to 1MB.
	return io.ReadAll(io.LimitReader(resp.Body, 1<<20))
}
