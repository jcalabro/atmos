//go:build js

package identity

// canDNS reports whether direct DNS resolution (UDP) is available.
//
// Web browsers do not expose any API for performing DNS lookups. There is
// no equivalent of net.LookupTXT in JavaScript — the Fetch API resolves
// hostnames internally but provides no access to arbitrary record types.
//
// As a result, handle resolution in WASM environments uses only the HTTP
// method (GET https://<handle>/.well-known/atproto-did). The DNS TXT
// method (_atproto.<handle>) is not attempted.
const canDNS = false
