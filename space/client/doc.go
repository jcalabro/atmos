// Package client provides endpoint-bound clients for AT Protocol spaces.
//
// The high-level account and reader clients deliberately do not expose their
// authenticated HTTP or XRPC clients. Authentication is selected per operation,
// and every request is bound to a strictly resolved DID-document endpoint.
// Native clients default to pooled HTTP/1.1 and HTTP/2 with a fresh DPoP proof
// generated at every transport connection attempt, including hidden retries.
package client
