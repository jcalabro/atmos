// Package client provides endpoint-bound clients for AT Protocol spaces.
//
// The high-level account and reader clients deliberately do not expose their
// authenticated HTTP or XRPC clients. Authentication is selected per operation,
// and every request is bound to a strictly resolved DID-document endpoint.
package client
