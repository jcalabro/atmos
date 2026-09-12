// Package host implements a mountable AT Protocol spaces authority host.
//
// The package intentionally does not implement an account PDS or permissioned
// repo host. Embedders provide account authentication, strict identity
// resolution, durable state/replay stores, signing keys, policy evaluation,
// notification delivery, a clock, and structured event handling.
// AlphaLimits supplies bounded first-release settings; durable stores must
// enforce the corresponding quotas and outbox limits atomically.
package host
