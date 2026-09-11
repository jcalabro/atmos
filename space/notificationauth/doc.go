// Package notificationauth implements the role- and payload-bound service-auth
// profiles used by AT Protocol space notifications.
//
// Signature verification alone does not authorize a notification. The
// verification functions in this package also bind the issuer and audience to
// the notification hop, bind the issuer to the space authority or writer named
// by the payload, enforce the exact XRPC method, and atomically consume the JWT
// identifier only after every other check succeeds.
package notificationauth
