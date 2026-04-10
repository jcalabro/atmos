//go:build js && wasm

package main

import (
	"context"
	"syscall/js"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
)

func registerIdentity(atp js.Value) {
	ns := js.Global().Get("Object").New()
	ns.Set("resolve", js.FuncOf(jsResolve))
	atp.Set("identity", ns)
}

// jsResolve resolves a DID or handle to an identity document.
//
// Note: handle resolution in WASM uses HTTP only (fetching
// https://<handle>/.well-known/atproto-did). DNS TXT record resolution
// (_atproto.<handle>) is not supported because web browsers do not
// provide any API for performing DNS lookups.
func jsResolve(_ js.Value, args []js.Value) any {
	input := args[0].String()
	promise, resolve, reject := newPromise()

	go func() {
		ctx := context.Background()
		dir := &identity.Directory{
			Resolver: &identity.DefaultResolver{},
		}

		id, err := atmos.ParseATIdentifier(input)
		if err != nil {
			reject("identity.resolve: " + err.Error())
			return
		}

		ident, err := dir.Lookup(ctx, id)
		if err != nil {
			reject("identity.resolve: " + err.Error())
			return
		}

		result := jsObj(
			"did", ident.DID.String(),
			"handle", ident.Handle.String(),
			"pds", ident.PDSEndpoint(),
		)

		pub, err := ident.PublicKey()
		if err == nil {
			result.Set("signingKey", pub.DIDKey())
		}

		resolve(result)
	}()

	return promise
}
