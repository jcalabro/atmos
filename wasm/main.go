//go:build js && wasm

package main

import "syscall/js"

func main() {
	atp := js.Global().Get("Object").New()

	registerSyntax(atp)
	registerCBOR(atp)
	registerCrypto(atp)
	registerOAuth(atp)
	registerIdentity(atp)
	registerXRPC(atp)
	registerFirehose(atp)

	js.Global().Set("atmos", atp)

	// Block forever — the Go runtime must stay alive for callbacks.
	select {}
}
