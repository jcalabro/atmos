//go:build js && wasm

package main

import (
	"syscall/js"

	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/oauth"
)

func registerOAuth(atp js.Value) {
	ns := js.Global().Get("Object").New()
	ns.Set("generatePKCE", js.FuncOf(jsGeneratePKCE))
	ns.Set("createDPoPProof", js.FuncOf(jsCreateDPoPProof))
	ns.Set("publicJWK", js.FuncOf(jsPublicJWK))
	atp.Set("oauth", ns)
}

func jsGeneratePKCE(_ js.Value, _ []js.Value) any {
	pkce, err := oauth.GeneratePKCE()
	if err != nil {
		panic("oauth.generatePKCE: " + err.Error())
	}
	return jsObj(
		"verifier", pkce.Verifier,
		"challenge", pkce.Challenge,
		"method", pkce.Method,
	)
}

func jsCreateDPoPProof(_ js.Value, args []js.Value) any {
	keyBytes := jsBytesFromJS(args[0])
	method := args[1].String()
	url := args[2].String()
	nonce := args[3].String()
	accessToken := args[4].String()

	key, err := crypto.ParsePrivateP256(keyBytes)
	if err != nil {
		panic("oauth.createDPoPProof: invalid key: " + err.Error())
	}

	proof, err := oauth.CreateDPoPProof(key, method, url, nonce, accessToken)
	if err != nil {
		panic("oauth.createDPoPProof: " + err.Error())
	}
	return proof
}

func jsPublicJWK(_ js.Value, args []js.Value) any {
	pubBytes := jsBytesFromJS(args[0])
	pub, err := crypto.ParsePublicBytesP256(pubBytes)
	if err != nil {
		panic("oauth.publicJWK: " + err.Error())
	}
	jwk := oauth.PublicJWK(pub)
	return jsObj(
		"kty", jwk.KTY,
		"crv", jwk.CRV,
		"x", jwk.X,
		"y", jwk.Y,
	)
}
