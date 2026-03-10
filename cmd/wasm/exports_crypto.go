//go:build js && wasm

package main

import (
	"syscall/js"

	"github.com/jcalabro/atmos/crypto"
)

func registerCrypto(atp js.Value) {
	ns := js.Global().Get("Object").New()
	ns.Set("generateP256", js.FuncOf(jsGenerateP256))
	ns.Set("generateK256", js.FuncOf(jsGenerateK256))
	ns.Set("sign", js.FuncOf(jsSign))
	ns.Set("verify", js.FuncOf(jsVerify))
	ns.Set("parseDIDKey", js.FuncOf(jsParseDIDKey))
	atp.Set("crypto", ns)
}

func jsGenerateP256(_ js.Value, _ []js.Value) any {
	key, err := crypto.GenerateP256()
	if err != nil {
		panic("crypto.generateP256: " + err.Error())
	}
	pub := key.PublicKey()
	return jsObj(
		"privateKey", jsUint8Array(key.Bytes()),
		"publicKey", jsUint8Array(pub.Bytes()),
		"didKey", pub.DIDKey(),
	)
}

func jsGenerateK256(_ js.Value, _ []js.Value) any {
	key, err := crypto.GenerateK256()
	if err != nil {
		panic("crypto.generateK256: " + err.Error())
	}
	pub := key.PublicKey()
	return jsObj(
		"privateKey", jsUint8Array(key.Bytes()),
		"publicKey", jsUint8Array(pub.Bytes()),
		"didKey", pub.DIDKey(),
	)
}

func jsSign(_ js.Value, args []js.Value) any {
	keyBytes := jsBytesFromJS(args[0])
	content := jsBytesFromJS(args[1])

	// Try P-256 first (32 bytes), then K-256.
	key, err := crypto.ParsePrivateP256(keyBytes)
	if err != nil {
		k256, err2 := crypto.ParsePrivateK256(keyBytes)
		if err2 != nil {
			panic("crypto.sign: unrecognized key format")
		}
		sig, err2 := k256.HashAndSign(content)
		if err2 != nil {
			panic("crypto.sign: " + err2.Error())
		}
		return jsUint8Array(sig)
	}

	sig, err := key.HashAndSign(content)
	if err != nil {
		panic("crypto.sign: " + err.Error())
	}
	return jsUint8Array(sig)
}

func jsVerify(_ js.Value, args []js.Value) any {
	pubBytes := jsBytesFromJS(args[0])
	content := jsBytesFromJS(args[1])
	sig := jsBytesFromJS(args[2])

	// Try P-256 first, then K-256.
	pub, err := crypto.ParsePublicBytesP256(pubBytes)
	if err != nil {
		k256, err2 := crypto.ParsePublicBytesK256(pubBytes)
		if err2 != nil {
			panic("crypto.verify: unrecognized key format")
		}
		return k256.HashAndVerify(content, sig) == nil
	}
	return pub.HashAndVerify(content, sig) == nil
}

func jsParseDIDKey(_ js.Value, args []js.Value) any {
	s := args[0].String()
	pub, err := crypto.ParsePublicDIDKey(s)
	if err != nil {
		panic("crypto.parseDIDKey: " + err.Error())
	}
	keyType := "unknown"
	switch pub.(type) {
	case *crypto.P256PublicKey:
		keyType = "p256"
	case *crypto.K256PublicKey:
		keyType = "k256"
	}
	return jsObj(
		"type", keyType,
		"publicKey", jsUint8Array(pub.Bytes()),
		"multibase", pub.Multibase(),
	)
}
