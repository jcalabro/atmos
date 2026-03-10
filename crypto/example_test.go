package crypto_test

import (
	"fmt"
	"strings"

	"github.com/jcalabro/atmos/crypto"
)

func ExampleGenerateP256() {
	priv, err := crypto.GenerateP256()
	if err != nil {
		panic(err)
	}
	pub := priv.PublicKey()

	// Sign and verify.
	msg := []byte("hello atproto")
	sig, err := priv.HashAndSign(msg)
	if err != nil {
		panic(err)
	}
	err = pub.HashAndVerify(msg, sig)
	fmt.Println(err)
	fmt.Println(strings.HasPrefix(pub.DIDKey(), "did:key:z"))
	// Output:
	// <nil>
	// true
}
