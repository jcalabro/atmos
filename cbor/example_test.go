package cbor_test

import (
	"fmt"

	"github.com/jcalabro/atmos/cbor"
)

func ExampleComputeCID() {
	data := []byte{0xa0} // empty CBOR map
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	fmt.Println(cid.Defined())
	fmt.Println(len(cid.Bytes())) // CIDv1 dag-cbor SHA-256 = 36 bytes
	// Output:
	// true
	// 36
}

func ExampleMarshal() {
	data, err := cbor.Marshal(map[string]any{
		"hello": "world",
		"n":     int64(42),
	})
	if err != nil {
		panic(err)
	}
	val, err := cbor.Unmarshal(data)
	if err != nil {
		panic(err)
	}
	m, _ := val.(map[string]any)
	fmt.Println(m["hello"])
	fmt.Println(m["n"])
	// Output:
	// world
	// 42
}
