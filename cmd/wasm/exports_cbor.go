//go:build js && wasm

package main

import (
	"bytes"
	"encoding/json"
	"syscall/js"

	"github.com/jcalabro/atmos/cbor"
)

func registerCBOR(atp js.Value) {
	ns := js.Global().Get("Object").New()
	ns.Set("encode", js.FuncOf(jsCBOREncode))
	ns.Set("decode", js.FuncOf(jsCBORDecode))
	ns.Set("computeCID", js.FuncOf(jsCBORComputeCID))
	atp.Set("cbor", ns)
}

func jsCBOREncode(_ js.Value, args []js.Value) any {
	// Accept a JS object, JSON-stringify it, then encode to DAG-CBOR.
	jsonStr := js.Global().Get("JSON").Call("stringify", args[0]).String()

	var v any
	if err := json.Unmarshal([]byte(jsonStr), &v); err != nil {
		panic("cbor.encode: invalid JSON: " + err.Error())
	}

	var buf bytes.Buffer
	enc := cbor.NewEncoder(&buf)
	if err := enc.WriteValue(v); err != nil {
		panic("cbor.encode: " + err.Error())
	}
	return jsUint8Array(buf.Bytes())
}

func jsCBORDecode(_ js.Value, args []js.Value) any {
	data := jsBytesFromJS(args[0])

	dec := cbor.NewDecoder(bytes.NewReader(data))
	v, err := dec.ReadValue()
	if err != nil {
		panic("cbor.decode: " + err.Error())
	}

	// Convert to JSON and parse on JS side.
	jsonBytes, err := json.Marshal(v)
	if err != nil {
		panic("cbor.decode: marshal result: " + err.Error())
	}
	return js.Global().Get("JSON").Call("parse", string(jsonBytes))
}

func jsCBORComputeCID(_ js.Value, args []js.Value) any {
	data := jsBytesFromJS(args[0])
	cid := cbor.ComputeCID(cbor.CodecDagCBOR, data)
	return cid.String()
}
