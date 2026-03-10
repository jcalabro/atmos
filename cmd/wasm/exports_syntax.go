//go:build js && wasm

package main

import (
	"syscall/js"

	"github.com/jcalabro/atmos"
)

func registerSyntax(atp js.Value) {
	atp.Set("parseDID", js.FuncOf(jsParseDID))
	atp.Set("parseHandle", js.FuncOf(jsParseHandle))
	atp.Set("parseATURI", js.FuncOf(jsParseATURI))
	atp.Set("parseTID", js.FuncOf(jsParseTID))
	atp.Set("generateTID", js.FuncOf(jsGenerateTID))
	atp.Set("parseNSID", js.FuncOf(jsParseNSID))
}

func jsParseDID(_ js.Value, args []js.Value) any {
	did, err := atmos.ParseDID(args[0].String())
	if err != nil {
		panic(err.Error())
	}
	return jsObj(
		"method", did.Method(),
		"identifier", did.Identifier(),
	)
}

func jsParseHandle(_ js.Value, args []js.Value) any {
	h, err := atmos.ParseHandle(args[0].String())
	if err != nil {
		panic(err.Error())
	}
	return h.String()
}

func jsParseATURI(_ js.Value, args []js.Value) any {
	u, err := atmos.ParseATURI(args[0].String())
	if err != nil {
		panic(err.Error())
	}
	return jsObj(
		"authority", u.Authority().String(),
		"collection", u.Collection().String(),
		"rkey", u.RecordKey().String(),
	)
}

func jsParseTID(_ js.Value, args []js.Value) any {
	t, err := atmos.ParseTID(args[0].String())
	if err != nil {
		panic(err.Error())
	}
	return jsObj(
		"time", t.Integer(),
		"integer", t.Integer(),
	)
}

func jsGenerateTID(_ js.Value, _ []js.Value) any {
	return atmos.NewTIDNow(0).String()
}

func jsParseNSID(_ js.Value, args []js.Value) any {
	n, err := atmos.ParseNSID(args[0].String())
	if err != nil {
		panic(err.Error())
	}
	return jsObj(
		"authority", n.Authority(),
		"name", n.Name(),
	)
}
