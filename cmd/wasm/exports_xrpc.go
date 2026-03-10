//go:build js && wasm

package main

import (
	"context"
	"encoding/json"
	"syscall/js"

	"github.com/jcalabro/atmos/xrpc"
)

func registerXRPC(atp js.Value) {
	ns := js.Global().Get("Object").New()
	ns.Set("query", js.FuncOf(jsXRPCQuery))
	ns.Set("procedure", js.FuncOf(jsXRPCProcedure))
	atp.Set("xrpc", ns)
}

func jsXRPCQuery(_ js.Value, args []js.Value) any {
	host := args[0].String()
	nsid := args[1].String()
	params := jsObjToMap(args[2])
	promise, resolve, reject := newPromise()

	go func() {
		client := &xrpc.Client{Host: host}
		var out json.RawMessage
		if err := client.Query(context.Background(), nsid, params, &out); err != nil {
			reject("xrpc.query: " + err.Error())
			return
		}
		resolve(js.Global().Get("JSON").Call("parse", string(out)))
	}()

	return promise
}

func jsXRPCProcedure(_ js.Value, args []js.Value) any {
	host := args[0].String()
	nsid := args[1].String()
	inputJSON := js.Global().Get("JSON").Call("stringify", args[2]).String()
	promise, resolve, reject := newPromise()

	go func() {
		client := &xrpc.Client{Host: host}
		var input json.RawMessage
		input = json.RawMessage(inputJSON)

		var out json.RawMessage
		if err := client.Procedure(context.Background(), nsid, input, &out); err != nil {
			reject("xrpc.procedure: " + err.Error())
			return
		}
		resolve(js.Global().Get("JSON").Call("parse", string(out)))
	}()

	return promise
}

func jsObjToMap(v js.Value) map[string]any {
	if v.IsNull() || v.IsUndefined() {
		return nil
	}
	keys := js.Global().Get("Object").Call("keys", v)
	m := make(map[string]any, keys.Length())
	for i := range keys.Length() {
		k := keys.Index(i).String()
		m[k] = v.Get(k).String()
	}
	return m
}
