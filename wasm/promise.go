//go:build js && wasm

package main

import "syscall/js"

// newPromise creates a JS Promise and returns (promise, resolve, reject).
func newPromise() (js.Value, func(any), func(string)) {
	var resolve, reject js.Value
	promise := js.Global().Get("Promise").New(js.FuncOf(func(_ js.Value, args []js.Value) any {
		resolve = args[0]
		reject = args[1]
		return nil
	}))
	return promise, func(v any) { resolve.Invoke(v) }, func(msg string) {
		reject.Invoke(js.Global().Get("Error").New(msg))
	}
}

// throwError throws a JS Error with the given message.
func throwError(msg string) {
	js.Global().Get("Error").New(msg)
	panic(msg)
}

// jsObj creates a JS object from key-value pairs.
func jsObj(kvs ...any) js.Value {
	obj := js.Global().Get("Object").New()
	for i := 0; i < len(kvs); i += 2 {
		obj.Set(kvs[i].(string), kvs[i+1])
	}
	return obj
}

// jsUint8Array creates a JS Uint8Array from Go bytes.
func jsUint8Array(data []byte) js.Value {
	arr := js.Global().Get("Uint8Array").New(len(data))
	js.CopyBytesToJS(arr, data)
	return arr
}

// jsBytesFromJS extracts Go bytes from a JS Uint8Array.
func jsBytesFromJS(v js.Value) []byte {
	buf := make([]byte, v.Get("length").Int())
	js.CopyBytesToGo(buf, v)
	return buf
}
