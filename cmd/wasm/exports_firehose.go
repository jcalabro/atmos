//go:build js && wasm

package main

import (
	"context"
	"syscall/js"

	"github.com/jcalabro/atmos/streaming"
)

func registerFirehose(atp js.Value) {
	ns := js.Global().Get("Object").New()
	ns.Set("connect", js.FuncOf(jsFirehoseConnect))
	atp.Set("firehose", ns)
}

func jsFirehoseConnect(_ js.Value, args []js.Value) any {
	relayURL := args[0].String()

	// Build streaming options.
	opts := streaming.Options{
		URL: relayURL,
	}

	client, err := streaming.NewClient(opts)
	if err != nil {
		return js.Null()
	}
	ctx, cancel := context.WithCancel(context.Background())

	source := js.Global().Get("Object").New()

	var callback js.Value
	source.Set("onEvent", js.FuncOf(func(_ js.Value, args []js.Value) any {
		callback = args[0]
		return nil
	}))
	source.Set("close", js.FuncOf(func(_ js.Value, _ []js.Value) any {
		cancel()
		_ = client.Close()
		return nil
	}))

	// Start consuming events in a goroutine.
	go func() {
		for evt, err := range client.Events(ctx) {
			if err != nil {
				continue
			}
			if callback.IsUndefined() {
				continue
			}

			for op, opErr := range evt.Operations() {
				if opErr != nil {
					continue
				}
				callback.Invoke(jsObj(
					"seq", evt.Seq,
					"action", string(op.Action),
					"collection", op.Collection,
					"repo", op.Repo,
					"rkey", op.RKey,
				))
			}
		}
	}()

	return source
}
