//go:build js && wasm

package main

import (
	"context"
	"syscall/js"

	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/gt"
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

	// Optional second argument: options object with collections and dids filters.
	if len(args) > 1 && args[1].Type() == js.TypeObject {
		jsOpts := args[1]
		if cols := jsOpts.Get("collections"); !cols.IsUndefined() && cols.Length() > 0 {
			var collections []string
			for i := range cols.Length() {
				collections = append(collections, cols.Index(i).String())
			}
			opts.Collections = gt.Some(collections)
		}
		if dids := jsOpts.Get("dids"); !dids.IsUndefined() && dids.Length() > 0 {
			var didList []string
			for i := range dids.Length() {
				didList = append(didList, dids.Index(i).String())
			}
			opts.DIDs = gt.Some(didList)
		}
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
		go func() { _ = client.Close() }()
		return nil
	}))

	// Start consuming events in a goroutine.
	go func() {
		for batch, err := range client.Events(ctx) {
			if err != nil {
				continue
			}
			if callback.IsUndefined() {
				continue
			}

			for _, evt := range batch {
				// Jetstream events: emit the rich event directly.
				if js := evt.Jetstream; js != nil && js.Commit != nil {
					obj := jsObj(
						"kind", js.Kind,
						"did", js.DID,
						"timeUS", js.TimeUS,
						"operation", js.Commit.Operation,
						"collection", js.Commit.Collection,
						"rkey", js.Commit.RKey,
					)
					if len(js.Commit.Record) > 0 {
						obj.Set("record", string(js.Commit.Record))
					}
					callback.Invoke(obj)
					continue
				}

				// Firehose events: emit operations.
				for op, opErr := range evt.Operations() {
					if opErr != nil {
						continue
					}
					callback.Invoke(jsObj(
						"seq", evt.Seq,
						"kind", "commit",
						"operation", string(op.Action),
						"collection", op.Collection,
						"did", op.Repo,
						"rkey", op.RKey,
					))
				}
			}
		}
	}()

	return source
}
