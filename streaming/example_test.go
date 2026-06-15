package streaming_test

import (
	"context"
	"fmt"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/streaming"
	"github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
)

func ExampleClient_firehose() {
	client, err := streaming.NewClient(streaming.Options{
		URL: "wss://relay1.us-east.bsky.network/xrpc/com.atproto.sync.subscribeRepos",
	})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	for batch, err := range client.Events(context.Background()) {
		if err != nil {
			panic(err)
		}
		for _, evt := range batch {
			for op, err := range evt.Operations() {
				if err != nil {
					panic(err)
				}
				fmt.Printf("%s %s/%s from %s\n", op.Action, op.Collection, op.RKey, op.Repo)
			}
		}
	}
}

func ExampleClient_jetstream() {
	client, err := streaming.NewClient(streaming.Options{
		URL: "wss://jetstream2.us-east.bsky.network/subscribe",
		Collections: gt.Some([]string{
			"app.bsky.feed.post",
		}),
	})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	for batch, err := range client.Events(context.Background()) {
		if err != nil {
			panic(err)
		}
		for _, evt := range batch {
			js := evt.Jetstream
			if js == nil || js.Commit == nil {
				continue
			}
			fmt.Printf("%s %s/%s %s\n",
				js.Commit.Operation,
				js.Commit.Collection,
				js.Commit.RKey,
				js.Commit.Record,
			)
		}
	}
}

func ExampleClient_labeler() {
	client, err := streaming.NewClient(streaming.Options{
		URL: "wss://mod.bsky.app/xrpc/com.atproto.label.subscribeLabels",
	})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	for batch, err := range client.Events(context.Background()) {
		if err != nil {
			panic(err)
		}
		for _, evt := range batch {
			for _, label := range evt.Labels() {
				fmt.Printf("label src=%s uri=%s val=%s neg=%v\n",
					label.Src,
					label.URI,
					label.Val,
					label.Neg.ValOr(false),
				)
			}
		}
	}
}

func ExampleClient_firehoseWithVerifier() {
	dir := &identity.Directory{
		Resolver:               &identity.DefaultResolver{},
		Cache:                  identity.NewLRUCache(100_000, 24*time.Hour),
		SkipHandleVerification: true,
	}

	xc := &xrpc.Client{Host: "https://relay1.us-east.bsky.network"}
	syncClient := sync.NewClient(sync.Options{
		Client:    xc,
		Directory: gt.Some(dir),
	})

	verifier, err := sync.NewVerifier(sync.VerifierOptions{
		Directory:  dir,
		StateStore: sync.NewMemStateStore(), // production: use durable storage
		SyncClient: gt.Some(syncClient),
		OnVerificationFailure: gt.Some(func(did atmos.DID, err error) {
			fmt.Printf("verification failure did=%s err=%v\n", did, err)
		}),
		OnResync: gt.Some(func(did atmos.DID, oldRev, newRev string, reason sync.ResyncReason) {
			fmt.Printf("resync did=%s old=%s new=%s reason=%s\n", did, oldRev, newRev, reason)
		}),
		OnAccountStateChanged: gt.Some(func(did atmos.DID, state sync.HostingState) {
			fmt.Printf("account did=%s active=%v status=%s\n", did, state.Active, state.Status)
		}),
	})
	if err != nil {
		panic(err)
	}
	defer verifier.Close()

	client, err := streaming.NewClient(streaming.Options{
		URL:         "wss://relay1.us-east.bsky.network/xrpc/com.atproto.sync.subscribeRepos",
		SyncClient:  gt.Some(syncClient),
		Verifier:    gt.Some(verifier),
		Parallelism: gt.Some(32),
	})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	for batch, err := range client.Events(context.Background()) {
		if err != nil {
			panic(err)
		}
		for _, evt := range batch {
			for op, err := range evt.Operations() {
				if err != nil {
					panic(err)
				}
				fmt.Printf("%s %s/%s from %s\n", op.Action, op.Collection, op.RKey, op.Repo)
			}
		}
	}
}
