package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/jcalabro/gt"
	"github.com/jcalabro/atmos/api/bsky"
	"github.com/jcalabro/atmos/streaming"
)

func main() {
	url := flag.String("url", "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos", "WebSocket URL")
	cursor := flag.Int64("cursor", 0, "sequence number to resume from (0 = live)")
	dumpErrors := flag.Bool("dump-errors", false, "dump hex of frames that fail to decode")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	opts := streaming.Options{
		URL: *url,
		OnReconnect: gt.Some(func(attempt int, delay time.Duration) {
			log.Printf("reconnecting (attempt %d, delay %s)", attempt, delay)
		}),
	}
	if *cursor != 0 {
		opts.Cursor = gt.Some(*cursor)
	}
	client, err := streaming.NewClient(opts)
	if err != nil {
		log.Fatalf("create streaming client: %v", err)
	}
	defer func() { _ = client.Close() }()

	for evt, err := range client.Events(ctx) {
		if err != nil {
			if *dumpErrors {
				if raw := streaming.ErrorRawFrame(err); raw != nil {
					fmt.Fprintf(os.Stderr, "error: %v\nhex: %s\n", err, hex.EncodeToString(raw))
					continue
				}
			}
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			continue
		}

		for op, err := range evt.Operations() {
			if err != nil {
				fmt.Fprintf(os.Stderr, "car error: %v\n", err)
				continue
			}

			rec, err := op.Record(bsky.DecodeRecord)
			if err != nil {
				continue
			}

			switch v := rec.(type) {
			case *bsky.FeedPost:
				fmt.Printf("[%s] %s\n", op.Repo, v.Text)
			}
		}
	}
}
