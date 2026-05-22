package streaming

import (
	"context"
	"fmt"

	"github.com/jcalabro/atmos/sync"
)

// verifyResult is the output of running per-event verification. It is
// produced by verifyOne and consumed by the collector goroutine.
//
// hookErr carries a verification failure that should be surfaced to
// the consumer as a (nil, err) yield.
//
// accountErr signals an OnAccountEvent infrastructure failure. The
// verifier's bookkeeping failed, but the underlying #account event
// still flows to the consumer; the collector yields the error AND
// appends the event.
//
// silentDrop is true when the verifier ran successfully but produced
// no operations (returned (nil, nil)). Three causes: the event was a
// rev replay, was queued for async resync, or was appended to a
// per-DID pending buffer during an in-flight resync. The firehose seq
// advances but the event is NOT delivered to the consumer.
type verifyResult struct {
	evt        Event
	hookErr    error
	accountErr error
	silentDrop bool
}

// verifyOne runs the per-event verifier work for one decoded event:
//   - feeds #account events into the verifier's HostingState tracker;
//   - runs Sync 1.1 verification on #commit/#sync events when a
//     verifier is configured;
//   - leaves jetstream and label-stream events untouched.
//
// Safe to call from N goroutines concurrently (the verifier itself is
// thread-safe via per-DID mutexes).
func (c *Client) verifyOne(ctx context.Context, evt Event) verifyResult {
	res := verifyResult{evt: evt}

	// No verifier configured (None or Some(nil)): pass through.
	if !c.opts.Verifier.HasVal() || c.opts.Verifier.Val() == nil {
		return res
	}
	v := c.opts.Verifier.Val()

	// #account: feed into HostingState. Doesn't suppress delivery.
	// Restricted to firehose; jetstream's isn't part of the verifier
	// contract.
	if !c.isJetstream && evt.Account != nil {
		if aErr := v.OnAccountEvent(ctx, evt.Account); aErr != nil {
			res.accountErr = fmt.Errorf("OnAccountEvent: %w", aErr)
			// Fall through; we still verify a #commit if one is on
			// the same Event (shouldn't happen in practice; defensive).
		}
	}

	// Sync 1.1: verify #commit and #sync.
	if evt.Commit != nil || evt.Sync != nil {
		var (
			ops  []sync.VerifierOp
			vErr error
		)
		switch {
		case evt.Commit != nil:
			ops, vErr = v.VerifyCommit(ctx, evt.Commit)
		case evt.Sync != nil:
			ops, vErr = v.VerifySync(ctx, evt.Sync)
		}
		if vErr != nil {
			res.hookErr = vErr
			return res
		}
		if ops == nil {
			// Rev replay / queued for async resync / pending during
			// in-flight resync. Seq must still advance, but the event
			// is dropped from delivery.
			res.silentDrop = true
			return res
		}
		res.evt.verifiedOps = convertVerifierOps(ops)
		res.evt.verifierRan = true
	}

	return res
}
