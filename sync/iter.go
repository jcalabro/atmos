package sync

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"sync/atomic"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/repo"
)

const (
	// lowSpeedLimit is the minimum transfer speed in bytes per second.
	// If the average speed stays below this for lowSpeedTime, the transfer
	// is considered stalled and killed. Modeled after curl's
	// CURLOPT_LOW_SPEED_LIMIT / CURLOPT_LOW_SPEED_TIME.
	lowSpeedLimit = 1000 // bytes/sec

	// lowSpeedTime is how long the transfer speed must remain below
	// lowSpeedLimit before aborting. Sampled once per second.
	lowSpeedTime = 30 * time.Second
)

// slowTransferReader wraps a reader and aborts if the transfer speed stays
// below a threshold for too long. Unlike a simple idle timeout, this catches
// adversarially slow servers that trickle data (e.g. 1 byte/second) to keep
// the connection alive. Inspired by curl's --speed-limit / --speed-time.
type slowTransferReader struct {
	r          io.Reader
	closer     io.Closer // closed to unblock a stuck Read when transfer is too slow
	bytesRead  int64     // bytes read since last sample
	speedLimit int64     // bytes per tick threshold

	// tickC delivers sampling signals. Production wires it to a
	// time.Ticker.C; tests inject their own channel so threshold
	// logic is asserted without wall-clock dependency.
	tickC <-chan time.Time

	// stopTicker stops the underlying time.Ticker. nil for
	// test-driven readers whose tickC is owned by the test.
	stopTicker func()

	done chan struct{}
	errp atomic.Pointer[error] // set by monitor, read by Read

	// monitor-only state.
	slowTicks int
	maxSlow   int

	// onSample fires after each tick is processed. nil in production;
	// tests use it as a barrier to inject the next tick only after
	// the previous tick's bookkeeping has settled.
	onSample func()
}

// newSlowTransferReaderFromBody wraps an HTTP response body with slow
// transfer detection. The body is used as both the reader and the closer
// (closing the body unblocks any stuck Read).
func newSlowTransferReaderFromBody(body io.ReadCloser) *slowTransferReader {
	return newSlowTransferReaderWithConfig(body, body, lowSpeedLimit, int(lowSpeedTime/time.Second), time.Second)
}

// newSlowTransferReaderWithConfig creates a production slowTransferReader
// driven by a real time.Ticker. speedLimit is bytes per tick, maxSlow is
// how many consecutive slow ticks before abort, tickInterval is the
// sampling period. closer is called to unblock a stuck Read when the
// transfer is too slow; pass the HTTP response body or pipe closer.
func newSlowTransferReaderWithConfig(r io.Reader, closer io.Closer, speedLimit int64, maxSlow int, tickInterval time.Duration) *slowTransferReader {
	t := time.NewTicker(tickInterval)
	str := &slowTransferReader{
		r:          r,
		closer:     closer,
		speedLimit: speedLimit,
		tickC:      t.C,
		stopTicker: t.Stop,
		done:       make(chan struct{}),
		maxSlow:    maxSlow,
	}
	go str.monitor()
	return str
}

// newSlowTransferReaderWithTickChannel is the test entry point. The
// caller owns tickC and drives samples deterministically. Threshold
// logic asserted via this constructor is independent of goroutine
// scheduling. onSample fires after each tick's bookkeeping completes
// — tests use it to wait before injecting the next tick.
func newSlowTransferReaderWithTickChannel(r io.Reader, closer io.Closer, speedLimit int64, maxSlow int, tickC <-chan time.Time, onSample func()) *slowTransferReader {
	str := &slowTransferReader{
		r:          r,
		closer:     closer,
		speedLimit: speedLimit,
		tickC:      tickC,
		done:       make(chan struct{}),
		maxSlow:    maxSlow,
		onSample:   onSample,
	}
	go str.monitor()
	return str
}

func (s *slowTransferReader) monitor() {
	for {
		select {
		case <-s.tickC:
			bytes := atomic.LoadInt64(&s.bytesRead)
			atomic.AddInt64(&s.bytesRead, -bytes)

			if bytes < s.speedLimit {
				s.slowTicks++
			} else {
				s.slowTicks = 0
			}

			if s.slowTicks >= s.maxSlow {
				err := fmt.Errorf("sync: transfer too slow (<%d B/s for %d ticks)", s.speedLimit, s.maxSlow)
				s.errp.Store(&err)
				// Close the underlying reader to unblock any stuck Read.
				if s.closer != nil {
					_ = s.closer.Close()
				}
				if s.onSample != nil {
					s.onSample()
				}
				return
			}
			if s.onSample != nil {
				s.onSample()
			}
		case <-s.done:
			return
		}
	}
}

func (s *slowTransferReader) Read(p []byte) (int, error) {
	if ep := s.errp.Load(); ep != nil {
		return 0, *ep
	}
	n, err := s.r.Read(p)
	if n > 0 {
		atomic.AddInt64(&s.bytesRead, int64(n))
	}
	return n, err
}

func (s *slowTransferReader) Close() {
	if s.stopTicker != nil {
		s.stopTicker()
	}
	close(s.done)
}

var errBreak = errors.New("break")

// IterRecords downloads a repo and yields every record as an iterator.
// The HTTP body is streamed directly into the CAR reader (not buffered).
// All blocks are loaded into a MemBlockStore for MST traversal.
//
// If Options.Directory is set, the commit signature is verified before
// yielding any records.
func (c *Client) IterRecords(ctx context.Context, did atmos.DID) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		body, err := c.GetRepoStream(ctx, did, "")
		if err != nil {
			yield(Record{}, err)
			return
		}
		defer func() { _ = body.Close() }()

		// Wrap with low-speed detection to kill stalled PDS connections.
		// Aborts if transfer speed stays below 1KB/s for 30 consecutive
		// seconds. Large repos streaming at any reasonable speed are
		// unaffected; adversarially slow trickle connections are killed.
		str := newSlowTransferReaderFromBody(body)
		defer str.Close()

		// Wrap in bufio.Reader to batch small reads from the network.
		// CAR varint parsing reads 1 byte at a time; without buffering
		// each would be a separate syscall over HTTP.
		rp, commit, err := repo.LoadFromCAR(bufio.NewReader(str))
		if err != nil {
			yield(Record{}, err)
			return
		}

		// Optional signature verification.
		if c.opts.Directory.HasVal() {
			if err := c.VerifyCommit(ctx, commit); err != nil {
				yield(Record{}, err)
				return
			}
		}

		// Walk MST, yield each record.
		_ = rp.Tree.Walk(func(key string, val cbor.CID) error {
			col, rkey := repo.SplitMSTKey(key)

			data, err := rp.Store.GetBlock(val)
			if err != nil {
				if !yield(Record{}, err) {
					return errBreak
				}
				return nil
			}

			if !yield(Record{Collection: col, RKey: rkey, CID: val, Data: data, Rev: commit.Rev}, nil) {
				return errBreak
			}
			return nil
		})
	}
}
