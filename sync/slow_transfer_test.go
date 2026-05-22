package sync

import (
	"bytes"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fastTick is the sampling interval for the wall-clock-driven tests
// below (the ones that don't use the deterministic ticker). Sized to
// absorb scheduling jitter when this package's CPU-heavy verifier
// swarm tests run alongside under -race. The threshold-precision
// tests use newSlowTransferReaderWithTickChannel so their assertions
// are independent of wall-clock scheduling.
const fastTick = 20 * time.Millisecond

// nopCloser wraps a reader with a no-op Close for tests that don't need
// close-to-unblock behavior.
type nopCloser struct{ io.Reader }

func (nopCloser) Close() error { return nil }

// driver is the deterministic-ticker harness used by tests that
// assert exact threshold behavior. The test pushes data, calls
// pulse() to inject one sample, and waits for the monitor's
// bookkeeping to complete before asserting.
type driver struct {
	tickC    chan time.Time
	sampleCh chan struct{}
}

// newDriver builds a deterministic-ticker harness wired to a
// fresh slowTransferReader. The returned driver lets the test
// drive ticks one at a time via pulse().
func newDriver(t *testing.T, r io.Reader, closer io.Closer, speedLimit int64, maxSlow int) (*slowTransferReader, *driver) {
	t.Helper()
	d := &driver{
		tickC:    make(chan time.Time, 1),
		sampleCh: make(chan struct{}, 1),
	}
	str := newSlowTransferReaderWithTickChannel(
		r, closer, speedLimit, maxSlow, d.tickC,
		func() { d.sampleCh <- struct{}{} },
	)
	t.Cleanup(str.Close)
	return str, d
}

// pulse injects one tick into the monitor and blocks until the
// monitor has finished processing it (via onSample). Returns true if
// the tick caused the monitor to trip and exit; false if the monitor
// continues. After a true return, no further pulses will be observed.
func (d *driver) pulse(t *testing.T) bool {
	t.Helper()
	d.tickC <- time.Now()
	select {
	case <-d.sampleCh:
		return false // not necessarily; check errp via the str
	case <-time.After(time.Second):
		t.Fatal("monitor did not process tick within 1s")
		return false
	}
}

func TestSlowTransferReader_FastReader(t *testing.T) {
	t.Parallel()

	data := bytes.Repeat([]byte("x"), 100_000)
	r := bytes.NewReader(data)
	str, d := newDriver(t, r, nopCloser{r}, 100, 3)

	// Read in one shot — buffer holds everything.
	buf := make([]byte, len(data)+1)
	n, _ := str.Read(buf)
	require.Equal(t, len(data), n)

	// Inject several ticks. With 100k bytes counted on the first read,
	// the first tick zeros the counter; subsequent ticks see 0 bytes
	// and increment slowTicks. Trip after maxSlow=3.
	d.pulse(t) // n=100k, slow=0
	d.pulse(t) // n=0,    slow=1
	d.pulse(t) // n=0,    slow=2
	d.pulse(t) // n=0,    slow=3 → trip

	// At this point the monitor has tripped; the error is stored.
	assert.Error(t, asError(str.errp.Load()))
}

// TestSlowTransferReader_StaysHealthyAtThreshold asserts the
// "exactly at speed limit, never trips" property — the case the old
// flaky ExactThreshold test was reaching for. With a deterministic
// ticker each pulse counts exactly speedLimit bytes, so slowTicks
// stays at 0 indefinitely.
func TestSlowTransferReader_StaysHealthyAtThreshold(t *testing.T) {
	t.Parallel()

	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }()
	str, d := newDriver(t, pr, pr, 100, 5)

	// Read in a goroutine so writes drain.
	readDone := make(chan struct{})
	totalRead := atomic.Int64{}
	go func() {
		defer close(readDone)
		buf := make([]byte, 1024)
		for {
			n, err := str.Read(buf)
			totalRead.Add(int64(n))
			if err != nil {
				return
			}
		}
	}()

	chunk := bytes.Repeat([]byte("z"), 100)
	for range 20 {
		_, err := pw.Write(chunk)
		require.NoError(t, err)
		// Wait until the read goroutine has accounted for this chunk.
		// We can't observe bytesRead directly, so spin briefly via
		// totalRead which mirrors it.
		require.Eventually(t, func() bool {
			return atomic.LoadInt64(&str.bytesRead) >= 100
		}, time.Second, 100*time.Microsecond,
			"read goroutine did not account for the write before tick")
		d.pulse(t)
		assert.Nil(t, str.errp.Load(), "must not trip at exact threshold")
	}

	_ = pw.Close()
	<-readDone
	assert.Equal(t, int64(100*20), totalRead.Load())
}

// TestSlowTransferReader_TripsBelowThreshold asserts the inverse:
// a transfer running at speedLimit-1 bytes/tick trips after exactly
// maxSlow consecutive ticks.
func TestSlowTransferReader_TripsBelowThreshold(t *testing.T) {
	t.Parallel()

	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }()
	str, d := newDriver(t, pr, pr, 100, 3)

	totalRead := atomic.Int64{}
	go func() {
		buf := make([]byte, 1024)
		for {
			n, err := str.Read(buf)
			totalRead.Add(int64(n))
			if err != nil {
				return
			}
		}
	}()

	chunk := bytes.Repeat([]byte("z"), 99)
	// Pulse twice with sub-threshold writes — slow counter at 1, then 2.
	for range 2 {
		_, err := pw.Write(chunk)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return atomic.LoadInt64(&str.bytesRead) >= 99
		}, time.Second, 100*time.Microsecond)
		d.pulse(t)
		assert.Nil(t, str.errp.Load(), "should not have tripped yet")
	}
	// Third sub-threshold pulse — slow counter hits maxSlow=3, trip.
	_, err := pw.Write(chunk)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return atomic.LoadInt64(&str.bytesRead) >= 99
	}, time.Second, 100*time.Microsecond)
	d.pulse(t)
	assert.Error(t, asError(str.errp.Load()), "should have tripped on third slow tick")
}

// TestSlowTransferReader_SlowThenFastResetsCounter asserts the
// counter-reset semantic: a tick at-or-above threshold zeroes the
// slow tick counter, so a subsequent slow phase needs the full
// maxSlow streak to trip.
func TestSlowTransferReader_SlowThenFastResetsCounter(t *testing.T) {
	t.Parallel()

	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }()
	str, d := newDriver(t, pr, pr, 100, 3)

	totalRead := atomic.Int64{}
	go func() {
		buf := make([]byte, 1024)
		for {
			n, err := str.Read(buf)
			totalRead.Add(int64(n))
			if err != nil {
				return
			}
		}
	}()

	pulseAfterWrite := func(data []byte) {
		t.Helper()
		_, err := pw.Write(data)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return atomic.LoadInt64(&str.bytesRead) >= int64(len(data))
		}, time.Second, 100*time.Microsecond)
		d.pulse(t)
	}

	// Two slow ticks (counter → 2, just shy of trip).
	pulseAfterWrite([]byte("x"))
	pulseAfterWrite([]byte("x"))
	require.Nil(t, str.errp.Load())

	// One fast tick — resets counter to 0.
	pulseAfterWrite(bytes.Repeat([]byte("y"), 200))
	require.Nil(t, str.errp.Load())

	// Two more slow ticks — counter at 2, no trip yet.
	pulseAfterWrite([]byte("x"))
	pulseAfterWrite([]byte("x"))
	require.Nil(t, str.errp.Load(),
		"counter should have reset after the fast tick; two slow ticks alone shouldn't trip")
}

// asError unwraps the atomic.Pointer[error] result for assertion clarity.
func asError(p *error) error {
	if p == nil {
		return nil
	}
	return *p
}

// --- The following tests use the wall-clock-driven constructor ---
//
// They assert qualitative properties (the monitor unblocks a stuck
// read; an error eventually fires; bookkeeping is concurrent-safe).
// Their assertions are robust to scheduling jitter — they do not
// depend on exact byte counts within exact tick windows.

func TestSlowTransferReader_CompletelyStalled(t *testing.T) {
	t.Parallel()

	// Reader that blocks forever. The monitor should close it to unblock.
	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }()

	str := newSlowTransferReaderWithConfig(pr, pr, 100, 3, fastTick)
	defer str.Close()

	buf := make([]byte, 1024)
	done := make(chan error, 1)
	go func() {
		for {
			_, err := str.Read(buf)
			if err != nil {
				done <- err
				return
			}
		}
	}()

	select {
	case err := <-done:
		// Should get either our slow transfer error or a closed pipe error.
		// The important thing is that the read unblocked.
		assert.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for slow transfer detection to unblock read")
	}
}

func TestSlowTransferReader_TrickleAttack(t *testing.T) {
	t.Parallel()

	// Sends 1 byte per tick — well below the 100 bytes/tick threshold.
	// The trickle attack assertion is qualitative ("eventually errors"),
	// so wall-clock timing is fine.
	pr, pw := io.Pipe()
	str := newSlowTransferReaderWithConfig(pr, pr, 100, 3, fastTick)
	defer str.Close()

	go func() {
		defer func() { _ = pw.Close() }()
		for range 20 {
			_, _ = pw.Write([]byte("x"))
			time.Sleep(fastTick)
		}
	}()

	_, err := io.ReadAll(str)
	assert.Error(t, err)
}

func TestSlowTransferReader_CloseStopsMonitor(t *testing.T) {
	t.Parallel()

	pr, pw := io.Pipe()
	str := newSlowTransferReaderWithConfig(pr, pr, 100, 3, fastTick)

	// Close immediately — monitor should exit via done channel.
	str.Close()
	_ = pw.Close()
	_ = pr.Close()

	// The error should not be set (we closed before any ticks fired).
	time.Sleep(fastTick * 5)
	assert.Nil(t, str.errp.Load())
}

func TestSlowTransferReader_ErrorStickyAfterTrigger(t *testing.T) {
	t.Parallel()

	// Empty reader = immediate EOF on first read, then 0 bytes/tick.
	r := strings.NewReader("")
	str := newSlowTransferReaderWithConfig(r, nopCloser{r}, 100, 2, fastTick)
	defer str.Close()

	// Wait for the error to trigger.
	time.Sleep(fastTick * 5)

	buf := make([]byte, 10)
	_, err1 := str.Read(buf)
	_, err2 := str.Read(buf)
	assert.Error(t, err1)
	assert.Error(t, err2)
}

func TestSlowTransferReader_ByteCountingAccuracy(t *testing.T) {
	t.Parallel()

	pr, pw := io.Pipe()
	// High maxSlow so it never triggers; we just verify byte counts.
	str := newSlowTransferReaderWithConfig(pr, pr, 1, 1000, fastTick)
	defer str.Close()

	totalWritten := int64(0)
	go func() {
		defer func() { _ = pw.Close() }()
		chunk := bytes.Repeat([]byte("a"), 1000)
		for range 10 {
			n, _ := pw.Write(chunk)
			totalWritten += int64(n)
			time.Sleep(fastTick / 2)
		}
	}()

	totalRead := int64(0)
	buf := make([]byte, 256)
	for {
		n, err := str.Read(buf)
		totalRead += int64(n)
		if err != nil {
			break
		}
	}

	assert.Equal(t, totalWritten, totalRead)
}

func TestSlowTransferReader_ConcurrentReadSafety(t *testing.T) {
	t.Parallel()

	// Verify the atomic byte counting doesn't race with the monitor.
	pr, pw := io.Pipe()
	str := newSlowTransferReaderWithConfig(pr, pr, 1, 100, time.Millisecond)
	defer str.Close()

	var readOps atomic.Int64

	go func() {
		defer func() { _ = pw.Close() }()
		for range 1000 {
			_, _ = pw.Write([]byte("x"))
		}
	}()

	done := make(chan struct{})
	go func() {
		buf := make([]byte, 1)
		for {
			_, err := str.Read(buf)
			readOps.Add(1)
			if err != nil {
				break
			}
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}

	assert.Greater(t, readOps.Load(), int64(0))
}

func TestSlowTransferReader_CloseUnblocksMonitorBeforeTrigger(t *testing.T) {
	t.Parallel()

	// Start a reader that will be slow, but Close() before the threshold
	// is reached. The monitor should exit cleanly without setting an error.
	pr, pw := io.Pipe()
	// maxSlow=100 so it would take 100 ticks to trigger — we close after 3.
	str := newSlowTransferReaderWithConfig(pr, pr, 10000, 100, fastTick)

	time.Sleep(fastTick * 3)
	str.Close()
	_ = pw.Close()

	assert.Nil(t, str.errp.Load())
}
