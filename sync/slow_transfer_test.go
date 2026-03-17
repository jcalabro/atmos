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

// fastTick is the sampling interval for tests. Short enough for fast tests,
// long enough to avoid flakes.
const fastTick = 20 * time.Millisecond

// nopCloser wraps a reader with a no-op Close for tests that don't need
// close-to-unblock behavior.
type nopCloser struct{ io.Reader }

func (nopCloser) Close() error { return nil }

func TestSlowTransferReader_FastReader(t *testing.T) {
	t.Parallel()

	data := bytes.Repeat([]byte("x"), 100_000)
	r := bytes.NewReader(data)
	str := newSlowTransferReaderWithConfig(r, nopCloser{r}, 100, 3, fastTick)
	defer str.Close()

	got, err := io.ReadAll(str)
	require.NoError(t, err)
	assert.Equal(t, len(data), len(got))
}

func TestSlowTransferReader_CompletelyStalled(t *testing.T) {
	t.Parallel()

	// Reader that blocks forever. The monitor should close it to unblock.
	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }()

	// speedLimit=100, maxSlow=3, tick=20ms → triggers after ~60ms
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

	// Sends 1 byte per tick — below the 100 bytes/tick threshold.
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

func TestSlowTransferReader_SlowThenFast(t *testing.T) {
	t.Parallel()

	// Slow for 2 ticks (below maxSlow=5), then speeds up.
	// The slow counter should reset when speed recovers.
	pr, pw := io.Pipe()
	str := newSlowTransferReaderWithConfig(pr, pr, 100, 5, fastTick)
	defer str.Close()

	go func() {
		defer func() { _ = pw.Close() }()

		// Slow phase: 2 ticks of 1 byte each.
		for range 2 {
			_, _ = pw.Write([]byte("x"))
			time.Sleep(fastTick)
		}

		// Fast phase: blast data above threshold.
		chunk := bytes.Repeat([]byte("y"), 10_000)
		for range 20 {
			_, _ = pw.Write(chunk)
			time.Sleep(fastTick)
		}
	}()

	got, err := io.ReadAll(str)
	require.NoError(t, err)
	assert.Greater(t, len(got), 10_000)
}

func TestSlowTransferReader_ExactThreshold(t *testing.T) {
	t.Parallel()

	// Sends exactly at the speed limit — should NOT trigger.
	pr, pw := io.Pipe()
	str := newSlowTransferReaderWithConfig(pr, pr, 100, 5, fastTick)
	defer str.Close()

	go func() {
		defer func() { _ = pw.Close() }()
		chunk := bytes.Repeat([]byte("z"), 100)
		for range 20 {
			_, _ = pw.Write(chunk)
			time.Sleep(fastTick)
		}
	}()

	got, err := io.ReadAll(str)
	require.NoError(t, err)
	assert.Equal(t, 100*20, len(got))
}

func TestSlowTransferReader_JustBelowThreshold(t *testing.T) {
	t.Parallel()

	// Sends 99 bytes per tick, just below the 100 threshold.
	pr, pw := io.Pipe()
	str := newSlowTransferReaderWithConfig(pr, pr, 100, 3, fastTick)
	defer str.Close()

	go func() {
		defer func() { _ = pw.Close() }()
		chunk := bytes.Repeat([]byte("z"), 99)
		for range 50 {
			_, _ = pw.Write(chunk)
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
	// High speed limit so it never triggers; we just verify byte counts.
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
