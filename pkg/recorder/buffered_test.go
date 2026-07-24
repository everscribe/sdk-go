package recorder

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/everscribe/sdk-go/pkg/event"
	"github.com/stretchr/testify/require"
)

// silentLogger discards diagnostic logging from BufferedRecorder so test
// output stays clean.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// captureRec implements only Recorder. Used to exercise the looped-Record
// fallback path in flushBatch.
type captureRec struct {
	mu      sync.Mutex
	records []event.Event
	calls   int
	err     error // returned from Record
}

func (c *captureRec) Record(_ context.Context, e *event.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.records = append(c.records, *e)
	return c.err
}

func (c *captureRec) snapshot() ([]event.Event, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]event.Event(nil), c.records...), c.calls
}

// captureBatchRec implements both Recorder and BatchRecorder. Used to
// exercise the BatchRecorder-preferred path.
type captureBatchRec struct {
	captureRec
	bmu        sync.Mutex
	batches    [][]event.Event
	batchCalls int
	batchErr   error
}

func (c *captureBatchRec) RecordBatch(_ context.Context, events []event.Event) error {
	c.bmu.Lock()
	defer c.bmu.Unlock()
	c.batchCalls++
	c.batches = append(c.batches, append([]event.Event(nil), events...))
	return c.batchErr
}

func (c *captureBatchRec) batchSnapshot() ([][]event.Event, int) {
	c.bmu.Lock()
	defer c.bmu.Unlock()
	out := make([][]event.Event, len(c.batches))
	for i, b := range c.batches {
		out[i] = append([]event.Event(nil), b...)
	}
	return out, c.batchCalls
}

// blockingRec blocks every Record call on a channel so tests can hold
// the inner recorder in flight to exercise back-pressure paths. actions
// records the Action of every event that actually reached Record, so
// tests can confirm whether a given event made it through, not just how
// many calls happened.
type blockingRec struct {
	release chan struct{}
	calls   int
	actions []string
	mu      sync.Mutex
}

func (b *blockingRec) Record(_ context.Context, e *event.Event) error {
	b.mu.Lock()
	b.calls++
	b.actions = append(b.actions, e.Action)
	b.mu.Unlock()
	<-b.release
	return nil
}

func (b *blockingRec) snapshot() ([]string, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.actions...), b.calls
}

// helper to build a BufferedRecorder with sensible test defaults: no
// auto-flush via interval, no diagnostic noise.
func newTestBuffered(t *testing.T, inner Recorder, extra ...BufferedOption) *BufferedRecorder {
	t.Helper()
	opts := append([]BufferedOption{
		WithFlushInterval(time.Hour),
		WithSlogLogger(silentLogger()),
	}, extra...)
	b := NewBufferedRecorder(inner, opts...)
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestBufferedRecorder_Record_PreparesEvent(t *testing.T) {
	t.Parallel()
	inner := &captureRec{}
	b := newTestBuffered(t, inner)

	e := &event.Event{Action: "user.login"}
	require.NoError(t, b.Record(context.Background(), e))
	require.NotEmpty(t, e.ID, "Record should populate ID before queueing")
	require.False(t, e.OccurredAt.IsZero(), "Record should populate OccurredAt before queueing")
}

func TestBufferedRecorder_Record_EmptyActionIsNoOp(t *testing.T) {
	t.Parallel()
	inner := &captureRec{}
	b := newTestBuffered(t, inner)

	require.NoError(t, b.Record(context.Background(), &event.Event{}))
	require.NoError(t, b.Record(context.Background(), nil))

	require.NoError(t, b.Flush(context.Background()))
	got, calls := inner.snapshot()
	require.Empty(t, got)
	require.Zero(t, calls)
}

func TestBufferedRecorder_Flush_DrainsPendingEvents(t *testing.T) {
	t.Parallel()
	inner := &captureRec{}
	b := newTestBuffered(t, inner)

	for _, action := range []string{"a.one", "a.two", "a.three"} {
		require.NoError(t, b.Record(context.Background(), event.New(action)))
	}
	require.NoError(t, b.Flush(context.Background()))

	got, _ := inner.snapshot()
	require.Len(t, got, 3)
	require.Equal(t, "a.one", got[0].Action)
	require.Equal(t, "a.two", got[1].Action)
	require.Equal(t, "a.three", got[2].Action)
}

func TestBufferedRecorder_Flush_PrefersBatchRecorder(t *testing.T) {
	t.Parallel()
	inner := &captureBatchRec{}
	b := newTestBuffered(t, inner)

	for _, action := range []string{"a.one", "a.two"} {
		require.NoError(t, b.Record(context.Background(), event.New(action)))
	}
	require.NoError(t, b.Flush(context.Background()))

	batches, batchCalls := inner.batchSnapshot()
	require.Equal(t, 1, batchCalls, "expected RecordBatch to be called once")
	require.Len(t, batches[0], 2)

	_, recordCalls := inner.snapshot()
	require.Zero(t, recordCalls, "RecordBatch should pre-empt Record fallback")
}

func TestBufferedRecorder_Flush_FallsBackToSerialRecord(t *testing.T) {
	t.Parallel()
	inner := &captureRec{}
	b := newTestBuffered(t, inner)

	for _, action := range []string{"a.one", "a.two", "a.three"} {
		require.NoError(t, b.Record(context.Background(), event.New(action)))
	}
	require.NoError(t, b.Flush(context.Background()))

	_, calls := inner.snapshot()
	require.Equal(t, 3, calls, "non-BatchRecorder inner should be called once per event")
}

func TestBufferedRecorder_Flush_PropagatesInnerError(t *testing.T) {
	t.Parallel()
	want := errors.New("inner failed")
	inner := &captureBatchRec{}
	inner.batchErr = want

	b := newTestBuffered(t, inner)
	require.NoError(t, b.Record(context.Background(), event.New("a.one")))

	err := b.Flush(context.Background())
	require.ErrorIs(t, err, want)
	require.Equal(t, int64(1), b.Stats().FlushErrs)
}

func TestBufferedRecorder_Flush_RespectsCtxCancel(t *testing.T) {
	t.Parallel()
	blocker := &blockingRec{release: make(chan struct{})}
	defer close(blocker.release) // unblock at test end so Close can drain

	b := newTestBuffered(t, blocker)
	require.NoError(t, b.Record(context.Background(), event.New("a.one")))

	// Force a flush so the inner is in flight, then issue a Flush whose
	// ctx is already canceled - it should bail out without waiting.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := b.Flush(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestBufferedRecorder_FlushOnSizeThreshold(t *testing.T) {
	t.Parallel()
	inner := &captureBatchRec{}
	b := newTestBuffered(t, inner, WithFlushSize(3))

	for _, a := range []string{"a.one", "a.two", "a.three"} {
		require.NoError(t, b.Record(context.Background(), event.New(a)))
	}

	require.Eventually(t, func() bool {
		_, calls := inner.batchSnapshot()
		return calls == 1
	}, time.Second, 5*time.Millisecond, "expected size-triggered flush")
}

func TestBufferedRecorder_FlushOnInterval(t *testing.T) {
	t.Parallel()
	inner := &captureBatchRec{}
	b := NewBufferedRecorder(inner,
		WithFlushInterval(20*time.Millisecond),
		WithSlogLogger(silentLogger()),
	)
	t.Cleanup(func() { _ = b.Close() })

	require.NoError(t, b.Record(context.Background(), event.New("a.one")))
	require.Eventually(t, func() bool {
		_, calls := inner.batchSnapshot()
		return calls >= 1
	}, time.Second, 5*time.Millisecond, "expected interval-triggered flush")
}

func TestBufferedRecorder_Close_DrainsPending(t *testing.T) {
	t.Parallel()
	inner := &captureBatchRec{}
	b := NewBufferedRecorder(inner,
		WithFlushInterval(time.Hour),
		WithSlogLogger(silentLogger()),
	)
	for _, a := range []string{"a.one", "a.two"} {
		require.NoError(t, b.Record(context.Background(), event.New(a)))
	}
	require.NoError(t, b.Close())

	batches, calls := inner.batchSnapshot()
	require.Equal(t, 1, calls)
	require.Len(t, batches[0], 2)
}

func TestBufferedRecorder_Close_Idempotent(t *testing.T) {
	t.Parallel()
	inner := &captureRec{}
	b := newTestBuffered(t, inner)
	require.NoError(t, b.Close())
	require.NoError(t, b.Close(), "second Close should be a no-op")
}

func TestBufferedRecorder_RecordAfterClose_IsDropped(t *testing.T) {
	t.Parallel()
	inner := &captureRec{}
	b := newTestBuffered(t, inner)
	require.NoError(t, b.Close())

	// Should not panic, should not deliver, should not error.
	require.NoError(t, b.Record(context.Background(), event.New("a.one")))
	got, _ := inner.snapshot()
	require.Empty(t, got)
}

func TestBufferedRecorder_FlushAfterClose_IsNoOp(t *testing.T) {
	t.Parallel()
	inner := &captureRec{}
	b := newTestBuffered(t, inner)
	require.NoError(t, b.Close())

	require.NoError(t, b.Flush(context.Background()))
}

func TestBufferedRecorder_OverflowPolicyDropNewest_CountsDrops(t *testing.T) {
	t.Parallel()
	blocker := &blockingRec{release: make(chan struct{})}
	defer close(blocker.release)

	b := NewBufferedRecorder(blocker,
		WithBufferSize(1),
		WithFlushSize(1),
		WithFlushInterval(time.Hour),
		WithOverflowPolicy(PolicyDropNewest),
		WithSlogLogger(silentLogger()),
	)
	t.Cleanup(func() { _ = b.Close() })

	// First Record fills buffer; background goroutine pulls it and blocks
	// inside inner.Record. Subsequent records find the buffer full.
	require.NoError(t, b.Record(context.Background(), event.New("a.one")))
	// Give the goroutine time to pick up the first event.
	require.Eventually(t, func() bool {
		blocker.mu.Lock()
		defer blocker.mu.Unlock()
		return blocker.calls == 1
	}, time.Second, 5*time.Millisecond)

	// Now fill the buffer again and try to push - these should drop.
	require.NoError(t, b.Record(context.Background(), event.New("a.two")))
	for i := 0; i < 5; i++ {
		require.NoError(t, b.Record(context.Background(), event.New("dropped")))
	}
	require.GreaterOrEqual(t, b.Stats().Dropped, int64(1))
}

func TestBufferedRecorder_OverflowPolicyError_ReturnsErrBufferFull(t *testing.T) {
	t.Parallel()
	blocker := &blockingRec{release: make(chan struct{})}
	defer close(blocker.release)

	b := NewBufferedRecorder(blocker,
		WithBufferSize(1),
		WithFlushSize(1),
		WithFlushInterval(time.Hour),
		WithOverflowPolicy(PolicyError),
		WithSlogLogger(silentLogger()),
	)
	t.Cleanup(func() { _ = b.Close() })

	require.NoError(t, b.Record(context.Background(), event.New("a.one")))
	require.Eventually(t, func() bool {
		blocker.mu.Lock()
		defer blocker.mu.Unlock()
		return blocker.calls == 1
	}, time.Second, 5*time.Millisecond)

	require.NoError(t, b.Record(context.Background(), event.New("a.two"))) // fills buffer
	err := b.Record(context.Background(), event.New("a.three"))            // overflow
	require.ErrorIs(t, err, ErrBufferFull)
}

func TestBufferedRecorder_OverflowPolicyBlock_RespectsCtxCancel(t *testing.T) {
	t.Parallel()
	blocker := &blockingRec{release: make(chan struct{})}
	defer close(blocker.release)

	b := NewBufferedRecorder(blocker,
		WithBufferSize(1),
		WithFlushSize(1),
		WithFlushInterval(time.Hour),
		WithOverflowPolicy(PolicyBlock),
		WithSlogLogger(silentLogger()),
	)
	t.Cleanup(func() { _ = b.Close() })

	require.NoError(t, b.Record(context.Background(), event.New("a.one")))
	require.Eventually(t, func() bool {
		blocker.mu.Lock()
		defer blocker.mu.Unlock()
		return blocker.calls == 1
	}, time.Second, 5*time.Millisecond)
	require.NoError(t, b.Record(context.Background(), event.New("a.two"))) // fills buffer

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := b.Record(ctx, event.New("a.three"))
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestBufferedRecorder_OverflowPolicyBlock_UnblocksWhenSpaceFrees(t *testing.T) {
	t.Parallel()
	blocker := &blockingRec{release: make(chan struct{})}

	b := NewBufferedRecorder(blocker,
		WithBufferSize(1),
		WithFlushSize(1),
		WithFlushInterval(time.Hour),
		WithOverflowPolicy(PolicyBlock),
		WithSlogLogger(silentLogger()),
	)
	t.Cleanup(func() { _ = b.Close() })

	// Pipeline: event 1 in flight at inner, event 2 fills buffer.
	require.NoError(t, b.Record(context.Background(), event.New("a.one")))
	require.Eventually(t, func() bool {
		blocker.mu.Lock()
		defer blocker.mu.Unlock()
		return blocker.calls == 1
	}, time.Second, 5*time.Millisecond)
	require.NoError(t, b.Record(context.Background(), event.New("a.two")))

	// Third Record blocks in PolicyBlock until space frees.
	done := make(chan error, 1)
	go func() {
		done <- b.Record(context.Background(), event.New("a.three"))
	}()

	// Releasing the blocker lets the inner finish, the run goroutine
	// drains the buffer, and the blocked Record can enqueue.
	close(blocker.release)

	select {
	case err := <-done:
		require.NoError(t, err, "PolicyBlock should return nil once space frees")
	case <-time.After(time.Second):
		t.Fatal("PolicyBlock Record should have unblocked once buffer freed")
	}
}

func TestBufferedRecorder_OverflowPolicyBlock_ReturnsNilOnClose(t *testing.T) {
	t.Parallel()
	blocker := &blockingRec{release: make(chan struct{})}
	defer close(blocker.release)

	b := NewBufferedRecorder(blocker,
		WithBufferSize(1),
		WithFlushSize(1),
		WithFlushInterval(time.Hour),
		WithOverflowPolicy(PolicyBlock),
		WithSlogLogger(silentLogger()),
		// Short drain timeout - the inner is held by the blocker, so the
		// run goroutine can't actually drain. We only care that the
		// blocked Record returns nil when stop fires.
		WithDrainTimeout(50*time.Millisecond),
	)

	require.NoError(t, b.Record(context.Background(), event.New("a.one")))
	require.Eventually(t, func() bool {
		blocker.mu.Lock()
		defer blocker.mu.Unlock()
		return blocker.calls == 1
	}, time.Second, 5*time.Millisecond)
	require.NoError(t, b.Record(context.Background(), event.New("a.two"))) // fills buffer

	done := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		done <- b.Record(context.Background(), event.New("a.three"))
	}()
	<-started
	// Give the goroutine time to enter the PolicyBlock select. The post-
	// stop early-out at the top of Record would otherwise short-circuit
	// to nil before reaching the case <-b.stop branch we want to cover.
	time.Sleep(20 * time.Millisecond)

	_ = b.Close() // returns DeadlineExceeded; we only care about Record below

	select {
	case err := <-done:
		require.NoError(t, err, "PolicyBlock should return nil when Close fires")
	case <-time.After(time.Second):
		t.Fatal("PolicyBlock Record should have returned nil when Close fired")
	}
}

func TestBufferedRecorder_DefaultOverflowPolicyIsDropNewest(t *testing.T) {
	t.Parallel()
	blocker := &blockingRec{release: make(chan struct{})}
	defer close(blocker.release)

	// No WithOverflowPolicy - should default to PolicyDropNewest.
	b := NewBufferedRecorder(blocker,
		WithBufferSize(1),
		WithFlushSize(1),
		WithFlushInterval(time.Hour),
		WithSlogLogger(silentLogger()),
	)
	t.Cleanup(func() { _ = b.Close() })

	require.NoError(t, b.Record(context.Background(), event.New("a.one")))
	require.Eventually(t, func() bool {
		blocker.mu.Lock()
		defer blocker.mu.Unlock()
		return blocker.calls == 1
	}, time.Second, 5*time.Millisecond)
	require.NoError(t, b.Record(context.Background(), event.New("a.two"))) // fills buffer

	// Default policy: overflow returns nil (not ErrBufferFull) and
	// increments dropped.
	require.NoError(t, b.Record(context.Background(), event.New("a.three")),
		"default policy must not return ErrBufferFull")
	require.Eventually(t, func() bool {
		return b.Stats().Dropped >= 1
	}, time.Second, 5*time.Millisecond)
}

func TestBufferedRecorder_Stats_TracksFlushed(t *testing.T) {
	t.Parallel()
	inner := &captureBatchRec{}
	b := newTestBuffered(t, inner)

	for _, a := range []string{"a.one", "a.two", "a.three"} {
		require.NoError(t, b.Record(context.Background(), event.New(a)))
	}
	require.NoError(t, b.Flush(context.Background()))

	require.Equal(t, int64(3), b.Stats().Flushed)
}

func TestBufferedRecorder_ImplementsRecorder(t *testing.T) {
	t.Parallel()
	var _ Recorder = (*BufferedRecorder)(nil)
}

// TestBufferedRecorder_DroppedEvent_IsNotMarkedRecorded is the falsification
// for I3: Record used to call event.PrepareEvent before the overflow-policy
// switch, so an event dropped under PolicyDropNewest (or PolicyError, or a
// canceled PolicyBlock) was already marked recorded on the request-scoped
// event. That marking makes end()'s CompareAndSwap a no-op, so the auto-record
// backstop never fires and the event is lost entirely, not just delayed.
//
// This fills a buffer-size-1 recorder so the request-scoped event is
// dropped, then calls end() and confirms the event still reaches the inner
// recorder once the buffer has room. That is only possible if the dropped
// Record call left the recorded flag untouched, since end() only records
// when its CompareAndSwap(false, true) succeeds.
func TestBufferedRecorder_DroppedEvent_IsNotMarkedRecorded(t *testing.T) {
	t.Parallel()
	blocker := &blockingRec{release: make(chan struct{})}
	b := NewBufferedRecorder(blocker,
		WithBufferSize(1),
		WithFlushSize(1),
		WithFlushInterval(time.Hour),
		WithOverflowPolicy(PolicyDropNewest),
		WithSlogLogger(silentLogger()),
	)
	t.Cleanup(func() { _ = b.Close() })

	ctx, end := event.Begin(context.Background(), &event.Event{}, nil, b, nil)
	event.Current(ctx).Action = "test.dropped"

	// Fill the buffer: the background goroutine picks up "a.one" and blocks
	// inside inner.Record, holding the single buffer slot occupied by
	// "a.two" once it is enqueued.
	require.NoError(t, b.Record(context.Background(), event.New("a.one")))
	require.Eventually(t, func() bool {
		_, calls := blocker.snapshot()
		return calls == 1
	}, time.Second, 5*time.Millisecond)
	require.NoError(t, b.Record(context.Background(), event.New("a.two")))

	// The request-scoped event finds the buffer full and is dropped under
	// the default PolicyDropNewest.
	require.NoError(t, b.Record(ctx, event.Current(ctx)))
	require.Equal(t, int64(1), b.Stats().Dropped, "the request-scoped event must be counted as dropped")

	// Release the blocked inner call so the buffer drains.
	close(blocker.release)
	require.Eventually(t, func() bool {
		_, calls := blocker.snapshot()
		return calls >= 2
	}, time.Second, 5*time.Millisecond)

	// end() must still record the dropped event now that there is room: if
	// Record had already marked it recorded before the drop, this
	// CompareAndSwap would no-op and "test.dropped" would never reach the
	// inner recorder.
	end()
	require.Eventually(t, func() bool {
		actions, _ := blocker.snapshot()
		for _, a := range actions {
			if a == "test.dropped" {
				return true
			}
		}
		return false
	}, time.Second, 5*time.Millisecond, "the dropped event must still be recorded by end() once the buffer has room")
}
