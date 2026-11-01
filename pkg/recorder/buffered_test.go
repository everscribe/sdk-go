package recorder

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"testing/synctest"
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
// fallback path in deliver.
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
	require.NoError(t, b.Record(t.Context(), e))
	require.NotEmpty(t, e.ID, "Record should populate ID before queueing")
	require.False(t, e.OccurredAt.IsZero(), "Record should populate OccurredAt before queueing")
}

func TestBufferedRecorder_Record_EmptyActionIsNoOp(t *testing.T) {
	t.Parallel()
	inner := &captureRec{}
	b := newTestBuffered(t, inner)

	require.NoError(t, b.Record(t.Context(), &event.Event{}))
	require.NoError(t, b.Record(t.Context(), nil))

	require.NoError(t, b.Flush(t.Context()))
	got, calls := inner.snapshot()
	require.Empty(t, got)
	require.Zero(t, calls)
}

func TestBufferedRecorder_Flush_DrainsPendingEvents(t *testing.T) {
	t.Parallel()
	inner := &captureRec{}
	b := newTestBuffered(t, inner)

	for _, action := range []string{"a.one", "a.two", "a.three"} {
		require.NoError(t, b.Record(t.Context(), event.New(action)))
	}
	require.NoError(t, b.Flush(t.Context()))

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
		require.NoError(t, b.Record(t.Context(), event.New(action)))
	}
	require.NoError(t, b.Flush(t.Context()))

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
		require.NoError(t, b.Record(t.Context(), event.New(action)))
	}
	require.NoError(t, b.Flush(t.Context()))

	_, calls := inner.snapshot()
	require.Equal(t, 3, calls, "non-BatchRecorder inner should be called once per event")
}

func TestBufferedRecorder_Flush_RejectedDoesNotFail(t *testing.T) {
	t.Parallel()
	want := errors.New("inner failed")
	inner := &captureBatchRec{}
	inner.batchErr = want
	drops := &dropLog{}

	b := newTestBuffered(t, inner, drops.opt())
	e := event.New("a.one")
	require.NoError(t, b.Record(t.Context(), e))

	require.NoError(t, b.Flush(t.Context()), "a drop must not fail Flush")
	require.Equal(t, int64(1), b.Stats().FlushErrs)
	require.Equal(t, []string{e.ID}, drops.droppedIDs(DropRejected))
	calls, _ := drops.snapshot()
	require.Len(t, calls, 1)
	require.ErrorIs(t, calls[0].err, want)
}

func TestBufferedRecorder_Flush_RespectsCtxCancel(t *testing.T) {
	t.Parallel()
	blocker := &blockingRec{release: make(chan struct{})}
	defer close(blocker.release) // unblock at test end so Close can drain

	b := newTestBuffered(t, blocker)
	require.NoError(t, b.Record(t.Context(), event.New("a.one")))

	// Force a flush so the inner is in flight, then issue a Flush whose
	// ctx is already canceled - it should bail out without waiting.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := b.Flush(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestBufferedRecorder_FlushOnSizeThreshold(t *testing.T) {
	t.Parallel()
	inner := &captureBatchRec{}
	b := newTestBuffered(t, inner, WithFlushSize(3))

	for _, a := range []string{"a.one", "a.two", "a.three"} {
		require.NoError(t, b.Record(t.Context(), event.New(a)))
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

	require.NoError(t, b.Record(t.Context(), event.New("a.one")))
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
		require.NoError(t, b.Record(t.Context(), event.New(a)))
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
	require.NoError(t, b.Record(t.Context(), event.New("a.one")))
	got, _ := inner.snapshot()
	require.Empty(t, got)
}

func TestBufferedRecorder_FlushAfterClose_IsNoOp(t *testing.T) {
	t.Parallel()
	inner := &captureRec{}
	b := newTestBuffered(t, inner)
	require.NoError(t, b.Close())

	require.NoError(t, b.Flush(t.Context()))
}

func TestBufferedRecorder_OverflowPolicy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		opts           []BufferedOption // nil exercises the default policy
		overflowCtxTTL time.Duration    // 0 means t.Context()
		extraOverflows int
		wantErr        error
		wantDropped    bool
	}{
		{
			name:           "drop newest counts drops",
			opts:           []BufferedOption{WithOverflowPolicy(PolicyDropNewest)},
			extraOverflows: 5, // repeated overflow must keep returning nil
			wantDropped:    true,
		},
		{
			name:    "error returns ErrBufferFull",
			opts:    []BufferedOption{WithOverflowPolicy(PolicyError)},
			wantErr: ErrBufferFull,
		},
		{
			name:           "block respects ctx cancel",
			opts:           []BufferedOption{WithOverflowPolicy(PolicyBlock)},
			overflowCtxTTL: 50 * time.Millisecond,
			wantErr:        context.DeadlineExceeded,
		},
		{
			// No WithOverflowPolicy: overflow must return nil (not
			// ErrBufferFull) and increment dropped.
			name:        "default policy is drop newest",
			wantDropped: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			blocker := &blockingRec{release: make(chan struct{})}
			defer close(blocker.release)

			opts := append([]BufferedOption{
				WithBufferSize(1),
				WithFlushSize(1),
				WithFlushInterval(time.Hour),
				WithSlogLogger(silentLogger()),
			}, tc.opts...)
			b := NewBufferedRecorder(blocker, opts...)
			t.Cleanup(func() { _ = b.Close() })

			// The first Record fills the buffer; the background goroutine
			// pulls it and blocks inside inner.Record, so the next one
			// occupies the single slot and everything after it overflows.
			require.NoError(t, b.Record(t.Context(), event.New("a.one")))
			require.Eventually(t, func() bool {
				_, calls := blocker.snapshot()
				return calls == 1
			}, time.Second, 5*time.Millisecond)
			require.NoError(t, b.Record(t.Context(), event.New("a.two"))) // fills buffer

			ctx := t.Context()
			if tc.overflowCtxTTL > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.overflowCtxTTL)
				defer cancel()
			}

			err := b.Record(ctx, event.New("a.three")) // overflow
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err, "this policy must not return ErrBufferFull")
			}
			for i := 0; i < tc.extraOverflows; i++ {
				require.NoError(t, b.Record(t.Context(), event.New("dropped")))
			}

			if tc.wantDropped {
				require.Eventually(t, func() bool {
					return b.Stats().Dropped >= 1
				}, time.Second, 5*time.Millisecond)
				return
			}
			require.Zero(t, b.Stats().Dropped, "only the drop-newest policy counts drops")
		})
	}
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
	require.NoError(t, b.Record(t.Context(), event.New("a.one")))
	require.Eventually(t, func() bool {
		blocker.mu.Lock()
		defer blocker.mu.Unlock()
		return blocker.calls == 1
	}, time.Second, 5*time.Millisecond)
	require.NoError(t, b.Record(t.Context(), event.New("a.two")))

	// Third Record blocks in PolicyBlock until space frees.
	done := make(chan error, 1)
	go func() {
		done <- b.Record(t.Context(), event.New("a.three"))
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

	require.NoError(t, b.Record(t.Context(), event.New("a.one")))
	require.Eventually(t, func() bool {
		blocker.mu.Lock()
		defer blocker.mu.Unlock()
		return blocker.calls == 1
	}, time.Second, 5*time.Millisecond)
	require.NoError(t, b.Record(t.Context(), event.New("a.two"))) // fills buffer

	done := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		done <- b.Record(t.Context(), event.New("a.three"))
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

func TestBufferedRecorder_Stats_TracksFlushed(t *testing.T) {
	t.Parallel()
	inner := &captureBatchRec{}
	b := newTestBuffered(t, inner)

	for _, a := range []string{"a.one", "a.two", "a.three"} {
		require.NoError(t, b.Record(t.Context(), event.New(a)))
	}
	require.NoError(t, b.Flush(t.Context()))

	require.Equal(t, int64(3), b.Stats().Flushed)
}

func TestBufferedRecorder_ImplementsRecorder(t *testing.T) {
	t.Parallel()
	var _ Recorder = (*BufferedRecorder)(nil)
}

// TestBufferedRecorder_DroppedEvent_IsNotMarkedRecorded guards against a
// regression (I3): Record used to mark the event recorded before the
// overflow-policy switch, so a dropped event's CompareAndSwap became a
// no-op and end()'s auto-record backstop never fired, losing it for good.
//
// It fills a buffer-size-1 recorder so the event is dropped, then calls
// end() and confirms it still reaches the inner recorder once the buffer
// has room - only possible if the drop left the recorded flag untouched.
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

	ctx, end := event.Begin(t.Context(), &event.Event{}, nil, b, nil)
	event.Current(ctx).Action = "test.dropped"

	// Fill the buffer: the background goroutine picks up "a.one" and blocks
	// inside inner.Record, holding the single buffer slot occupied by
	// "a.two" once it is enqueued.
	require.NoError(t, b.Record(t.Context(), event.New("a.one")))
	require.Eventually(t, func() bool {
		_, calls := blocker.snapshot()
		return calls == 1
	}, time.Second, 5*time.Millisecond)
	require.NoError(t, b.Record(t.Context(), event.New("a.two")))

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

// recordN records n events and returns their IDs in record order.
func recordN(t *testing.T, b *BufferedRecorder, n int) []string {
	t.Helper()
	ids := make([]string, n)
	for i := range ids {
		e := event.New("a.test")
		require.NoError(t, b.Record(t.Context(), e))
		ids[i] = e.ID
	}
	return ids
}

// slowRec takes delay per call and notes whether its ctx was cancelled.
type slowRec struct {
	delay     time.Duration
	mu        sync.Mutex
	delivered []string
	cancelled []bool
}

func (s *slowRec) Record(ctx context.Context, e *event.Event) error {
	return s.RecordBatch(ctx, []event.Event{*e})
}

func (s *slowRec) RecordBatch(ctx context.Context, events []event.Event) error {
	time.Sleep(s.delay)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancelled = append(s.cancelled, ctx.Err() != nil)
	s.delivered = append(s.delivered, eventIDs(events)...)
	return nil
}

func (s *slowRec) snapshot() (delivered []string, cancelled []bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.delivered...), append([]bool(nil), s.cancelled...)
}

func TestBufferedRecorder_OrderingDuringRetry(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inner := &scriptedRec{fn: seq(err503, err503)}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops, WithFlushSize(3))

		first := recordN(t, b, 3)
		synctest.Wait() // the first batch failed and is parked in backoff
		second := recordN(t, b, 3)
		require.NoError(t, b.Flush(t.Context()))

		calls, _, delivered := inner.snapshot()
		require.Equal(t, append(first, second...), delivered)
		require.Equal(t, [][]string{first, first, first, second}, calls)
		dc, _ := drops.snapshot()
		require.Empty(t, dc)
	})
}

func TestBufferedRecorder_Flush_SendsBatchesOfFlushSize(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		inner := &scriptedRec{fn: func(n int, _ []string) error {
			if n == 0 {
				<-gate
			}
			return nil
		}}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops, WithFlushSize(100))

		// The first 100 go out on the size threshold and hold the worker,
		// so the other 150 are still queued when Flush is called.
		ids := recordN(t, b, 100)
		synctest.Wait()
		ids = append(ids, recordN(t, b, 150)...)

		done := make(chan error, 1)
		go func() { done <- b.Flush(t.Context()) }()
		synctest.Wait()
		close(gate)
		require.NoError(t, <-done)

		calls, _, delivered := inner.snapshot()
		var sizes []int
		for _, c := range calls {
			sizes = append(sizes, len(c))
		}
		require.Equal(t, []int{100, 100, 50}, sizes)
		require.Equal(t, ids, delivered, "batches go out in record order")
	})
}

func TestBufferedRecorder_Flush_ShortCtxDoesNotCancelSend(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inner := &slowRec{delay: 200 * time.Millisecond}
		b := newTestBuffered(t, inner)
		ids := recordN(t, b, 1)

		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, b.Flush(ctx), context.DeadlineExceeded)

		require.NoError(t, b.Flush(t.Context()))
		delivered, cancelled := inner.snapshot()
		require.Equal(t, ids, delivered)
		require.Equal(t, []bool{false}, cancelled)
	})
}

func TestBufferedRecorder_Flush_RepeatedShortFlushesDeliverAll(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inner := &slowRec{delay: 50 * time.Millisecond}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops)

		var ids []string
		for range 10 {
			ids = append(ids, recordN(t, b, 1)...)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
			_ = b.Flush(ctx)
			cancel()
		}

		require.NoError(t, b.Flush(t.Context()))
		delivered, cancelled := inner.snapshot()
		require.Equal(t, ids, delivered)
		require.NotContains(t, cancelled, true)
		dc, _ := drops.snapshot()
		require.Empty(t, dc)
	})
}

func TestBufferedRecorder_BufferFullDuringOutage(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inner := &scriptedRec{fn: always(err503)}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops,
			WithBufferSize(5),
			WithFlushSize(1),
			WithMaxRetryDuration(time.Second))

		ids := recordN(t, b, 1)
		synctest.Wait() // the worker holds the first event in its retry loop
		ids = append(ids, recordN(t, b, 19)...)

		// ids[1:6] fill the buffer; the remaining 14 overflow.
		require.Equal(t, ids[6:], drops.droppedIDs(DropBufferFull))
		require.EqualValues(t, 14, b.Stats().Dropped)
	})
}

func TestBufferedRecorder_Flush_OverlappingFlushesEachCountTheirOwn(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		// Each send blocks until the test releases it.
		gates := make([]chan struct{}, 4)
		for i := range gates {
			gates[i] = make(chan struct{})
		}
		inner := &scriptedRec{fn: func(n int, _ []string) error {
			<-gates[n]
			return nil
		}}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops, WithFlushSize(2))

		flush := func() chan error {
			done := make(chan error, 1)
			go func() { done <- b.Flush(t.Context()) }()
			return done
		}
		returned := func(done chan error) bool {
			select {
			case err := <-done:
				require.NoError(t, err)
				return true
			default:
				return false
			}
		}

		ids := recordN(t, b, 2) // call 0 holds the worker
		synctest.Wait()
		ids = append(ids, recordN(t, b, 4)...)
		a := flush() // counts ids[2:6]
		synctest.Wait()

		close(gates[0]) // A is accepted; call 1 sends ids[2:4]
		synctest.Wait()
		ids = append(ids, recordN(t, b, 2)...)
		bf := flush() // accepted while A is pending; counts ids[4:8]
		synctest.Wait()

		close(gates[1]) // call 2 sends ids[4:6]
		synctest.Wait()
		require.False(t, returned(a), "A still has ids[4:6] in flight")
		require.False(t, returned(bf))

		close(gates[2]) // A's events are resolved; call 3 sends ids[6:8]
		synctest.Wait()
		require.True(t, returned(a), "A must not wait for events recorded after it")
		require.False(t, returned(bf), "B still has ids[6:8] in flight")

		close(gates[3])
		synctest.Wait()
		require.True(t, returned(bf))

		calls, _, delivered := inner.snapshot()
		require.Equal(t, [][]string{ids[0:2], ids[2:4], ids[4:6], ids[6:8]}, calls)
		require.Equal(t, ids, delivered)
		dc, _ := drops.snapshot()
		require.Empty(t, dc)
	})
}
