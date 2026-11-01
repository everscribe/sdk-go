package recorder

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
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

func TestBufferedRecorder_Close_ConcurrentWaitsForFirst(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		inner   Recorder
		wantErr error
	}{
		{name: "drain completes", inner: &slowRec{delay: 100 * time.Millisecond}},
		{name: "drain times out", inner: &ctxRec{}, wantErr: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				drops := &dropLog{}
				b := newDeliverRec(t, tc.inner, drops,
					WithFlushSize(2),
					WithDrainTimeout(time.Second))
				ids := recordN(t, b, 6)

				first := make(chan error, 1)
				go func() { first <- b.Close() }()
				synctest.Wait() // the first Close is draining

				second := b.Close()
				select {
				case <-b.done:
				default:
					t.Error("second Close returned while the drain was running")
				}
				require.ErrorIs(t, <-first, tc.wantErr)
				if tc.wantErr == nil {
					require.NoError(t, second)
				} else {
					require.ErrorIs(t, second, tc.wantErr)
				}

				got := drops.droppedIDs(DropShutdown)
				if s, ok := tc.inner.(*slowRec); ok {
					delivered, _ := s.snapshot()
					got = append(got, delivered...)
				}
				require.ElementsMatch(t, ids, got, "every event delivered or reported")
			})
		})
	}
}

func TestBufferedRecorder_Close_FromOnDropDuringClose(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		inner func(gate chan struct{}) Recorder
	}{
		// The worker reports the cancelled batch and its onDrop waits on the first Close.
		{name: "worker", inner: func(chan struct{}) Recorder { return &ctxRec{} }},
		// The first Close reports the queue itself and its onDrop calls Close.
		{name: "close fallback", inner: func(gate chan struct{}) Recorder {
			return &scriptedRec{fn: func(int, []string) error { <-gate; return nil }}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				gate := make(chan struct{})
				defer close(gate)
				var b *BufferedRecorder
				nested := make(chan error, 8)
				b = newTestBuffered(t, tc.inner(gate),
					WithFlushSize(1),
					WithDrainTimeout(100*time.Millisecond),
					WithOnDrop(func(_ []event.Event, r DropReason, _ error) {
						if r == DropShutdown {
							nested <- b.Close()
						}
					}))
				recordN(t, b, 1)
				synctest.Wait() // the worker is stuck in the first send
				recordN(t, b, 2)

				start := time.Now()
				require.ErrorIs(t, b.Close(), context.DeadlineExceeded)
				require.LessOrEqual(t, time.Since(start), 1100*time.Millisecond)
				require.ErrorIs(t, <-nested, context.DeadlineExceeded)
			})
		})
	}
}

func TestBufferedRecorder_FlushSizeZero_UsesDefault(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		end  func(*BufferedRecorder) error
	}{
		{name: "close", end: func(b *BufferedRecorder) error { return b.Close() }},
		{name: "flush", end: func(b *BufferedRecorder) error { return b.Flush(context.Background()) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				inner := &slowRec{delay: time.Millisecond}
				drops := &dropLog{}
				b := newDeliverRec(t, inner, drops, WithFlushSize(0))
				ids := recordN(t, b, 50)

				require.NoError(t, tc.end(b))
				delivered, _ := inner.snapshot()
				require.ElementsMatch(t, ids, delivered)
				dc, _ := drops.snapshot()
				require.Empty(t, dc)
			})
		})
	}
}

func TestBufferedRecorder_RecordAfterClose_IsDropped(t *testing.T) {
	t.Parallel()
	inner := &captureRec{}
	drops := &dropLog{}
	b := newTestBuffered(t, inner, drops.opt())
	require.NoError(t, b.Close())

	// Should not panic, should not deliver, should not error.
	e := event.New("a.one")
	require.NoError(t, b.Record(t.Context(), e))
	got, _ := inner.snapshot()
	require.Empty(t, got)
	require.NotEmpty(t, e.ID)
	require.Equal(t, []string{e.ID}, drops.droppedIDs(DropClosed))
	require.EqualValues(t, 1, b.Stats().DroppedByReason[DropClosed])
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
	drops := &dropLog{}

	b := NewBufferedRecorder(blocker,
		drops.opt(),
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
	blocked := event.New("a.three")
	go func() {
		close(started)
		done <- b.Record(t.Context(), blocked)
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
	require.Equal(t, []string{blocked.ID}, drops.droppedIDs(DropClosed))
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
	require.Zero(t, b.Stats().Dropped, "a drop end() will retry is not counted yet")

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

// ctxRec blocks every send until its ctx ends and notes the ctx error.
type ctxRec struct {
	mu   sync.Mutex
	errs []error
}

func (c *ctxRec) Record(ctx context.Context, e *event.Event) error {
	return c.RecordBatch(ctx, []event.Event{*e})
}

func (c *ctxRec) RecordBatch(ctx context.Context, _ []event.Event) error {
	<-ctx.Done()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, ctx.Err())
	return ctx.Err()
}

func (c *ctxRec) snapshot() []error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.errs)
}

func TestBufferedRecorder_Close_HungRequest(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inner := &ctxRec{}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops, WithDrainTimeout(200*time.Millisecond))

		ids := recordN(t, b, 3)
		start := time.Now()
		require.ErrorIs(t, b.Close(), context.DeadlineExceeded)
		require.Less(t, time.Since(start), time.Second)

		require.Equal(t, []error{context.Canceled}, inner.snapshot(), "Close cancels the in-flight send")
		require.Equal(t, ids, drops.droppedIDs(DropShutdown))
		calls, _ := drops.snapshot()
		for _, c := range calls {
			require.NoError(t, c.err, "shutdown drops carry a nil err")
		}
		select {
		case <-b.done:
		default:
			t.Fatal("worker still running after Close returned")
		}
	})
}

func TestBufferedRecorder_Close_InnerIgnoresCtx(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		result       error
		wantInFlight bool // in-flight batch reported shutdown
	}{
		{name: "in-flight send succeeds", result: nil},
		{name: "in-flight send fails", result: err503, wantInFlight: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				gate := make(chan struct{})
				inner := &scriptedRec{fn: func(n int, _ []string) error {
					if n == 0 {
						<-gate // ignores ctx
						return tc.result
					}
					return nil
				}}
				drops := &dropLog{}
				drain := 200 * time.Millisecond
				b := newDeliverRec(t, inner, drops,
					WithFlushSize(2),
					WithDrainTimeout(drain))

				inFlight := recordN(t, b, 2)
				synctest.Wait() // the worker is stuck in the first send
				queued := recordN(t, b, 3)

				start := time.Now()
				require.ErrorIs(t, b.Close(), context.DeadlineExceeded)
				require.LessOrEqual(t, time.Since(start), drain+1500*time.Millisecond)
				require.Equal(t, queued, drops.droppedIDs(DropShutdown), "Close reports what is still queued")

				close(gate)
				<-b.done
				want := queued
				if tc.wantInFlight {
					want = append(slices.Clone(queued), inFlight...)
				}
				require.Equal(t, want, drops.droppedIDs(DropShutdown))
				_, _, delivered := inner.snapshot()
				if tc.wantInFlight {
					require.Empty(t, delivered)
				} else {
					require.Equal(t, inFlight, delivered)
				}
			})
		})
	}
}

func TestBufferedRecorder_RecordCloseRace(t *testing.T) {
	t.Parallel()
	inner := &captureBatchRec{}
	drops := &dropLog{}
	b := newTestBuffered(t, inner, drops.opt(), WithBufferSize(64), WithFlushSize(16))

	const workers, perWorker = 50, 100
	var recorded atomic.Int64
	ids := make([][]string, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range perWorker {
				e := event.New("a.race")
				if err := b.Record(context.Background(), e); err != nil {
					t.Errorf("Record: %v", err)
				}
				ids[w] = append(ids[w], e.ID)
				recorded.Add(1)
			}
		}()
	}
	closeErr := make(chan error, 1)
	go func() {
		<-start
		for recorded.Load() < workers*perWorker/2 {
			runtime.Gosched()
		}
		closeErr <- b.Close()
	}()
	close(start)
	wg.Wait()
	require.NoError(t, <-closeErr)

	seen := map[string]int{}
	batches, _ := inner.batchSnapshot()
	for _, batch := range batches {
		for _, id := range eventIDs(batch) {
			seen[id]++
		}
	}
	calls, _ := drops.snapshot()
	for _, c := range calls {
		for _, id := range eventIDs(c.events) {
			seen[id]++
		}
	}
	var all []string
	for _, w := range ids {
		all = append(all, w...)
	}
	require.Len(t, all, workers*perWorker)
	for _, id := range all {
		require.NotEmpty(t, id)
		require.Equal(t, 1, seen[id], "event %s delivered or reported exactly once", id)
	}
	require.Len(t, seen, len(all), "nothing delivered or reported that was not recorded")
}

func TestBufferedRecorder_BlockWaiterReleasedByClose(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		drops := &dropLog{}
		b := newDeliverRec(t, &ctxRec{}, drops,
			WithBufferSize(1),
			WithFlushSize(1),
			WithOverflowPolicy(PolicyBlock),
			WithDrainTimeout(100*time.Millisecond))

		held := recordN(t, b, 1)
		synctest.Wait() // the worker holds the first event
		queued := recordN(t, b, 1)

		e := event.New("a.blocked")
		done := make(chan error, 1)
		go func() { done <- b.Record(t.Context(), e) }()
		synctest.Wait() // durably blocked on the full buffer

		require.ErrorIs(t, b.Close(), context.DeadlineExceeded)
		require.NoError(t, <-done)
		require.Equal(t, []string{e.ID}, drops.droppedIDs(DropClosed))
		require.Equal(t, append(held, queued...), drops.droppedIDs(DropShutdown))
	})
}

func TestBufferedRecorder_CallbackCallsClose_NoDeadlock(t *testing.T) {
	t.Parallel()
	var b *BufferedRecorder
	closed := make(chan error, 1)
	b = newTestBuffered(t, &ctxRec{},
		WithBufferSize(1),
		WithFlushSize(1),
		WithDrainTimeout(100*time.Millisecond),
		WithOnDrop(func(_ []event.Event, r DropReason, _ error) {
			if r == DropBufferFull {
				closed <- b.Close()
			}
		}))

	require.NoError(t, b.Record(t.Context(), event.New("a.one")))
	require.Eventually(t, func() bool { return len(b.events) == 0 }, time.Second, time.Millisecond)
	require.NoError(t, b.Record(t.Context(), event.New("a.two"))) // fills the buffer

	done := make(chan error, 1)
	go func() { done <- b.Record(t.Context(), event.New("a.three")) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Record deadlocked: onDrop called Close while Record held the lock")
	}
	require.ErrorIs(t, <-closed, context.DeadlineExceeded)
}

func TestBufferedRecorder_Close_ReleasesBlockedFlush(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := newDeliverRec(t, &ctxRec{}, &dropLog{}, WithDrainTimeout(100*time.Millisecond))
		recordN(t, b, 2)

		done := make(chan error, 1)
		go func() { done <- b.Flush(t.Context()) }()
		synctest.Wait() // Flush waits on the hung send

		require.ErrorIs(t, b.Close(), context.DeadlineExceeded)
		require.NoError(t, <-done)
	})
}

// fillBuffer leaves a bufferSize-1, flushSize-1 recorder full: the worker
// holds one event and a second waits in the buffer. Call inside synctest.
func fillBuffer(t *testing.T, b *BufferedRecorder) {
	t.Helper()
	recordN(t, b, 1)
	synctest.Wait()
	recordN(t, b, 1)
}

// errLog notes every Error message.
type errLog struct {
	mu   sync.Mutex
	msgs []string
}

func (l *errLog) Error(msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.msgs = append(l.msgs, msg)
}

func (l *errLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.msgs)
}

func TestBufferedRecorder_Backstop_DropThenDeliver(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inner := &blockingRec{release: make(chan struct{})}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops, WithBufferSize(1), WithFlushSize(1))
		fillBuffer(t, b)

		ctx, end := event.Begin(t.Context(), &event.Event{}, nil, b, nil)
		cur := event.Current(ctx)
		cur.Action = "a.scoped"
		require.NoError(t, b.Record(ctx, cur)) // dropped, left for end()
		require.Equal(t, event.AutoRecordPending, event.AutoRecord(ctx, cur), "the drop left it unmarked")

		close(inner.release)
		synctest.Wait() // the buffer drains
		end()
		require.NoError(t, b.Flush(t.Context()))

		actions, _ := inner.snapshot()
		require.Equal(t, 1, countOf(actions, "a.scoped"), "delivered once")
		calls, _ := drops.snapshot()
		require.Empty(t, calls)
		st := b.Stats()
		require.Zero(t, st.Dropped)
		require.Zero(t, st.DroppedByReason[DropBufferFull])
	})
}

func countOf(ss []string, want string) int {
	n := 0
	for _, s := range ss {
		if s == want {
			n++
		}
	}
	return n
}

func TestBufferedRecorder_Backstop_DropTwice(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		drops := &dropLog{}
		b := newDeliverRec(t, &ctxRec{}, drops, WithBufferSize(1), WithFlushSize(1))
		fillBuffer(t, b)

		ctx, end := event.Begin(t.Context(), &event.Event{}, nil, b, nil)
		cur := event.Current(ctx)
		cur.Action = "a.scoped"
		require.NoError(t, b.Record(ctx, cur))
		require.Empty(t, drops.droppedIDs(DropBufferFull), "end() will retry, so no report yet")

		end()
		require.Equal(t, []string{cur.ID}, drops.droppedIDs(DropBufferFull))
		calls, _ := drops.snapshot()
		require.Len(t, calls, 1)
		require.NoError(t, calls[0].err)
		st := b.Stats()
		require.EqualValues(t, 1, st.Dropped)
		require.Equal(t, st.Dropped, st.DroppedByReason[DropBufferFull])
	})
}

func TestBufferedRecorder_Backstop_ClosedOnce(t *testing.T) {
	t.Parallel()
	drops := &dropLog{}
	b := newDeliverRec(t, &captureRec{}, drops)
	require.NoError(t, b.Close())

	ctx, end := event.Begin(t.Context(), &event.Event{}, nil, b, nil)
	cur := event.Current(ctx)
	cur.Action = "a.scoped"
	require.NoError(t, b.Record(ctx, cur))
	require.Empty(t, drops.droppedIDs(DropClosed), "end() will retry, so no report yet")
	require.Zero(t, b.Stats().DroppedByReason[DropClosed])

	end()
	require.Equal(t, []string{cur.ID}, drops.droppedIDs(DropClosed))
	calls, _ := drops.snapshot()
	require.Len(t, calls, 1)
	require.EqualValues(t, 1, b.Stats().DroppedByReason[DropClosed])
}

func TestBufferedRecorder_AutoRecord_ErrorPolicy(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		drops := &dropLog{}
		b := newDeliverRec(t, &ctxRec{}, drops,
			WithBufferSize(1), WithFlushSize(1), WithOverflowPolicy(PolicyError))
		fillBuffer(t, b)

		log := &errLog{}
		ctx, end := event.Begin(t.Context(), &event.Event{}, nil, b, log)
		cur := event.Current(ctx)
		cur.Action = "a.scoped"
		end()

		require.Equal(t, []string{cur.ID}, drops.droppedIDs(DropBufferFull))
		calls, _ := drops.snapshot()
		require.Len(t, calls, 1)
		require.ErrorIs(t, calls[0].err, ErrBufferFull)
		require.Equal(t, []string{"everscribe: auto-record failed"}, log.snapshot())
		st := b.Stats()
		require.EqualValues(t, 1, st.Dropped)
		require.Equal(t, st.Dropped, st.DroppedByReason[DropBufferFull])
	})
}

func TestBufferedRecorder_AutoRecord_BlockTimeout(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		drops := &dropLog{}
		b := newDeliverRec(t, &ctxRec{}, drops,
			WithBufferSize(1), WithFlushSize(1), WithOverflowPolicy(PolicyBlock))
		fillBuffer(t, b)

		log := &errLog{}
		ctx, end := event.Begin(t.Context(), &event.Event{}, nil, b, log)
		cur := event.Current(ctx)
		cur.Action = "a.scoped"
		end() // waits out the record timeout

		require.Equal(t, []string{cur.ID}, drops.droppedIDs(DropBufferFull))
		calls, _ := drops.snapshot()
		require.Len(t, calls, 1)
		require.ErrorIs(t, calls[0].err, context.DeadlineExceeded)
		require.Equal(t, []string{"everscribe: auto-record failed"}, log.snapshot())
		require.EqualValues(t, 1, b.Stats().Dropped)
	})
}

func TestBufferedRecorder_Direct_ErrorPolicyNoReport(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		drops := &dropLog{}
		b := newDeliverRec(t, &ctxRec{}, drops,
			WithBufferSize(1), WithFlushSize(1), WithOverflowPolicy(PolicyError))
		fillBuffer(t, b)

		require.ErrorIs(t, b.Record(t.Context(), event.New("a.direct")), ErrBufferFull)
		calls, _ := drops.snapshot()
		require.Empty(t, calls)
		require.Zero(t, b.Stats().Dropped)
	})
}

func TestBufferedRecorder_Direct_BlockCtxNoReport(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		drops := &dropLog{}
		b := newDeliverRec(t, &ctxRec{}, drops,
			WithBufferSize(1), WithFlushSize(1), WithOverflowPolicy(PolicyBlock))
		fillBuffer(t, b)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, b.Record(ctx, event.New("a.direct")), context.Canceled)
		calls, _ := drops.snapshot()
		require.Empty(t, calls)
		require.Zero(t, b.Stats().Dropped)
	})
}
