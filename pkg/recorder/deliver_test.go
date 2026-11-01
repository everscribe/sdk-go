package recorder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/everscribe/sdk-go/pkg/event"
)

var (
	err400 = &HTTPError{StatusCode: http.StatusBadRequest}
	err401 = &HTTPError{StatusCode: http.StatusUnauthorized}
	err413 = &HTTPError{StatusCode: http.StatusRequestEntityTooLarge}
	err500 = &HTTPError{StatusCode: http.StatusInternalServerError}
	err503 = &HTTPError{StatusCode: http.StatusServiceUnavailable}
)

// scriptedRec is a BatchRecorder whose result per call comes from fn.
type scriptedRec struct {
	mu        sync.Mutex
	fn        func(n int, ids []string) error
	calls     [][]string
	times     []time.Time
	delivered []string
}

func (s *scriptedRec) Record(ctx context.Context, e *event.Event) error {
	return s.RecordBatch(ctx, []event.Event{*e})
}

func (s *scriptedRec) RecordBatch(_ context.Context, events []event.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := eventIDs(events)
	err := s.fn(len(s.calls), ids)
	s.calls = append(s.calls, ids)
	s.times = append(s.times, time.Now())
	if err == nil {
		s.delivered = append(s.delivered, ids...)
	}
	return err
}

func (s *scriptedRec) snapshot() (calls [][]string, times []time.Time, delivered []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.calls), slices.Clone(s.times), slices.Clone(s.delivered)
}

// serialRec implements only Recorder, scripted per call.
type serialRec struct {
	s *scriptedRec
}

func (r serialRec) Record(ctx context.Context, e *event.Event) error {
	return r.s.RecordBatch(ctx, []event.Event{*e})
}

// seq returns errs in order, then nil.
func seq(errs ...error) func(int, []string) error {
	return func(n int, _ []string) error {
		if n < len(errs) {
			return errs[n]
		}
		return nil
	}
}

func always(err error) func(int, []string) error {
	return func(int, []string) error { return err }
}

func eventIDs(events []event.Event) []string {
	ids := make([]string, len(events))
	for i, e := range events {
		ids[i] = e.ID
	}
	return ids
}

func mkEvents(n int) []event.Event {
	out := make([]event.Event, n)
	for i := range out {
		out[i] = event.Event{ID: fmt.Sprintf("e%03d", i), Action: "a.test"}
	}
	return out
}

type dropLog struct {
	mu    sync.Mutex
	calls []dropCall
	at    []time.Time
}

func (d *dropLog) opt() BufferedOption {
	return WithOnDrop(func(evs []event.Event, r DropReason, err error) {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.calls = append(d.calls, dropCall{evs, r, err})
		d.at = append(d.at, time.Now())
	})
}

func (d *dropLog) snapshot() ([]dropCall, []time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.calls), slices.Clone(d.at)
}

// droppedIDs flattens every report with reason r, in report order.
func (d *dropLog) droppedIDs(r DropReason) []string {
	calls, _ := d.snapshot()
	var ids []string
	for _, c := range calls {
		if c.reason == r {
			ids = append(ids, eventIDs(c.events)...)
		}
	}
	return ids
}

func identityJitter(d time.Duration) time.Duration { return d }

func newDeliverRec(t *testing.T, inner Recorder, drops *dropLog, extra ...BufferedOption) *BufferedRecorder {
	t.Helper()
	opts := append([]BufferedOption{withJitter(identityJitter), drops.opt()}, extra...)
	return newTestBuffered(t, inner, opts...)
}

func without(ids []string, drop string) []string {
	return slices.DeleteFunc(slices.Clone(ids), func(s string) bool { return s == drop })
}

func TestDeliver_503Then200(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inner := &scriptedRec{fn: seq(err503)}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops)
		batch := mkEvents(3)

		rem, err := b.deliver(t.Context(), batch)
		require.NoError(t, err)
		require.Nil(t, rem)
		_, _, delivered := inner.snapshot()
		require.Equal(t, eventIDs(batch), delivered)
		require.EqualValues(t, 1, b.Stats().Retries)
		require.EqualValues(t, 1, b.Stats().FlushErrs)
		calls, _ := drops.snapshot()
		require.Empty(t, calls)
	})
}

func TestDeliver_ConnRefusedThenSuccess(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		refused := &url.Error{Op: "Post", URL: "http://x", Err: syscall.ECONNREFUSED}
		inner := &scriptedRec{fn: seq(refused)}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops)
		batch := mkEvents(2)

		_, err := b.deliver(t.Context(), batch)
		require.NoError(t, err)
		calls, _, delivered := inner.snapshot()
		require.Len(t, calls, 2)
		require.Equal(t, eventIDs(batch), delivered)
		dc, _ := drops.snapshot()
		require.Empty(t, dc)
	})
}

func TestDeliver_RetryTiming(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inner := &scriptedRec{fn: always(err503)}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops,
			WithInitialBackoff(500*time.Millisecond),
			WithMaxBackoff(2*time.Second),
			WithMaxRetryDuration(10*time.Second))
		batch := mkEvents(2)

		_, err := b.deliver(t.Context(), batch)
		require.NoError(t, err)

		_, times, _ := inner.snapshot()
		var rel []time.Duration
		for _, tm := range times {
			rel = append(rel, tm.Sub(times[0]))
		}
		ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
		require.Equal(t, []time.Duration{0, ms(500), ms(1500), ms(3500), ms(5500), ms(7500), ms(9500), ms(10000)}, rel)

		calls, _ := drops.snapshot()
		require.Len(t, calls, 1)
		require.Equal(t, DropRetriesExhausted, calls[0].reason)
		require.Equal(t, eventIDs(batch), eventIDs(calls[0].events))
		require.ErrorIs(t, calls[0].err, err503)
		require.EqualValues(t, 7, b.Stats().Retries)
		require.EqualValues(t, 8, b.Stats().FlushErrs)
	})
}

func TestDeliver_ShortMaxRetryDuration(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inner := &scriptedRec{fn: always(err503)}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops,
			WithInitialBackoff(500*time.Millisecond),
			WithMaxRetryDuration(100*time.Millisecond))

		_, err := b.deliver(t.Context(), mkEvents(1))
		require.NoError(t, err)

		_, times, _ := inner.snapshot()
		require.Len(t, times, 2)
		require.Equal(t, 100*time.Millisecond, times[1].Sub(times[0]))
		calls, _ := drops.snapshot()
		require.Len(t, calls, 1)
		require.Equal(t, DropRetriesExhausted, calls[0].reason)
	})
}

func TestDeliver_PermanentFailureThenNextBatch(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		first := mkEvents(3)
		inner := &scriptedRec{fn: func(_ int, ids []string) error {
			if slices.Contains(ids, first[0].ID) {
				return err500
			}
			return nil
		}}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops, WithMaxRetryDuration(10*time.Second))

		rem, err := b.deliver(t.Context(), first)
		require.NoError(t, err)
		require.Nil(t, rem)
		require.Equal(t, eventIDs(first), drops.droppedIDs(DropRetriesExhausted))

		second := []event.Event{{ID: "n1", Action: "a.test"}, {ID: "n2", Action: "a.test"}}
		rem, err = b.deliver(t.Context(), second)
		require.NoError(t, err)
		require.Nil(t, rem)
		_, _, delivered := inner.snapshot()
		require.Equal(t, []string{"n1", "n2"}, delivered)
	})
}

func TestDeliver_OneBadEventSplits(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		batch := mkEvents(100)
		batch[37].ID = "bad"
		inner := &scriptedRec{fn: func(_ int, ids []string) error {
			if slices.Contains(ids, "bad") {
				return err400
			}
			return nil
		}}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops)

		_, err := b.deliver(t.Context(), batch)
		require.NoError(t, err)
		calls, _, delivered := inner.snapshot()
		require.Equal(t, without(eventIDs(batch), "bad"), delivered)
		require.LessOrEqual(t, len(calls), 1+32)

		dc, _ := drops.snapshot()
		require.Len(t, dc, 1)
		require.Equal(t, DropRejected, dc[0].reason)
		require.Equal(t, []string{"bad"}, eventIDs(dc[0].events))
		require.ErrorIs(t, dc[0].err, err400)
	})
}

func TestDeliver_OversizedBatch400Splits(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) { testOversizedSplits(t, err400) })
}

func TestDeliver_413Splits(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) { testOversizedSplits(t, err413) })
}

// testOversizedSplits rejects any batch over 25 events with status.
func testOversizedSplits(t *testing.T, status *HTTPError) {
	inner := &scriptedRec{fn: func(_ int, ids []string) error {
		if len(ids) > 25 {
			return status
		}
		return nil
	}}
	drops := &dropLog{}
	b := newDeliverRec(t, inner, drops)
	batch := mkEvents(100)

	_, err := b.deliver(t.Context(), batch)
	require.NoError(t, err)
	_, _, delivered := inner.snapshot()
	require.Equal(t, eventIDs(batch), delivered)
	dc, _ := drops.snapshot()
	require.Empty(t, dc)
}

func TestDeliver_AllBadHitsBudget(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inner := &scriptedRec{fn: always(err400)}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops)
		batch := mkEvents(100)

		_, err := b.deliver(t.Context(), batch)
		require.NoError(t, err)
		calls, _, delivered := inner.snapshot()
		require.Len(t, calls, 1+32)
		require.Empty(t, delivered)
		require.Equal(t, eventIDs(batch), drops.droppedIDs(DropRejected))
		dc, _ := drops.snapshot()
		for _, c := range dc {
			require.Equal(t, DropRejected, c.reason)
			require.ErrorIs(t, c.err, err400)
		}
	})
}

func TestDeliver_SplitHalf503DoesNotSpendBudget(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		batch := mkEvents(100)
		batch[80].ID = "bad"
		// The first good half sent fails 40 times; were retries to spend
		// budget, good events would be rejected.
		var flakes int
		inner := &scriptedRec{fn: func(_ int, ids []string) error {
			switch {
			case slices.Contains(ids, "bad"):
				return err400
			case flakes < 40:
				flakes++
				return err503
			}
			return nil
		}}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops,
			WithMaxBackoff(time.Second), WithMaxRetryDuration(time.Hour))

		_, err := b.deliver(t.Context(), batch)
		require.NoError(t, err)
		calls, _, delivered := inner.snapshot()
		require.Equal(t, without(eventIDs(batch), "bad"), delivered)
		require.EqualValues(t, 40, b.Stats().Retries)
		require.LessOrEqual(t, len(calls)-int(b.Stats().Retries), 1+32)
		require.Equal(t, []string{"bad"}, drops.droppedIDs(DropRejected))
		require.Empty(t, drops.droppedIDs(DropRetriesExhausted))
	})
}

func TestDeliver_SplitHalfInheritsDeadline(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		batch := mkEvents(4)
		// 503 then 400 makes the split happen at 500ms, not at the start.
		inner := &scriptedRec{fn: func(n int, ids []string) error {
			switch {
			case n == 0:
				return err503
			case n == 1:
				return err400
			case slices.Contains(ids, batch[0].ID):
				return err503
			}
			return nil
		}}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops, WithMaxRetryDuration(2*time.Second))
		start := time.Now()

		_, err := b.deliver(t.Context(), batch)
		require.NoError(t, err)

		dc, at := drops.snapshot()
		require.Len(t, dc, 1)
		require.Equal(t, DropRetriesExhausted, dc[0].reason)
		require.Equal(t, eventIDs(batch[:2]), eventIDs(dc[0].events))
		require.Equal(t, 2*time.Second, at[0].Sub(start))
		_, _, delivered := inner.snapshot()
		require.Equal(t, eventIDs(batch[2:]), delivered)
	})
}

func TestDeliver_PartAfterDeadlineGetsOneAttempt(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		// 503 at 0 and 500ms, then the clamped final attempt at 1s gets a
		// 400, so both halves are first sent after the deadline.
		inner := &scriptedRec{fn: func(n int, _ []string) error {
			if n == 2 {
				return err400
			}
			return err503
		}}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops,
			WithInitialBackoff(500*time.Millisecond),
			WithMaxRetryDuration(time.Second))
		batch := mkEvents(4)
		start := time.Now()

		_, err := b.deliver(t.Context(), batch)
		require.NoError(t, err)

		calls, times, _ := inner.snapshot()
		require.Equal(t, [][]string{
			eventIDs(batch), eventIDs(batch), eventIDs(batch),
			eventIDs(batch[:2]), eventIDs(batch[2:]),
		}, calls)
		require.Equal(t, time.Second, times[2].Sub(start))
		require.EqualValues(t, 2, b.Stats().Retries)

		dc, _ := drops.snapshot()
		require.Len(t, dc, 2)
		for i, want := range [][]event.Event{batch[:2], batch[2:]} {
			require.Equal(t, DropRetriesExhausted, dc[i].reason)
			require.Equal(t, eventIDs(want), eventIDs(dc[i].events))
			require.ErrorIs(t, dc[i].err, err503)
		}
	})
}

func TestDeliver_MarshalErrorSplits(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var received []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Events []event.Event `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = append(received, eventIDs(body.Events)...)
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(srv.Close)

	inner := NewHTTPRecorder("proj", "key", WithBaseURL(srv.URL))
	drops := &dropLog{}
	b := newDeliverRec(t, inner, drops)
	batch := mkEvents(10)
	batch[6].Metadata = map[string]any{"x": math.NaN()}

	_, err := b.deliver(t.Context(), batch)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, without(eventIDs(batch), batch[6].ID), received)
	dc, _ := drops.snapshot()
	require.Len(t, dc, 1)
	require.Equal(t, DropRejected, dc[0].reason)
	require.Equal(t, []string{batch[6].ID}, eventIDs(dc[0].events))
	var uv *json.UnsupportedValueError
	require.ErrorAs(t, dc[0].err, &uv)
}

func TestDeliver_SingleEvent400(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inner := &scriptedRec{fn: always(err400)}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops)

		_, err := b.deliver(t.Context(), mkEvents(1))
		require.NoError(t, err)
		calls, _, _ := inner.snapshot()
		require.Len(t, calls, 1)
		require.Equal(t, []string{"e000"}, drops.droppedIDs(DropRejected))
	})
}

func TestDeliver_401DropsWholeBatch(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inner := &scriptedRec{fn: always(err401)}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops)
		batch := mkEvents(100)

		_, err := b.deliver(t.Context(), batch)
		require.NoError(t, err)
		calls, _, _ := inner.snapshot()
		require.Len(t, calls, 1)
		dc, _ := drops.snapshot()
		require.Len(t, dc, 1)
		require.Equal(t, DropRejected, dc[0].reason)
		require.Equal(t, eventIDs(batch), eventIDs(dc[0].events))
		require.ErrorIs(t, dc[0].err, err401)
	})
}

func TestDeliver_UnknownErrorNotRetried(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		boom := errors.New("boom")
		inner := &scriptedRec{fn: always(boom)}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops)
		batch := mkEvents(5)

		_, err := b.deliver(t.Context(), batch)
		require.NoError(t, err)
		calls, _, _ := inner.snapshot()
		require.Len(t, calls, 1)
		require.EqualValues(t, 0, b.Stats().Retries)
		dc, _ := drops.snapshot()
		require.Len(t, dc, 1)
		require.Equal(t, DropRejected, dc[0].reason)
		require.ErrorIs(t, dc[0].err, boom)
	})
}

func TestDeliver_CustomTransientRetried(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inner := &scriptedRec{fn: seq(transientErr{}, transientErr{})}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops)
		batch := mkEvents(2)

		_, err := b.deliver(t.Context(), batch)
		require.NoError(t, err)
		_, _, delivered := inner.snapshot()
		require.Equal(t, eventIDs(batch), delivered)
		require.EqualValues(t, 2, b.Stats().Retries)
		dc, _ := drops.snapshot()
		require.Empty(t, dc)
	})
}

func TestDeliver_SerialFallbackResendsOnlyFailures(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inner := &scriptedRec{fn: seq(nil, err503)}
		drops := &dropLog{}
		b := newDeliverRec(t, serialRec{inner}, drops)
		batch := mkEvents(3)

		_, err := b.deliver(t.Context(), batch)
		require.NoError(t, err)
		calls, _, delivered := inner.snapshot()
		require.Equal(t, [][]string{{"e000"}, {"e001"}, {"e002"}, {"e001"}}, calls)
		require.ElementsMatch(t, eventIDs(batch), delivered)
		require.EqualValues(t, 1, b.Stats().Retries)
		dc, _ := drops.snapshot()
		require.Empty(t, dc)
	})
}

func TestDeliver_SerialFallbackRejectsSingleEvent(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inner := &scriptedRec{fn: seq(nil, err400)}
		drops := &dropLog{}
		b := newDeliverRec(t, serialRec{inner}, drops)

		_, err := b.deliver(t.Context(), mkEvents(3))
		require.NoError(t, err)
		calls, _, delivered := inner.snapshot()
		require.Len(t, calls, 3)
		require.Equal(t, []string{"e000", "e002"}, delivered)
		require.Equal(t, []string{"e001"}, drops.droppedIDs(DropRejected))
		require.EqualValues(t, 0, b.Stats().Retries)
	})
}

func TestDeliver_RetrySameIDs(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inner := &scriptedRec{fn: seq(err503, err503, err503)}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops)

		_, err := b.deliver(t.Context(), mkEvents(5))
		require.NoError(t, err)
		calls, _, _ := inner.snapshot()
		require.Len(t, calls, 4)
		for _, c := range calls[1:] {
			require.Equal(t, calls[0], c)
		}
	})
}

func TestDeliver_CtxCancelReturnsRemainder(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		inner := &scriptedRec{fn: always(err503)}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops)
		batch := mkEvents(3)
		ctx, cancel := context.WithCancel(t.Context())

		type result struct {
			rem []event.Event
			err error
		}
		done := make(chan result, 1)
		go func() {
			rem, err := b.deliver(ctx, batch)
			done <- result{rem, err}
		}()
		synctest.Wait() // deliver is now parked in its backoff wait
		cancel()
		r := <-done

		require.ErrorIs(t, r.err, context.Canceled)
		require.Equal(t, eventIDs(batch), eventIDs(r.rem))
		calls, _, _ := inner.snapshot()
		require.Len(t, calls, 1)
		dc, _ := drops.snapshot()
		require.Empty(t, dc)
	})
}

func TestDeliver_CtxCancelMidSplitKeepsOrder(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		// 8 and its first half get 400; e000-e001 succeed; e002-e003 hit 503.
		inner := &scriptedRec{fn: func(n int, ids []string) error {
			switch {
			case n <= 1:
				return err400
			case ids[0] == "e002":
				return err503
			}
			return nil
		}}
		drops := &dropLog{}
		b := newDeliverRec(t, inner, drops)
		batch := mkEvents(8)
		ctx, cancel := context.WithCancel(t.Context())

		type result struct {
			rem []event.Event
			err error
		}
		done := make(chan result, 1)
		go func() {
			rem, err := b.deliver(ctx, batch)
			done <- result{rem, err}
		}()
		synctest.Wait()
		cancel()
		r := <-done

		require.ErrorIs(t, r.err, context.Canceled)
		require.Equal(t, eventIDs(batch[2:]), eventIDs(r.rem))
		_, _, delivered := inner.snapshot()
		require.Equal(t, []string{"e000", "e001"}, delivered)
		dc, _ := drops.snapshot()
		require.Empty(t, dc)
	})
}

func TestDeliver_SplitLogsAtDebug(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		batch := mkEvents(4)
		batch[0].ID = "bad"
		inner := &scriptedRec{fn: func(_ int, ids []string) error {
			if slices.Contains(ids, "bad") {
				return err400
			}
			return nil
		}}
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		b := newDeliverRec(t, inner, &dropLog{}, WithSlogLogger(logger))

		_, err := b.deliver(t.Context(), batch)
		require.NoError(t, err)

		var debug, warn []string
		for _, line := range strings.Split(buf.String(), "\n") {
			if !strings.Contains(line, "recorder delivery attempt failed") {
				continue
			}
			switch {
			case strings.Contains(line, "level=DEBUG"):
				debug = append(debug, line)
			case strings.Contains(line, "level=WARN"):
				warn = append(warn, line)
			}
		}
		require.Len(t, debug, 2, "the 4- and 2-event rejections split")
		require.Len(t, warn, 1, "only the single-event rejection warns")
		require.Contains(t, warn[0], "batch_size=1")
	})
}
