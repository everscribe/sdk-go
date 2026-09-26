package recorder

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/everscribe/sdk-go/pkg/event"
)

// Errors returned by Recorder implementations.
var (
	// ErrBufferFull is returned by BufferedRecorder when its overflow
	// policy is PolicyError and the buffer has no space. Other overflow
	// policies do not return this error.
	ErrBufferFull = errors.New("recorder: buffer full")
)

// OverflowPolicy controls BufferedRecorder's behavior when Record is
// called and the internal event buffer has no space.
type OverflowPolicy int

const (
	// PolicyDropNewest silently discards the incoming event and increments
	// the dropped counter. Emits a slog warning on the first drop and
	// every 1000th subsequent drop. Default policy - keeps the request
	// path fast at the cost of losing events under sustained pressure.
	// Operators should monitor the dropped counter and resize the buffer
	// (or downstream throughput) when it grows.
	PolicyDropNewest OverflowPolicy = iota

	// PolicyBlock blocks the caller until space is available in the
	// buffer (or ctx is canceled, or the recorder is closed). Preserves
	// every event but adds unbounded latency to the caller.
	PolicyBlock

	// PolicyError returns ErrBufferFull immediately. Callers decide
	// whether to retry, drop, or alert.
	PolicyError
)

// BufferedOption configures a BufferedRecorder. BufferedOption also
// satisfies RecorderOption, so it can be passed directly to New.
type BufferedOption func(*bufferedConfig)

// applyRecorder lets BufferedOption satisfy RecorderOption - see recorder.go.
func (o BufferedOption) applyRecorder(c *recorderConfig) {
	c.bufferedOpts = append(c.bufferedOpts, o)
}

type bufferedConfig struct {
	bufferSize    int
	flushSize     int
	flushInterval time.Duration
	flushTimeout  time.Duration
	overflow      OverflowPolicy
	drainTimeout  time.Duration
	logger        *slog.Logger
}

func defaultBufferedConfig() bufferedConfig {
	return bufferedConfig{
		bufferSize:    1000,
		flushSize:     100,
		flushInterval: 5 * time.Second,
		flushTimeout:  30 * time.Second,
		overflow:      PolicyDropNewest,
		drainTimeout:  30 * time.Second,
		logger:        slog.Default(),
	}
}

// WithBufferSize sets the capacity of the internal event channel.
// Default 1000.
func WithBufferSize(n int) BufferedOption {
	return func(c *bufferedConfig) { c.bufferSize = n }
}

// WithFlushSize sets the number of events that triggers an immediate
// flush. The background goroutine flushes when pending events reach
// this size, without waiting for the flush interval. Default 100.
func WithFlushSize(n int) BufferedOption {
	return func(c *bufferedConfig) { c.flushSize = n }
}

// WithFlushInterval sets the maximum time between flushes. Events are
// flushed at least this often even if the size threshold is not
// reached. Default 5 seconds.
func WithFlushInterval(d time.Duration) BufferedOption {
	return func(c *bufferedConfig) { c.flushInterval = d }
}

// WithFlushTimeout sets the context timeout used for each flush call
// to the inner Recorder. Default 30 seconds.
func WithFlushTimeout(d time.Duration) BufferedOption {
	return func(c *bufferedConfig) { c.flushTimeout = d }
}

// WithOverflowPolicy sets the policy for Record calls that find the
// buffer full. Default is PolicyDropNewest.
func WithOverflowPolicy(p OverflowPolicy) BufferedOption {
	return func(c *bufferedConfig) { c.overflow = p }
}

// WithDrainTimeout sets the maximum time Close will wait for in-flight
// events to flush. Events remaining after the timeout are dropped.
// Default 30 seconds.
func WithDrainTimeout(d time.Duration) BufferedOption {
	return func(c *bufferedConfig) { c.drainTimeout = d }
}

// WithSlogLogger sets the slog.Logger used for BufferedRecorder's
// internal diagnostics (overflow warnings, flush errors). Defaults to
// slog.Default().
func WithSlogLogger(l *slog.Logger) BufferedOption {
	return func(c *bufferedConfig) { c.logger = l }
}

// BufferedRecorder wraps another Recorder to add asynchronous batched
// writes. Events enqueue on an internal channel; a background goroutine
// flushes them to the inner Recorder when the pending batch reaches the
// size threshold or the flush interval elapses, whichever comes first.
//
// BufferedRecorder itself implements Recorder. It uses the inner
// Recorder's RecordBatch when available, or loops serial Record calls
// otherwise.
//
// Call Close to drain pending events and stop the background goroutine;
// only the first call has effect.
type BufferedRecorder struct {
	inner  Recorder
	cfg    bufferedConfig
	events chan event.Event

	dropped   atomic.Int64
	flushed   atomic.Int64
	flushErrs atomic.Int64

	flushReq chan chan error
	done     chan struct{}
	stop     chan struct{}
	stopOnce sync.Once
}

// NewBufferedRecorder wraps inner with asynchronous batched writes.
// Spawns a background goroutine that runs until Close is called.
func NewBufferedRecorder(inner Recorder, opts ...BufferedOption) *BufferedRecorder {
	cfg := defaultBufferedConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	b := &BufferedRecorder{
		inner:    inner,
		cfg:      cfg,
		events:   make(chan event.Event, cfg.bufferSize),
		flushReq: make(chan chan error),
		done:     make(chan struct{}),
		stop:     make(chan struct{}),
	}
	go b.run()
	return b
}

// Record implements Recorder. Behavior when the buffer is full depends
// on the configured OverflowPolicy. Empty-Action events are no-ops.
func (b *BufferedRecorder) Record(ctx context.Context, e *event.Event) error {
	if e == nil || e.Action == "" {
		return nil
	}
	// Post-Close guard - drop silently rather than block or panic.
	select {
	case <-b.stop:
		return nil
	default:
	}

	// PrepareEventFields fills ID, OccurredAt, and Result now, but defers
	// the dedupe mark: every overflow path below can discard the event
	// instead of enqueuing it. Marking it recorded before it lands in the
	// buffer would suppress end()'s auto-record backstop on exactly the
	// events that get dropped. mark is only invoked after a send succeeds.
	mark := event.PrepareEventFields(ctx, e)

	switch b.cfg.overflow {
	case PolicyBlock:
		select {
		case b.events <- *e:
			mark()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-b.stop:
			return nil
		}

	case PolicyError:
		select {
		case b.events <- *e:
			mark()
			return nil
		default:
			return ErrBufferFull
		}

	default: // PolicyDropNewest
		select {
		case b.events <- *e:
			mark()
			return nil
		default:
			n := b.dropped.Add(1)
			if n == 1 || n%1000 == 0 {
				b.cfg.logger.Warn("recorder buffer full, event dropped",
					"action", e.Action,
					"dropped_total", n,
					"buffer_size", cap(b.events))
			}
			return nil
		}
	}
}

// Flush forces an immediate flush of events buffered at the time of the
// call, blocking until they're persisted to the inner Recorder or ctx
// is canceled. Events enqueued after the call are not guaranteed to be
// included.
//
// Returns nil on success, ctx.Err() on cancellation, or the inner
// Recorder's error on flush failure. Calling Flush after Close is a
// no-op returning nil.
func (b *BufferedRecorder) Flush(ctx context.Context) error {
	select {
	case <-b.stop:
		return nil
	default:
	}
	ack := make(chan error, 1)
	select {
	case b.flushReq <- ack:
	case <-ctx.Done():
		return ctx.Err()
	case <-b.stop:
		return nil
	}
	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close drains pending events and stops the background goroutine.
// Blocks until the drain completes or the configured drain timeout
// elapses. Returns context.DeadlineExceeded if the drain timed out
// with events still pending.
func (b *BufferedRecorder) Close() error {
	b.stopOnce.Do(func() {
		close(b.stop)
	})
	select {
	case <-b.done:
		return nil
	case <-time.After(b.cfg.drainTimeout):
		return context.DeadlineExceeded
	}
}

// Stats returns cumulative counters for observability.
func (b *BufferedRecorder) Stats() BufferedStats {
	return BufferedStats{
		Dropped:    b.dropped.Load(),
		Flushed:    b.flushed.Load(),
		FlushErrs:  b.flushErrs.Load(),
		Pending:    len(b.events),
		BufferSize: cap(b.events),
	}
}

// BufferedStats reports BufferedRecorder's runtime counters.
type BufferedStats struct {
	Dropped    int64 // total events dropped due to overflow
	Flushed    int64 // total events successfully flushed to inner Recorder
	FlushErrs  int64 // total flush calls that returned an error
	Pending    int   // events currently in the buffer
	BufferSize int   // buffer capacity
}

// run is the background goroutine that drains events and flushes on
// size/time thresholds, then drains remaining events on shutdown.
func (b *BufferedRecorder) run() {
	defer close(b.done)

	batch := make([]event.Event, 0, b.cfg.flushSize)
	ticker := time.NewTicker(b.cfg.flushInterval)
	defer ticker.Stop()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := b.flushBatch(batch)
		batch = batch[:0]
		return err
	}

	for {
		select {
		case e := <-b.events:
			batch = append(batch, e)
			if len(batch) >= b.cfg.flushSize {
				_ = flush()
			}

		case <-ticker.C:
			_ = flush()

		case ack := <-b.flushReq:
			// Drain anything currently in the channel so the synchronous
			// flush includes events that were enqueued just before the
			// caller's Flush.
		drain:
			for {
				select {
				case e := <-b.events:
					batch = append(batch, e)
				default:
					break drain
				}
			}
			ack <- flush()

		case <-b.stop:
			// Drain whatever's already in the channel. Record calls
			// racing with stop see the closed stop chan and bail out.
			for {
				select {
				case e := <-b.events:
					batch = append(batch, e)
					if len(batch) >= b.cfg.flushSize {
						_ = flush()
					}
				default:
					_ = flush()
					return
				}
			}
		}
	}
}

func (b *BufferedRecorder) flushBatch(batch []event.Event) error {
	n := len(batch)
	if n == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), b.cfg.flushTimeout)
	defer cancel()

	var err error
	if br, ok := b.inner.(BatchRecorder); ok {
		err = br.RecordBatch(ctx, batch)
	} else {
		for i := range batch {
			if ierr := b.inner.Record(ctx, &batch[i]); ierr != nil {
				err = ierr
				// continue the loop - one failure should not abort the batch
			}
		}
	}
	if err != nil {
		b.flushErrs.Add(1)
		b.cfg.logger.Error("recorder flush failed",
			"error", err,
			"batch_size", n,
			"flush_errs_total", b.flushErrs.Load())
		return err
	}
	b.flushed.Add(int64(n))
	return nil
}

var _ Recorder = (*BufferedRecorder)(nil)
