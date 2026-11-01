package recorder

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
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
	// PolicyDropNewest discards the incoming event, increments the dropped
	// counter, and reports it to the drop callback as buffer_full. Emits a
	// slog warning on the first drop and every 1000th subsequent drop.
	// Default policy - keeps the request path fast at the cost of losing
	// events under sustained pressure. Operators should monitor the
	// dropped counter and resize the buffer (or downstream throughput)
	// when it grows.
	PolicyDropNewest OverflowPolicy = iota

	// PolicyBlock blocks the caller until space is available in the
	// buffer (or ctx is canceled, or the recorder is closed). Preserves
	// every event but adds unbounded latency to the caller.
	PolicyBlock

	// PolicyError returns ErrBufferFull immediately. Callers decide
	// whether to retry, drop, or alert.
	PolicyError
)

// closeGrace is how long Close waits for the worker after cancelling it.
const closeGrace = time.Second

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
	onDrop        func(events []event.Event, reason DropReason, err error)

	initialBackoff   time.Duration
	maxBackoff       time.Duration
	maxRetryDuration time.Duration
	jitter           func(time.Duration) time.Duration
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

		initialBackoff:   500 * time.Millisecond,
		maxBackoff:       30 * time.Second,
		maxRetryDuration: 5 * time.Minute,
		jitter:           fullJitter,
	}
}

// fullJitter returns a uniform random duration in [0, d].
func fullJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(d) + 1))
}

// WithBufferSize sets the capacity of the internal event channel.
// Default 1000.
func WithBufferSize(n int) BufferedOption {
	return func(c *bufferedConfig) { c.bufferSize = n }
}

// WithFlushSize sets the number of events that triggers an immediate
// flush, and caps every send, including Flush and the Close drain.
// Values <= 0 keep the default of 100.
func WithFlushSize(n int) BufferedOption {
	return func(c *bufferedConfig) {
		if n > 0 {
			c.flushSize = n
		}
	}
}

// WithFlushInterval sets the maximum time between flushes. Events are
// flushed at least this often even if the size threshold is not
// reached. Default 5 seconds.
func WithFlushInterval(d time.Duration) BufferedOption {
	return func(c *bufferedConfig) { c.flushInterval = d }
}

// WithFlushTimeout sets the context timeout for each delivery attempt
// to the inner Recorder; every retry gets a fresh timeout. Default 30
// seconds.
func WithFlushTimeout(d time.Duration) BufferedOption {
	return func(c *bufferedConfig) { c.flushTimeout = d }
}

// WithOverflowPolicy sets the policy for Record calls that find the
// buffer full. Default is PolicyDropNewest.
func WithOverflowPolicy(p OverflowPolicy) BufferedOption {
	return func(c *bufferedConfig) { c.overflow = p }
}

// WithDrainTimeout sets how long Close waits for pending events to
// flush. Events remaining after the timeout are reported to the drop
// callback as shutdown. Close then cancels the in-flight send and waits
// up to 1s more, so it can take drainTimeout + 1s. Default 30 seconds.
func WithDrainTimeout(d time.Duration) BufferedOption {
	return func(c *bufferedConfig) { c.drainTimeout = d }
}

// WithInitialBackoff sets the base wait before the first retry of a
// failed batch. Values <= 0 keep the default of 500ms.
func WithInitialBackoff(d time.Duration) BufferedOption {
	return func(c *bufferedConfig) {
		if d > 0 {
			c.initialBackoff = d
		}
	}
}

// WithMaxBackoff caps a single wait between retries. Values <= 0 keep
// the default of 30 seconds.
func WithMaxBackoff(d time.Duration) BufferedOption {
	return func(c *bufferedConfig) {
		if d > 0 {
			c.maxBackoff = d
		}
	}
}

// WithMaxRetryDuration sets how long one batch is retried before its
// events are dropped as retries_exhausted. Values <= 0 keep the default
// of 5 minutes.
func WithMaxRetryDuration(d time.Duration) BufferedOption {
	return func(c *bufferedConfig) {
		if d > 0 {
			c.maxRetryDuration = d
		}
	}
}

// withJitter replaces the backoff jitter; tests use it for exact timing.
func withJitter(fn func(time.Duration) time.Duration) BufferedOption {
	return func(c *bufferedConfig) {
		if fn != nil {
			c.jitter = fn
		}
	}
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
// only the first call has effect, and later calls wait for it.
type BufferedRecorder struct {
	inner  Recorder
	cfg    bufferedConfig
	events chan event.Event

	dropped   atomic.Int64
	flushed   atomic.Int64
	flushErrs atomic.Int64
	retries   atomic.Int64

	// dropCounts is built once in the constructor and never mutated.
	dropCounts map[DropReason]*atomic.Int64

	// workCtx bounds every send; only Close cancels it.
	workCtx    context.Context
	cancelWork context.CancelFunc

	// mu orders Record's stop check and send before Close's drain.
	mu sync.RWMutex

	flushReq chan chan struct{}
	done     chan struct{}
	stop     chan struct{} // closed first by Close; Record stops sending
	drain    chan struct{} // closed once no send can follow; worker drains
	closed   chan struct{} // closed once closeErr is set by the first Close
	closeErr error
	stopOnce sync.Once
}

// NewBufferedRecorder wraps inner with asynchronous batched writes.
// Spawns a background goroutine that runs until Close is called.
func NewBufferedRecorder(inner Recorder, opts ...BufferedOption) *BufferedRecorder {
	cfg := defaultBufferedConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	counts := make(map[DropReason]*atomic.Int64, len(allDropReasons))
	for _, r := range allDropReasons {
		counts[r] = new(atomic.Int64)
	}
	workCtx, cancelWork := context.WithCancel(context.Background())
	b := &BufferedRecorder{
		dropCounts: counts,
		inner:      inner,
		cfg:        cfg,
		events:     make(chan event.Event, cfg.bufferSize),
		workCtx:    workCtx,
		cancelWork: cancelWork,
		flushReq:   make(chan chan struct{}),
		done:       make(chan struct{}),
		stop:       make(chan struct{}),
		drain:      make(chan struct{}),
		closed:     make(chan struct{}),
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

	// PrepareEventFields fills ID, OccurredAt, and Result now, but defers
	// the dedupe mark: every path below can discard the event instead of
	// enqueuing it. Marking it recorded before it lands in the buffer
	// would suppress end()'s auto-record backstop on exactly the events
	// that get dropped. mark is only invoked after a send succeeds.
	mark := event.PrepareEventFields(ctx, e)

	// A drop end() will retry is left for its attempt to report, so each
	// event is reported at most once.
	auto := event.AutoRecord(ctx, e)

	// The read lock spans the stop check and the send, so Close's write
	// lock waits out every send in progress. Drops are reported after
	// unlocking: an onDrop that calls Close would otherwise deadlock.
	b.mu.RLock()
	select {
	case <-b.stop:
		b.mu.RUnlock()
		b.dropClosed(e, auto)
		return nil
	default:
	}

	switch b.cfg.overflow {
	case PolicyBlock:
		select {
		case b.events <- *e:
			b.mu.RUnlock()
			mark()
			return nil
		case <-ctx.Done():
			b.mu.RUnlock()
			return b.overflowErr(e, auto, ctx.Err())
		case <-b.stop:
			b.mu.RUnlock()
			b.dropClosed(e, auto)
			return nil
		}

	case PolicyError:
		select {
		case b.events <- *e:
			b.mu.RUnlock()
			mark()
			return nil
		default:
			b.mu.RUnlock()
			return b.overflowErr(e, auto, ErrBufferFull)
		}

	default: // PolicyDropNewest
		select {
		case b.events <- *e:
			b.mu.RUnlock()
			mark()
			return nil
		default:
			b.mu.RUnlock()
			// end() in pkg/event/lifecycle.go must retry a Pending drop, or this skip loses it.
			if auto == event.AutoRecordPending {
				return nil
			}
			n := b.reportDrop([]event.Event{*e}, DropBufferFull, nil)
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

// dropClosed reports an event recorded after Close, unless end() will
// retry it and report it then.
func (b *BufferedRecorder) dropClosed(e *event.Event, auto event.AutoRecordStatus) {
	if auto != event.AutoRecordPending {
		b.reportDrop([]event.Event{*e}, DropClosed, nil)
	}
}

// overflowErr returns err for an event the buffer had no room for. The
// auto-record path has no caller to see err, so its event is reported.
func (b *BufferedRecorder) overflowErr(e *event.Event, auto event.AutoRecordStatus, err error) error {
	if auto == event.AutoRecordInProgress {
		b.reportDrop([]event.Event{*e}, DropBufferFull, err)
	}
	return err
}

// Flush sends the events buffered at the time of the call, in batches
// of up to the flush size with the usual retries, and blocks until each
// is delivered or reported to the drop callback. Events enqueued after
// the call are not guaranteed to be included.
//
// Returns nil once they are resolved, including when some were dropped,
// or ctx.Err() if ctx ends first. Ending ctx never cancels a send: the
// events stay queued and the background goroutine keeps delivering them.
// Always pass a ctx with a deadline, since retries during an outage can
// take minutes. Calling Flush after Close is a no-op returning nil.
func (b *BufferedRecorder) Flush(ctx context.Context) error {
	select {
	case <-b.stop:
		return nil
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ack := make(chan struct{})
	select {
	case b.flushReq <- ack:
	case <-ctx.Done():
		return ctx.Err()
	case <-b.stop:
		return nil
	}
	select {
	case <-ack:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-b.closed:
		return nil
	}
}

// Close drains pending events, with retries, and stops the background
// goroutine. Records after Close, and PolicyBlock waiters it releases,
// are reported as closed. If the drain timeout elapses, Close cancels
// the in-flight send, reports what is left as shutdown, and returns
// context.DeadlineExceeded. An inner Recorder that ignores ctx gets a
// 1s grace; its in-flight batch is then reported when the call returns.
// Only the first call has effect; later calls wait for it and return
// its result.
func (b *BufferedRecorder) Close() error {
	first := false
	b.stopOnce.Do(func() {
		first = true
		close(b.stop) // releases PolicyBlock waiters
	})
	if !first {
		<-b.closed
		return b.closeErr
	}
	left, err := b.closeDrain()
	b.closeErr = err
	// closed is closed before reporting left, so an onDrop that calls
	// Close here does not wait on itself.
	close(b.closed)
	if len(left) > 0 {
		b.cfg.logger.Error("recorder dropped events",
			"reason", string(DropShutdown), "count", len(left), "error", err)
		b.reportDrop(left, DropShutdown, nil)
	}
	return err
}

// closeDrain runs the first Close. Every wait in it is bounded by a
// timer, so it returns even if the worker never does. left is the queue
// taken when the worker ignored cancellation.
func (b *BufferedRecorder) closeDrain() (left []event.Event, err error) {
	// Wait out every Record already past its stop check.
	b.mu.Lock()
	b.mu.Unlock() //nolint:staticcheck // empty critical section is the point
	close(b.drain)

	drain := time.NewTimer(b.cfg.drainTimeout)
	defer drain.Stop()
	select {
	case <-b.done:
		return nil, nil
	case <-drain.C:
	}

	b.cancelWork()
	grace := time.NewTimer(closeGrace)
	defer grace.Stop()
	select {
	case <-b.done:
		return nil, context.DeadlineExceeded
	case <-grace.C:
	}

	// The inner Recorder ignores ctx: take the queue to report.
queue:
	for {
		select {
		case e := <-b.events:
			left = append(left, e)
		default:
			break queue
		}
	}
	return left, context.DeadlineExceeded
}

// Stats returns cumulative counters for observability.
func (b *BufferedRecorder) Stats() BufferedStats {
	by := make(map[DropReason]int64, len(b.dropCounts))
	for r, c := range b.dropCounts {
		by[r] = c.Load()
	}
	return BufferedStats{
		Dropped:         b.dropped.Load(),
		DroppedByReason: by,
		Flushed:         b.flushed.Load(),
		FlushErrs:       b.flushErrs.Load(),
		Retries:         b.retries.Load(),
		Pending:         len(b.events),
		BufferSize:      cap(b.events),
	}
}

// BufferedStats reports BufferedRecorder's runtime counters.
type BufferedStats struct {
	Dropped         int64                // events dropped because the buffer was full; always equals DroppedByReason[DropBufferFull]
	DroppedByReason map[DropReason]int64 // events dropped, per reason
	Flushed         int64                // total events successfully flushed to inner Recorder
	FlushErrs       int64                // failed delivery attempts (one flush can make several)
	Retries         int64                // total delivery retries
	Pending         int                  // events currently in the buffer
	BufferSize      int                  // buffer capacity
}

// flushWaiter is a pending Flush: ack closes once left more events are
// delivered or reported.
type flushWaiter struct {
	left int
	ack  chan struct{}
}

// run is the background goroutine. It sends batches of up to flushSize
// on the size and time thresholds, sends back to back while a Flush is
// pending, and drains the buffer on shutdown.
func (b *BufferedRecorder) run() {
	defer close(b.done)
	defer b.cancelWork()

	batch := make([]event.Event, 0, b.cfg.flushSize)
	var waiters []*flushWaiter
	ticker := time.NewTicker(b.cfg.flushInterval)
	defer ticker.Stop()

	// fill tops batch up from the buffer without blocking.
	fill := func() {
		for len(batch) < b.cfg.flushSize {
			select {
			case e := <-b.events:
				batch = append(batch, e)
			default:
				return
			}
		}
	}
	// send resolves batch, then credits the events to pending Flushes.
	send := func() {
		n := len(batch)
		if n == 0 {
			return
		}
		if rem, err := b.deliver(b.workCtx, batch); err != nil {
			b.cfg.logger.Error("recorder dropped events",
				"reason", string(DropShutdown), "count", len(rem), "error", err)
			b.reportDrop(rem, DropShutdown, nil)
		}
		batch = batch[:0]
		kept := waiters[:0]
		for _, w := range waiters {
			if w.left -= n; w.left > 0 {
				kept = append(kept, w)
			} else {
				close(w.ack)
			}
		}
		clear(waiters[len(kept):])
		waiters = kept
	}
	// wait counts what is queued now: the held batch plus the buffer.
	wait := func(ack chan struct{}) {
		n := len(batch) + len(b.events)
		if n == 0 {
			close(ack)
			return
		}
		waiters = append(waiters, &flushWaiter{left: n, ack: ack})
	}
	shutdown := func() {
		for {
			fill()
			if len(batch) == 0 {
				break
			}
			send()
		}
		for _, w := range waiters {
			close(w.ack)
		}
	}

	for {
		if len(waiters) > 0 {
			select {
			case ack := <-b.flushReq:
				wait(ack)
			case <-b.drain:
				shutdown()
				return
			default:
				fill()
				if len(batch) == 0 { // counts are exact; guard against a spin
					for _, w := range waiters {
						close(w.ack)
					}
					waiters = nil
				}
				send()
			}
			continue
		}

		// Take a waiting Flush first so its count starts right after
		// the batch it waited on, not after one more send.
		select {
		case ack := <-b.flushReq:
			wait(ack)
			continue
		default:
		}

		select {
		case e := <-b.events:
			batch = append(batch, e)
			if len(batch) >= b.cfg.flushSize {
				send()
			}

		case <-ticker.C:
			send()

		case ack := <-b.flushReq:
			wait(ack)

		case <-b.drain:
			// Close closes drain only once no Record can still send.
			shutdown()
			return
		}
	}
}

var _ Recorder = (*BufferedRecorder)(nil)
