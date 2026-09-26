package recorder

import (
	"context"

	"github.com/everscribe/sdk-go/pkg/event"
)

// Recorder records audit events. Implementations may be synchronous
// (HTTPRecorder) or buffered (BufferedRecorder wraps another Recorder
// for asynchronous batching).
//
// An Event with an empty Action must be a no-op returning nil without
// recording, so the `defer Record` idiom is safe for handlers that
// bail out before setting Action.
//
// When called from an HTTP handler whose context carries a
// middleware-wrapped ResponseWriter, implementations should populate
// e.Result from the captured HTTP status if e.Result.Status is empty;
// an explicit e.Result from the caller wins over auto-capture.
type Recorder interface {
	Record(ctx context.Context, e *event.Event) error
}

// BatchRecorder is an optional capability. Implementations that can
// persist multiple events more efficiently than N serial Record calls
// should implement it. BufferedRecorder uses it when available and falls
// back to looped Record calls otherwise.
type BatchRecorder interface {
	RecordBatch(ctx context.Context, events []event.Event) error
}

// RecorderOption configures the Recorder returned by New. Both
// HTTPOption and BufferedOption satisfy RecorderOption, so existing
// options can be passed to New without wrapping:
//
//	r := recorder.New(projectID, apiKey,
//	    recorder.WithBufferSize(500),   // BufferedOption
//	    recorder.WithBaseURL(staging),  // HTTPOption
//	)
type RecorderOption interface {
	applyRecorder(*recorderConfig)
}

// recorderConfig accumulates options before they are dispatched to
// NewHTTPRecorder and NewBufferedRecorder inside New.
type recorderConfig struct {
	httpOpts     []HTTPOption
	bufferedOpts []BufferedOption
}

// New is the recommended entry point. It returns a *BufferedRecorder
// wrapping an HTTPRecorder with the package's default buffer/flush
// settings. Both HTTPOption and BufferedOption can be passed via opts;
// each is dispatched to the appropriate inner constructor.
//
// Use NewHTTPRecorder and NewBufferedRecorder directly for a custom
// inner Recorder (dual-write, custom transport, instrumented wrapper)
// or for synchronous writes.
//
// Call Close on the returned Recorder during shutdown to drain pending
// events.
func New(projectID, apiKey string, opts ...RecorderOption) *BufferedRecorder {
	cfg := recorderConfig{}
	for _, opt := range opts {
		opt.applyRecorder(&cfg)
	}
	inner := NewHTTPRecorder(projectID, apiKey, cfg.httpOpts...)
	return NewBufferedRecorder(inner, cfg.bufferedOpts...)
}
