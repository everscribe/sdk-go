package audit

import "context"

// Recorder records audit events. Implementations may be synchronous
// (HTTPRecorder) or buffered (BufferedRecorder wraps another Recorder
// to add asynchronous batching).
//
// Implementations must treat an Event with an empty Action as a no-op
// and return nil without recording. This enables the `defer Record`
// idiom where handlers that bail out before setting Action produce no
// event.
//
// When called from an HTTP handler whose request context carries a
// middleware-wrapped ResponseWriter, implementations should populate
// e.Result from the captured HTTP status when e.Result.Status is empty.
// Callers that set e.Result explicitly win over auto-capture.
type Recorder interface {
	Record(ctx context.Context, e *Event) error
}

// BatchRecorder is an optional capability. Implementations that can
// persist multiple events more efficiently than N serial Record calls
// should implement it. BufferedRecorder uses it when available and falls
// back to looped Record calls otherwise.
type BatchRecorder interface {
	RecordBatch(ctx context.Context, events []Event) error
}

// RecorderOption configures the Recorder returned by NewRecorder. Both
// HTTPOption and BufferedOption satisfy RecorderOption, so existing
// options can be passed to NewRecorder without wrapping:
//
//	r := audit.NewRecorder(projectID, apiKey,
//	    audit.WithBufferSize(500),   // BufferedOption
//	    audit.WithBaseURL(staging),  // HTTPOption
//	)
type RecorderOption interface {
	applyRecorder(*recorderConfig)
}

// recorderConfig accumulates options before they are dispatched to
// NewHTTPRecorder and NewBufferedRecorder inside NewRecorder.
type recorderConfig struct {
	httpOpts     []HTTPOption
	bufferedOpts []BufferedOption
}

// NewRecorder is the recommended entry point. It returns a
// *BufferedRecorder that wraps an HTTPRecorder using the package's
// default buffer/flush settings. Both HTTPOption and BufferedOption can
// be passed via opts; each is dispatched to the appropriate inner
// constructor.
//
// Use NewHTTPRecorder and NewBufferedRecorder directly when you need a
// custom inner Recorder (dual-write, custom transport, instrumented
// wrapper) or synchronous writes.
//
// Call Close on the returned Recorder during shutdown to drain pending
// events.
func NewRecorder(projectID, apiKey string, opts ...RecorderOption) *BufferedRecorder {
	cfg := recorderConfig{}
	for _, opt := range opts {
		opt.applyRecorder(&cfg)
	}
	inner := NewHTTPRecorder(projectID, apiKey, cfg.httpOpts...)
	return NewBufferedRecorder(inner, cfg.bufferedOpts...)
}
