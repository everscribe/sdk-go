// Package stdlib is the net/http adapter. It also covers chi and
// gorilla/mux, which are both plain func(http.Handler) http.Handler and
// need no adapter of their own.
package stdlib

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/everscribe/sdk-go/pkg/event"
)

// Options configures the middleware.
type Options struct {
	// Resolve derives the Actor. nil yields an anonymous actor.
	Resolve event.ActorResolver
	// Recorder receives the auto-recorded event. nil installs the event
	// but does not auto-record.
	Recorder event.Recorder
	// Logger receives record failures. nil defaults to slog.Default().
	Logger event.Logger
}

// New returns middleware that installs a per-request event and records it
// once after the handler completes. Handlers reach it with
// event.Current(r.Context()) and name it by setting Action; an unnamed
// event is never recorded.
//
// Mount it AFTER any auth middleware, since Resolve typically reads
// session state. end is deferred, so it also runs while a panic unwinds.
func New(opts Options) func(http.Handler) http.Handler {
	resolve := opts.Resolve
	if resolve == nil {
		resolve = func(context.Context) event.Actor { return event.Actor{Type: "anonymous"} }
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rw := &responseWriter{ResponseWriter: w}
			tmpl := &event.Event{
				Actor:  resolve(r.Context()),
				Origin: event.OriginFrom(r.Header.Get, r.RemoteAddr),
			}
			ctx, end := event.Begin(r.Context(), tmpl, rw, opts.Recorder, logger)
			defer end()
			next.ServeHTTP(rw, r.WithContext(ctx))
		})
	}
}

// responseWriter captures the final status and doubles as the
// event.OutcomeCapture.
type responseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (rw *responseWriter) WriteHeader(code int) {
	if !rw.wroteHeader {
		rw.status = code
		rw.wroteHeader = true
	}
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.wroteHeader {
		rw.status = http.StatusOK
		rw.wroteHeader = true
	}
	return rw.ResponseWriter.Write(b)
}

// Outcome implements event.OutcomeCapture. ok is false until a response is
// actually written, which distinguishes "still in flight, or the handler
// panicked" from a real status. A bare integer could not: gRPC's OK is
// code 0, the same value that used to mean nothing written.
func (rw *responseWriter) Outcome() (event.Result, bool) {
	if !rw.wroteHeader {
		return event.Result{}, false
	}
	return event.ResultFromHTTPStatus(rw.status), true
}
