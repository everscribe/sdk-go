// Package stdlib is a PROTOTYPE net/http adapter for the record lifecycle in
// docs/specs/sdk-adapter-architecture.md. It exists to validate the adapter
// contract (Begin, Current, OutcomeCapture, adapter-owned recording) before
// the real core refactor, and to exercise the two cases the Postgres contract
// harness could not reach: client disconnect and a panicking handler.
//
// It lives in the sdk-go module for now. The real one gets its own go.mod.
package stdlib

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/everscribe/sdk-go/pkg/event"
)

// Options configures the middleware. An options struct rather than positional
// parameters, matching ExpressMiddlewareOptions and EverscribeMiddleware.
type Options struct {
	// Resolve derives the Actor. nil yields an anonymous actor.
	Resolve event.ActorResolver
	// Recorder receives the auto-recorded event. nil installs the event but
	// does not auto-record.
	Recorder event.Recorder
	// Logger receives record failures. nil defaults to slog.Default().
	Logger event.Logger
}

// New returns middleware that installs a per-request event and records it once
// after the handler completes. Handlers reach it via event.Current(ctx) and
// name it by setting Action; an unnamed event is never recorded.
//
// end is deferred, so it also runs while a panic unwinds. The capture then
// reports no outcome and core supplies the "no response written" diagnostic.
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
				Actor: resolve(r.Context()),
				Origin: event.OriginFrom(
					func(name string) string { return r.Header.Get(name) },
					r.RemoteAddr,
				),
			}

			ctx, end := event.Begin(r.Context(), tmpl, rw, opts.Recorder, logger)
			defer end()

			next.ServeHTTP(rw, r.WithContext(ctx))
		})
	}
}

// responseWriter captures the final status and doubles as the OutcomeCapture.
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

// Outcome implements event.OutcomeCapture. ok is false until a response has
// actually been written, which is what distinguishes "still in flight, or the
// handler panicked" from a real status. A bare integer could not: gRPC's OK is
// code 0, the same value that used to mean "nothing written".
func (rw *responseWriter) Outcome() (event.Result, bool) {
	if !rw.wroteHeader {
		return event.Result{}, false
	}
	return event.ResultFromHTTPStatus(rw.status), true
}
