// Package stdlib is the net/http adapter. It also covers chi and
// gorilla/mux, which are both plain func(http.Handler) http.Handler and
// need no adapter of their own.
package stdlib

import (
	"bufio"
	"context"
	"log/slog"
	"net"
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

func (o Options) resolve() event.ActorResolver {
	if o.Resolve == nil {
		return func(context.Context) event.Actor { return event.Actor{Type: "anonymous"} }
	}
	return o.Resolve
}

func (o Options) logger() event.Logger {
	if o.Logger == nil {
		return slog.Default()
	}
	return o.Logger
}

// New returns middleware that installs a per-request event and records it
// once after the handler completes. Handlers reach it with
// event.Current(r.Context()) and name it by setting Action; an unnamed
// event is never recorded.
//
// Mount it AFTER any auth middleware, since Resolve typically reads
// session state. end is deferred, so it also runs while a panic unwinds.
func New(opts Options) func(http.Handler) http.Handler {
	resolve, logger := opts.resolve(), opts.logger()

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
//
// Embedding http.ResponseWriter only promotes the three methods that
// interface declares (Header, Write, WriteHeader). It does not make
// responseWriter satisfy http.Flusher, http.Hijacker, or anything else the
// underlying writer might implement, so a handler behind this middleware
// that needs SSE (Flusher) or a websocket upgrade (Hijacker) would silently
// lose that capability. Unwrap lets http.ResponseController reach through
// to the real writer; Flush and Hijack are explicit passthroughs for
// callers that type-assert directly instead of going through the
// controller.
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

// Unwrap returns the wrapped http.ResponseWriter. http.ResponseController
// uses this to reach optional interfaces (Flusher, Hijacker, and the rest)
// that responseWriter does not itself implement, per the Unwrap contract
// ResponseController documents.
func (rw *responseWriter) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}

// Flush implements http.Flusher for callers that type-assert directly
// rather than going through http.ResponseController. no-ops when the
// underlying writer does not support flushing, the same as a bare
// http.ResponseWriter that lacks Flusher would from the caller's
// perspective (a failed type assertion just skips the call).
func (rw *responseWriter) Flush() {
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack implements http.Hijacker for callers that type-assert directly
// rather than going through http.ResponseController. Returns
// http.ErrNotSupported when the underlying writer does not support
// hijacking, matching net/http's own convention for writers without it.
func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := rw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	return hj.Hijack()
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
