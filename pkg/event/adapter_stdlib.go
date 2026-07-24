package event

import (
	"bufio"
	"net"
	"net/http"
)

// Middleware is the net/http adapter. It also covers chi and gorilla/mux,
// which are both plain func(http.Handler) http.Handler and need no adapter
// of their own.
//
// It returns middleware that installs a per-request event and records it
// once after the handler completes. Handlers reach it with
// Current(r.Context()) and name it by setting Action; an unnamed event is
// never recorded.
//
// Mount it AFTER any auth middleware, since Resolve typically reads
// session state. end is deferred, so it also runs while a panic unwinds.
func Middleware(opts Options) func(http.Handler) http.Handler {
	resolve, logger := opts.resolve(), opts.logger()

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rw := &stdlibResponseWriter{ResponseWriter: w}
			tmpl := &Event{
				Actor:  resolve(r.Context()),
				Origin: OriginFrom(r.Header.Get, r.RemoteAddr),
			}
			ctx, end := Begin(r.Context(), tmpl, rw, opts.Recorder, logger)
			defer end()
			next.ServeHTTP(rw, r.WithContext(ctx))
		})
	}
}

// stdlibResponseWriter captures the final status and doubles as the
// OutcomeCapture.
//
// Embedding http.ResponseWriter only promotes the three methods that
// interface declares (Header, Write, WriteHeader). It does not make
// stdlibResponseWriter satisfy http.Flusher, http.Hijacker, or anything
// else the underlying writer might implement, so a handler behind this
// middleware that needs SSE (Flusher) or a websocket upgrade (Hijacker)
// would silently lose that capability. Unwrap lets http.ResponseController
// reach through to the real writer; Flush and Hijack are explicit
// passthroughs for callers that type-assert directly instead of going
// through the controller.
type stdlibResponseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (rw *stdlibResponseWriter) WriteHeader(code int) {
	if !rw.wroteHeader {
		rw.status = code
		rw.wroteHeader = true
	}
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *stdlibResponseWriter) Write(b []byte) (int, error) {
	if !rw.wroteHeader {
		rw.status = http.StatusOK
		rw.wroteHeader = true
	}
	return rw.ResponseWriter.Write(b)
}

// Unwrap returns the wrapped http.ResponseWriter. http.ResponseController
// uses this to reach optional interfaces (Flusher, Hijacker, and the rest)
// that stdlibResponseWriter does not itself implement, per the Unwrap
// contract ResponseController documents.
func (rw *stdlibResponseWriter) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}

// Flush implements http.Flusher for callers that type-assert directly
// rather than going through http.ResponseController. no-ops when the
// underlying writer does not support flushing, the same as a bare
// http.ResponseWriter that lacks Flusher would from the caller's
// perspective (a failed type assertion just skips the call).
func (rw *stdlibResponseWriter) Flush() {
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack implements http.Hijacker for callers that type-assert directly
// rather than going through http.ResponseController. Returns
// http.ErrNotSupported when the underlying writer does not support
// hijacking, matching net/http's own convention for writers without it.
func (rw *stdlibResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := rw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	return hj.Hijack()
}

// Outcome implements OutcomeCapture. ok is false until a response is
// actually written, which distinguishes "still in flight, or the handler
// panicked" from a real status. A bare integer could not: gRPC's OK is
// code 0, the same value that used to mean nothing written.
func (rw *stdlibResponseWriter) Outcome() (Result, bool) {
	if !rw.wroteHeader {
		return Result{}, false
	}
	return ResultFromHTTPStatus(rw.status), true
}
