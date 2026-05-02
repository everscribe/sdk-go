package event

import (
	"context"
	"net/http"
)

type eventTemplateKey struct{}
type wrappedWriterKey struct{}

// NewMiddleware returns an http middleware that installs an Event
// template on the request context for downstream handlers to retrieve
// via FromContext. The template has Origin populated from the
// request and Actor populated by the resolver.
//
// Handlers typically defer Recorder.Record at the top of the handler,
// then enrich the event (Action, Target, Metadata) and let the deferred
// Record call persist it. The returned middleware also wraps the
// ResponseWriter and stashes it on the context so Record can
// auto-populate Event.Result from the final HTTP status when the
// handler has not set it explicitly.
//
// The returned middleware must run AFTER any auth middleware that
// attaches session data to the request context — the ActorResolver
// typically reads from session state. If resolve is nil, an anonymous
// actor is installed.
//
// Typical chain order: Logging -> CSRF -> Session -> Audit -> Routes.
func NewMiddleware(resolve ActorResolver) func(http.Handler) http.Handler {
	if resolve == nil {
		resolve = func(context.Context) Actor {
			return Actor{Type: "anonymous"}
		}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tmpl := &Event{
				Actor:  resolve(r.Context()),
				Origin: originFromRequest(r),
			}
			ww := wrapWriter(w)
			ctx := context.WithValue(r.Context(), eventTemplateKey{}, tmpl)
			ctx = context.WithValue(ctx, wrappedWriterKey{}, ww)
			next.ServeHTTP(ww, r.WithContext(ctx))
		})
	}
}

// ActorResolver derives an Actor from request context. Typically reads
// session data attached by an upstream auth middleware. The recorder package
// does not know about any specific session type — each server wires up
// a resolver that matches its own auth model.
type ActorResolver func(ctx context.Context) Actor

// responseWriter wraps http.ResponseWriter to capture the final HTTP
// status code so Recorder implementations can derive Event.Result from
// it.
type responseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func wrapWriter(w http.ResponseWriter) *responseWriter {
	if rw, ok := w.(*responseWriter); ok {
		return rw
	}
	return &responseWriter{ResponseWriter: w}
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

// Status returns the HTTP status code captured by the wrapped writer,
// or 0 if no header has been written yet.
func (rw *responseWriter) Status() int {
	return rw.status
}

// resultFromWrappedWriter returns a Result derived from a wrapped
// ResponseWriter's captured status. Status 0 (no response written) maps
// to an error result with a descriptive message — this typically
// indicates a panic or early return before any response.
func resultFromWrappedWriter(rw *responseWriter) Result {
	status := rw.Status()
	if status == 0 {
		return Result{Status: "error", Message: "no response written"}
	}
	r := Result{Code: status}
	switch {
	case status >= 200 && status < 400:
		r.Status = "ok"
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		r.Status = "denied"
	default:
		r.Status = "error"
	}
	return r
}
