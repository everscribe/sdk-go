package adapters

import (
	echov4 "github.com/labstack/echo/v4"

	"github.com/everscribe/sdk-go/pkg/event"
)

// EchoEventMiddleware is the labstack/echo adapter. It returns echo
// middleware that installs a per-request event and records it once after
// the handler completes. Handlers reach it with
// event.Current(c.Request().Context()).
//
// Mount it AFTER any auth middleware, since Resolve typically reads
// session state.
func EchoEventMiddleware(opts Options) echov4.MiddlewareFunc {
	resolve, logger := opts.resolve(), opts.logger()

	return func(next echov4.HandlerFunc) echov4.HandlerFunc {
		return func(c echov4.Context) error {
			r := c.Request()
			tmpl := &event.Event{
				Actor:  resolve(r.Context()),
				Origin: event.OriginFrom(r.Header.Get, r.RemoteAddr),
			}
			ctx, end := event.Begin(r.Context(), tmpl, echoCapture{c}, opts.Recorder, logger)
			defer end()
			c.SetRequest(r.WithContext(ctx))
			return next(c)
		}
	}
}

// echoCapture reads echo's own response state.
type echoCapture struct{ c echov4.Context }

// Outcome implements event.OutcomeCapture.
//
// Committed is load-bearing for the same reason gin needs Written():
// echo initializes Status to 200 before anything is written, so a
// handler that returns without writing would otherwise report a
// successful 200.
func (e echoCapture) Outcome() (event.Result, bool) {
	res := e.c.Response()
	if !res.Committed {
		return event.Result{}, false
	}
	return event.ResultFromHTTPStatus(res.Status), true
}
