package event

import (
	echov4 "github.com/labstack/echo/v4"
)

// EchoV4Middleware is the labstack/echo adapter. It returns echo middleware
// that installs a per-request event and records it once after the handler
// completes. Handlers reach it with Current(c.Request().Context()).
//
// Mount it AFTER any auth middleware, since ActorResolver typically reads
// session state.
func EchoV4Middleware(opts Options) echov4.MiddlewareFunc {
	resolve, logger := opts.resolve(), opts.logger()

	return func(next echov4.HandlerFunc) echov4.HandlerFunc {
		return func(c echov4.Context) error {
			r := c.Request()
			tmpl := &Event{
				Actor:  resolve(r.Context()),
				Origin: OriginFrom(r.Header.Get, r.RemoteAddr),
			}
			ctx, end := Begin(r.Context(), tmpl, echoCapture{c}, opts.Recorder, logger)
			defer end()
			c.SetRequest(r.WithContext(ctx))
			return next(c)
		}
	}
}

// echoCapture reads echo's own response state.
type echoCapture struct{ c echov4.Context }

// Outcome implements OutcomeCapture.
//
// Committed is load-bearing for the same reason gin needs Written(): echo
// initializes Status to 200 before anything is written, so a handler that
// returns without writing would otherwise report a successful 200.
func (e echoCapture) Outcome() (Result, bool) {
	res := e.c.Response()
	if !res.Committed {
		return Result{}, false
	}
	return ResultFromHTTPStatus(res.Status), true
}
