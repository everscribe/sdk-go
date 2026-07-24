// Package echo is the labstack/echo adapter.
package echo

import (
	"context"
	"log/slog"

	echov4 "github.com/labstack/echo/v4"

	"github.com/everscribe/sdk-go/pkg/event"
)

// Options configures the middleware. Same shape as every other adapter.
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

// New returns echo middleware that installs a per-request event and
// records it once after the handler completes. Handlers reach it with
// event.Current(c.Request().Context()).
//
// Mount it AFTER any auth middleware, since Resolve typically reads
// session state.
func New(opts Options) echov4.MiddlewareFunc {
	resolve, logger := opts.resolve(), opts.logger()

	return func(next echov4.HandlerFunc) echov4.HandlerFunc {
		return func(c echov4.Context) error {
			r := c.Request()
			tmpl := &event.Event{
				Actor:  resolve(r.Context()),
				Origin: event.OriginFrom(r.Header.Get, r.RemoteAddr),
			}
			ctx, end := event.Begin(r.Context(), tmpl, capture{c}, opts.Recorder, logger)
			defer end()
			c.SetRequest(r.WithContext(ctx))
			return next(c)
		}
	}
}

// capture reads echo's own response state.
type capture struct{ c echov4.Context }

// Outcome implements event.OutcomeCapture.
//
// Committed is load-bearing for the same reason gin needs Written():
// echo initializes Status to 200 before anything is written, so a
// handler that returns without writing would otherwise report a
// successful 200.
func (e capture) Outcome() (event.Result, bool) {
	res := e.c.Response()
	if !res.Committed {
		return event.Result{}, false
	}
	return event.ResultFromHTTPStatus(res.Status), true
}
