// Package gin is the gin-gonic adapter.
package gin

import (
	"context"
	"log/slog"

	gingonic "github.com/gin-gonic/gin"

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

// New returns gin middleware that installs a per-request event and records
// it once after the handler chain completes. Handlers reach it with
// event.Current(c.Request.Context()).
//
// Mount it AFTER any auth middleware, since Resolve typically reads
// session state.
func New(opts Options) gingonic.HandlerFunc {
	resolve, logger := opts.resolve(), opts.logger()

	return func(c *gingonic.Context) {
		tmpl := &event.Event{
			Actor:  resolve(c.Request.Context()),
			Origin: event.OriginFrom(c.GetHeader, c.Request.RemoteAddr),
		}
		ctx, end := event.Begin(c.Request.Context(), tmpl, capture{c}, opts.Recorder, logger)
		defer end()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}

// capture reads gin's own response state. gin already tracks the status,
// so there is no ResponseWriter to wrap.
type capture struct{ c *gingonic.Context }

// Outcome implements event.OutcomeCapture.
//
// Written() is load-bearing and Status() alone is not sufficient: gin
// initializes the status to 200 before anything is written, so a panic or
// an abort with no write would otherwise report a successful 200.
func (g capture) Outcome() (event.Result, bool) {
	if !g.c.Writer.Written() {
		return event.Result{}, false
	}
	return event.ResultFromHTTPStatus(g.c.Writer.Status()), true
}
