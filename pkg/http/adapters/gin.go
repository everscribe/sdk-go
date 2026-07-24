package adapters

import (
	gingonic "github.com/gin-gonic/gin"

	"github.com/everscribe/sdk-go/pkg/event"
)

// GinEventMiddleware is the gin-gonic adapter. It returns gin middleware
// that installs a per-request event and records it once after the handler
// chain completes. Handlers reach it with
// event.Current(c.Request.Context()).
//
// Mount it AFTER any auth middleware, since Resolve typically reads
// session state.
func GinEventMiddleware(opts Options) gingonic.HandlerFunc {
	resolve, logger := opts.resolve(), opts.logger()

	return func(c *gingonic.Context) {
		tmpl := &event.Event{
			Actor:  resolve(c.Request.Context()),
			Origin: event.OriginFrom(c.GetHeader, c.Request.RemoteAddr),
		}
		ctx, end := event.Begin(c.Request.Context(), tmpl, ginCapture{c}, opts.Recorder, logger)
		defer end()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}

// ginCapture reads gin's own response state. gin already tracks the
// status, so there is no ResponseWriter to wrap.
type ginCapture struct{ c *gingonic.Context }

// Outcome implements event.OutcomeCapture.
//
// Written() is load-bearing and Status() alone is not sufficient: gin
// initializes the status to 200 before anything is written, so a panic or
// an abort with no write would otherwise report a successful 200.
func (g ginCapture) Outcome() (event.Result, bool) {
	if !g.c.Writer.Written() {
		return event.Result{}, false
	}
	return event.ResultFromHTTPStatus(g.c.Writer.Status()), true
}
