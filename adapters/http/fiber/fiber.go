// Package fiber is the gofiber/fiber v3 adapter.
//
// Fiber is built on fasthttp rather than net/http, so there is no
// *http.Request to reach for: headers come from c.Get, the client IP from
// c.IP(), and the status from c.Response().StatusCode(). This is the one
// adapter where the abstraction leaks, and it is contained here.
//
// v3 only. In v3, fiber.Ctx implements context.Context via Context() and
// SetContext, so ActorResolver takes it directly. v2 used c.UserContext()
// and c.SetUserContext, which v3 renamed (and repurposed Context() /
// SetContext for the fasthttp-backed context.Context, moving the old
// fasthttp accessor to RequestCtx()), so a v2 adapter would need its own
// module.
//
// Limitation: fasthttp's Response.StatusCode() defaults to 200 whether or
// not a handler wrote anything, and fiber keeps no "was anything written"
// flag on Ctx or on the underlying fasthttp response. So this adapter
// cannot distinguish "handler returned nil without writing" from "handler
// wrote a 200" by inspecting the response alone; both look identical at
// that layer, unlike gin's Writer.Written() or echo's Response().Committed.
// Completion is instead tracked explicitly: the capture's completed field
// is set only after c.Next() returns, so a handler that panics never sets
// it and Outcome reports ok == false, same as an in-flight request. A
// handler that returns nil having written nothing is indistinguishable
// from one that wrote 200, and is recorded as ok == true, Result{Code:
// 200, Status: "ok"}.
package fiber

import (
	"context"
	"log/slog"

	fiberv3 "github.com/gofiber/fiber/v3"

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

// New returns fiber middleware that installs a per-request event and
// records it once after the handler chain completes. Handlers reach it
// with event.Current(c.Context()).
//
// Mount it AFTER any auth middleware, since Resolve typically reads
// session state. If a panic-recovery middleware (such as
// gofiber/fiber/v3/middleware/recover) is also mounted, put it BEFORE this
// one (outermost), so a panicking handler still crashes past this
// middleware's own defer instead of skipping it: the audited event still
// records with ok == false, and recover then converts the panic into the
// response.
func New(opts Options) fiberv3.Handler {
	resolve := opts.Resolve
	if resolve == nil {
		resolve = func(context.Context) event.Actor { return event.Actor{Type: "anonymous"} }
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return func(c fiberv3.Ctx) error {
		oc := &capture{c: c}
		tmpl := &event.Event{
			Actor: resolve(c.Context()),
			Origin: event.OriginFrom(
				func(name string) string { return c.Get(name) },
				c.IP(),
			),
		}
		ctx, end := event.Begin(c.Context(), tmpl, oc, opts.Recorder, logger)
		defer end()
		c.SetContext(ctx)

		err := c.Next()
		oc.completed = true // reached only if the chain returned without panicking
		return err
	}
}

// capture reads fiber's response state.
//
// Fiber has no "was anything written" flag: Response().StatusCode()
// defaults to 200 whether or not the handler wrote. So completion is
// tracked explicitly instead. A panicking handler never sets it, which is
// what makes ok == false meaningful here. See the package doc comment for
// the resulting limitation: a handler that writes nothing and returns nil
// is recorded identically to one that wrote a real 200.
type capture struct {
	c         fiberv3.Ctx
	completed bool
}

// Outcome implements event.OutcomeCapture.
func (oc *capture) Outcome() (event.Result, bool) {
	if !oc.completed {
		return event.Result{}, false
	}
	return event.ResultFromHTTPStatus(oc.c.Response().StatusCode()), true
}
