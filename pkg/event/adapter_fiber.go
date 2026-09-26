package event

import (
	fiberv3 "github.com/gofiber/fiber/v3"
)

// FiberV3Middleware is the gofiber/fiber v3 adapter.
//
// Fiber is built on fasthttp, not net/http, so there is no *http.Request
// to reach for: headers come from c.Get, the remote address from
// c.RequestCtx().RemoteAddr(), and the status from
// c.Response().StatusCode(). This is the one adapter where the
// abstraction leaks, contained here.
//
// The remote address goes through RequestCtx().RemoteAddr(), not the more
// obvious c.IP(): c.IP() returns a bare IP with no port, but OriginFrom's
// remoteAddr expects a host:port pair and strips a trailing port when
// present. A bare IPv6 literal would corrupt there - "2001:db8::1" back as
// "2001:db8:" - since its own colons look like a port separator.
// RemoteAddr().String() always includes the port (bracketing IPv6), so
// it's unambiguous.
//
// v3 only, for now. fiber.Ctx satisfies context.Context, but its Value
// method reads fiber Locals, not a Go context chain, so this adapter
// passes c.Context() to the resolver and to Begin, never c itself.
// Handlers must do the same: Current(c) compiles but returns a throwaway
// that is never recorded. v2 used c.UserContext()/SetUserContext, which
// v3 renamed to Context()/SetContext (moving the old fasthttp accessor
// to RequestCtx()).
//
// A FiberV2Middleware could live in this same package: fiber/v2 and
// fiber/v3 are distinct module paths under semantic import versioning, so
// one package may import both and minimal version selection resolves each
// independently. The cost is fiber v2's dependency tree joining the
// module graph.
//
// It returns fiber middleware that installs a per-request event and
// records it once after the handler chain completes. Handlers reach it
// with Current(c.Context()).
//
// Mount it AFTER any auth middleware, since ActorResolver typically reads
// session state. If a panic-recovery middleware (e.g.
// gofiber/fiber/v3/middleware/recover) is also mounted, put it BEFORE this
// one (outermost), so a panicking handler still crashes past this
// middleware's defer: the event still records with ok == false, and
// recover then converts the panic into the response.
func FiberV3Middleware(opts Options) fiberv3.Handler {
	resolve, logger := opts.resolve(), opts.logger()

	return func(c fiberv3.Ctx) error {
		oc := &fiberCapture{c: c}
		tmpl := &Event{
			Actor: resolve(c.Context()),
			Origin: OriginFrom(
				func(name string) string { return c.Get(name) },
				c.RequestCtx().RemoteAddr().String(),
			),
		}
		ctx, end := Begin(c.Context(), tmpl, oc, opts.Recorder, logger)
		defer end()
		c.SetContext(ctx)

		err := c.Next()
		oc.completed = true // reached only if the chain returned without panicking
		return err
	}
}

// fiberCapture reads fiber's response state.
//
// Limitation: fasthttp's StatusCode() defaults to 200 whether or not a
// handler wrote anything, and fiber has no "was anything written" flag
// (unlike gin's Writer.Written() or echo's Response().Committed), so a
// nil-without-writing handler is indistinguishable from one that wrote
// 200 and records as ok == true, Result{Code: 200, Status: "ok"}.
// completed is set only after c.Next() returns, so a panicking handler
// leaves it false and Outcome reports ok == false, like an in-flight request.
type fiberCapture struct {
	c         fiberv3.Ctx
	completed bool
}

// Outcome implements OutcomeCapture.
func (oc *fiberCapture) Outcome() (Result, bool) {
	if !oc.completed {
		return Result{}, false
	}
	return ResultFromHTTPStatus(oc.c.Response().StatusCode()), true
}
