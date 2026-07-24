package event

import (
	fiberv3 "github.com/gofiber/fiber/v3"
)

// FiberV3Middleware is the gofiber/fiber v3 adapter.
//
// Fiber is built on fasthttp rather than net/http, so there is no
// *http.Request to reach for: headers come from c.Get, the remote address
// from c.RequestCtx().RemoteAddr(), and the status from
// c.Response().StatusCode(). This is the one adapter where the abstraction
// leaks, and it is contained here.
//
// The remote address deliberately goes through RequestCtx().RemoteAddr()
// rather than the more obvious c.IP(): c.IP() returns a bare IP with no
// port, but OriginFrom's remoteAddr parameter expects a real host:port pair
// (it strips a trailing port when present). Passing a bare IPv6 literal
// there corrupts it - "2001:db8::1" would come back as "2001:db8:" -
// because the bare address has colons of its own that look like a port
// separator. RemoteAddr().String() always includes the port (and brackets
// IPv6 hosts), so it is unambiguous.
//
// v3 only. In v3, fiber.Ctx implements context.Context via Context() and
// SetContext, so ActorResolver takes it directly. v2 used c.UserContext()
// and c.SetUserContext, which v3 renamed (and repurposed Context() /
// SetContext for the fasthttp-backed context.Context, moving the old
// fasthttp accessor to RequestCtx()), so a v2 adapter would need its own
// module.
//
// It returns fiber middleware that installs a per-request event and
// records it once after the handler chain completes. Handlers reach it
// with Current(c.Context()).
//
// Mount it AFTER any auth middleware, since Resolve typically reads
// session state. If a panic-recovery middleware (such as
// gofiber/fiber/v3/middleware/recover) is also mounted, put it BEFORE this
// one (outermost), so a panicking handler still crashes past this
// middleware's own defer instead of skipping it: the audited event still
// records with ok == false, and recover then converts the panic into the
// response.
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
