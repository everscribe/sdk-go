# Adapter comparison

Reference for choosing an adapter. Read once before mounting one; not a
tutorial. All six adapters live in the root `github.com/everscribe/sdk-go`
module, in package `github.com/everscribe/sdk-go/pkg/event`, alongside the
rest of the event lifecycle.

## At a glance

| Adapter | Framework / version | Mount signature | Outcome derived from |
|---|---|---|---|
| `pkg/event` (stdlib) | `net/http` (stdlib), Go 1.25.0. Also covers chi and gorilla/mux, which are both plain `func(http.Handler) http.Handler` | `event.Middleware(event.Options{...}) func(http.Handler) http.Handler` | A wrapping `stdlibResponseWriter`'s own `wroteHeader` flag |
| `pkg/event` (gin) | `github.com/gin-gonic/gin` v1.10.0 | `event.GinMiddleware(event.Options{...}) gin.HandlerFunc` | `c.Writer.Written()` and `c.Writer.Status()` |
| `pkg/event` (echo) | `github.com/labstack/echo/v4` v4.12.0 | `event.EchoMiddleware(event.Options{...}) echo.MiddlewareFunc` | `c.Response().Committed` and `c.Response().Status` |
| `pkg/event` (fiber) | `github.com/gofiber/fiber/v3` v3.4.0 (v3 only; see below) | `event.FiberMiddleware(event.Options{...}) fiber.Handler` | An explicit `completed` flag set after `c.Next()` returns, plus `c.Response().StatusCode()` |
| `pkg/event` (gRPC) | `google.golang.org/grpc` v1.68.0 | `event.UnaryInterceptor(event.Options{...})` / `event.StreamInterceptor(event.Options{...})` | The error returned by the handler, mapped through the canonical gRPC-to-HTTP status table |

All six mount points share one `event.Options`: `Resolve`
(`event.ActorResolver`, nil yields an anonymous actor), `Recorder`
(`event.Recorder`, nil installs the event but does not auto-record), and
`Logger` (`event.Logger`, nil defaults to `slog.Default()`).

## Behavioral divergences

These are the places where the same handler behavior is recorded
differently depending on which adapter sits in front of it. Read this
section before assuming an adapter's behavior generalizes to another one.

### "Handler returned without writing a response"

gin, echo, and stdlib can all tell the difference between "a real 200 was
written" and "the handler returned having written nothing" (a panic, an
early return with no write, a bug), because each of their underlying
frameworks exposes a real written-signal:

- gin: `c.Writer.Written()`
- echo: `c.Response().Committed`
- stdlib: the adapter's own `stdlibResponseWriter.wroteHeader`, since
  `net/http` itself exposes no such flag

All three record that case as `Result{Status: "error", Message: "no
response written"}`, with `Code` left at zero.

**fiber records that same case as `ok` / 200.** fasthttp, which fiber v3 is
built on, exposes no written-signal at all:
`(*fasthttp.ResponseHeader).StatusCode()` returns `200` whenever the
internal status code field is its zero value, regardless of whether a
handler ever wrote anything, and there is no additional "was anything
written" flag anywhere on the request or response struct. A handler that
returns `nil` without writing is therefore indistinguishable, at the
fasthttp layer, from one that deliberately wrote `200`. The fiber adapter
tracks its own `completed` flag (set only after `c.Next()` returns without
panicking) so it can still report `ok == false` while a request is in
flight or the handler panicked, but once `c.Next()` returns normally with
nothing written, the adapter has no way to tell that apart from a real
200 and records `Result{Code: 200, Status: "ok"}`.

In practice: the exact same handler bug (a code path that returns without
calling `w.WriteHeader` or `w.Write`) shows up as a recorded `error` under
gin, echo, or stdlib, and as a recorded `ok` / 200 under fiber. This is a
real, permanent limitation of building on fasthttp, not a bug in the
adapter, and it is documented in the fiber adapter's doc comment
(`pkg/event/adapter_fiber.go`).

### gRPC records every RPC by default

The HTTP adapters record nothing until a handler explicitly names the
event by setting `Action` (typically via `event.Current(ctx).Action = "..."`).
An unnamed event is never recorded - a handler that early-returns before
naming anything just does not emit garbage events.

The gRPC adapter is different: both `UnaryInterceptor` and
`StreamInterceptor` stamp `Action = info.FullMethod` on the request-scoped
event immediately after `Begin` returns, so every RPC is recorded unless
the handler deliberately clears `event.Current(ctx).Action`. This is
intentional: gRPC method names are a closed, meaningful set in a way
arbitrary HTTP routes are not, so recording every call by default is the
more useful default for this protocol.

The stamp is applied to `event.Current(ctx)`, the request-scoped event,
not to the template `Begin` installs. `event.FromContext` clones the
template, so a clone a handler makes for a secondary event does **not**
inherit the RPC method name - it comes back unnamed, exactly like the HTTP
adapters, and is dropped by the empty-`Action` guard every stock recorder
applies unless the handler names it itself.

### fiber's v3-only constraint

The fiber adapter targets `gofiber/fiber/v3` only. In v3, `fiber.Ctx`
implements `context.Context` directly via `Context()` / `SetContext`,
which is what lets `ActorResolver` take it as-is. v2 used
`c.UserContext()` / `c.SetUserContext` for the same purpose and repurposed
`Context()` for the fasthttp-backed context, so a v2 adapter would need
its own package rather than a version bump of this one.
