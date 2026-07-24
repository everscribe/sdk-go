<div align="center">

<p>
  <img src="assets/everscribe.svg" alt="Everscribe" height="64" align="middle">
  &nbsp;&nbsp;<b>+</b>&nbsp;&nbsp;
  <img src="assets/go.svg" alt="Go" height="56" align="middle">
</p>

<p>
  <a href="https://pkg.go.dev/github.com/everscribe/sdk-go"><img src="https://pkg.go.dev/badge/github.com/everscribe/sdk-go.svg" alt="Go Reference"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT"></a>
</p>

# everscribe/sdk-go

Go SDK for the [Everscribe](https://everscribe.io) API. It exposes **middleware**
that records an audit event per request, a **recorder** for writing append-only
audit events directly, and a **minter** for issuing short-lived browser tokens
for [embeddable components](https://github.com/everscribe/components).

## 📖 Documentation

[Setup with AI](https://everscribe.io/docs/quickstart/overview#pick-a-setup-path): hosted agent or BYOK Claude Code skill

[DIY](https://everscribe.io/docs/sdks/go-install): install and wire the SDK up yourself

[Full-stack runnable examples](https://github.com/everscribe/examples): end-to-end sample apps

</div>

## Supported frameworks

All adapters live in `pkg/event` and take the same `event.Options`.

| Protocol | Framework | Mount |
|---|---|---|
| HTTP | `net/http`, chi, gorilla/mux | `event.Middleware` |
| HTTP | gin | `event.GinMiddleware` |
| HTTP | echo v4 | `event.EchoV4Middleware` |
| HTTP | fiber v3 | `event.FiberV3Middleware` |
| gRPC | grpc-go | `event.UnaryInterceptor`, `event.StreamInterceptor` |

chi and gorilla/mux need no adapter of their own: both are plain
`func(http.Handler) http.Handler`, so `event.Middleware` mounts directly.

```go
mw := event.Middleware(event.Options{
    Recorder: recorder.New(projectID, apiKey),
    Resolve:  func(ctx context.Context) event.Actor { /* ... */ },
})

func handleLogin(w http.ResponseWriter, r *http.Request) {
    event.Current(r.Context()).Action = "user.login"
    // the middleware records it once the handler returns
}
```

Adapters are not identical in every respect. [docs/adapters.md](docs/adapters.md)
compares them and documents two behavioral divergences worth knowing before you
assume one framework's behavior carries to another.

A framework missing? [Open an issue](https://github.com/everscribe/sdk-go/issues).
Adding one is a single file against the same lifecycle.
