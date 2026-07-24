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

</div>

## 📖 Setup Documentation

| Setup link | What it is | Pick it when |
|---|---|---|
| [Hosted AI agent](https://everscribe.io/docs/quickstart/hosted-ui) | You install our GitHub App. Our server-side agent walks your repo, proposes a plan, and opens a PR you review. | You want zero hand-wiring and are fine granting Everscribe read access to your repo. Paid feature. |
| [BYOK Claude Code skill](https://everscribe.io/docs/quickstart/skill) | Install the [`es` CLI](https://github.com/everscribe/cli) and run `es skills download setup`. That drops a skill into your Claude Code skills directory, and your own local Claude agent does the setup. Same behavior as the hosted agent, but against *your* Anthropic key, and your source never leaves your machine. | You do not want to grant Everscribe read access to your repo, and/or you want a free option and already have Claude Code. |
| [Wire it up yourself](https://everscribe.io/docs/sdks/go-install) | Install the SDK and add the calls by hand. | You want full control, or you are instrumenting a small surface. |

## Supported frameworks

- `net/http`
- `chi`
- `gorilla/mux`
- `gin`
- `echo v4`
- `fiber v3`

Your framework missing? [Open an issue](https://github.com/everscribe/sdk-go/issues).



**gRPC**

- grpc-go: `event.UnaryInterceptor`, `event.StreamInterceptor`

chi and gorilla/mux need no adapter of their own: both are plain
`func(http.Handler) http.Handler`, so `event.Middleware` mounts directly.

```go
mw := event.Middleware(event.Options{
    Recorder:      recorder.New(projectID, apiKey),
    ActorResolver: func(ctx context.Context) event.Actor { /* ... */ },
})

func handleLogin(w http.ResponseWriter, r *http.Request) {
    event.Current(r.Context()).Action = "user.login"
    // the middleware records it once the handler returns
}
```

Adapters are not identical in every respect. [docs/adapters.md](docs/adapters.md)
compares them and documents two behavioral divergences worth knowing before you
assume one framework's behavior carries to another.

Adding one is a single file against the same lifecycle.

## Additional resources

[Runnable examples](https://github.com/everscribe/examples): end-to-end sample
apps, front end through back end, for every Everscribe SDK.