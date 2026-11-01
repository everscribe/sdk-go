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
that records one audit event per request once your handler names it, a **recorder** for writing append-only
audit events directly, and a **minter** for issuing short-lived browser tokens
for [embeddable components](https://github.com/everscribe/components).

</div>

## 📖 Setup Documentation

| Setup link | What it is | Pick it when |
|---|---|---|
| [BYOK Claude Code skill](https://everscribe.io/docs/quickstart/skill) | Install the [`es` CLI](https://github.com/everscribe/cli) and run `es skills download setup`. That drops a skill into your Claude Code skills directory, and your own local Claude agent does the setup. It runs against your Anthropic key, and your source never leaves your machine. | You already use Claude Code and want the agent to do the wiring. |
| [Wire it up yourself](https://everscribe.io/docs/sdks/go-install) | Install the SDK and add the calls by hand. | You want full control, or you are instrumenting a small surface. |

## Install

```sh
go get github.com/everscribe/sdk-go
```

Requires Go 1.25 or later.

## Quick start

Bind your credentials once, build a recorder, and mount the middleware. Each handler names its event, and the middleware records it when the handler returns.

```go
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/everscribe/sdk-go"
	"github.com/everscribe/sdk-go/pkg/event"
)

func main() {
	// Reads EVERSCRIBE_PROJECT_ID and EVERSCRIBE_API_KEY.
	es, err := everscribe.NewFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	rec := es.NewRecorder()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /users/{id}/lock", func(w http.ResponseWriter, r *http.Request) {
		e := event.Current(r.Context())
		e.Action = "user.lock"
		e.Target = event.Target{Type: "user", ID: r.PathValue("id")}
		// No Record call: the middleware records e when this handler returns.
	})

	audit := event.Middleware(event.Options{
		ActorResolver: actorFromSession,
		Recorder:      rec,
	})
	srv := &http.Server{Addr: ":8080", Handler: audit(mux)}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Print(err)
			stop()
		}
	}()
	<-ctx.Done()

	if err := srv.Shutdown(context.Background()); err != nil {
		log.Print(err)
	}
	// Close after Shutdown so in-flight requests can still record.
	if err := rec.Close(); err != nil {
		log.Printf("everscribe: drain incomplete: %v", err)
	}
}

// actorFromSession turns the identity your auth middleware planted on
// the context into an Actor.
func actorFromSession(ctx context.Context) event.Actor {
	return event.Actor{Type: "anonymous"}
}
```

A handler that never sets `Action` records nothing. In a server, close the recorder on a real shutdown path as above: a deferred `Close` in `main` never runs when the process is killed by a signal. See [Where `Close` goes](https://everscribe.io/docs/sdks/go-buffered-recorder#where-close-goes).

## Supported frameworks

- `net/http`
- `chi`
- `gorilla/mux`
- `gin`
- `echo v4`
- `fiber v3`

> [!NOTE]
> Interceptors available for gRPC servers. They name each RPC's event after its method, so every RPC is recorded by default.

All adapters live in `pkg/event` and take the same `event.Options`. See [Middleware and interceptors](https://everscribe.io/docs/sdks/go-http-middleware) for the mount call per framework.

Your framework missing? [Open an issue](https://github.com/everscribe/sdk-go/issues).

## Recording outside a request

Cron jobs, queue workers and CLIs build the event themselves and set `Result` by hand, since there is no response status to derive it from:

```go
e := event.New("secret.rotate")
e.Actor = event.Actor{Type: "service", ID: "rotation-worker"}
e.Target = event.Target{Type: "secret", ID: secretID}
e.Result = event.Result{Status: "ok"}
_ = rec.Record(ctx, e)
```

A handler that emits several events per request uses `event.NewFromContext` for the extras and records each one itself. See [Recorder](https://everscribe.io/docs/sdks/go-recorder).

## Embed tokens

The minter issues short-lived tokens for the [embeddable components](https://github.com/everscribe/components), so your frontend can show events without seeing your API key:

```go
m := es.NewMinter()
token, err := m.MintToken(ctx, minter.TokenOptions{
	TenantID:  user.TenantID, // omit for an unscoped, every-tenant view
	ExpiresIn: time.Hour,
})
```

Serve the token from a `POST` route behind your app's authentication, with `Cache-Control: no-store`. See [Minter](https://everscribe.io/docs/sdks/go-minter).

## What happens when delivery fails

`Record` only puts the event on an in-memory buffer, so your request path never waits on the network. A background worker sends batches and handles failures:

| Failure | What happens |
|---|---|
| Network error, timeout, `429` or `5xx` | Retried with jittered exponential backoff: 500ms first wait, 30s max wait, server `Retry-After` honored up to that cap. After 5 minutes the batch is dropped as `retries_exhausted`. Tune with `WithInitialBackoff`, `WithMaxBackoff` and `WithMaxRetryDuration`. |
| `400`, `409` or `413` | The batch is split in half and each half retried, so one bad event does not sink the rest. An event the server still refuses is dropped as `rejected`. |
| Any other `4xx` | The whole batch is dropped as `rejected`. |
| Buffer full | Set by the overflow policy. `PolicyDropNewest` (default) drops the new event as `buffer_full`, `PolicyBlock` makes `Record` wait for space, `PolicyError` returns `ErrBufferFull`. |
| `Close` drain timeout | The in-flight send is canceled and what is left is dropped as `shutdown`. |
| `Record` after `Close` | The event is dropped as `closed`, as is a `PolicyBlock` wait that `Close` ends. |

Pass `WithOnDrop` to hear about every dropped event:

```go
rec := recorder.New(projectID, apiKey,
    recorder.WithOnDrop(func(events []event.Event, reason recorder.DropReason, err error) {
        log.Printf("everscribe: dropped %d events (%s): %v", len(events), reason, err)
    }),
)
```

- `reason` is `buffer_full`, `rejected`, `retries_exhausted`, `shutdown` or `closed`.
- `err` is the last delivery error for `rejected` and `retries_exhausted`, the overflow error for a `buffer_full` drop from the middleware's own record, and nil otherwise.
- The callback must not block and must not call `Record`, `Flush` or `Close` on the same recorder. It may run on several goroutines at once, so make it safe for concurrent use.
- A dropped event may have been delivered anyway, for example when the response was lost. Resending it with its original ID is safe: the server deduplicates it.
- Delivered events are immutable. Dropped events cannot be recovered, so route `onDrop` to your own logs or queue.

**Always call `Close`.** It drains the buffer, with retries, for up to the drain timeout (30s by default, `WithDrainTimeout`). At the timeout it cancels the send, waits up to 1s more, and returns `context.DeadlineExceeded`.

**Give `Flush` a deadline.** `Flush` returns `nil` once the events queued at the call are delivered or reported to `onDrop`, or `ctx.Err()` if your context ends first. It does not return delivery errors and never cancels a send. Retries during an outage can take minutes, so use `context.WithTimeout`.

**Watch `Stats()`.** `DroppedByReason` counts drops per reason, `Retries` counts retries, and `Dropped` counts only `buffer_full` drops.

## Go SDK reference

| Page | Covers |
|---|---|
| [Install](https://everscribe.io/docs/sdks/go-install) | Module path and imports. |
| [Credential binding](https://everscribe.io/docs/sdks/go-credential-binding) | `everscribe.New`, `NewFromEnv`, and single-surface constructors. |
| [Event shape](https://everscribe.io/docs/sdks/go-event-shape) | The `Event` type and its fields. |
| [BufferedRecorder](https://everscribe.io/docs/sdks/go-buffered-recorder) | Options, overflow policies, `Close`, `Flush` and delivery failures. |
| [Middleware and interceptors](https://everscribe.io/docs/sdks/go-http-middleware) | Mounting per framework, actor resolvers, gRPC. |
| [Recorder](https://everscribe.io/docs/sdks/go-recorder) | Diffs, redaction, several events per request, direct recording. |
| [Minter](https://everscribe.io/docs/sdks/go-minter) | Embed tokens, tenant scoping, `TokenOptions`. |
| [Idempotency](https://everscribe.io/docs/sdks/go-idempotency) | `IdempotencyKey` and `WithAutoIdempotencyKey`. |
| [Behaviors worth knowing](https://everscribe.io/docs/sdks/go-behaviors) | Empty `Action`, non-blocking `Record`, context cancellation. |

## Additional resources

- [Runnable examples](https://github.com/everscribe/examples): full-stack runnable
apps for every Everscribe SDK.

- [AI-assisted setup demo](https://github.com/everscribe/ai-assisted-setup-demo): Sample repo for trying AI setup against something disposable.

- [Everscribe CLI](https://github.com/everscribe/cli): CLI for downloading the Claude skill and managing projects, API keys, events and more.

- [Embeddable components](https://github.com/everscribe/components): drop-in audit
trail UI for single and multi-tenant apps.
