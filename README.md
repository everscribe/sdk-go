# sdk-go

Go SDK for the Everscribe audit-log API. Two coordinated surfaces:

- **Recorder** — append-only event ingest. Records who did what, when,
  on what resource, and — for mutation events — how the resource
  changed.
- **Auditor** — mints short-lived embed tokens that let a customer's
  frontend mount the Everscribe embeddable component
  (e.g. `<EverscribeEvents />`) to display events without exposing
  the project API key to the browser.

Single runtime dependency (`github.com/google/uuid`). Requires Go 1.25+.

---

## Table of contents

- [Install](#install)
- [Quickstart](#quickstart)
- [Three key behaviors](#three-key-behaviors)
- [The `Event` shape](#the-event-shape)
- [`BufferedRecorder`](#bufferedrecorder)
- [Idempotency](#idempotency)
- [Embedded views](#embedded-views)

---

## Install

```sh
go get github.com/everscribe/sdk-go
```

```go
import (
    "github.com/everscribe/sdk-go"                  // Client, New, NewRecorder, NewAuditor
    "github.com/everscribe/sdk-go/pkg/recorder"     // BufferedRecorder, HTTPRecorder, options
    "github.com/everscribe/sdk-go/pkg/auditor"      // Auditor client, TokenOptions
    "github.com/everscribe/sdk-go/pkg/event"        // Event, Actor, Target, Result, NewMiddleware
)
```

The root `everscribe` package is the entry point — bind credentials
once and hand out per-surface clients. Customers who only need one
surface can call `recorder.New` or `auditor.New` directly to skip the
SDK-client step.

---

## Quickstart

### 1. Bind credentials and construct subclients

The root `everscribe` package binds the project ID and API key once
and lets you build per-surface clients without re-passing them:

```go
es, err := everscribe.New(projectID, apiKey)
if err != nil {
    log.Fatal(err) // empty/whitespace credentials surface here, not at the first network call
}
rec := es.NewRecorder()
defer rec.Close()
```

For 12-factor / containerized deployments, read credentials from the
environment instead — `NewFromEnv` reads `EVERSCRIBE_PROJECT_ID` and
`EVERSCRIBE_API_KEY` and returns an error naming the missing variable
if either is unset or empty:

```go
es, err := everscribe.NewFromEnv()
if err != nil {
    log.Fatal(err)
}
```

Override defaults by passing options to the subclient constructor:

```go
rec := es.NewRecorder(
    recorder.WithBufferSize(2000),
    recorder.WithFlushInterval(2*time.Second),
    recorder.WithOverflowPolicy(recorder.PolicyBlock),
)
```

Customers who only need the recorder can skip the SDK client:

```go
rec := recorder.New(projectID, apiKey,
    recorder.WithBufferSize(2000),
)
```

Both shapes are supported. The SDK client is the recommended path
once you wire up more than one surface (recorder + auditor); the
direct constructor is a one-line shortcut for ingest-only setups.

| Option                     | Description                                                                                | Default            |
|----------------------------|--------------------------------------------------------------------------------------------|--------------------|
| `WithBufferSize(n)`        | Capacity of the in-memory event buffer.                                                    | `1000`             |
| `WithFlushSize(n)`         | Pending-event count that triggers an immediate flush.                                      | `100`              |
| `WithFlushInterval(d)`     | Maximum time between flushes when the size threshold isn't reached.                        | `5s`               |
| `WithFlushTimeout(d)`      | Context timeout applied to each flush call against the inner recorder.                     | `30s`              |
| `WithOverflowPolicy(p)`    | Behavior when `Record` finds the buffer full. See [overflow policies](#overflow-policies). | `PolicyDropNewest` |
| `WithDrainTimeout(d)`      | Maximum time `Close` waits for in-flight events to flush before returning.                 | `30s`              |
| `WithSlogLogger(l)`        | `*slog.Logger` for SDK diagnostics (overflow warnings, flush errors).                      | `slog.Default()`   |
| `WithBaseURL(url)`         | Override the ingestion endpoint. Used for tests and staging environments.                  | production URL     |
| `WithHTTPClient(c)`        | Custom `*http.Client` for outbound requests.                                               | `Timeout: 10s`     |
| `WithAutoIdempotencyKey()` | Copy `Event.ID` into `Event.IdempotencyKey` at send time when the latter is empty.         | off                |

### 2. Define your Actor Resolver

Your `ActorResolver` function should have the following signature:

```go
type ActorResolver func(ctx context.Context) event.Actor
```

The resolver bridges session-provisioned request context to an `event.Actor`.

So lets say your middleware for provisioninig the request context looks like:

```go
// Your project's session shape — whatever your auth produces.
type Session struct {
    UserID, Username, Email string
    IsAdmin  bool
}

// Context key + helper for retrieval.
type sessionKey struct{}

// Stand-in for your real session store (DB, Redis, signed cookie, etc.).
var sessions = map[string]Session{
    "sess_abc123": {UserID: "u_42", Username: "alice", Email: "alice@example.com", IsAdmin: false},
    "sess_def456": {UserID: "u_99", Username: "admin",  Email: "admin@example.com", IsAdmin: true},
}

// An example of some middleware you might have for provisioning requests with session data.
sessionMW := func(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        c, err := r.Cookie("session_id")
        if err == nil {
            s, ok := sessions[c.Value]
            if ok {
                r = r.WithContext(context.WithValue(r.Context(), sessionKey{}, s))
            }
        }
        next.ServeHTTP(w, r)
    })
}
```

You would then define an `actorResolver` like this:

```go
// Reads what sessionMW attached and returns an Actor.
actorResolver := func(ctx context.Context) event.Actor {
    s, ok := ctx.Value(sessionKey{}).(Session)
    if !ok {
        return event.Actor{Type: "anonymous"}
    }
    actorType := "user"
    if s.IsAdmin {
        actorType = "admin"
    }
    return event.Actor{
        Type:        actorType,
        ID:          s.UserID,
        DisplayName: s.Username,
        Email:       s.Email,
    }
}
```

### 3. Wire up the middleware


**Ordering matters.** The audit middleware must run **after** any
middleware that provisions the request context with session data —
the producer (`sessionMW` above) has to run before the consumer (the
audit middleware, which calls your `actorResolver`):

```go
eventMw := event.NewMiddleware(actorResolver)

// ✅ Session attaches identity first, then audit reads it.
handler := sessionMW(eventMw(mux))

// ❌ Audit runs before session — actorResolver sees no session so
//    every event is provisioned with an anonymous Actor.
handler := eventMw(sessionMW(mux))
```

### 4. Record events in handlers

```go
http.Handle("POST /api-keys", eventMw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()
    e := event.FromContext(ctx)
    defer rec.Record(ctx, e)

    e.Action = "api_key.create"

    key, err := createAPIKey(ctx, r) // your handler logic
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return // Action set, but Result auto-captures as {error, 500}.
    }
    e.Target = event.Target{Type: "api_key", ID: key.ID}

    w.WriteHeader(http.StatusCreated)
})))
```

#### Recording state changes

For mutation events, attach the before/after state with `Diff`.
```go
http.Handle("PATCH /users/{id}", eventMw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()
    e := event.FromContext(ctx)
    defer rec.Record(ctx, e)

    userID := r.PathValue("id")
    e.Action = "user.update"
    e.Target = event.Target{Type: "user", ID: userID}

    user, err := loadUser(ctx, userID) // returns *User
    if err != nil {
        http.Error(w, "not found", http.StatusNotFound)
        return
    }
    before := *user // dereference to copy the value, not the pointer

    user.Email = r.FormValue("email") // apply the patch

    after, err := saveUser(ctx, user)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    e.Diff(before, *after, 
        // Redact sensitive fields from the diff
        event.WithRedactedFields("/password_hash"),
    )
    w.WriteHeader(http.StatusOK)
})))
```

`WithRedactedFields` is an `EventDiffOption` that scrubs sensitive
values before they leave the process. Its arguments are
[JSON Pointer](https://datatracker.ietf.org/doc/html/rfc6901) paths
(RFC 6901): leading `/`, slashes for nesting (`/billing/credit_card`),
integer segments for array indices (`/api_keys/0`). The audit API
uses the same syntax in the JSON Patch it computes from
`before`/`after`, so paths you redact are the same shape as paths
you'll see in the diff output.

#### Recording multiple events per request

Some handlers fan out — one privileged operation can affect many
resources, and each one is independently audit-worthy. A common
incident-response example is revoking every active session for a
compromised account: investigators need to see *which* sessions were
killed, not just that a bulk action ran. Call `FromContext` once
per event so each gets a fresh clone of the per-request template
(Actor, Origin) without sharing or mutating metadata:

```go
http.Handle("POST /users/{id}/sessions/revoke-all", eventMw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()
    userID := r.PathValue("id")

    sessions, err := listActiveSessions(ctx, userID)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    for _, s := range sessions {
        e := event.FromContext(ctx) // fresh clone per session
        e.Action = "session.revoke"
        e.Target = event.Target{Type: "session", ID: s.ID}
        e.WithFields("user_id", userID, "reason", r.FormValue("reason"))

        if err := revokeSession(ctx, s.ID); err != nil {
            e.Result = event.Result{Status: "error", Message: err}
        }
        _ = rec.Record(ctx, e)
    }

    w.WriteHeader(http.StatusNoContent)
})))
```

The buffered recorder coalesces these (and events from other
concurrent requests) into a single `RecordBatch` call to the
ingestion API on each flush — no need to assemble batches yourself.

## Three key behaviors

**Empty `Action` is a no-op**

```go
http.Handle("POST /users/{id}/lock", eventMw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()
    e := event.FromContext(ctx)
    defer rec.Record(ctx, e)

    userID := r.PathValue("id")
    user, err := loadUser(ctx, userID)
    if err != nil {
        http.Error(w, "not found", http.StatusNotFound)
        return // No Action was set - we don't care about recording audit logs for attempts to lock a user account that does not exist.
    }
    if user.Locked {
        w.WriteHeader(http.StatusOK)
        return // No Action was set - we don't care about recording audit logs for attempting to lock a user account thats already locked.
    }

    e.Action = "user.lock"
    e.Target = event.Target{Type: "user", ID: userID}

    if err := lockUser(ctx, userID); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.WriteHeader(http.StatusOK)
})))
```

**Overriding the resolver's `Actor`** — when there's no session yet
(login, signup) or when the actor isn't a session user (webhooks,
system tasks), the handler overrides `e.Actor` directly. Login is the
canonical case: failed and successful attempts are both
security-relevant, but at handler entry the resolver returns
`anonymous` because the session doesn't exist until authentication
succeeds:

```go
http.Handle("POST /login", eventMw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()
    e := event.FromContext(ctx)
    defer rec.Record(ctx, e)

    user, err := authenticate(ctx, r) // your auth check
    if err != nil {
        // Failed login: actor stays "anonymous" from the resolver.
        // Capture the attempted identifier for investigators.
        e.Action = "user.login_failed"
        e.WithFields("attempted_email", r.FormValue("email"))
        http.Error(w, "invalid credentials", http.StatusUnauthorized)
        return
    }

    // Successful login: override the resolver's "anonymous" with the
    // user we just authenticated.
    e.Actor = event.Actor{
        Type:        "user",
        ID:          user.ID,
        DisplayName: user.Username,
        Email:       user.Email,
    }
    e.Action = "user.login"

    issueSessionCookie(w, user)
    w.WriteHeader(http.StatusOK)
})))
```

**Explicit `Result` wins over auto-capture** — when the HTTP status
doesn't reflect the operation's audit outcome. Password reset is the
canonical case: anti-enumeration security requires the API to redirect
to the same "check your email" page whether the email matched a real
account or not, so the user-facing response is identical. Audit
monitoring still needs to know which actually happened — repeated
"no match" entries are how you spot credential-stuffing campaigns:

```go
http.Handle("POST /password/reset", eventMw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()
    e := event.FromContext(ctx)
    defer rec.Record(ctx, e)

    email := r.FormValue("email")
    e.Action = "password.reset_requested"
    e.WithFields("attempted_email", email)

    user, found := lookupByEmail(ctx, email)
    if !found {
        // Explicitly set a denied status  instead of letting the
        // recorder auto-set it based on http status code.
        e.Result = event.Result{Status: "denied", Message: "no account for email"}
        http.Redirect(w, r, "/password/check-your-email", http.StatusSeeOther)
        return
    }

    if err := sendResetEmail(ctx, user); err != nil {
        e.Result = event.Result{Status: "error", Message: err}
        http.Redirect(w, r, "/password/check-your-email", http.StatusSeeOther)
        return
    }
    e.Target = event.Target{Type: "user", ID: user.ID}
    http.Redirect(w, r, "/password/check-your-email", http.StatusSeeOther)
    // happy path: auto-captures as {ok, 303}.
})))
```

---

## The `Event` shape

```go
type Event struct {
    ID             string         // uuid v4; auto-generated if empty
    TenantID       string         // optional within-project dimension
    OccurredAt     time.Time      // auto-populated if zero
    Actor          Actor          // who caused the event
    Action         string         // dotted verb, e.g. "user.lock"
    Target         Target         // what was acted on
    Metadata       map[string]any // freeform context
    Origin         Origin         // IP, user-agent, request ID
    Result         Result         // outcome: ok | error | denied
    Change         *Change        // before/after state for mutations
    IdempotencyKey string         // optional dedup key
}
```

`projectID` is set once at `New` and sent on every request.

`TenantID` groups events one level above the actor — set it when you
run a multi-tenant SaaS and want events queryable per workspace, org,
or connected account (multi-tenant CRMs, Stripe Connect-style
platforms, B2B tools). Single-tenant apps (B2C products, internal
dashboards) leave it blank.

`Result.Message` is `any` and special-cases `error` — pass an `error`
directly and it marshals as the result of `err.Error()`:

```go
e.Result = event.Result{Status: "error", Message: err}
```

Two helpers attach metadata in slog style:

```go
e.WithField("reason", "policy_violation")
e.WithFields("reason", "spam", "severity", "high", "count", 3)
```

For non-HTTP callers, build events directly:

```go
e := event.New("subscription.trial_expired")
e.Actor = event.Actor{Type: "system", ID: "trial_expirer"}
e.Target = event.Target{Type: "subscription", ID: subID}
_ = rec.Record(ctx, e)
```

---

## BufferedRecorder

`New` returns a `*BufferedRecorder` — events enqueue on an
internal channel and a background goroutine flushes batches to the
HTTP recorder when the size threshold or flush interval is reached.
Tuning knobs live in the [Quickstart options table](#1-initialize-a-recorder);
the subsections below cover runtime concerns.

### Overflow policies

When the buffer is full at `Record` time:

| Policy             | Behavior                                                     |
|--------------------|--------------------------------------------------------------|
| `PolicyDropNewest` | Drop the incoming event, increment counter, slog warn. Default. |
| `PolicyBlock`      | Block until space, ctx cancel, or `Close`.                   |
| `PolicyError`      | Return `ErrBufferFull`.                                      |

A full buffer means you're misconfigured — resize, speed up
downstream, or scale out. Watch `Stats().Dropped`.

### `Flush` and `Stats`

`Flush(ctx)` synchronously drains everything buffered at the time of
the call. Useful for tests and graceful shutdown sync points. `Close`
calls a final drain — you don't need to `Flush` before `Close`.

`Stats()` exposes counters for export to Prometheus/Datadog:

```go
type BufferedStats struct {
    Dropped    int64 // total events dropped due to overflow
    Flushed    int64 // total events successfully flushed to inner
    FlushErrs  int64 // total flush calls that returned an error
    Pending    int   // events currently in the buffer
    BufferSize int   // buffer capacity
}
```


## Idempotency

`Event.IdempotencyKey` is for caller-supplied stable keys — webhook
event IDs, upstream request IDs, anything that identifies "the same
logical event" across retries the SDK can't see:

```go
e.IdempotencyKey = stripeEvent.ID // dedup if Stripe redelivers
```

For SDK-internal safety against double-sending the same `*Event`
(e.g., `defer Record` plus a manual `Record` on a redirect path),
enable `WithAutoIdempotencyKey`. It copies `Event.ID` into
`IdempotencyKey` at send time when the key is empty:

```go
rec := recorder.New(projectID, apiKey, recorder.WithAutoIdempotencyKey())
```

Off by default. Caller-supplied keys always win — auto-population
only fills empty keys.

---

## Embedded views

The `pkg/auditor` subpackage mints short-lived JWT tokens that let a
customer's frontend mount the Everscribe embeddable component (e.g.
`<EverscribeEvents />`) without exposing the project API key to the
browser.

The flow has three actors:

1. **Customer's backend** (this SDK) holds the project API key and
   mints embed tokens via `auditor.Client.MintToken`.
2. **Customer's frontend** receives the token from a route the
   customer's backend exposes, and passes it as a prop to the React
   component. Never sees the API key.
3. **Everscribe API** verifies the token on each read and scopes
   results to the token's claims (tenant, columns, actions).

### Minting a token

The cleanest path is via the SDK client, which already holds the
credentials:

```go
import (
    "github.com/everscribe/sdk-go"
    "github.com/everscribe/sdk-go/pkg/auditor"
)

es := everscribe.New(projectID, apiKey)
rec := es.NewRecorder()
defer rec.Close()

aud := es.NewAuditor()

token, err := aud.MintToken(ctx, auditor.TokenOptions{
    TenantID:       "acme-corp",
    ExpiresIn:      time.Hour,
    AllowedColumns: []string{"occurred_at", "action", "actor"},
    AllowedActions: []string{"user.*", "billing.invoice.created"},
})
// token is a JWT string; hand to the customer's frontend via their own route.
```

Customers who only need the auditor surface can construct it directly:

```go
aud := auditor.New(projectID, apiKey)
token, err := aud.MintToken(ctx, auditor.TokenOptions{...})
```

Recorder and auditor are independent surfaces that share auth — a
customer who only mints view tokens (e.g. a separate read-side
service) doesn't need to construct a recorder. Future auditor-side
surfaces (`aud.RotateSecret()`, listing active tokens once revocation
lands) will land naturally on `*auditor.Client`.

### `TokenOptions`

| Field            | Type            | Behavior |
|------------------|-----------------|----------|
| `TenantID`       | `string`        | Optional. Scopes reads to events with the matching `tenant_id`. Trimmed by the SDK; rejected if empty after trim or > 256 chars. |
| `ExpiresIn`      | `time.Duration` | Token lifetime. Server clamps to `[60s, 24h]`. Zero uses the server default (1h). |
| `AllowedColumns` | `[]string`      | Optional whitelist of `Event` JSON field names. `nil` means no restriction; an empty non-nil slice is rejected (avoids silently widening scope when callers build the list from filtered user input). The SDK validates against `event.Event`'s struct tags via reflection at startup. |
| `AllowedActions` | `[]string`      | Optional filter of allowed actions. Each entry is exact (`user.login`) or a suffix wildcard (`user.*`). `nil` means no restriction; empty non-nil slice rejected. Bare `*`, prefix wildcards (`*.create`), mid-string wildcards (`user.*.create`), and wildcards without a preceding dot (`user*`) are rejected. |

### Errors

`MintToken` returns one of:

- A wrapped validation error from the SDK (caller-supplied options
  fail client-side checks; no HTTP call is made).
- `*auditor.Error` for non-2xx responses from the mint endpoint —
  inspect via `errors.As`. Status codes match the spec: 400 for
  invalid options, 401 for bad auth, 404 for missing/soft-deleted
  project.
- A wrapped transport error (timeout, connection refused, etc.).

### Configuration

`auditor.New` accepts options analogous to the recorder:

- `auditor.WithBaseURL(url)` — override the API host (tests, staging).
- `auditor.WithHTTPClient(c)` — supply a custom `*http.Client`.