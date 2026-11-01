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

The official Go SDK for the [Everscribe](https://everscribe.io) API. It exposes **middleware** for hooking events into incoming requests, a **recorder** for writing append-only
audit events, and a **minter** for issuing short-lived browser tokens
for [embeddable components](https://github.com/everscribe/components).

</div>

## Coding with Claude?

Install the [`es` CLI](https://github.com/everscribe/cli) to download the [Claude Code Setup Skill](https://everscribe.io/docs/quickstart/skill), which finds your handlers, proposes audit events, and wires up the SDK and [embeddable component](https://github.com/everscribe/components) for you.

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
