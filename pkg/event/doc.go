// Package event is the audit-event domain: the record type, the per-request
// record lifecycle, and the framework adapters that drive it.
//
// It spans three concerns that used to be separate packages, so it is worth
// knowing which part you are looking at:
//
//   - The record. Event and its parts (Actor, Target, Origin, Result, Change),
//     plus the constructors New and FromContext and the enrichment helpers
//     WithField, WithFields, Diff, and RawDiff.
//   - The lifecycle. Begin and Current, which let an adapter install a
//     per-request event, hand it to the handler, and record it once when the
//     handler completes.
//   - The adapters. Middleware, GinMiddleware, EchoV4Middleware,
//     FiberV3Middleware, UnaryInterceptor, and StreamInterceptor, all
//     configured by a single Options.
//
// # Getting started
//
// Mount an adapter with a recorder and an actor resolver, then name the event
// from inside the handler:
//
//	rec := recorder.New(projectID, apiKey)
//	mw := event.Middleware(event.Options{
//	    Recorder:      rec,
//	    ActorResolver: func(ctx context.Context) event.Actor {
//	        s, ok := session.FromContext(ctx)
//	        if !ok {
//	            return event.Actor{Type: "anonymous"}
//	        }
//	        return event.Actor{Type: "user", ID: s.UserID, Email: s.Email}
//	    },
//	})
//
//	func handleLogin(w http.ResponseWriter, r *http.Request) {
//	    e := event.Current(r.Context())
//	    e.Action = "user.login"
//	    e.Target = event.Target{Type: "user", ID: userID}
//	    // no Record call: the adapter records e once the handler returns
//	}
//
// Mount the adapter AFTER any auth middleware, since ActorResolver typically reads
// session state.
//
// # Who records
//
// The adapter does, not the handler. Begin installs the event and returns an
// end func the adapter defers; end records once the handler completes. That
// placement is not cosmetic. For gRPC the outcome is derived from the error
// the handler returns, which does not exist until every defer inside that
// handler has run, so a handler-side defer could never observe it.
//
// Two consequences fall out of it:
//
// An event the handler never named is never recorded. Empty Action means the
// handler early-returned without deciding anything was worth auditing, so
// nothing is emitted.
//
// Recording outlives the request. end records against a cancel-free context
// under a bounded timeout, because for HTTP the request context is already
// canceled by the time end runs. Otherwise aborted and client-disconnected
// requests, often the most interesting ones, would be exactly the events that
// got dropped.
//
// # Recording explicitly
//
// Calling a recorder yourself still works, for background jobs, cron, and CLIs
// that have no adapter, and for handlers that emit several events per request:
//
//	e := event.New("subscription.renewed")
//	_ = rec.Record(ctx, e)
//
// Inside a handler, use FromContext for extra events rather than Current.
// Current returns the one event the adapter will record; FromContext returns
// an independent clone with a fresh ID. Recording a clone does not suppress
// the adapter's own record, and the adapter's record does not suppress yours.
//
// Recording Current yourself is also fine. It is deduplicated: whichever path
// submits first wins and the other is a no-op, and both paths carry the same
// idempotency key, so a duplicate that slips through is absorbed by the server
// rather than colliding.
//
// # Concurrency
//
// The event returned by Current belongs to the request goroutine. That covers
// all access, not just field assignment: passing it to a recorder counts,
// because recording mutates it (filling ID, OccurredAt, and Result) and
// marshals every field. Two goroutines recording the same event race
// regardless of the deduplication above. Hand FromContext clones, not Current,
// to anything that records outside the request goroutine.
//
// # Adapters do not all behave identically
//
// Two divergences are worth knowing before you assume behavior carries across
// frameworks. Both are documented in full in docs/adapters.md.
//
// A handler that returns without writing a response is recorded as an error by
// the stdlib, gin, and echo adapters, which can all detect it, and as a
// successful 200 by the fiber adapter, which cannot: fasthttp exposes no
// written-signal at all.
//
// The gRPC interceptors name every RPC by default from the full method name,
// so every call is recorded. The HTTP adapters record nothing until a handler
// names the event. That asymmetry is deliberate: gRPC method names are already
// a closed, meaningful set in a way arbitrary HTTP routes are not, so recording
// everything is the more useful default there and noise everywhere else.
//
// # A note on dependencies
//
// The adapters live here rather than in separate packages so call sites read
// as event.GinMiddleware instead of colliding with the framework's own package
// name. The cost is that importing this package pulls in gin, echo, fiber,
// fasthttp, and grpc even if you use only net/http. That was a deliberate
// trade of dependency weight for call-site clarity.
package event
