// Package recorder provides an append-only event recording client for the
// audit-log ingestion API. Events capture who did what, when, on what
// resource, and — for mutation events — how the resource changed.
//
// # Overview
//
// New is the recommended entry point. It returns a
// *BufferedRecorder that wraps an HTTPRecorder using sensible defaults.
// New accepts both HTTPOption and BufferedOption arguments —
// pass either type directly, no wrapping needed.
//
//	rec := recorder.New(projectID, apiKey,
//	    recorder.WithBufferSize(500),
//	    recorder.WithFlushInterval(5*time.Second),
//	    recorder.WithOverflowPolicy(recorder.PolicyDropNewest),
//	)
//	defer rec.Close()
//
// For advanced cases — custom inner Recorder, synchronous writes, an
// instrumented transport — use the building blocks directly:
//
//   - HTTPRecorder posts events to the audit-log ingestion API.
//   - BufferedRecorder wraps any Recorder with asynchronous batching.
//
// For tests, override the ingestion endpoint with WithBaseURL pointing
// at an httptest server.
//
// # Idempotency
//
// Event.IdempotencyKey is for caller-supplied stable keys (webhook
// event IDs, upstream request IDs, etc.) that identify "the same
// logical event" across retries the SDK doesn't see. Set it explicitly
// per event when you need that.
//
// For SDK-internal safety against double-sending the same *Event,
// enable WithAutoIdempotencyKey() — it copies Event.ID into IdempotencyKey
// at send time when the key is empty. Caller-supplied keys win.
//
// # HTTP handlers: the defer pattern
//
// For HTTP handlers, the recommended idiom is to defer Record at the
// top of the handler, then enrich the Event as the handler runs:
//
//	func (s *Server) handleLockUser(w http.ResponseWriter, r *http.Request) {
//	    e := event.FromContext(r.Context())
//	    defer s.recorder.Record(r.Context(), e)
//
//	    userID := r.PathValue("id")
//	    e.Action = "user.lock"
//	    e.Target = event.Target{Type: "user", ID: userID}
//	    e.WithFields("reason", r.FormValue("reason"))
//
//	    if err := s.store.LockUser(r.Context(), userID); err != nil {
//	        e.Result = event.Result{Status: "error", Message: err}
//	        http.Error(w, "...", http.StatusInternalServerError)
//	        return
//	    }
//	    http.Redirect(w, r, "/users/"+userID, http.StatusSeeOther)
//	}
//
// The deferred Record call reads the final HTTP status from the
// middleware-wrapped ResponseWriter (stashed on the request context by
// event.NewMiddleware) and auto-populates Event.Result when it is unset.
// Handlers override by setting e.Result explicitly — useful for
// POST-redirect-GET flows where HTTP status is the same on success and
// failure.
//
// If Action is empty at Record time, the call is a no-op. Handlers that
// early-return before setting Action do not emit garbage events.
//
// # Recording state changes
//
// For events that mutate a resource, attach the before/after state with
// Diff so the audit UI can render a diff:
//
//	before, _ := s.store.GetUser(ctx, id)
//	if err := s.store.UpdateUser(ctx, id, patch); err != nil {
//	    return err
//	}
//	after, _ := s.store.GetUser(ctx, id)
//	e.Diff(before, after)
//
// For sensitive fields (passwords, API keys, PII), pass
// WithRedactedFields with JSON pointer paths to scrub before sending:
//
//	e.Diff(before, after,
//	    event.WithRedactedFields("/password_hash", "/api_keys"),
//	)
//
// The ingestion API computes the JSON Patch on receipt; the SDK only
// ships the marshaled state.
//
// # Non-HTTP callers
//
// Background jobs, cron, and CLIs use NewEvent directly — no special
// argument changes are needed since Record only takes a context:
//
//	e := event.New("subscription.trial_expired")
//	e.Actor = event.Actor{Type: "system", ID: "trial_expirer"}
//	e.Target = event.Target{Type: "subscription", ID: subID}
//	_ = rec.Record(ctx, e)
//
// # Middleware and ActorResolver
//
// HTTP servers mount event.NewMiddleware to pre-populate request contexts
// with an Event template and stash the wrapped ResponseWriter for
// status auto-capture. The middleware takes an ActorResolver that
// derives the Actor from session state — the recorder package has no
// opinion about what a "session" is, so each server wires up a resolver
// matching its own auth model:
//
//	auditMW := event.NewMiddleware(func(ctx context.Context) event.Actor {
//	    s, ok := session.FromContext(ctx)
//	    if !ok {
//	        return event.Actor{Type: "anonymous"}
//	    }
//	    return event.Actor{
//	        Type:        "user",
//	        ID:          s.UserID,
//	        DisplayName: s.Username,
//	        Email:       s.Email,
//	    }
//	})
//
// Middleware ordering: Logging -> CSRF -> Session -> Audit -> Routes.
// The audit middleware must run AFTER session middleware because the
// resolver reads session data from context.
//
// # Overriding the resolver's Actor
//
// Set e.Actor in handlers only when the resolver's default would be
// wrong. Pre-session requests (login, signup), non-user actors
// (webhooks, cron tasks), and non-HTTP callers are the main cases.
// Authenticated handlers inherit from the resolver and should leave
// e.Actor alone.
//
// # Multiple events per handler
//
// Handlers that record multiple events per request call FromContext
// once per event (each call returns a fresh clone of the template)
// and Record explicitly for each. The defer pattern is for the common
// single-event case.
package recorder
