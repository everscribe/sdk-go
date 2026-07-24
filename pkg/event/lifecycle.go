package event

import (
	"context"
	"sync/atomic"
	"time"
)

// Recorder is the minimal sink pkg/event needs to own the record lifecycle.
// pkg/recorder implementations satisfy it structurally; it is redeclared here
// because pkg/recorder imports pkg/event and not the reverse.
type Recorder interface {
	Record(ctx context.Context, e *Event) error
}

// Logger receives diagnostics for record failures, which are never propagated:
// an audit failure must not break the response.
type Logger interface {
	Error(msg string, args ...any)
}

// OutcomeCapture reports the adapter-derived result for an in-flight call.
// ok is false when the call has not produced an outcome yet, replacing the
// old "status == 0" sentinel that collided with gRPC's OK code.
type OutcomeCapture interface {
	Outcome() (Result, bool)
}

// recordTimeout bounds how long end blocks after the handler returns. Chosen
// above HTTPRecorder's 10s client default so the backstop never truncates an
// in-flight POST; what it actually bounds is BufferedRecorder under
// PolicyBlock, which a cancel-free context would otherwise leave unbounded.
const recordTimeout = 15 * time.Second

// requestState is the per-request lifecycle state Begin installs on the
// context.
//
// The recorded flag lives here rather than on Event deliberately:
// sync/atomic.Bool embeds noCopy, and Event is copied by value in NewFromContext
// (clone := *tmpl), in BufferedRecorder.Record (b.events <- *e), and in
// RecordBatch's slice elements, so a flag on Event would fail go vet's
// copylocks check.
type requestState struct {
	current  *Event
	capture  OutcomeCapture
	recorder Recorder
	logger   Logger
	recorded atomic.Bool
}

type requestStateKey struct{}

// Begin installs the request-scoped template, capture, and recorder, and
// returns a context plus an end func. Adapters call end exactly once, after
// the handler completes.
//
// Begin stamps IdempotencyKey = ID on the request-scoped event, unconditionally
// and never on the template. Both the manual and the auto-record path must
// submit the same key for the server's ON CONFLICT arbiter to absorb a
// duplicate; stamping in end would key only the second submission, which
// collides on the id primary key instead. Caller-supplied keys still win by
// ordering, since the handler runs after Begin and simply overwrites.
//
// Clones from NewFromContext deliberately do not inherit the key: they come from
// the template, which is left unstamped. Distinct IDs sharing one key would be
// silently deduped against each other.
func Begin(ctx context.Context, tmpl *Event, capture OutcomeCapture, rec Recorder, log Logger) (context.Context, func()) {
	if tmpl == nil {
		tmpl = &Event{}
	}
	ctx = context.WithValue(ctx, eventTemplateKey{}, tmpl)

	current := NewFromContext(ctx) // a clone, so the template stays unstamped
	current.IdempotencyKey = current.ID

	st := &requestState{current: current, capture: capture, recorder: rec, logger: log}
	ctx = context.WithValue(ctx, requestStateKey{}, st)

	return ctx, func() { st.end(ctx) }
}

// Current returns the request-scoped mutable event installed by Begin: the
// event the adapter will auto-record. Handlers recording several events per
// request use NewFromContext instead, which returns a clone with a fresh ID.
// If no adapter installed one (no Begin has run on this context), Current
// returns a throwaway *Event{} - the same "no template present" fallback
// NewFromContext documents, so calling it outside a lifecycle is harmless but
// its return value is never recorded.
//
// The concurrency contract covers all access to the returned event, not just
// field assignment: only the request goroutine may touch it, including
// handing it to a recorder. Record itself mutates the event, not just reads
// it, so this is not a theoretical concern: PrepareEvent writes ID,
// OccurredAt, and Result, and the HTTP recorder then writes IdempotencyKey
// on top of that. Record also marshals every field to build the request
// body. A second goroutine calling Record on this same event, even read-only
// as far as its own code is concerned, races both of those writes and the
// marshaling, regardless of the recorded flag; the flag deduplicates
// submissions, it does not make the event itself safe for concurrent Record
// calls. Pass the result of NewFromContext, not Current, to any code that
// records outside the request goroutine.
func Current(ctx context.Context) *Event {
	st, _ := ctx.Value(requestStateKey{}).(*requestState)
	if st == nil {
		return &Event{}
	}
	return st.current
}

// end records the request-scoped event once, if the handler named it.
func (st *requestState) end(ctx context.Context) {
	if st.recorder == nil || st.current.Action == "" {
		return
	}
	// Loser of the CAS is a no-op. The manual path may already have won it
	// via PrepareEvent.
	if !st.recorded.CompareAndSwap(false, true) {
		return
	}

	// Record against a cancel-free context: for HTTP, r.Context() is already
	// canceled by the time end runs, and both recorders honor cancellation,
	// so the aborted and client-disconnected requests would be exactly the
	// ones dropped.
	recCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recordTimeout)
	defer cancel()

	applyOutcome(st, st.current, true)
	if err := st.recorder.Record(recCtx, st.current); err != nil && st.logger != nil {
		st.logger.Error("everscribe: auto-record failed", "error", err)
	}
}

// applyOutcome fills Result from the capture when the handler has not set
// one. final distinguishes end(), which runs after the handler has
// genuinely completed and is guaranteed to be the last word on the
// event's outcome, from PrepareEvent, which can run mid-handler (see the
// multiple-events-per-handler pattern in pkg/recorder/doc.go). Only a
// final caller may stamp the "no response written" sentinel when the
// capture reports ok == false: from PrepareEvent, ok == false just means
// "nothing written yet", not "nothing ever will be", and stamping the
// sentinel there would bake a false error into an event recorded before
// the response.
func applyOutcome(st *requestState, e *Event, final bool) {
	if e.Result.Status != "" || st.capture == nil {
		return
	}
	if r, ok := st.capture.Outcome(); ok {
		e.Result = r
		return
	}
	if !final {
		return
	}
	// ok == false: no response was produced. Keeps today's diagnostic, which
	// lives in core so no adapter can silently drop it.
	e.Result = Result{Status: "error", Message: "no response written"}
}

// ResultFromHTTPStatus derives a Result from an HTTP status code. Opt-in: core
// never calls this implicitly. Adapters over an HTTP-shaped status call it
// rather than each carrying a copy of the table, and §5 routes gRPC through it
// via the canonical gRPC-to-HTTP mapping so the tri-state rule cannot drift.
//
// code 0 means "no response written" and yields the same diagnostic the
// ok == false path produces.
func ResultFromHTTPStatus(code int) Result {
	if code == 0 {
		return Result{Status: "error", Message: "no response written"}
	}
	r := Result{Code: code}
	switch {
	case code >= 200 && code < 400:
		r.Status = "ok"
	case code == 401 || code == 403:
		r.Status = "denied"
	default:
		r.Status = "error"
	}
	return r
}

// OriginFrom builds Origin from a header lookup closure and a remote address.
//
// header MUST be case-insensitive (gRPC metadata keys are lowercase-normalized)
// and MUST return the first value for multi-value keys. Unlike the old
// originFromRequest there is no nil tolerance; adapters with no headers pass a
// closure returning "".
func OriginFrom(header func(name string) string, remoteAddr string) Origin {
	return Origin{
		IP:        clientIPFrom(header, remoteAddr),
		UserAgent: header("User-Agent"),
		RequestID: header("X-Request-ID"),
	}
}

func clientIPFrom(header func(name string) string, remoteAddr string) string {
	if xff := header("X-Forwarded-For"); xff != "" {
		for i := 0; i < len(xff); i++ {
			if xff[i] == ',' {
				return trimSpace(xff[:i])
			}
		}
		return trimSpace(xff)
	}
	if xri := header("X-Real-IP"); xri != "" {
		return xri
	}
	return stripPort(remoteAddr)
}

// stripPort removes a trailing ":port" from remoteAddr, but only when doing
// so is unambiguous: a bracketed IPv6 host ("[::1]:8080", host returned
// without the brackets), or a plain host:port pair with exactly one colon
// ("127.0.0.1:54321"). A bare IPv6 literal has no brackets and more than one
// colon of its own ("2001:db8::1", "::1") and is returned unchanged - there
// is no port to strip, and naively truncating after the last colon corrupts
// the address (formerly "2001:db8::1" -> "2001:db8:", "::1" -> ":").
func stripPort(remoteAddr string) string {
	if remoteAddr == "" {
		return remoteAddr
	}
	if remoteAddr[0] == '[' {
		for i := 1; i < len(remoteAddr); i++ {
			if remoteAddr[i] == ']' {
				return remoteAddr[1:i]
			}
		}
		return remoteAddr
	}
	colons, last := 0, -1
	for i := 0; i < len(remoteAddr); i++ {
		if remoteAddr[i] == ':' {
			colons++
			last = i
		}
	}
	if colons == 1 {
		return remoteAddr[:last]
	}
	return remoteAddr
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}
