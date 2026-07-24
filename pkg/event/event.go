package event

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Event is the canonical audit record. Construct via NewEvent (non-HTTP)
// or FromContext (HTTP, after an adapter's Begin has run), populate the
// handler-specific fields (Action, Target, Metadata, optionally Result),
// and pass to Recorder.Record.
type Event struct {
	ID             string         `json:"id"`
	TenantID       string         `json:"tenant_id,omitempty"`
	OccurredAt     time.Time      `json:"occurred_at"`
	Actor          Actor          `json:"actor"`
	Action         string         `json:"action"`
	Target         Target         `json:"target,omitzero"`
	Metadata       map[string]any `json:"metadata,omitempty"`
	Origin         Origin         `json:"origin,omitzero"`
	Result         Result         `json:"result,omitzero"`
	Change         *Change        `json:"change,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
}

// Change records a state transition for mutation events. Before and After
// hold the JSON-encoded resource state on either side of the change; the
// audit-log API computes the patch on ingest. Patch is optional and only
// populated when the caller already has a precomputed JSON Patch
// (RFC 6902) to send via RawDiff.
type Change struct {
	Before json.RawMessage `json:"before,omitempty"`
	After  json.RawMessage `json:"after,omitempty"`
	Patch  json.RawMessage `json:"patch,omitempty"`
}

// Actor identifies who caused the event. Type values are conventional,
// not enforced: "user", "admin", "system", "api_key", "anonymous".
type Actor struct {
	Type        string `json:"type"`
	ID          string `json:"id,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
	Email       string `json:"email,omitempty"`
}

// ActorResolver derives an Actor from request context. Typically reads
// session data attached by an upstream auth middleware. The recorder
// package does not know about any specific session type - each adapter
// wires up a resolver that matches its own auth model.
type ActorResolver func(ctx context.Context) Actor

// eventTemplateKey is the context key an adapter's Begin call uses to
// install the per-request Event template that FromContext reads.
type eventTemplateKey struct{}

// Target identifies what the event was acting on. Empty means no target.
type Target struct {
	Type string `json:"type,omitempty"`
	ID   string `json:"id,omitempty"`
}

// Origin captures the network/request context where the event was emitted.
type Origin struct {
	IP        string `json:"ip,omitempty"`
	UserAgent string `json:"user_agent,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

// New returns a new Event with ID, OccurredAt, and Action populated.
// Use for non-HTTP callers (background jobs, cron, CLI). HTTP handlers
// should prefer FromContext, which additionally populates Origin and
// Actor from the request.
func New(action string) *Event {
	return &Event{
		ID:         uuid.NewString(),
		OccurredAt: time.Now().UTC(),
		Action:     action,
	}
}

// FromContext returns a fresh Event pre-populated from the request-scoped
// template installed by an adapter's Begin call. If no template is present
// (no adapter mounted, or called outside the request path), returns a
// minimal Event equivalent to NewEvent("").
//
// Each call returns an independent Event - mutating the returned value
// does not affect other events derived from the same context. Handlers
// that record multiple events per request call FromContext once per event.
func FromContext(ctx context.Context) *Event {
	tmpl, ok := ctx.Value(eventTemplateKey{}).(*Event)
	if !ok || tmpl == nil {
		return &Event{
			ID:         uuid.NewString(),
			OccurredAt: time.Now().UTC(),
		}
	}
	clone := *tmpl
	clone.ID = uuid.NewString()
	clone.OccurredAt = time.Now().UTC()
	clone.Metadata = nil // each event owns its own metadata map
	return &clone
}

// WithField sets a single metadata key/value pair. Allocates the Metadata
// map on first use. Returns the receiver for chaining.
func (e *Event) WithField(key string, value any) *Event {
	if e.Metadata == nil {
		e.Metadata = make(map[string]any)
	}
	e.Metadata[key] = value
	return e
}

// WithFields sets metadata from alternating key/value pairs, slog-style.
//
//	e.WithFields("reason", "spam", "severity", "high")
//
// Odd-length argument lists drop the trailing value. Non-string keys are
// silently skipped. Returns the receiver for chaining.
func (e *Event) WithFields(args ...any) *Event {
	if len(args) == 0 {
		return e
	}
	if e.Metadata == nil {
		e.Metadata = make(map[string]any)
	}
	for i := 0; i+1 < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			continue
		}
		e.Metadata[key] = args[i+1]
	}
	return e
}

// EventDiffOption configures Event.Diff.
type EventDiffOption func(*eventDiffConfig)

type eventDiffConfig struct {
	redactPaths []string
}

// WithRedactedFields replaces the values at the given JSON pointer
// paths (RFC 6901) with "[REDACTED]" in both before and after before
// they leave the process. Use for fields that must not appear in audit
// logs: password hashes, API keys, PII.
//
//	e.Diff(before, after,
//	    recorder.WithRedactedFields("/password_hash", "/api_keys/0"),
//	)
//
// Paths that don't exist in the document are silently skipped.
func WithRedactedFields(paths ...string) EventDiffOption {
	return func(c *eventDiffConfig) { c.redactPaths = paths }
}

// Diff records a state transition for mutation events. Both before and
// after are JSON-marshaled and stored on the Event so the audit UI can
// render a diff. The server computes the patch on ingest.
//
// Pass WithRedactedFields to scrub sensitive paths before marshaling.
//
// Returns the receiver for chaining. Marshal errors are silently
// ignored; the Change is left unset so the caller's audit event still
// records.
func (e *Event) Diff(before, after any, opts ...EventDiffOption) *Event {
	cfg := eventDiffConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	beforeJSON, err1 := marshalRedacted(before, cfg.redactPaths)
	afterJSON, err2 := marshalRedacted(after, cfg.redactPaths)
	if err1 != nil || err2 != nil {
		return e
	}
	e.Change = &Change{
		Before: beforeJSON,
		After:  afterJSON,
	}
	return e
}

// RawDiff is an escape hatch for callers that already have JSON-encoded
// before/after state, or who want to supply their own pre-computed
// patch. Any of the three may be nil.
func (e *Event) RawDiff(before, after, patch json.RawMessage) *Event {
	if before == nil && after == nil && patch == nil {
		return e
	}
	e.Change = &Change{
		Before: before,
		After:  after,
		Patch:  patch,
	}
	return e
}

// PrepareEvent fills defaults on e: ID if empty, OccurredAt if zero,
// and Result auto-captured from the in-scope outcome capture when Result
// is unset. Recorder implementations call this on each Event before
// persisting so handlers can rely on auto-populated fields.
//
// Result population here is never final: PrepareEvent can run mid-handler
// (see the multiple-events-per-handler pattern in pkg/recorder/doc.go), so
// when the capture reports ok == false it leaves Result untouched instead
// of stamping the "no response written" sentinel. From here, ok == false
// only means "nothing written yet", not "nothing ever will be" - that
// sentinel is end()'s to stamp, since only end() runs after the handler
// has genuinely finished.
//
// It also has a dedupe side effect, which is not obvious from the name:
// when e is the request-scoped event installed by Begin, PrepareEvent marks
// it recorded so the adapter's end does not submit it a second time. This
// only records that a submission happened; it cannot abort one, so a
// recorder that might still discard e after this call (for example a
// buffered recorder whose overflow policy drops the event) must not call
// PrepareEvent directly, since the mark cannot be undone once the event is
// lost. Use PrepareEventFields instead and only invoke the returned mark
// func once the event has actually been accepted.
func PrepareEvent(ctx context.Context, e *Event) {
	mark := PrepareEventFields(ctx, e)
	mark()
}

// PrepareEventFields performs the same field population as PrepareEvent
// (ID, OccurredAt, Result) but defers the dedupe mark: it returns a func
// that must be called once the caller has committed to actually recording
// e. Skipping the returned func leaves the request-scoped event eligible
// for end()'s auto-record backstop, which is what lets a recorder abandon
// a dropped event correctly instead of losing it silently.
//
// The returned func is a no-op when e is not the request-scoped event
// installed by Begin - pointer identity, not ID equality, since
// FromContext clones are distinct events and must not be suppressed.
func PrepareEventFields(ctx context.Context, e *Event) (mark func()) {
	if e.ID == "" {
		e.ID = uuid.NewString()
	}
	if e.OccurredAt.IsZero() {
		e.OccurredAt = time.Now().UTC()
	}

	st, ok := ctx.Value(requestStateKey{}).(*requestState)
	if !ok || st == nil {
		return func() {}
	}
	applyOutcome(st, e, false)
	if e != st.current {
		return func() {}
	}
	return func() { st.recorded.Store(true) }
}

// marshalRedacted marshals v to JSON, then walks the result and replaces
// the values at the given JSON pointer paths with "[REDACTED]". Paths
// that do not exist in the document are silently skipped.
func marshalRedacted(v any, paths []string) (json.RawMessage, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return raw, nil
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	for _, p := range paths {
		doc = redactPath(doc, p)
	}
	return json.Marshal(doc)
}

// redactPath replaces the value at the given JSON pointer (RFC 6901)
// with the string "[REDACTED]". The empty pointer redacts the whole doc.
// Numeric path tokens index into arrays; string tokens index into objects.
func redactPath(doc any, pointer string) any {
	if pointer == "" {
		return "[REDACTED]"
	}
	if !strings.HasPrefix(pointer, "/") {
		return doc
	}
	tokens := splitPointer(pointer[1:])
	return redactTokens(doc, tokens)
}

func redactTokens(node any, tokens []string) any {
	if len(tokens) == 0 {
		return "[REDACTED]"
	}
	head, rest := tokens[0], tokens[1:]
	switch n := node.(type) {
	case map[string]any:
		if _, ok := n[head]; !ok {
			return n
		}
		n[head] = redactTokens(n[head], rest)
		return n
	case []any:
		idx, ok := atoi(head)
		if !ok || idx < 0 || idx >= len(n) {
			return n
		}
		n[idx] = redactTokens(n[idx], rest)
		return n
	default:
		return node
	}
}

// splitPointer splits an RFC 6901 reference body by '/' and unescapes
// the standard "~1" → "/" and "~0" → "~" sequences.
func splitPointer(body string) []string {
	parts := strings.Split(body, "/")
	for i, p := range parts {
		p = strings.ReplaceAll(p, "~1", "/")
		p = strings.ReplaceAll(p, "~0", "~")
		parts[i] = p
	}
	return parts
}

// atoi parses a base-10 unsigned integer used to resolve JSON pointer
// array indices. Avoids importing strconv for a single call site.
// Returns false on any non-digit character or empty input.
func atoi(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}
