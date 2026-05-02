package recorder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/everscribe/sdk-go/pkg/event"
)

// defaultBaseURL is the production ingestion endpoint. Tests and staging
// environments override via WithBaseURL.
const defaultBaseURL = "https://everscribe.io/api"

// HTTPRecorder posts events to the audit-log ingestion API. Implements
// both Recorder and BatchRecorder — wrapping it in a BufferedRecorder
// provides asynchronous batched delivery with configurable overflow
// policies.
//
// Wire format:
//
//	POST {baseURL}/v1/projects/{projectID}/events
//	    body: single Event JSON object
//	POST {baseURL}/v1/projects/{projectID}/events/batch
//	    body: {"events": [Event, Event, ...]}
//	Authorization: Bearer {apiKey}
//	Content-Type:  application/json
//
// Responses:
//
//	2xx — accepted
//	4xx — permanent failure (bad request, unauthorized, payload too large)
//	5xx — transient failure; callers can retry
//	429 — rate limited; callers should back off
//
// Non-2xx responses are returned as *HTTPError. Use HTTPError.Transient
// to distinguish retryable failures.
type HTTPRecorder struct {
	baseURL            string
	projectID          string
	apiKey             string
	client             *http.Client
	autoIdempotencyKey bool
}

// HTTPOption configures HTTPRecorder. HTTPOption also satisfies
// RecorderOption, so it can be passed directly to New.
type HTTPOption func(*HTTPRecorder)

// applyRecorder lets HTTPOption satisfy RecorderOption — see recorder.go.
func (o HTTPOption) applyRecorder(c *recorderConfig) {
	c.httpOpts = append(c.httpOpts, o)
}

// WithHTTPClient overrides the default http.Client. Useful for tests
// (httptest) and for setting custom transports, timeouts, or middleware.
func WithHTTPClient(c *http.Client) HTTPOption {
	return func(l *HTTPRecorder) { l.client = c }
}

// WithBaseURL overrides the default ingestion endpoint. Primarily for
// tests against httptest.Server, and for staging environments.
func WithBaseURL(url string) HTTPOption {
	return func(l *HTTPRecorder) { l.baseURL = strings.TrimRight(url, "/") }
}

// WithAutoIdempotencyKey makes the recorder copy Event.ID into
// Event.IdempotencyKey at send time when IdempotencyKey is empty. This
// gives in-process safety against double-sends of the same *event.Event
// without callers having to think about it.
//
// Callers who set IdempotencyKey explicitly (e.g., to a webhook event
// ID for cross-retry idempotency) win — auto-population only fills
// empty keys. Off by default.
func WithAutoIdempotencyKey() HTTPOption {
	return func(l *HTTPRecorder) { l.autoIdempotencyKey = true }
}

// NewHTTPRecorder returns a Recorder that POSTs events to the audit-log
// ingestion API for the given project, authenticating with apiKey as a
// bearer token. The default HTTP client has a 10-second request timeout.
func NewHTTPRecorder(projectID, apiKey string, opts ...HTTPOption) *HTTPRecorder {
	l := &HTTPRecorder{
		baseURL:   defaultBaseURL,
		projectID: projectID,
		apiKey:    apiKey,
		client:    &http.Client{Timeout: 10 * time.Second},
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Record implements Recorder.
func (l *HTTPRecorder) Record(ctx context.Context, e *event.Event) error {
	if e == nil || e.Action == "" {
		return nil
	}
	event.PrepareEvent(ctx, e)
	l.finalize(e)
	body, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("recorder: marshal event: %w", err)
	}
	return l.post(ctx, l.eventsPath(), body)
}

// RecordBatch implements BatchRecorder. Filters out empty-Action events
// before posting.
func (l *HTTPRecorder) RecordBatch(ctx context.Context, events []event.Event) error {
	if len(events) == 0 {
		return nil
	}
	filtered := make([]event.Event, 0, len(events))
	for i := range events {
		if events[i].Action == "" {
			continue
		}
		event.PrepareEvent(ctx, &events[i])
		l.finalize(&events[i])
		filtered = append(filtered, events[i])
	}
	if len(filtered) == 0 {
		return nil
	}
	body, err := json.Marshal(struct {
		Events []event.Event `json:"events"`
	}{Events: filtered})
	if err != nil {
		return fmt.Errorf("recorder: marshal batch: %w", err)
	}
	return l.post(ctx, l.batchPath(), body)
}

// finalize applies recorder-config-dependent fixups after PrepareEvent.
// Currently just copies ID into IdempotencyKey when WithAutoIdempotencyKey
// is enabled; centralized so future per-recorder finalization steps
// have one place to live.
func (l *HTTPRecorder) finalize(e *event.Event) {
	if l.autoIdempotencyKey && e.IdempotencyKey == "" {
		e.IdempotencyKey = e.ID
	}
}

func (l *HTTPRecorder) eventsPath() string {
	return "/v1/projects/" + l.projectID + "/events"
}

func (l *HTTPRecorder) batchPath() string {
	return "/v1/projects/" + l.projectID + "/events/batch"
}

func (l *HTTPRecorder) post(ctx context.Context, path string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, l.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("recorder: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+l.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := l.client.Do(req)
	if err != nil {
		return fmt.Errorf("recorder: post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}

	buf, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return &HTTPError{
		StatusCode: resp.StatusCode,
		Body:       strings.TrimSpace(string(buf)),
	}
}

// HTTPError is returned by HTTPRecorder when the ingestion endpoint
// responds with a non-2xx status. Callers type-assert to distinguish
// permanent failures from transient ones via HTTPError.Transient.
type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("recorder: http %d: %s", e.StatusCode, e.Body)
}

// Transient reports whether the error is likely to resolve on retry
// (5xx server errors and 429 rate limits).
func (e *HTTPError) Transient() bool {
	return e.StatusCode >= 500 || e.StatusCode == http.StatusTooManyRequests
}

var (
	_ Recorder      = (*HTTPRecorder)(nil)
	_ BatchRecorder = (*HTTPRecorder)(nil)
)
