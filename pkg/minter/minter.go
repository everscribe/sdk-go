// Package minter mints embed tokens used by the Everscribe embeddable
// component to display audit events from a customer's frontend without
// exposing the project API key.
//
// An embed token is a short-lived, signed JWT minted by the customer's
// backend (using the project API key) and forwarded to the frontend.
// The frontend passes it to the React component, which authenticates
// read-only requests to /api/v1/embed/events.
//
// See the sdk-go README "Embedded views" section for the full flow.
package minter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/everscribe/sdk-go/pkg/event"
)

const defaultBaseURL = "https://everscribe.io/api"

// Lifetime bounds enforced by the server. ExpiresIn outside this range
// is rejected at mint with 400.
const (
	MinExpiresIn = 60 * time.Second
	MaxExpiresIn = 24 * time.Hour
)

// actionGrammar matches valid action filter entries: ASCII alphanumeric
// and underscore, dot-separated segments, optional .* suffix. Mirrors
// the server's grammar — see "Wildcard syntax for actions" in the spec.
var actionGrammar = regexp.MustCompile(`^[a-zA-Z0-9_]+(\.[a-zA-Z0-9_]+)*(\.\*)?$`)

// allowedColumns is the set of valid Event field names, derived from
// event.Event's JSON struct tags at package init. Adding a json-tagged
// field to event.Event automatically extends the allowlist, matching
// the server's reflection-based behavior.
var allowedColumns = buildAllowedColumns()

func buildAllowedColumns() map[string]struct{} {
	set := make(map[string]struct{})
	t := reflect.TypeOf(event.Event{})
	for i := 0; i < t.NumField(); i++ {
		tag := t.Field(i).Tag.Get("json")
		if tag == "" || tag == "-" {
			continue
		}
		if comma := strings.IndexByte(tag, ','); comma >= 0 {
			tag = tag[:comma]
		}
		if tag == "" || tag == "-" {
			continue
		}
		set[tag] = struct{}{}
	}
	return set
}

// Client mints embed tokens for a single project. Construct one via New
// and reuse for the lifetime of the process — Client is safe for
// concurrent use.
type Client struct {
	baseURL   string
	projectID string
	apiKey    string
	client    *http.Client
}

// Option configures a Client at construction.
type Option func(*Client)

// WithHTTPClient overrides the default *http.Client. Useful for tests
// (httptest) and for setting custom transports or timeouts.
func WithHTTPClient(c *http.Client) Option {
	return func(cl *Client) { cl.client = c }
}

// WithBaseURL overrides the default API host. Primarily for tests
// against httptest.Server and for staging environments. Trailing
// slashes are trimmed.
func WithBaseURL(url string) Option {
	return func(cl *Client) { cl.baseURL = strings.TrimRight(url, "/") }
}

// New returns a Client bound to projectID, authenticating mint
// requests with apiKey as a bearer token. The default HTTP client has
// a 10-second request timeout.
func New(projectID, apiKey string, opts ...Option) *Client {
	c := &Client{
		baseURL:   defaultBaseURL,
		projectID: projectID,
		apiKey:    apiKey,
		client:    &http.Client{Timeout: 10 * time.Second},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// TokenOptions configures a token mint request. All fields are optional;
// the zero value mints a 1-hour, full-project, read-only token.
type TokenOptions struct {
	// TenantID, if non-empty, scopes the token's reads to events with
	// a matching tenant_id. The SDK trims the value before sending.
	// Rejected if empty after trim or > 256 chars.
	TenantID string

	// ExpiresIn requests a token lifetime. The server clamps to
	// [MinExpiresIn, MaxExpiresIn]. Zero uses the server default
	// (1 hour).
	ExpiresIn time.Duration

	// AllowedColumns whitelists Event field names (JSON tags) the
	// token's reads return. Nil means no restriction (all fields).
	// An empty non-nil slice is rejected — the server requires
	// explicit nil/omit for "all" to avoid silently widening scope
	// when callers build the list from filtered user input.
	AllowedColumns []string

	// AllowedActions filters reads to events whose action matches any
	// entry. Entries are exact (`user.login`) or suffix wildcards
	// (`user.*`). Nil means no restriction; empty non-nil slice is
	// rejected (same reasoning as AllowedColumns).
	AllowedActions []string
}

// MintToken requests a new embed token from the API and returns the
// JWT string on success. Validates options client-side before sending;
// validation errors short-circuit the round-trip. Server-side 4xx
// responses are returned as *Error.
func (c *Client) MintToken(ctx context.Context, opts TokenOptions) (string, error) {
	body, err := opts.marshal()
	if err != nil {
		return "", err
	}

	path := "/v1/projects/" + c.projectID + "/embed-tokens"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("minter: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("minter: post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated {
		var out struct {
			Token string `json:"token"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return "", fmt.Errorf("minter: decode response: %w", err)
		}
		return out.Token, nil
	}

	buf, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return "", &Error{
		StatusCode: resp.StatusCode,
		Body:       strings.TrimSpace(string(buf)),
	}
}

// Error is returned when the mint endpoint responds with a non-2xx
// status. Use errors.As to inspect the status code and body.
type Error struct {
	StatusCode int
	Body       string
}

func (e *Error) Error() string {
	return fmt.Sprintf("minter: http %d: %s", e.StatusCode, e.Body)
}

// marshal validates opts and produces the request JSON body.
func (o TokenOptions) marshal() ([]byte, error) {
	type wire struct {
		TenantID  string   `json:"tenant_id,omitempty"`
		ExpiresIn int      `json:"expires_in,omitempty"`
		Columns   []string `json:"columns,omitempty"`
		Actions   []string `json:"actions,omitempty"`
	}

	var w wire

	if o.TenantID != "" {
		trimmed := strings.TrimSpace(o.TenantID)
		if trimmed == "" {
			return nil, errors.New("minter: TenantID is empty after trim")
		}
		if len(trimmed) > 256 {
			return nil, errors.New("minter: TenantID exceeds 256 chars")
		}
		w.TenantID = trimmed
	}

	if o.ExpiresIn != 0 {
		if o.ExpiresIn < MinExpiresIn {
			return nil, fmt.Errorf("minter: ExpiresIn %s below minimum %s", o.ExpiresIn, MinExpiresIn)
		}
		if o.ExpiresIn > MaxExpiresIn {
			return nil, fmt.Errorf("minter: ExpiresIn %s above maximum %s", o.ExpiresIn, MaxExpiresIn)
		}
		w.ExpiresIn = int(o.ExpiresIn / time.Second)
	}

	if o.AllowedColumns != nil {
		if len(o.AllowedColumns) == 0 {
			return nil, errors.New("minter: AllowedColumns is empty; pass nil for no restriction")
		}
		for _, col := range o.AllowedColumns {
			if _, ok := allowedColumns[col]; !ok {
				return nil, fmt.Errorf("minter: unknown column name %q", col)
			}
		}
		w.Columns = o.AllowedColumns
	}

	if o.AllowedActions != nil {
		if len(o.AllowedActions) == 0 {
			return nil, errors.New("minter: AllowedActions is empty; pass nil for no restriction")
		}
		for _, a := range o.AllowedActions {
			if !actionGrammar.MatchString(a) {
				return nil, fmt.Errorf("minter: action entry %q does not match grammar [a-zA-Z0-9_]+(\\.[a-zA-Z0-9_]+)*(\\.\\*)?", a)
			}
		}
		w.Actions = o.AllowedActions
	}

	return json.Marshal(w)
}
