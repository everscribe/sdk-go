package event

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewEvent_PopulatesDefaults(t *testing.T) {
	t.Parallel()
	e := New("user.login")
	require.NotEmpty(t, e.ID)
	require.False(t, e.OccurredAt.IsZero())
	require.Equal(t, "user.login", e.Action)
}

func TestFromContext_NoTemplate_ReturnsMinimalEvent(t *testing.T) {
	t.Parallel()
	e := FromContext(context.Background())
	require.NotEmpty(t, e.ID)
	require.False(t, e.OccurredAt.IsZero())
	require.Empty(t, e.Action)
}

func TestFromContext_WithTemplate_CopiesActorAndOrigin(t *testing.T) {
	t.Parallel()
	tmpl := &Event{
		Actor:  Actor{Type: "user", ID: "u1", DisplayName: "alice", Email: "a@b"},
		Origin: Origin{IP: "1.2.3.4", UserAgent: "ua", RequestID: "req1"},
	}
	ctx := context.WithValue(context.Background(), eventTemplateKey{}, tmpl)

	e := FromContext(ctx)
	require.Equal(t, tmpl.Actor, e.Actor)
	require.Equal(t, tmpl.Origin, e.Origin)
	require.NotEmpty(t, e.ID)
	require.False(t, e.OccurredAt.IsZero())
}

func TestFromContext_ReturnsIndependentClones(t *testing.T) {
	t.Parallel()
	tmpl := &Event{Actor: Actor{Type: "user", ID: "u1"}}
	ctx := context.WithValue(context.Background(), eventTemplateKey{}, tmpl)

	e1 := FromContext(ctx)
	e2 := FromContext(ctx)

	require.NotEqual(t, e1.ID, e2.ID, "each call should produce a unique ID")

	e1.WithField("k", "v1")
	e2.WithField("k", "v2")
	require.Equal(t, "v1", e1.Metadata["k"])
	require.Equal(t, "v2", e2.Metadata["k"])
	require.Nil(t, tmpl.Metadata, "template should be untouched")
}

func TestFromContext_MetadataIsolation(t *testing.T) {
	t.Parallel()
	tmpl := &Event{
		Actor:    Actor{Type: "user"},
		Metadata: map[string]any{"shared": "yes"},
	}
	ctx := context.WithValue(context.Background(), eventTemplateKey{}, tmpl)

	e := FromContext(ctx)
	// FromContext nils metadata so each event owns its own map.
	require.Nil(t, e.Metadata)
	e.WithField("own", "value")
	require.NotContains(t, e.Metadata, "shared")
}

func TestWithField_AllocatesMapOnFirstUse(t *testing.T) {
	t.Parallel()
	e := &Event{}
	require.Nil(t, e.Metadata)
	e.WithField("k", "v")
	require.Equal(t, "v", e.Metadata["k"])
}

func TestWithField_Chainable(t *testing.T) {
	t.Parallel()
	e := (&Event{}).WithField("a", 1).WithField("b", 2)
	require.Equal(t, 1, e.Metadata["a"])
	require.Equal(t, 2, e.Metadata["b"])
}

func TestWithFields(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		args     []any
		expected map[string]any // nil means e.Metadata should remain nil
	}{
		{
			name:     "slog style pairs",
			args:     []any{"reason", "spam", "severity", "high", "count", 3},
			expected: map[string]any{"reason": "spam", "severity": "high", "count": 3},
		},
		{
			name:     "odd length drops trailing value",
			args:     []any{"reason", "spam", "orphan"},
			expected: map[string]any{"reason": "spam"},
		},
		{
			name:     "non string keys skipped",
			args:     []any{"ok", 1, 42, "bad", "also_ok", 2},
			expected: map[string]any{"ok": 1, "also_ok": 2},
		},
		{
			name:     "empty args leaves metadata nil",
			args:     nil,
			expected: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := (&Event{}).WithFields(tc.args...)
			if tc.expected == nil {
				require.Nil(t, e.Metadata)
				return
			}
			require.Equal(t, tc.expected, e.Metadata)
		})
	}
}

func TestEvent_Diff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		before      any
		after       any
		redactPaths []string
		wantBefore  string // expected JSON
		wantAfter   string // expected JSON
	}{
		{
			name:       "marshals before and after",
			before:     map[string]any{"id": "u1", "email": "old@example.com"},
			after:      map[string]any{"id": "u1", "email": "new@example.com"},
			wantBefore: `{"id":"u1","email":"old@example.com"}`,
			wantAfter:  `{"id":"u1","email":"new@example.com"}`,
		},
		{
			name: "redacts listed paths in both before and after",
			before: map[string]any{
				"id":            "u1",
				"password_hash": "old_hash",
				"api_key":       "k_old",
				"email":         "a@b",
			},
			after: map[string]any{
				"id":            "u1",
				"password_hash": "new_hash",
				"api_key":       "k_new",
				"email":         "a@b",
			},
			redactPaths: []string{"/password_hash", "/api_key"},
			wantBefore:  `{"id":"u1","email":"a@b","password_hash":"[REDACTED]","api_key":"[REDACTED]"}`,
			wantAfter:   `{"id":"u1","email":"a@b","password_hash":"[REDACTED]","api_key":"[REDACTED]"}`,
		},
		{
			name:        "redact paths that don't exist are silently skipped",
			before:      map[string]any{"id": "u1"},
			after:       map[string]any{"id": "u1"},
			redactPaths: []string{"/nonexistent", "/also/missing"},
			wantBefore:  `{"id":"u1"}`,
			wantAfter:   `{"id":"u1"}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var opts []EventDiffOption
			if len(tc.redactPaths) > 0 {
				opts = append(opts, WithRedactedFields(tc.redactPaths...))
			}
			e := New("user.update").Diff(tc.before, tc.after, opts...)

			require.NotNil(t, e.Change)
			require.JSONEq(t, tc.wantBefore, string(e.Change.Before))
			require.JSONEq(t, tc.wantAfter, string(e.Change.After))
			require.Nil(t, e.Change.Patch)
		})
	}
}

func TestEvent_Diff_ReturnsReceiverForChaining(t *testing.T) {
	t.Parallel()
	e := New("user.update")
	require.Same(t, e, e.Diff(map[string]any{}, map[string]any{}))
}

func TestEvent_RawDiff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		before           json.RawMessage
		after            json.RawMessage
		patch            json.RawMessage
		wantChangeNonNil bool // expect e.Change to be set (non-nil)
	}{
		{
			name:             "populates all three fields",
			before:           json.RawMessage(`{"v":1}`),
			after:            json.RawMessage(`{"v":2}`),
			patch:            json.RawMessage(`[{"op":"replace","path":"/v","value":2}]`),
			wantChangeNonNil: true,
		},
		{
			name:             "all-nil is a no-op",
			wantChangeNonNil: false,
		},
		{
			name:             "patch only is sufficient",
			patch:            json.RawMessage(`[]`),
			wantChangeNonNil: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := New("x").RawDiff(tc.before, tc.after, tc.patch)
			if !tc.wantChangeNonNil {
				require.Nil(t, e.Change)
				return
			}
			require.NotNil(t, e.Change)
			require.Equal(t, tc.before, e.Change.Before)
			require.Equal(t, tc.after, e.Change.After)
			require.Equal(t, tc.patch, e.Change.Patch)
		})
	}
}

func TestClientIP(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		headers    map[string]string
		remoteAddr string
		want       string
	}{
		{
			name:    "x forwarded for first entry",
			headers: map[string]string{"X-Forwarded-For": "1.2.3.4, 10.0.0.1, 10.0.0.2"},
			want:    "1.2.3.4",
		},
		{
			name:    "x real ip",
			headers: map[string]string{"X-Real-IP": "5.6.7.8"},
			want:    "5.6.7.8",
		},
		{
			name: "x forwarded for preferred over x real ip",
			headers: map[string]string{
				"X-Forwarded-For": "1.2.3.4",
				"X-Real-IP":       "5.6.7.8",
			},
			want: "1.2.3.4",
		},
		{
			name:       "fallback to remote addr strips port",
			remoteAddr: "9.10.11.12:54321",
			want:       "9.10.11.12",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			for k, v := range tc.headers {
				r.Header.Set(k, v)
			}
			if tc.remoteAddr != "" {
				r.RemoteAddr = tc.remoteAddr
			}
			require.Equal(t, tc.want, clientIP(r))
		})
	}
}

func TestOriginFromRequest_NilRequest(t *testing.T) {
	t.Parallel()
	require.Equal(t, Origin{}, originFromRequest(nil))
}

func TestOriginFromRequest_FullPopulation(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Header.Set("X-Forwarded-For", "1.2.3.4")
	r.Header.Set("User-Agent", "test-ua/1.0")
	r.Header.Set("X-Request-ID", "req-abc")

	o := originFromRequest(r)
	require.Equal(t, "1.2.3.4", o.IP)
	require.Equal(t, "test-ua/1.0", o.UserAgent)
	require.Equal(t, "req-abc", o.RequestID)
}
