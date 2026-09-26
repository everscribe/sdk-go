package event

import (
	"context"
	"encoding/json"
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

func TestNewFromContext_NoTemplate_ReturnsMinimalEvent(t *testing.T) {
	t.Parallel()
	e := NewFromContext(t.Context())
	require.NotEmpty(t, e.ID)
	require.False(t, e.OccurredAt.IsZero())
	require.Empty(t, e.Action)
}

func TestNewFromContext_WithTemplate_CopiesActorAndOrigin(t *testing.T) {
	t.Parallel()
	tmpl := &Event{
		Actor:  Actor{Type: "user", ID: "u1", DisplayName: "alice", Email: "a@b"},
		Origin: Origin{IP: "1.2.3.4", UserAgent: "ua", RequestID: "req1"},
	}
	ctx := context.WithValue(t.Context(), eventTemplateKey{}, tmpl)

	e := NewFromContext(ctx)
	require.Equal(t, tmpl.Actor, e.Actor)
	require.Equal(t, tmpl.Origin, e.Origin)
	require.NotEmpty(t, e.ID)
	require.False(t, e.OccurredAt.IsZero())
}

func TestNewFromContext_ReturnsIndependentClones(t *testing.T) {
	t.Parallel()
	tmpl := &Event{Actor: Actor{Type: "user", ID: "u1"}}
	ctx := context.WithValue(t.Context(), eventTemplateKey{}, tmpl)

	e1 := NewFromContext(ctx)
	e2 := NewFromContext(ctx)

	require.NotEqual(t, e1.ID, e2.ID, "each call should produce a unique ID")

	e1.WithField("k", "v1")
	e2.WithField("k", "v2")
	require.Equal(t, "v1", e1.Metadata["k"])
	require.Equal(t, "v2", e2.Metadata["k"])
	require.Nil(t, tmpl.Metadata, "template should be untouched")
}

func TestNewFromContext_MetadataIsolation(t *testing.T) {
	t.Parallel()
	tmpl := &Event{
		Actor:    Actor{Type: "user"},
		Metadata: map[string]any{"shared": "yes"},
	}
	ctx := context.WithValue(t.Context(), eventTemplateKey{}, tmpl)

	e := NewFromContext(ctx)
	// NewFromContext nils metadata so each event owns its own map.
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
