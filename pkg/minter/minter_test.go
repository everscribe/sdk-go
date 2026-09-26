package minter

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testProjectID = "proj_123"

// echoServer returns a httptest.Server that records the request it
// received and replies 201 with a fixed token. Test handlers can wrap
// this for richer behavior.
type recordedRequest struct {
	path        string
	auth        string
	contentType string
	body        []byte
}

func mintServer(t *testing.T, capture *recordedRequest) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if capture != nil {
			capture.path = r.URL.Path
			capture.auth = r.Header.Get("Authorization")
			capture.contentType = r.Header.Get("Content-Type")
			capture.body, _ = io.ReadAll(r.Body)
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token":      "the.test.token",
			"expires_at": "2026-05-02T13:30:00Z",
			"expires_in": 3600,
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestMintToken_PostsExpectedRequest(t *testing.T) {
	t.Parallel()
	var got recordedRequest
	srv := mintServer(t, &got)

	c := New(testProjectID, "secret-key", WithBaseURL(srv.URL))
	token, err := c.MintToken(t.Context(), TokenOptions{
		TenantID:       "acme",
		ExpiresIn:      time.Hour,
		AllowedColumns: []string{"occurred_at", "action"},
		AllowedActions: []string{"user.login", "user.*"},
	})

	require.NoError(t, err)
	require.Equal(t, "the.test.token", token)
	require.Equal(t, "/v1/projects/"+testProjectID+"/embed-tokens", got.path)
	require.Equal(t, "Bearer secret-key", got.auth)
	require.Equal(t, "application/json", got.contentType)

	var body map[string]any
	require.NoError(t, json.Unmarshal(got.body, &body))
	require.Equal(t, "acme", body["tenant_id"])
	require.Equal(t, float64(3600), body["expires_in"])
	require.Equal(t, []any{"occurred_at", "action"}, body["columns"])
	require.Equal(t, []any{"user.login", "user.*"}, body["actions"])
}

func TestMintToken_ZeroOptionsSendsEmptyJSON(t *testing.T) {
	t.Parallel()
	var got recordedRequest
	srv := mintServer(t, &got)

	c := New(testProjectID, "k", WithBaseURL(srv.URL))
	_, err := c.MintToken(t.Context(), TokenOptions{})
	require.NoError(t, err)
	require.Equal(t, "{}", string(got.body),
		"empty TokenOptions should marshal to {} so the server applies defaults")
}

func TestMintToken_TenantIDIsTrimmed(t *testing.T) {
	t.Parallel()
	var got recordedRequest
	srv := mintServer(t, &got)

	c := New(testProjectID, "k", WithBaseURL(srv.URL))
	_, err := c.MintToken(t.Context(), TokenOptions{TenantID: "  acme  "})
	require.NoError(t, err)

	var body map[string]any
	require.NoError(t, json.Unmarshal(got.body, &body))
	require.Equal(t, "acme", body["tenant_id"])
}

func TestMintToken_ValidationErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		opts     TokenOptions
		contains string
	}{
		{
			name:     "tenant id empty after trim",
			opts:     TokenOptions{TenantID: "   "},
			contains: "TenantID is empty after trim",
		},
		{
			name:     "tenant id too long",
			opts:     TokenOptions{TenantID: strings.Repeat("a", 257)},
			contains: "TenantID exceeds 256 chars",
		},
		{
			name:     "expires_in below minimum",
			opts:     TokenOptions{ExpiresIn: 30 * time.Second},
			contains: "below minimum",
		},
		{
			name:     "expires_in above maximum",
			opts:     TokenOptions{ExpiresIn: 25 * time.Hour},
			contains: "above maximum",
		},
		{
			name:     "empty allowed columns rejected",
			opts:     TokenOptions{AllowedColumns: []string{}},
			contains: "AllowedColumns is empty",
		},
		{
			name:     "unknown column name",
			opts:     TokenOptions{AllowedColumns: []string{"not_a_field"}},
			contains: "unknown column name",
		},
		{
			name:     "empty allowed actions rejected",
			opts:     TokenOptions{AllowedActions: []string{}},
			contains: "AllowedActions is empty",
		},
		{
			name:     "bare star action rejected",
			opts:     TokenOptions{AllowedActions: []string{"*"}},
			contains: "does not match grammar",
		},
		{
			name:     "prefix wildcard rejected",
			opts:     TokenOptions{AllowedActions: []string{"*.create"}},
			contains: "does not match grammar",
		},
		{
			name:     "mid-string wildcard rejected",
			opts:     TokenOptions{AllowedActions: []string{"user.*.create"}},
			contains: "does not match grammar",
		},
		{
			name:     "wildcard without preceding dot rejected",
			opts:     TokenOptions{AllowedActions: []string{"user*"}},
			contains: "does not match grammar",
		},
		{
			name:     "empty action entry rejected",
			opts:     TokenOptions{AllowedActions: []string{""}},
			contains: "does not match grammar",
		},
	}

	c := New(testProjectID, "k") // no server needed; client-side validation only
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := c.MintToken(t.Context(), tc.opts)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.contains)
		})
	}
}

func TestMintToken_AllowedActionFormsAccepted(t *testing.T) {
	t.Parallel()
	srv := mintServer(t, nil)

	tests := []struct {
		name    string
		actions []string
	}{
		{"exact single segment", []string{"login"}},
		{"exact multi segment", []string{"user.login"}},
		{"deep multi segment", []string{"billing.invoice.created"}},
		{"suffix wildcard", []string{"user.*"}},
		{"deep suffix wildcard", []string{"billing.invoice.*"}},
		{"mixed case allowed", []string{"User.Login", "Billing.Invoice"}},
		{"underscores in segments", []string{"v1_create", "user.password_reset"}},
		{"mixed exact and wildcard", []string{"user.login", "user.*", "billing.*"}},
	}

	c := New(testProjectID, "k", WithBaseURL(srv.URL))
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := c.MintToken(t.Context(), TokenOptions{AllowedActions: tc.actions})
			require.NoError(t, err)
		})
	}
}

func TestMintToken_AllEventColumnsAccepted(t *testing.T) {
	t.Parallel()
	srv := mintServer(t, nil)

	// All json-tagged fields on event.Event derived via reflection at
	// package init. The server allowlists the same set the same way,
	// so every column known to sdk-go's Event must be accepted.
	expected := []string{
		"id", "tenant_id", "occurred_at", "actor", "action", "target",
		"metadata", "origin", "result", "change", "idempotency_key",
	}

	c := New(testProjectID, "k", WithBaseURL(srv.URL))
	_, err := c.MintToken(t.Context(), TokenOptions{AllowedColumns: expected})
	require.NoError(t, err)

	// Sanity check that the runtime-derived set matches the expected columns.
	require.Len(t, allowedColumns, len(expected))
	for _, col := range expected {
		_, ok := allowedColumns[col]
		require.True(t, ok, "expected %q in allowedColumns", col)
	}
}

func TestMintToken_HTTPErrorReturnedAsError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		status     int
		body       string
		wantStatus int
		wantBody   string
	}{
		{"400 bad request", http.StatusBadRequest, "invalid tenant_id", http.StatusBadRequest, "invalid tenant_id"},
		{"401 unauthorized", http.StatusUnauthorized, "", http.StatusUnauthorized, ""},
		{"404 not found", http.StatusNotFound, "project soft-deleted", http.StatusNotFound, "project soft-deleted"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				if tc.body != "" {
					_, _ = w.Write([]byte(tc.body))
				}
			}))
			defer srv.Close()

			c := New(testProjectID, "k", WithBaseURL(srv.URL))
			_, err := c.MintToken(t.Context(), TokenOptions{})
			require.Error(t, err)

			var embedErr *Error
			require.True(t, errors.As(err, &embedErr))
			require.Equal(t, tc.wantStatus, embedErr.StatusCode)
			require.Equal(t, tc.wantBody, embedErr.Body)
		})
	}
}

func TestMintToken_ContextCancellation(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Block until the test's context cancels - never actually
		// responds.
		<-r.Context().Done()
	}))
	defer srv.Close()

	c := New(testProjectID, "k", WithBaseURL(srv.URL))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := c.MintToken(ctx, TokenOptions{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestNew_DefaultBaseURL(t *testing.T) {
	t.Parallel()
	c := New(testProjectID, "k")
	require.Equal(t, defaultBaseURL, c.baseURL)
}

func TestWithBaseURL_TrimsTrailingSlash(t *testing.T) {
	t.Parallel()
	c := New(testProjectID, "k", WithBaseURL("https://example.com/api/"))
	require.Equal(t, "https://example.com/api", c.baseURL)
}

func TestWithHTTPClient_SetsClient(t *testing.T) {
	t.Parallel()
	custom := &http.Client{}
	c := New(testProjectID, "k", WithHTTPClient(custom))
	require.Same(t, custom, c.client)
}
