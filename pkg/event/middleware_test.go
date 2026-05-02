package event

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMiddleware_InstallsTemplate(t *testing.T) {
	t.Parallel()
	var got *Event
	resolver := func(ctx context.Context) Actor {
		return Actor{Type: "user", ID: "u1", DisplayName: "alice"}
	}
	handler := NewMiddleware(resolver)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = FromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Forwarded-For", "1.2.3.4")
	req.Header.Set("User-Agent", "ua")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.NotNil(t, got)
	require.Equal(t, "user", got.Actor.Type)
	require.Equal(t, "u1", got.Actor.ID)
	require.Equal(t, "alice", got.Actor.DisplayName)
	require.Equal(t, "1.2.3.4", got.Origin.IP)
	require.Equal(t, "ua", got.Origin.UserAgent)
}

func TestMiddleware_NilResolverDefaultsToAnonymous(t *testing.T) {
	t.Parallel()
	var got *Event
	handler := NewMiddleware(nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = FromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, "anonymous", got.Actor.Type)
}

func TestMiddleware_StashesWrappedWriterOnContext(t *testing.T) {
	t.Parallel()
	var seenStatus int
	handler := NewMiddleware(nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		// After WriteHeader runs, PrepareEvent (called via Recorder.Record)
		// should be able to pull the wrapped writer from context and read
		// the captured status.
		rw, ok := r.Context().Value(wrappedWriterKey{}).(*responseWriter)
		require.True(t, ok)
		seenStatus = rw.Status()
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, http.StatusTeapot, seenStatus)
}

func TestResponseWriter_StatusCapture(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		do   func(rw *responseWriter) // operation under test
		want int
	}{
		{
			name: "captures explicit WriteHeader status",
			do:   func(rw *responseWriter) { rw.WriteHeader(http.StatusNotFound) },
			want: http.StatusNotFound,
		},
		{
			name: "Write without WriteHeader implies 200 OK",
			do:   func(rw *responseWriter) { _, _ = rw.Write([]byte("hi")) },
			want: http.StatusOK,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rw := wrapWriter(httptest.NewRecorder())
			tc.do(rw)
			require.Equal(t, tc.want, rw.Status())
		})
	}
}

func TestResponseWriter_IdempotentWrapping(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	rw1 := wrapWriter(rec)
	rw2 := wrapWriter(rw1)
	require.Same(t, rw1, rw2, "wrapping a wrapped writer should return the same instance")
}

func TestResultFromWrappedWriter(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		status      int // 0 means "do not write a header"
		wantStatus  string
		wantCode    int
		wantMessage string // substring match if non-empty
	}{
		{name: "2xx maps to ok", status: http.StatusOK, wantStatus: "ok", wantCode: http.StatusOK},
		{name: "3xx maps to ok (POST-redirect-GET happy path)", status: http.StatusSeeOther, wantStatus: "ok", wantCode: http.StatusSeeOther},
		{name: "401 maps to denied", status: http.StatusUnauthorized, wantStatus: "denied", wantCode: http.StatusUnauthorized},
		{name: "403 maps to denied", status: http.StatusForbidden, wantStatus: "denied", wantCode: http.StatusForbidden},
		{name: "5xx maps to error", status: http.StatusInternalServerError, wantStatus: "error", wantCode: http.StatusInternalServerError},
		{name: "no write maps to error with message", status: 0, wantStatus: "error", wantMessage: "no response written"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rw := wrapWriter(httptest.NewRecorder())
			if tc.status != 0 {
				rw.WriteHeader(tc.status)
			}
			r := resultFromWrappedWriter(rw)
			require.Equal(t, tc.wantStatus, r.Status)
			require.Equal(t, tc.wantCode, r.Code)
			if tc.wantMessage != "" {
				require.Contains(t, r.Message, tc.wantMessage)
			}
		})
	}
}
