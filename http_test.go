package audit

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

const testProjectID = "proj_123"

func TestHTTPRecorder_Record_PostsSingleEvent(t *testing.T) {
	t.Parallel()

	var gotPath, gotAuth, gotContentType string
	var gotBody Event
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotContentType = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	rec := NewHTTPRecorder(testProjectID, "secret-key", WithBaseURL(srv.URL))
	e := NewEvent("user.login")
	e.Actor = Actor{Type: "user", ID: "u1"}
	require.NoError(t, rec.Record(context.Background(), e))

	require.Equal(t, "/v1/projects/"+testProjectID+"/events", gotPath)
	require.Equal(t, "Bearer secret-key", gotAuth)
	require.Equal(t, "application/json", gotContentType)
	require.Equal(t, "user.login", gotBody.Action)
	require.Equal(t, "u1", gotBody.Actor.ID)
}

func TestHTTPRecorder_Record_EmptyActionIsNoOp(t *testing.T) {
	t.Parallel()

	hit := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	rec := NewHTTPRecorder(testProjectID, "k", WithBaseURL(srv.URL))
	require.NoError(t, rec.Record(context.Background(), &Event{}))
	require.False(t, hit)
}

func TestHTTPRecorder_RecordBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		events     []Event
		wantHit    bool
		wantPath   string
		wantCount  int
		wantFirst  string // optional: first event action in filtered body
		wantSecond string // optional: second event action in filtered body
	}{
		{
			name: "posts array body with project-scoped batch path",
			events: []Event{
				{Action: "user.login", Actor: Actor{Type: "user", ID: "u1"}},
				{Action: "user.logout", Actor: Actor{Type: "user", ID: "u1"}},
			},
			wantHit:    true,
			wantPath:   "/v1/projects/" + testProjectID + "/events/batch",
			wantCount:  2,
			wantFirst:  "user.login",
			wantSecond: "user.logout",
		},
		{
			name: "filters out empty action events",
			events: []Event{
				{Action: "user.login"},
				{Action: ""},
				{Action: "user.logout"},
			},
			wantHit:    true,
			wantPath:   "/v1/projects/" + testProjectID + "/events/batch",
			wantCount:  2,
			wantFirst:  "user.login",
			wantSecond: "user.logout",
		},
		{
			name:    "nil slice is no-op",
			events:  nil,
			wantHit: false,
		},
		{
			name:    "empty slice is no-op",
			events:  []Event{},
			wantHit: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var (
				hit     bool
				gotPath string
				gotBody struct {
					Events []Event `json:"events"`
				}
			)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hit = true
				gotPath = r.URL.Path
				body, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(body, &gotBody)
				w.WriteHeader(http.StatusAccepted)
			}))
			defer srv.Close()

			rec := NewHTTPRecorder(testProjectID, "k", WithBaseURL(srv.URL))
			require.NoError(t, rec.RecordBatch(context.Background(), tc.events))

			require.Equal(t, tc.wantHit, hit)
			if !tc.wantHit {
				return
			}
			require.Equal(t, tc.wantPath, gotPath)
			require.Len(t, gotBody.Events, tc.wantCount)
			if tc.wantFirst != "" {
				require.Equal(t, tc.wantFirst, gotBody.Events[0].Action)
			}
			if tc.wantSecond != "" {
				require.Equal(t, tc.wantSecond, gotBody.Events[1].Action)
			}
		})
	}
}

func TestHTTPRecorder_Record_ErrorStatusClassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		status        int
		body          string
		wantTransient bool
		wantBody      string // substring check on HTTPError.Body
	}{
		{name: "400 is permanent", status: http.StatusBadRequest, body: "invalid event", wantTransient: false, wantBody: "invalid event"},
		{name: "401 is permanent", status: http.StatusUnauthorized, wantTransient: false},
		{name: "500 is transient", status: http.StatusInternalServerError, wantTransient: true},
		{name: "502 is transient", status: http.StatusBadGateway, wantTransient: true},
		{name: "503 is transient", status: http.StatusServiceUnavailable, wantTransient: true},
		{name: "429 is transient", status: http.StatusTooManyRequests, wantTransient: true},
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

			rec := NewHTTPRecorder(testProjectID, "k", WithBaseURL(srv.URL))
			err := rec.Record(context.Background(), NewEvent("test"))
			require.Error(t, err)

			var httpErr *HTTPError
			require.True(t, errors.As(err, &httpErr))
			require.Equal(t, tc.status, httpErr.StatusCode)
			require.Equal(t, tc.wantTransient, httpErr.Transient())
			if tc.wantBody != "" {
				require.Contains(t, httpErr.Body, tc.wantBody)
			}
		})
	}
}

func TestHTTPRecorder_WithBaseURL_TrimsTrailingSlash(t *testing.T) {
	t.Parallel()

	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	rec := NewHTTPRecorder(testProjectID, "k", WithBaseURL(srv.URL+"/"))
	require.NoError(t, rec.Record(context.Background(), NewEvent("t")))
	require.Equal(t, "/v1/projects/"+testProjectID+"/events", gotPath)
}

func TestHTTPRecorder_DefaultBaseURL(t *testing.T) {
	t.Parallel()

	rec := NewHTTPRecorder(testProjectID, "k")
	require.Equal(t, defaultBaseURL, rec.baseURL)
}

func TestHTTPRecorder_WithHTTPClient(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := &http.Client{}
	rec := NewHTTPRecorder(testProjectID, "k", WithBaseURL(srv.URL), WithHTTPClient(client))
	require.Same(t, client, rec.client)
}

func TestHTTPRecorder_ImplementsRecorderAndBatchRecorder(t *testing.T) {
	t.Parallel()
	var _ Recorder = (*HTTPRecorder)(nil)
	var _ BatchRecorder = (*HTTPRecorder)(nil)
}

func TestHTTPRecorder_AutoIdempotencyKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		opt     []HTTPOption
		preset  string // pre-set IdempotencyKey on the event before Record
		wantSet bool   // expect IdempotencyKey populated server-side
		wantEq  string // when wantSet is true and preset is "", IdempotencyKey should equal Event.ID
	}{
		{
			name:    "off by default - leaves IdempotencyKey empty",
			opt:     nil,
			wantSet: false,
		},
		{
			name:    "enabled - copies Event.ID into IdempotencyKey",
			opt:     []HTTPOption{WithAutoIdempotencyKey()},
			wantSet: true,
		},
		{
			name:    "enabled but caller-supplied key wins",
			opt:     []HTTPOption{WithAutoIdempotencyKey()},
			preset:  "stripe_evt_42",
			wantSet: true,
			wantEq:  "stripe_evt_42",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var got Event
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(body, &got)
				w.WriteHeader(http.StatusAccepted)
			}))
			defer srv.Close()

			opts := append([]HTTPOption{WithBaseURL(srv.URL)}, tc.opt...)
			rec := NewHTTPRecorder(testProjectID, "k", opts...)

			e := NewEvent("user.login")
			if tc.preset != "" {
				e.IdempotencyKey = tc.preset
			}
			require.NoError(t, rec.Record(context.Background(), e))

			if !tc.wantSet {
				require.Empty(t, got.IdempotencyKey)
				return
			}
			require.NotEmpty(t, got.IdempotencyKey)
			if tc.wantEq != "" {
				require.Equal(t, tc.wantEq, got.IdempotencyKey)
			} else {
				require.Equal(t, got.ID, got.IdempotencyKey)
			}
		})
	}
}

func TestHTTPRecorder_AutoIdempotencyKey_Batch(t *testing.T) {
	t.Parallel()

	var got struct {
		Events []Event `json:"events"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &got)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	rec := NewHTTPRecorder(testProjectID, "k", WithBaseURL(srv.URL), WithAutoIdempotencyKey())

	events := []Event{
		{Action: "a.one"}, // empty key → auto-fill
		{Action: "a.two", IdempotencyKey: "explicit-key-2"}, // preset → keep
	}
	require.NoError(t, rec.RecordBatch(context.Background(), events))

	require.Len(t, got.Events, 2)
	require.Equal(t, got.Events[0].ID, got.Events[0].IdempotencyKey)
	require.Equal(t, "explicit-key-2", got.Events[1].IdempotencyKey)
}
