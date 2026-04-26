package audit

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewRecorder_AppliesHTTPOption(t *testing.T) {
	t.Parallel()

	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	r := NewRecorder(testProjectID, "k",
		WithBaseURL(srv.URL),
		WithFlushInterval(20*time.Millisecond),
	)
	defer r.Close()

	require.NoError(t, r.Record(context.Background(), NewEvent("user.login")))
	require.NoError(t, r.Flush(context.Background()))
	require.Equal(t, "/v1/projects/"+testProjectID+"/events/batch", gotPath)
}

func TestNewRecorder_AppliesBufferedOption(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	r := NewRecorder(testProjectID, "k",
		WithBaseURL(srv.URL),
		WithBufferSize(7),
	)
	defer r.Close()

	require.Equal(t, 7, r.Stats().BufferSize)
}

func TestNewRecorder_RecordFlushClose(t *testing.T) {
	t.Parallel()

	var (
		mu      sync.Mutex
		batches [][]Event
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var got struct {
			Events []Event `json:"events"`
		}
		_ = json.Unmarshal(body, &got)
		mu.Lock()
		batches = append(batches, got.Events)
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	r := NewRecorder(testProjectID, "k",
		WithBaseURL(srv.URL),
		WithFlushInterval(time.Hour), // ensure only Flush triggers the send
	)
	defer r.Close()

	require.NoError(t, r.Record(context.Background(), NewEvent("a.one")))
	require.NoError(t, r.Record(context.Background(), NewEvent("a.two")))
	require.NoError(t, r.Flush(context.Background()))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, batches, 1)
	require.Len(t, batches[0], 2)
	require.Equal(t, "a.one", batches[0][0].Action)
	require.Equal(t, "a.two", batches[0][1].Action)
}

func TestNewRecorder_AutoCapturesResultViaMiddleware(t *testing.T) {
	t.Parallel()

	var captured Event
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &captured)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer apiSrv.Close()

	rec := NewHTTPRecorder(testProjectID, "k", WithBaseURL(apiSrv.URL))

	handler := NewMiddleware(nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e := EventFromContext(r.Context())
		e.Action = "user.lock"
		w.WriteHeader(http.StatusForbidden)
		require.NoError(t, rec.Record(r.Context(), e))
	}))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))

	require.Equal(t, "denied", captured.Result.Status)
	require.Equal(t, http.StatusForbidden, captured.Result.Code)
}
