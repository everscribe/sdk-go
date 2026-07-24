package stdlib_test

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/everscribe/sdk-go/adapters/http/stdlib"
	"github.com/everscribe/sdk-go/pkg/event"
	"github.com/everscribe/sdk-go/pkg/recorder"
)

// spyRecorder captures what the lifecycle submits.
//
// callPrepare models the difference that drives the whole dedupe design: the
// stock recorders call event.PrepareEvent (HTTPRecorder at http.go:105,
// BufferedRecorder at buffered.go:180), but a custom Recorder is free not to,
// which is the case the idempotency key exists to cover.
type spyRecorder struct {
	mu          sync.Mutex
	got         []event.Event
	callPrepare bool
}

func (s *spyRecorder) Record(ctx context.Context, e *event.Event) error {
	if s.callPrepare {
		event.PrepareEvent(ctx, e)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, *e) // by value: proves the flag is not on Event
	return nil
}

func (s *spyRecorder) events() []event.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]event.Event(nil), s.got...)
}

type nopLogger struct{}

func (nopLogger) Error(string, ...any) {}

// serve mounts h behind the middleware and returns a test server plus a
// channel closed once the middleware (including its deferred end) has fully
// returned, so assertions do not race the auto-record.
func serve(t *testing.T, rec event.Recorder, h http.HandlerFunc) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	mw := stdlib.New(stdlib.Options{Recorder: rec, Logger: nopLogger{}})
	finished := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		mw(h).ServeHTTP(w, r)
	}))
	srv.Config.ErrorLog = log.New(io.Discard, "", 0) // panic case logs; keep output clean
	t.Cleanup(srv.Close)
	return srv, finished
}

// TestAutoRecord_HappyPath is the baseline: the adapter records once, with the
// outcome derived from the response, and Begin's idempotency key in place.
func TestAutoRecord_HappyPath(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{callPrepare: true}
	srv, finished := serve(t, spy, func(w http.ResponseWriter, r *http.Request) {
		event.Current(r.Context()).Action = "user.login"
		w.WriteHeader(http.StatusOK)
	})

	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	resp.Body.Close()
	<-finished

	got := spy.events()
	require.Len(t, got, 1)
	require.Equal(t, "user.login", got[0].Action)
	require.Equal(t, "ok", got[0].Result.Status)
	require.Equal(t, 200, got[0].Result.Code)
	require.Equal(t, got[0].ID, got[0].IdempotencyKey,
		"Begin stamps the key so both submission paths agree")
}

// TestAutoRecord_UnnamedEventNotRecorded confirms Action is still a
// precondition: an event the handler never named is not recorded.
func TestAutoRecord_UnnamedEventNotRecorded(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{callPrepare: true}
	srv, finished := serve(t, spy, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	resp.Body.Close()
	<-finished

	require.Empty(t, spy.events())
}

// TestDedupe_ManualThenAuto_RecordsOnce is spec case 2. The handler records
// manually through a stock recorder, which calls PrepareEvent, which marks the
// request-scoped event by pointer identity, so end() skips.
func TestDedupe_ManualThenAuto_RecordsOnce(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{callPrepare: true}
	srv, finished := serve(t, spy, func(w http.ResponseWriter, r *http.Request) {
		e := event.Current(r.Context())
		e.Action = "user.login"
		w.WriteHeader(http.StatusOK)
		require.NoError(t, spy.Record(r.Context(), e))
	})

	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	resp.Body.Close()
	<-finished

	require.Len(t, spy.events(), 1, "the flag must suppress the adapter path")
}

// TestDedupe_CustomRecorderSkippingPrepare_BothCarrySameKey is spec case 3, the
// belt-and-braces path. A custom recorder never calls PrepareEvent, so the flag
// is never set and both paths submit. The design's guarantee is not that this
// produces one submission, but that both carry the SAME idempotency key, so the
// server's ON CONFLICT arbiter absorbs the duplicate instead of colliding on
// the id primary key.
//
// internal/contracttest/dedupe_test.go proves the server half of that claim.
func TestDedupe_CustomRecorderSkippingPrepare_BothCarrySameKey(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{callPrepare: false} // the custom recorder
	srv, finished := serve(t, spy, func(w http.ResponseWriter, r *http.Request) {
		e := event.Current(r.Context())
		e.Action = "user.login"
		w.WriteHeader(http.StatusOK)
		require.NoError(t, spy.Record(r.Context(), e))
	})

	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	resp.Body.Close()
	<-finished

	got := spy.events()
	require.Len(t, got, 2, "without PrepareEvent the flag cannot be set, so both fire")
	require.Equal(t, got[0].ID, got[1].ID)
	require.Equal(t, got[0].IdempotencyKey, got[1].IdempotencyKey)
	require.NotEmpty(t, got[0].IdempotencyKey,
		"both submissions must be keyed or the second collides on the primary key")
}

// TestClones_StayKeyless is spec constraint 2. Begin stamps the request-scoped
// event, never the template, so FromContext clones inherit no key. Distinct IDs
// sharing one key would be silently deduped against each other.
func TestClones_StayKeyless(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{callPrepare: true}
	var cloneA, cloneB *event.Event
	srv, finished := serve(t, spy, func(w http.ResponseWriter, r *http.Request) {
		event.Current(r.Context()).Action = "user.login"
		cloneA = event.FromContext(r.Context())
		cloneB = event.FromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	resp.Body.Close()
	<-finished

	require.Empty(t, cloneA.IdempotencyKey, "clones must not inherit the key")
	require.Empty(t, cloneB.IdempotencyKey)
	require.NotEqual(t, cloneA.ID, cloneB.ID, "clones get fresh IDs")
	require.NotEqual(t, cloneA.ID, spy.events()[0].ID)
}

// TestPanickingHandler_RecordsNoResponseWritten is spec case 8. end() is
// deferred, so it runs while the panic unwinds. The capture reports no outcome,
// and core supplies the diagnostic rather than a fabricated 200.
func TestPanickingHandler_RecordsNoResponseWritten(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{callPrepare: true}
	srv, finished := serve(t, spy, func(w http.ResponseWriter, r *http.Request) {
		event.Current(r.Context()).Action = "user.login"
		panic("boom")
	})

	resp, err := http.Get(srv.URL) //nolint:bodyclose // connection is torn down by the panic
	if err == nil {
		resp.Body.Close()
	}
	<-finished

	got := spy.events()
	require.Len(t, got, 1, "a panicking handler must still record")
	require.Equal(t, "error", got[0].Result.Status)
	require.Equal(t, "no response written", got[0].Result.Message)
	require.Zero(t, got[0].Result.Code)
}

// TestClientDisconnect_StillRecords is spec case 7, and the reason end() uses
// context.WithoutCancel. It runs the REAL HTTPRecorder against a stand-in
// ingest server, because the failure mode lives in
// http.NewRequestWithContext(ctx, ...) at http.go:160: with the request's own
// context, a disconnected client cancels the audit POST too, dropping exactly
// the events worth keeping.
func TestClientDisconnect_StillRecords(t *testing.T) {
	t.Parallel()

	ingested := make(chan event.Event, 4)
	ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var e event.Event
		if err := json.NewDecoder(r.Body).Decode(&e); err == nil {
			ingested <- e
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(ingest.Close)

	rec := recorder.NewHTTPRecorder("proj", "key", recorder.WithBaseURL(ingest.URL))

	started := make(chan struct{})
	release := make(chan struct{})
	srv, finished := serve(t, rec, func(w http.ResponseWriter, r *http.Request) {
		event.Current(r.Context()).Action = "user.login"
		close(started)
		<-release // hold the handler open while the client goes away
		w.WriteHeader(http.StatusOK)
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}()

	<-started
	cancel() // client disconnects mid-handler
	close(release)
	<-finished

	select {
	case e := <-ingested:
		require.Equal(t, "user.login", e.Action)
	case <-time.After(5 * time.Second):
		t.Fatal("event was dropped: end() recorded against a canceled context")
	}
}
