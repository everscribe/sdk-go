package event_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/everscribe/sdk-go/pkg/event"
	"github.com/everscribe/sdk-go/pkg/recorder"
)

// serve mounts h behind the middleware and returns a test server plus a
// channel closed once the middleware (including its deferred end) has fully
// returned, so assertions do not race the auto-record.
func serve(t *testing.T, rec event.Recorder, h http.HandlerFunc) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	mw := event.Middleware(event.Options{Recorder: rec, Logger: nopLogger{}})
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
// event, never the template, so NewFromContext clones inherit no key. Distinct IDs
// sharing one key would be silently deduped against each other.
func TestClones_StayKeyless(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{callPrepare: true}
	var cloneA, cloneB *event.Event
	srv, finished := serve(t, spy, func(w http.ResponseWriter, r *http.Request) {
		event.Current(r.Context()).Action = "user.login"
		cloneA = event.NewFromContext(r.Context())
		cloneB = event.NewFromContext(r.Context())
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

// TestMidHandlerRecord_DoesNotStampNoResponseWritten is the falsification for
// C1. PrepareEvent used to run applyOutcome's ok == false fallback
// unconditionally, so a NewFromContext clone recorded before the response is
// written - the pattern pkg/recorder/doc.go recommends for multiple events
// per handler - got a false "no response written" error baked into an
// immutable audit record, even though the handler simply had not written a
// response YET, not because it never would. That sentinel must be reserved
// for end(), which runs after the handler has genuinely finished.
func TestMidHandlerRecord_DoesNotStampNoResponseWritten(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{callPrepare: true}
	srv, finished := serve(t, spy, func(w http.ResponseWriter, r *http.Request) {
		event.Current(r.Context()).Action = "user.login"

		mid := event.NewFromContext(r.Context())
		mid.Action = "user.login.attempt"
		require.NoError(t, spy.Record(r.Context(), mid)) // recorded before any write

		w.WriteHeader(http.StatusOK)
	})

	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	resp.Body.Close()
	<-finished

	got := spy.events()
	require.Len(t, got, 2, "both the mid-handler clone and the auto-recorded event must land")

	var midEvent, autoEvent event.Event
	for _, e := range got {
		if e.Action == "user.login.attempt" {
			midEvent = e
		} else {
			autoEvent = e
		}
	}

	require.NotEqual(t, "error", midEvent.Result.Status,
		"a mid-handler record must not be stamped with the final-outcome sentinel")
	require.NotEqual(t, "no response written", midEvent.Result.Message,
		"the response had not been written YET, which is not the same as never")

	require.Equal(t, "ok", autoEvent.Result.Status, "the auto-recorded event must still get the real outcome")
	require.Equal(t, 200, autoEvent.Result.Code)
}

func TestNilResolverDefaultsToAnonymous(t *testing.T) {
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

	require.Equal(t, "anonymous", spy.events()[0].Actor.Type)
}

func TestOriginPopulatedFromRequest(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{callPrepare: true}
	srv, finished := serve(t, spy, func(w http.ResponseWriter, r *http.Request) {
		event.Current(r.Context()).Action = "user.login"
		w.WriteHeader(http.StatusOK)
	})

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	req.Header.Set("User-Agent", "curl/8.0")
	req.Header.Set("X-Request-ID", "req-abc")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	<-finished

	got := spy.events()[0]
	require.Equal(t, "curl/8.0", got.Origin.UserAgent)
	require.Equal(t, "req-abc", got.Origin.RequestID)
	require.NotEmpty(t, got.Origin.IP)
}

// TestFlagUnderContention drives the CAS from two goroutines through a
// synthetic driver rather than two real Record calls. Two concurrent
// Records would fail -race on ID, OccurredAt, Result, and IdempotencyKey
// for reasons unrelated to the flag, which is a race the design permits
// and the concurrency contract forbids.
func TestFlagUnderContention(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{callPrepare: true}
	srv, finished := serve(t, spy, func(w http.ResponseWriter, r *http.Request) {
		e := event.Current(r.Context())
		e.Action = "user.login"
		w.WriteHeader(http.StatusOK)
		// Preset, matching the precedent in pkg/event/lifecycle_test.go's
		// TestEnd_RacesWithPrepareEvent: every goroutine's applyOutcome call
		// becomes a pure read (the "already set" branch), isolating the race
		// to the recorded flag itself instead of tripping the already-known,
		// accepted data race on Event.Result when two callers populate the
		// same outcome concurrently.
		e.Result = event.Result{Status: "ok", Code: 200}

		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				event.PrepareEvent(r.Context(), e)
			}()
		}
		wg.Wait()
	})

	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	resp.Body.Close()
	<-finished

	require.Empty(t, spy.events(), "every PrepareEvent marked it; end must skip")
}

// TestFlush_SupportsServerSentEvents is the I4 falsification. stdlibResponseWriter
// embeds http.ResponseWriter with no Unwrap, Flush, or Hijack, so
// http.ResponseController and direct http.Flusher type assertions both stop
// working behind this middleware, breaking SSE and any other handler that
// needs to push a partial response before it finishes.
//
// The handler writes a first chunk, flushes it explicitly, then blocks on
// release before writing a second chunk. Without a working Flush the first
// chunk sits in net/http's own write buffer until the handler returns, so
// the client would never observe it before release is closed.
func TestFlush_SupportsServerSentEvents(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{callPrepare: true}
	release := make(chan struct{})

	srv, finished := serve(t, spy, func(w http.ResponseWriter, r *http.Request) {
		event.Current(r.Context()).Action = "stream.tail"
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, werr := w.Write([]byte("data: first\n\n"))
		require.NoError(t, werr)
		require.NoError(t, http.NewResponseController(w).Flush(),
			"http.NewResponseController(w).Flush() must reach the underlying writer")
		<-release
		_, _ = w.Write([]byte("data: second\n\n"))
	})

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	firstChunkSeen := make(chan string, 1)
	go func() {
		line, _ := reader.ReadString('\n')
		firstChunkSeen <- line
	}()

	select {
	case line := <-firstChunkSeen:
		require.True(t, strings.Contains(line, "first"),
			"expected the flushed first chunk, got %q", line)
	case <-time.After(2 * time.Second):
		t.Fatal("first chunk never arrived before release: Flush did not propagate to the client")
	}

	close(release)
	<-finished
}

// TestHijack_TypeAssertionSucceeds is the I4 falsification for websocket
// upgrades. A handler behind this middleware type-asserts
// w.(http.Hijacker) exactly as it would with no middleware mounted;
// stdlibResponseWriter must implement Hijack for that to succeed.
func TestHijack_TypeAssertionSucceeds(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{callPrepare: true}
	hijackerOK := make(chan bool, 1)

	srv, finished := serve(t, spy, func(w http.ResponseWriter, r *http.Request) {
		event.Current(r.Context()).Action = "ws.upgrade"
		hj, ok := w.(http.Hijacker)
		hijackerOK <- ok
		if !ok {
			w.WriteHeader(http.StatusOK)
			return
		}
		conn, _, err := hj.Hijack()
		require.NoError(t, err)
		conn.Close()
	})

	resp, err := http.Get(srv.URL) //nolint:bodyclose // the handler hijacks and closes the connection itself
	if err == nil {
		resp.Body.Close()
	}
	<-finished

	require.True(t, <-hijackerOK,
		"stdlibResponseWriter must implement http.Hijacker for the type assertion to succeed")
}
