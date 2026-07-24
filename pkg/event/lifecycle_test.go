package event

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResultFromHTTPStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		code       int
		wantStatus string
		wantCode   int
	}{
		{"ok", 200, "ok", 200},
		{"redirect is ok", 302, "ok", 302},
		{"unauthorized is denied", 401, "denied", 401},
		{"forbidden is denied", 403, "denied", 403},
		{"not found is error", 404, "error", 404},
		{"server error", 500, "error", 500},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := ResultFromHTTPStatus(tt.code)
			require.Equal(t, tt.wantStatus, got.Status)
			require.Equal(t, tt.wantCode, got.Code)
		})
	}
}

func TestResultFromHTTPStatus_ZeroIsNoResponseWritten(t *testing.T) {
	t.Parallel()
	got := ResultFromHTTPStatus(0)
	require.Equal(t, "error", got.Status)
	require.Equal(t, "no response written", got.Message)
	require.Zero(t, got.Code)
}

func TestOriginFrom_PrefersForwardedFor(t *testing.T) {
	t.Parallel()
	headers := map[string]string{
		"X-Forwarded-For": "203.0.113.9, 70.41.3.18",
		"X-Real-IP":       "198.51.100.7",
		"User-Agent":      "curl/8.0",
		"X-Request-ID":    "req-abc",
	}
	got := OriginFrom(func(n string) string { return headers[n] }, "10.0.0.1:54321")
	require.Equal(t, "203.0.113.9", got.IP, "first entry of X-Forwarded-For wins")
	require.Equal(t, "curl/8.0", got.UserAgent)
	require.Equal(t, "req-abc", got.RequestID)
}

func TestOriginFrom_FallsBackToRemoteAddr(t *testing.T) {
	t.Parallel()
	got := OriginFrom(func(string) string { return "" }, "10.0.0.1:54321")
	require.Equal(t, "10.0.0.1", got.IP, "port is stripped")
	require.Empty(t, got.UserAgent)
}

func TestOriginFrom_EmptyHeaderClosure(t *testing.T) {
	t.Parallel()
	got := OriginFrom(func(string) string { return "" }, "")
	require.Equal(t, Origin{}, got, "adapters with no headers pass a closure returning empty")
}

// TestClientIPFrom_PortStripping is the falsification for I2. clientIPFrom
// used to strip everything after the last colon unconditionally, assuming
// remoteAddr was always "host:port". That corrupts a bare IPv6 literal (no
// brackets, no port, but multiple colons of its own): "2001:db8::1" became
// "2001:db8:" and "::1" became ":". Stripping a trailing port is only
// unambiguous for bracketed IPv6 ("[::1]:8080") or a plain host:port with
// exactly one colon; a bare IPv6 literal must come back unchanged.
func TestClientIPFrom_PortStripping(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		remoteAddr string
		want       string
	}{
		{"ipv4 with port", "203.0.113.9:54321", "203.0.113.9"},
		{"ipv4 without port", "203.0.113.9", "203.0.113.9"},
		{"bracketed ipv6 with port", "[2001:db8::1]:54321", "2001:db8::1"},
		{"bare ipv6 without port", "2001:db8::1", "2001:db8::1"},
		{"bare loopback ipv6 without port", "::1", "::1"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := OriginFrom(func(string) string { return "" }, tt.remoteAddr)
			require.Equal(t, tt.want, got.IP)
		})
	}
}

type stubCapture struct {
	result Result
	ok     bool
}

func (s stubCapture) Outcome() (Result, bool) { return s.result, s.ok }

type stubRecorder struct {
	mu  sync.Mutex
	got []Event
}

func (s *stubRecorder) Record(_ context.Context, e *Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, *e) // by value: go vet copylocks fails if the flag moves onto Event
	return nil
}

type stubLogger struct{}

func (stubLogger) Error(string, ...any) {}

func TestBegin_StampsKeyOnCurrentNotTemplate(t *testing.T) {
	t.Parallel()
	tmpl := &Event{Action: "tmpl"}
	ctx, end := Begin(t.Context(), tmpl, stubCapture{}, &stubRecorder{}, stubLogger{})
	defer end()

	cur := Current(ctx)
	require.NotEmpty(t, cur.IdempotencyKey)
	require.Equal(t, cur.ID, cur.IdempotencyKey)
	require.Empty(t, tmpl.IdempotencyKey, "the template must stay unstamped")

	clone := FromContext(ctx)
	require.Empty(t, clone.IdempotencyKey, "clones must not inherit the key")
	require.NotEqual(t, cur.ID, clone.ID)
}

func TestEnd_SkipsUnnamedEvent(t *testing.T) {
	t.Parallel()
	rec := &stubRecorder{}
	_, end := Begin(t.Context(), &Event{}, stubCapture{Result{Status: "ok"}, true}, rec, stubLogger{})
	end()
	require.Empty(t, rec.got, "an event the handler never named is not recorded")
}

func TestEnd_RecordsOnceAndAppliesOutcome(t *testing.T) {
	t.Parallel()
	rec := &stubRecorder{}
	ctx, end := Begin(t.Context(), &Event{}, stubCapture{Result{Status: "ok", Code: 200}, true}, rec, stubLogger{})
	Current(ctx).Action = "user.login"
	end()
	end() // second call must be a no-op

	require.Len(t, rec.got, 1)
	require.Equal(t, "ok", rec.got[0].Result.Status)
	require.Equal(t, 200, rec.got[0].Result.Code)
}

// TestEnd_RacesWithPrepareEvent exercises the actual scenario the recorded
// flag's atomic.Bool/CompareAndSwap pair exists for: two library code paths
// writing the flag concurrently, not a single caller misusing end(). One
// goroutine calls the automatic path (end); a second goroutine calls the
// manual path's claim step (PrepareEvent). A third goroutine calls end again
// concurrently too, because racing a single end() against a single
// PrepareEvent call cannot, by construction, distinguish a correct
// CompareAndSwap from a naive load-then-store: PrepareEvent never performs a
// check-then-act on the flag (it unconditionally Stores when it owns the
// current event), so there is only ever one checker in that pairing and no
// checker can race itself. The regression this test must catch, two
// concurrent end() calls both winning, requires two checkers.
//
// Result is preset below so every goroutine's applyOutcome call is a pure
// read (the "already set" branch), not a write, keeping the race isolated to
// the recorded flag itself rather than tripping an unrelated, already-known
// data race on Event.Result when two callers populate the same outcome
// concurrently.
//
// Invariant: end() must never record more than once, no matter how many
// goroutines race the flag. It is not always exactly one, because when
// PrepareEvent's claim lands before either end() call's CompareAndSwap, both
// end() calls correctly back off and zero records land here; per the design,
// the manual caller who won that claim is the one responsible for recording,
// through a separate call this test does not simulate. Across many
// iterations the loop also confirms end() does win and record at least once,
// so the assertion is not vacuously true.
func TestEnd_RacesWithPrepareEvent(t *testing.T) {
	t.Parallel()

	const iterations = 300
	wonAtLeastOnce := false
	for i := 0; i < iterations; i++ {
		rec := &stubRecorder{}
		ctx, end := Begin(t.Context(), &Event{}, stubCapture{Result{Status: "ok", Code: 200}, true}, rec, stubLogger{})
		cur := Current(ctx)
		cur.Action = "user.login"
		cur.Result = Result{Status: "ok", Code: 200} // preset: see comment above

		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			end()
		}()
		go func() {
			defer wg.Done()
			end()
		}()
		go func() {
			defer wg.Done()
			PrepareEvent(ctx, Current(ctx))
		}()
		wg.Wait()

		require.LessOrEqualf(t, len(rec.got), 1, "iteration %d: end must never record more than once", i)
		if len(rec.got) == 1 {
			wonAtLeastOnce = true
		}
	}
	require.True(t, wonAtLeastOnce, "end must win the race and record at least once across many iterations")
}

func TestEnd_NoOutcomeYieldsDiagnostic(t *testing.T) {
	t.Parallel()
	rec := &stubRecorder{}
	ctx, end := Begin(t.Context(), &Event{}, stubCapture{ok: false}, rec, stubLogger{})
	Current(ctx).Action = "user.login"
	end()

	require.Len(t, rec.got, 1)
	require.Equal(t, "error", rec.got[0].Result.Status)
	require.Equal(t, "no response written", rec.got[0].Result.Message)
}

func TestPrepareEvent_MarksCurrentByPointerIdentity(t *testing.T) {
	t.Parallel()
	rec := &stubRecorder{}
	ctx, end := Begin(t.Context(), &Event{}, stubCapture{Result{Status: "ok"}, true}, rec, stubLogger{})
	cur := Current(ctx)
	cur.Action = "user.login"

	PrepareEvent(ctx, cur) // the manual path marks it
	end()                  // must now skip

	require.Empty(t, rec.got, "end must not submit an event the manual path already claimed")
}

func TestPrepareEvent_DoesNotMarkClones(t *testing.T) {
	t.Parallel()
	rec := &stubRecorder{}
	ctx, end := Begin(t.Context(), &Event{}, stubCapture{Result{Status: "ok"}, true}, rec, stubLogger{})
	Current(ctx).Action = "user.login"

	clone := FromContext(ctx)
	clone.Action = "user.logout"
	PrepareEvent(ctx, clone) // a different event: must not claim the slot
	end()

	require.Len(t, rec.got, 1, "recording a clone must not suppress the auto-record")
	require.Equal(t, "user.login", rec.got[0].Action)
}
