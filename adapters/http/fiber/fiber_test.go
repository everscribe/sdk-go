package fiber_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	fiberv3 "github.com/gofiber/fiber/v3"
	fiberrecover "github.com/gofiber/fiber/v3/middleware/recover"
	"github.com/stretchr/testify/require"

	everfiber "github.com/everscribe/sdk-go/adapters/http/fiber"
	"github.com/everscribe/sdk-go/pkg/event"
)

type spyRecorder struct {
	mu  sync.Mutex
	got []event.Event
}

func (s *spyRecorder) Record(ctx context.Context, e *event.Event) error {
	event.PrepareEvent(ctx, e)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, *e)
	return nil
}

func (s *spyRecorder) events() []event.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]event.Event(nil), s.got...)
}

type nopLogger struct{}

func (nopLogger) Error(string, ...any) {}

func newApp(spy *spyRecorder, h fiberv3.Handler) *fiberv3.App {
	app := fiberv3.New()
	app.Use(everfiber.New(everfiber.Options{Recorder: spy, Logger: nopLogger{}}))
	app.Get("/", h)
	return app
}

func TestFiber_RecordsOnce(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	app := newApp(spy, func(c fiberv3.Ctx) error {
		event.Current(c.Context()).Action = "user.login"
		return c.SendString("ok")
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/", nil))
	require.NoError(t, err)
	resp.Body.Close()

	got := spy.events()
	require.Len(t, got, 1)
	require.Equal(t, "user.login", got[0].Action)
	require.Equal(t, "ok", got[0].Result.Status)
	require.Equal(t, 200, got[0].Result.Code)
}

func TestFiber_DeniedStatus(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	app := newApp(spy, func(c fiberv3.Ctx) error {
		event.Current(c.Context()).Action = "user.login"
		return c.Status(http.StatusForbidden).SendString("nope")
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/", nil))
	require.NoError(t, err)
	resp.Body.Close()

	require.Equal(t, "denied", spy.events()[0].Result.Status)
}

// TestFiber_OriginFromHeaders confirms the c.Get wrapper, since fiber's
// Get takes variadic defaults and does not match OriginFrom's closure
// signature directly.
func TestFiber_OriginFromHeaders(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	app := newApp(spy, func(c fiberv3.Ctx) error {
		event.Current(c.Context()).Action = "user.login"
		return c.SendString("ok")
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("User-Agent", "curl/8.0")
	req.Header.Set("X-Request-ID", "req-abc")
	resp, err := app.Test(req)
	require.NoError(t, err)
	resp.Body.Close()

	got := spy.events()[0]
	require.Equal(t, "curl/8.0", got.Origin.UserAgent)
	require.Equal(t, "req-abc", got.Origin.RequestID)
}

func TestFiber_UnnamedEventNotRecorded(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	app := newApp(spy, func(c fiberv3.Ctx) error { return c.SendString("ok") })

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/", nil))
	require.NoError(t, err)
	resp.Body.Close()

	require.Empty(t, spy.events())
}

// TestFiber_NoWriteRecordsAsOK documents a real limitation of this
// adapter, called out in the package doc comment: fasthttp's
// Response().StatusCode() defaults to 200 regardless of whether the
// handler wrote anything, and fiber keeps no separate "was written" flag
// (unlike gin's Writer.Written() or echo's Response().Committed). A
// handler that names the event and returns nil without calling any Send*
// method is therefore indistinguishable from one that actually wrote a
// 200, and is recorded as such. This is the opposite of
// TestFiber_UnnamedEventNotRecorded, which covers the (distinguishable)
// case of never naming the event at all.
func TestFiber_NoWriteRecordsAsOK(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	app := newApp(spy, func(c fiberv3.Ctx) error {
		event.Current(c.Context()).Action = "user.login"
		return nil // no Send*/Status/etc. call
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/", nil))
	require.NoError(t, err)
	resp.Body.Close()

	got := spy.events()
	require.Len(t, got, 1)
	require.Equal(t, "ok", got[0].Result.Status, "fasthttp defaults StatusCode() to 200 with nothing written")
	require.Equal(t, 200, got[0].Result.Code)
}

// TestFiber_PanicYieldsNoResponseWritten verifies the completed flag's
// entire reason for existing: a handler that panics must not be recorded
// as a successful 200, even though fasthttp's Response().StatusCode()
// would report exactly that if read unconditionally.
//
// A raw panic with nothing downstream to recover it does not merely fail
// the request: fasthttp's own request-handling goroutine has no recover of
// its own (confirmed by reading valyala/fasthttp v1.72.0's server.go and
// workerpool.go, and by direct reproduction, which crashed the whole
// process, not just the request). So this test mounts fiber's own
// middleware/recover BEFORE (outer to) everfiber.New, which is what a real
// application needs to do regardless of this SDK to avoid a panicking
// handler taking down the whole server. That still exercises the case
// that matters here: the panic unwinds through this middleware's own
// defer end() before it reaches recover's defer, so completed is still
// false at that point and the audited event captures ok == false, i.e.
// Result{Status: "error", Message: "no response written"}, regardless of
// the 500 that recover's own error handler goes on to write afterward.
func TestFiber_PanicYieldsNoResponseWritten(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	app := fiberv3.New()
	app.Use(fiberrecover.New())
	app.Use(everfiber.New(everfiber.Options{Recorder: spy, Logger: nopLogger{}}))
	app.Get("/", func(c fiberv3.Ctx) error {
		event.Current(c.Context()).Action = "user.login"
		panic("boom")
	})

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/", nil))
	require.NoError(t, err)
	resp.Body.Close()

	// recover's own error handler still writes a real response to the
	// client; that is orthogonal to what this adapter audited.
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	got := spy.events()
	require.Len(t, got, 1)
	require.Equal(t, "error", got[0].Result.Status)
	require.Equal(t, "no response written", got[0].Result.Message)
	require.Zero(t, got[0].Result.Code, "must not report fasthttp's default 200")
}
