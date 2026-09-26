package event_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	fiberv3 "github.com/gofiber/fiber/v3"
	fiberrecover "github.com/gofiber/fiber/v3/middleware/recover"
	"github.com/stretchr/testify/require"

	"github.com/everscribe/sdk-go/pkg/event"
)

func newApp(spy *spyRecorder, h fiberv3.Handler) *fiberv3.App {
	app := fiberv3.New()
	app.Use(event.FiberV3Middleware(event.Options{Recorder: spy, Logger: nopLogger{}}))
	app.Get("/", h)
	return app
}

func TestFiber(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		handler       fiberv3.Handler
		reqHeaders    map[string]string
		wantEvents    int
		wantAction    string
		wantStatus    string
		wantCode      int
		wantUserAgent string
		wantRequestID string
	}{
		{
			name: "records once",
			handler: func(c fiberv3.Ctx) error {
				event.Current(c.Context()).Action = "user.login"
				return c.SendString("ok")
			},
			wantEvents: 1, wantAction: "user.login", wantStatus: "ok", wantCode: 200,
		},
		{
			name: "denied status",
			handler: func(c fiberv3.Ctx) error {
				event.Current(c.Context()).Action = "user.login"
				return c.Status(http.StatusForbidden).SendString("nope")
			},
			wantEvents: 1, wantAction: "user.login", wantStatus: "denied", wantCode: 403,
		},
		{
			// Confirms the c.Get wrapper, since fiber's Get takes
			// variadic defaults and does not match OriginFrom's closure
			// signature directly.
			name: "origin from headers",
			handler: func(c fiberv3.Ctx) error {
				event.Current(c.Context()).Action = "user.login"
				return c.SendString("ok")
			},
			reqHeaders:    map[string]string{"User-Agent": "curl/8.0", "X-Request-ID": "req-abc"},
			wantEvents:    1,
			wantAction:    "user.login",
			wantStatus:    "ok",
			wantCode:      200,
			wantUserAgent: "curl/8.0",
			wantRequestID: "req-abc",
		},
		{
			// The opposite of "no write records as ok" below: this one
			// never names the event at all.
			name:       "unnamed event not recorded",
			handler:    func(c fiberv3.Ctx) error { return c.SendString("ok") },
			wantEvents: 0,
		},
		{
			// A real limitation (see the package doc): fasthttp's
			// StatusCode() defaults to 200 regardless of whether the
			// handler wrote anything, so a named event that returns nil
			// without any Send* call is indistinguishable from one that
			// wrote 200, and records as such.
			name: "no write records as ok",
			handler: func(c fiberv3.Ctx) error {
				event.Current(c.Context()).Action = "user.login"
				return nil // no Send*/Status/etc. call
			},
			wantEvents: 1, wantAction: "user.login", wantStatus: "ok", wantCode: 200,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &spyRecorder{callPrepare: true}
			app := newApp(spy, tc.handler)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			for k, v := range tc.reqHeaders {
				req.Header.Set(k, v)
			}
			resp, err := app.Test(req)
			require.NoError(t, err)
			resp.Body.Close()

			got := spy.events()
			require.Len(t, got, tc.wantEvents)
			if tc.wantEvents == 0 {
				return
			}
			require.Equal(t, tc.wantAction, got[0].Action)
			require.Equal(t, tc.wantStatus, got[0].Result.Status)
			require.Equal(t, tc.wantCode, got[0].Result.Code)
			require.Equal(t, tc.wantUserAgent, got[0].Origin.UserAgent)
			require.Equal(t, tc.wantRequestID, got[0].Origin.RequestID)
		})
	}
}

// TestFiber_PanicYieldsNoResponseWritten verifies the completed flag's
// reason for existing: a panicking handler must not be recorded as a
// successful 200, even though StatusCode() would report that if read
// unconditionally.
//
// fasthttp's request goroutine has no recover of its own (confirmed
// against valyala/fasthttp v1.72.0's server.go/workerpool.go, and by
// direct reproduction, which crashed the process). This test mounts
// fiber's middleware/recover BEFORE (outer to) FiberV3Middleware, as a
// real app must too; the panic still unwinds through this middleware's
// defer end() before recover's, so completed stays false and the event
// captures ok == false regardless of the 500 recover writes afterward.
func TestFiber_PanicYieldsNoResponseWritten(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{callPrepare: true}
	app := fiberv3.New()
	app.Use(fiberrecover.New())
	app.Use(event.FiberV3Middleware(event.Options{Recorder: spy, Logger: nopLogger{}}))
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
