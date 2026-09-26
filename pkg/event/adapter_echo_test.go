package event_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	echov4 "github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	"github.com/everscribe/sdk-go/pkg/event"
)

func newEcho(spy *spyRecorder, h echov4.HandlerFunc) *echov4.Echo {
	e := echov4.New()
	e.Use(event.EchoV4Middleware(event.Options{Recorder: spy, Logger: nopLogger{}}))
	e.GET("/", h)
	return e
}

func TestEcho(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		handler     echov4.HandlerFunc
		wantEvents  int
		wantAction  string
		wantStatus  string
		wantCode    int
		wantMessage any
	}{
		{
			name: "records once",
			handler: func(c echov4.Context) error {
				event.Current(c.Request().Context()).Action = "user.login"
				return c.String(http.StatusOK, "ok")
			},
			wantEvents: 1, wantAction: "user.login", wantStatus: "ok", wantCode: 200,
		},
		{
			// The Committed case: echo initializes Status to 200, so
			// reading it alone would fabricate a success for a request
			// that wrote nothing.
			name: "handler returns without writing",
			handler: func(c echov4.Context) error {
				event.Current(c.Request().Context()).Action = "user.login"
				return nil // no write
			},
			wantEvents: 1, wantAction: "user.login", wantStatus: "error", wantMessage: "no response written",
		},
		{
			name: "denied status",
			handler: func(c echov4.Context) error {
				event.Current(c.Request().Context()).Action = "user.login"
				return c.String(http.StatusUnauthorized, "nope")
			},
			wantEvents: 1, wantAction: "user.login", wantStatus: "denied", wantCode: 401,
		},
		{
			name:       "unnamed event not recorded",
			handler:    func(c echov4.Context) error { return c.String(http.StatusOK, "ok") },
			wantEvents: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &spyRecorder{callPrepare: true}
			e := newEcho(spy, tc.handler)

			w := httptest.NewRecorder()
			e.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

			got := spy.events()
			require.Len(t, got, tc.wantEvents)
			if tc.wantEvents == 0 {
				return
			}
			require.Equal(t, tc.wantAction, got[0].Action)
			require.Equal(t, tc.wantStatus, got[0].Result.Status)
			require.Equal(t, tc.wantCode, got[0].Result.Code,
				"the code must reflect what the handler wrote, not echo's default 200")
			require.Equal(t, tc.wantMessage, got[0].Result.Message)
		})
	}
}
