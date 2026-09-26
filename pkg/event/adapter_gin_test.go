package event_test

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	gingonic "github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/everscribe/sdk-go/pkg/event"
)

// setModeOnce guards gingonic.SetMode, which writes unsynchronized
// package-level globals. Tests run with t.Parallel(), so calling it once
// per test would race under -race; sync.Once makes the single write
// happen-before every reader.
var setModeOnce sync.Once

func newRouter(spy *spyRecorder, h gingonic.HandlerFunc) *gingonic.Engine {
	setModeOnce.Do(func() { gingonic.SetMode(gingonic.TestMode) })
	r := gingonic.New()
	r.Use(event.GinMiddleware(event.Options{Recorder: spy, Logger: nopLogger{}}))
	r.GET("/", h)
	return r
}

func TestGin(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		handler     gingonic.HandlerFunc
		wantEvents  int
		wantAction  string
		wantStatus  string
		wantCode    int
		wantMessage any
	}{
		{
			name: "records once",
			handler: func(c *gingonic.Context) {
				event.Current(c.Request.Context()).Action = "user.login"
				c.String(http.StatusOK, "ok")
			},
			wantEvents: 1, wantAction: "user.login", wantStatus: "ok", wantCode: 200,
		},
		{
			// The case Written() exists for. gin initializes Status() to
			// 200, so reading it alone would record a successful 200 for
			// a request that produced no response at all.
			name: "abort with no write",
			handler: func(c *gingonic.Context) {
				event.Current(c.Request.Context()).Action = "user.login"
				c.Abort() // no write
			},
			wantEvents: 1, wantAction: "user.login", wantStatus: "error", wantMessage: "no response written",
		},
		{
			name: "denied status",
			handler: func(c *gingonic.Context) {
				event.Current(c.Request.Context()).Action = "user.login"
				c.String(http.StatusForbidden, "nope")
			},
			wantEvents: 1, wantAction: "user.login", wantStatus: "denied", wantCode: 403,
		},
		{
			name:       "unnamed event not recorded",
			handler:    func(c *gingonic.Context) { c.String(http.StatusOK, "ok") },
			wantEvents: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &spyRecorder{callPrepare: true}
			r := newRouter(spy, tc.handler)

			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

			got := spy.events()
			require.Len(t, got, tc.wantEvents)
			if tc.wantEvents == 0 {
				return
			}
			require.Equal(t, tc.wantAction, got[0].Action)
			require.Equal(t, tc.wantStatus, got[0].Result.Status)
			require.Equal(t, tc.wantCode, got[0].Result.Code,
				"the code must reflect what the handler wrote, not gin's default 200")
			require.Equal(t, tc.wantMessage, got[0].Result.Message)
		})
	}
}
