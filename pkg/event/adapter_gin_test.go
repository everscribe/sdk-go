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

func TestGin_RecordsOnce(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{callPrepare: true}
	r := newRouter(spy, func(c *gingonic.Context) {
		event.Current(c.Request.Context()).Action = "user.login"
		c.String(http.StatusOK, "ok")
	})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

	got := spy.events()
	require.Len(t, got, 1)
	require.Equal(t, "user.login", got[0].Action)
	require.Equal(t, "ok", got[0].Result.Status)
	require.Equal(t, 200, got[0].Result.Code)
}

// TestGin_AbortWithNoWrite is the case Written() exists for. gin
// initializes Status() to 200, so reading it alone would record a
// successful 200 for a request that produced no response at all.
func TestGin_AbortWithNoWrite(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{callPrepare: true}
	r := newRouter(spy, func(c *gingonic.Context) {
		event.Current(c.Request.Context()).Action = "user.login"
		c.Abort() // no write
	})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

	got := spy.events()
	require.Len(t, got, 1)
	require.Equal(t, "error", got[0].Result.Status)
	require.Equal(t, "no response written", got[0].Result.Message)
	require.Zero(t, got[0].Result.Code, "must not report gin's default 200")
}

func TestGin_DeniedStatus(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{callPrepare: true}
	r := newRouter(spy, func(c *gingonic.Context) {
		event.Current(c.Request.Context()).Action = "user.login"
		c.String(http.StatusForbidden, "nope")
	})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

	require.Equal(t, "denied", spy.events()[0].Result.Status)
}

func TestGin_UnnamedEventNotRecorded(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{callPrepare: true}
	r := newRouter(spy, func(c *gingonic.Context) { c.String(http.StatusOK, "ok") })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

	require.Empty(t, spy.events())
}
