package echo_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	echov4 "github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"

	everecho "github.com/everscribe/sdk-go/adapters/http/echo"
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

func newEcho(spy *spyRecorder, h echov4.HandlerFunc) *echov4.Echo {
	e := echov4.New()
	e.Use(everecho.New(everecho.Options{Recorder: spy, Logger: nopLogger{}}))
	e.GET("/", h)
	return e
}

func TestEcho_RecordsOnce(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	e := newEcho(spy, func(c echov4.Context) error {
		event.Current(c.Request().Context()).Action = "user.login"
		return c.String(http.StatusOK, "ok")
	})

	w := httptest.NewRecorder()
	e.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

	got := spy.events()
	require.Len(t, got, 1)
	require.Equal(t, "user.login", got[0].Action)
	require.Equal(t, "ok", got[0].Result.Status)
	require.Equal(t, 200, got[0].Result.Code)
}

// TestEcho_HandlerReturnsWithoutWriting is the Committed case: echo
// initializes Status to 200, so reading it alone would fabricate a
// success for a request that wrote nothing.
func TestEcho_HandlerReturnsWithoutWriting(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	e := newEcho(spy, func(c echov4.Context) error {
		event.Current(c.Request().Context()).Action = "user.login"
		return nil // no write
	})

	w := httptest.NewRecorder()
	e.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

	got := spy.events()
	require.Len(t, got, 1)
	require.Equal(t, "error", got[0].Result.Status)
	require.Equal(t, "no response written", got[0].Result.Message)
}

func TestEcho_DeniedStatus(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	e := newEcho(spy, func(c echov4.Context) error {
		event.Current(c.Request().Context()).Action = "user.login"
		return c.String(http.StatusUnauthorized, "nope")
	})

	w := httptest.NewRecorder()
	e.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

	require.Equal(t, "denied", spy.events()[0].Result.Status)
}

func TestEcho_UnnamedEventNotRecorded(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	e := newEcho(spy, func(c echov4.Context) error { return c.String(http.StatusOK, "ok") })

	w := httptest.NewRecorder()
	e.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

	require.Empty(t, spy.events())
}
