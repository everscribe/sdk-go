package event_test

import (
	"context"
	"sync"

	"github.com/everscribe/sdk-go/pkg/event"
)

// spyRecorder captures what the lifecycle submits.
//
// callPrepare models the difference that drives the whole dedupe design: the
// stock recorders call event.PrepareEvent (HTTPRecorder at http.go:105,
// BufferedRecorder at buffered.go:180), but a custom Recorder is free not to,
// which is the case the idempotency key exists to cover. The gRPC adapter
// tests only ever construct spyRecorder{} (callPrepare left at its zero
// value, false): end() already applies the outcome and marks the event
// recorded before calling Record, so whether the spy also calls PrepareEvent
// itself has no observable effect on those tests.
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
