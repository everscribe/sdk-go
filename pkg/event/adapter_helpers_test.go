package event_test

import (
	"context"
	"sync"

	"github.com/everscribe/sdk-go/pkg/event"
)

// spyRecorder captures what the lifecycle submits.
//
// callPrepare models whether the recorder calls event.PrepareEvent (as
// http.go's HTTPRecorder and buffered.go's BufferedRecorder do) or skips
// it, the case idempotency keys exist to cover. gRPC adapter tests leave
// it false since end() already applies the outcome and marks the event
// recorded before calling Record, so PrepareEvent there has no effect.
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
