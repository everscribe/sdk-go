package adapters_test

import (
	"context"
	"sync"

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
