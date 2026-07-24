// Package adapters wires the everscribe SDK's event lifecycle into HTTP
// frameworks: net/http (and, transitively, chi and gorilla/mux, which are
// both plain func(http.Handler) http.Handler and need no adapter of their
// own), gin, echo, and fiber v3.
//
// Each framework gets its own middleware entry point
// (StdlibEventMiddleware, GinEventMiddleware, EchoEventMiddleware,
// FiberEventMiddleware), all configured with the same Options shape.
package adapters

import (
	"context"
	"log/slog"

	"github.com/everscribe/sdk-go/pkg/event"
)

// Options configures the middleware. Every adapter in this package shares
// this one declaration.
type Options struct {
	// Resolve derives the Actor. nil yields an anonymous actor.
	Resolve event.ActorResolver
	// Recorder receives the auto-recorded event. nil installs the event
	// but does not auto-record.
	Recorder event.Recorder
	// Logger receives record failures. nil defaults to slog.Default().
	Logger event.Logger
}

func (o Options) resolve() event.ActorResolver {
	if o.Resolve == nil {
		return func(context.Context) event.Actor { return event.Actor{Type: "anonymous"} }
	}
	return o.Resolve
}

func (o Options) logger() event.Logger {
	if o.Logger == nil {
		return slog.Default()
	}
	return o.Logger
}
