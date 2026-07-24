package event

import (
	"context"
	"log/slog"
)

// Options configures the middleware and interceptors in this file group:
// Middleware, GinMiddleware, EchoV4Middleware, FiberV3Middleware,
// UnaryInterceptor, and StreamInterceptor all share this one declaration.
type Options struct {
	// ActorResolver derives the Actor. nil yields an anonymous actor.
	ActorResolver ActorResolver
	// Recorder receives the auto-recorded event. nil installs the event
	// but does not auto-record.
	Recorder Recorder
	// Logger receives record failures. nil defaults to slog.Default().
	Logger Logger
}

func (o Options) resolve() ActorResolver {
	if o.ActorResolver == nil {
		return func(context.Context) Actor { return Actor{Type: "anonymous"} }
	}
	return o.ActorResolver
}

func (o Options) logger() Logger {
	if o.Logger == nil {
		return slog.Default()
	}
	return o.Logger
}
