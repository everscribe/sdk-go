// Package grpc is the grpc-go server adapter.
//
// It sits at the protocol level, as a peer of the http adapters rather
// than a sibling of gin. If Connect support is added later it becomes
// adapters/connect, since Connect is its own protocol.
package grpc

import (
	"context"
	"log/slog"
	"time"

	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/everscribe/sdk-go/pkg/event"
)

// Options configures the interceptors. Same shape as every other adapter.
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

// UnaryInterceptor returns a grpc.UnaryServerInterceptor that installs a
// per-request event and records it once after the handler returns.
//
// Recording after handler() returns is the only point at which the outcome
// is knowable: the status derives from the error the handler returns, which
// does not exist until every defer inside that handler has run. A
// handler-side defer could never observe it.
//
// Action defaults to info.FullMethod, for example
// "/everscribe.v1.Ingest/Record". Handlers may overwrite it.
//
// Mount with grpc.ChainUnaryInterceptor.
func UnaryInterceptor(opts Options) googlegrpc.UnaryServerInterceptor {
	resolve, logger := opts.resolve(), opts.logger()

	return func(ctx context.Context, req any, info *googlegrpc.UnaryServerInfo, handler googlegrpc.UnaryHandler) (any, error) {
		cap := &statusCapture{}
		tmpl := &event.Event{
			Actor:  resolve(ctx),
			Action: info.FullMethod,
			Origin: originFrom(ctx),
		}
		ctx, end := event.Begin(ctx, tmpl, cap, opts.Recorder, logger)
		defer end() // runs after cap.set below, since defers run last

		resp, err := handler(ctx, req)
		cap.set(err)
		return resp, err
	}
}

// StreamInterceptor returns a grpc.StreamServerInterceptor.
//
// One event per stream, not one per message, recorded at stream close.
// OccurredAt is stamped at close rather than open: the poll endpoint
// filters occurred_at > since, so a stream open for an hour would record
// with an hour-old timestamp, land behind the caller's cursor, and never
// surface in the live tail.
//
// Shutdown needs no special handling. GracefulStop waits for in-flight
// RPCs, so those streams finish and record through the ordinary path;
// Stop terminates them and the handler returns an error, which also
// records through the ordinary path.
func StreamInterceptor(opts Options) googlegrpc.StreamServerInterceptor {
	resolve, logger := opts.resolve(), opts.logger()

	return func(srv any, ss googlegrpc.ServerStream, info *googlegrpc.StreamServerInfo, handler googlegrpc.StreamHandler) error {
		parent := ss.Context()
		cap := &statusCapture{}
		tmpl := &event.Event{
			Actor:  resolve(parent),
			Action: info.FullMethod,
			Origin: originFrom(parent),
		}
		ctx, end := event.Begin(parent, tmpl, cap, opts.Recorder, logger)
		defer end()

		err := handler(srv, &wrappedStream{ServerStream: ss, ctx: ctx})
		cap.set(err)
		// Stamp at close, overwriting Begin's open-time default.
		event.Current(ctx).OccurredAt = time.Now().UTC()
		return err
	}
}

// wrappedStream carries the lifecycle context so handlers reach the event
// with event.Current(ss.Context()).
type wrappedStream struct {
	googlegrpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

// statusCapture derives the outcome from the handler's returned error.
type statusCapture struct {
	result event.Result
	done   bool
}

func (s *statusCapture) set(err error) {
	st := status.Convert(err)
	s.result = event.ResultFromHTTPStatus(HTTPStatusFor(st.Code()))
	if msg := st.Message(); msg != "" {
		s.result.Message = msg
	}
	s.done = true
}

// Outcome implements event.OutcomeCapture.
func (s *statusCapture) Outcome() (event.Result, bool) {
	if !s.done {
		return event.Result{}, false
	}
	return s.result, true
}

// originFrom builds Origin from gRPC metadata and the peer address.
// Metadata keys are lowercase-normalized and multi-valued, which is
// exactly the contract OriginFrom's closure documents.
func originFrom(ctx context.Context) event.Origin {
	md, _ := metadata.FromIncomingContext(ctx)
	get := func(name string) string {
		vals := md.Get(name) // Get lowercases the key for us
		if len(vals) == 0 {
			return ""
		}
		return vals[0]
	}
	addr := ""
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		addr = p.Addr.String()
	}
	return event.OriginFrom(get, addr)
}
