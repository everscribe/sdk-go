package event

// This file is the grpc-go server adapter. See the package doc for how the
// lifecycle works and for the two ways these interceptors diverge from the
// HTTP adapters; the reasoning specific to this adapter is on
// UnaryInterceptor and StreamInterceptor.

import (
	"context"
	"time"

	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// UnaryInterceptor returns a grpc.UnaryServerInterceptor that installs a
// per-request event and records it once after the handler returns.
//
// Recording after handler() returns is the only point the outcome is
// knowable: the status derives from the error the handler returns, which
// doesn't exist until every defer inside that handler has run - a
// handler-side defer could never observe it.
//
// Action defaults to info.FullMethod (e.g. "/everscribe.v1.Ingest/Record");
// handlers may overwrite it. Mount with grpc.ChainUnaryInterceptor.
func UnaryInterceptor(opts Options) googlegrpc.UnaryServerInterceptor {
	resolve, logger := opts.resolve(), opts.logger()

	return func(ctx context.Context, req any, info *googlegrpc.UnaryServerInfo, handler googlegrpc.UnaryHandler) (any, error) {
		oc := &statusCapture{}
		tmpl := &Event{
			Actor:  resolve(ctx),
			Origin: originFrom(ctx),
		}
		ctx, end := Begin(ctx, tmpl, oc, opts.Recorder, logger)
		// Stamped on the request-scoped event, not the template: a template
		// Action would flow into every NewFromContext clone the handler makes, so
		// a secondary event the handler never named would inherit the RPC
		// method name instead of being dropped by the empty-Action guard every
		// stock recorder applies.
		Current(ctx).Action = info.FullMethod
		defer end() // runs after oc.set below, since defers run last

		resp, err := handler(ctx, req)
		oc.set(err)
		return resp, err
	}
}

// StreamInterceptor returns a grpc.StreamServerInterceptor.
//
// One event per stream, not per message, recorded at stream close.
// OccurredAt is stamped at close, not open: the poll endpoint filters
// occurred_at > since, so an hour-long stream stamped at open would land
// behind the caller's cursor and never surface in the live tail.
//
// Shutdown needs no special handling: GracefulStop waits for in-flight
// RPCs to finish and record normally; Stop terminates them and the
// handler's returned error also records normally.
func StreamInterceptor(opts Options) googlegrpc.StreamServerInterceptor {
	resolve, logger := opts.resolve(), opts.logger()

	return func(srv any, ss googlegrpc.ServerStream, info *googlegrpc.StreamServerInfo, handler googlegrpc.StreamHandler) error {
		parent := ss.Context()
		oc := &statusCapture{}
		tmpl := &Event{
			Actor:  resolve(parent),
			Origin: originFrom(parent),
		}
		ctx, end := Begin(parent, tmpl, oc, opts.Recorder, logger)
		// See the matching comment in UnaryInterceptor: stamped on the
		// request-scoped event, not the template, so NewFromContext clones stay
		// unnamed.
		Current(ctx).Action = info.FullMethod
		defer end()

		err := handler(srv, &wrappedStream{ServerStream: ss, ctx: ctx})
		oc.set(err)
		// Stamp at close, overwriting Begin's open-time default.
		Current(ctx).OccurredAt = time.Now().UTC()
		return err
	}
}

// wrappedStream carries the lifecycle context so handlers reach the event
// with Current(ss.Context()).
type wrappedStream struct {
	googlegrpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

// statusCapture derives the outcome from the handler's returned error.
type statusCapture struct {
	result Result
	done   bool
}

func (s *statusCapture) set(err error) {
	st := status.Convert(err)
	s.result = ResultFromHTTPStatus(HTTPStatusFor(st.Code()))
	if msg := st.Message(); msg != "" {
		s.result.Message = msg
	}
	s.done = true
}

// Outcome implements OutcomeCapture.
func (s *statusCapture) Outcome() (Result, bool) {
	if !s.done {
		return Result{}, false
	}
	return s.result, true
}

// originFrom builds Origin from gRPC metadata and the peer address.
// Metadata keys are lowercase-normalized and multi-valued, which is
// exactly the contract OriginFrom's closure documents.
func originFrom(ctx context.Context) Origin {
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
	return OriginFrom(get, addr)
}
