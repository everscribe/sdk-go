package adapters_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/everscribe/sdk-go/pkg/event"
	"github.com/everscribe/sdk-go/pkg/grpc/adapters"
)

// fakeServerStream is a minimal googlegrpc.ServerStream stand-in. It carries
// only the context StreamInterceptor needs; SendMsg/RecvMsg are no-ops since
// no test here exercises real wire encoding.
type fakeServerStream struct {
	ctx context.Context
}

func (f *fakeServerStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeServerStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeServerStream) SetTrailer(metadata.MD)       {}
func (f *fakeServerStream) Context() context.Context     { return f.ctx }
func (f *fakeServerStream) SendMsg(m any) error          { return nil }
func (f *fakeServerStream) RecvMsg(m any) error          { return nil }

// invokeStream wires spy up behind adapters.EventStreamInterceptor and runs
// handler through it, the same way grpc.ChainStreamInterceptor would.
func invokeStream(t *testing.T, spy *spyRecorder, handler googlegrpc.StreamHandler) error {
	t.Helper()
	ic := adapters.EventStreamInterceptor(adapters.Options{Recorder: spy, Logger: nopLogger{}})
	info := &googlegrpc.StreamServerInfo{FullMethod: "/everscribe.v1.Tail/Watch"}
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(
		"user-agent", "grpc-go/1.68",
		"x-request-id", "req-stream",
	))
	ss := &fakeServerStream{ctx: ctx}
	return ic(nil, ss, info, handler)
}

// TestStream_OneEventPerStreamNotPerMessage covers the headline contract:
// a stream that exchanges several messages still records exactly once, at
// stream close, not once per SendMsg/RecvMsg.
func TestStream_OneEventPerStreamNotPerMessage(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}

	err := invokeStream(t, spy, func(srv any, stream googlegrpc.ServerStream) error {
		for i := 0; i < 5; i++ {
			require.NoError(t, stream.SendMsg(struct{}{}))
			require.NoError(t, stream.RecvMsg(new(struct{})))
		}
		return nil
	})

	require.NoError(t, err)
	require.Len(t, spy.events(), 1, "a multi-message stream must record exactly one event")
}

// TestStream_OccurredAtStampedAtCloseNotOpen is the critical case: the
// server's live tail filters occurred_at > since, so a long-lived stream
// stamped at open would land behind the caller's cursor and never surface.
//
// The handler sleeps for a measurable interval so the test is genuinely
// falsifiable: if the stamp happened at stream open (before the sleep),
// OccurredAt would land only microseconds after `before`, well short of
// `sleep`. Only a close-time stamp lands at or after `before + sleep`.
func TestStream_OccurredAtStampedAtCloseNotOpen(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	const sleep = 40 * time.Millisecond

	before := time.Now()
	err := invokeStream(t, spy, func(srv any, stream googlegrpc.ServerStream) error {
		time.Sleep(sleep)
		return nil
	})
	after := time.Now()
	require.NoError(t, err)

	got := spy.events()
	require.Len(t, got, 1)
	occurredAt := got[0].OccurredAt

	require.GreaterOrEqualf(t, occurredAt.Sub(before), sleep,
		"OccurredAt (%v) landed only %v after stream open, want at least the %v handler sleep: looks stamped at open, not close",
		occurredAt, occurredAt.Sub(before), sleep)
	require.Falsef(t, occurredAt.After(after),
		"OccurredAt (%v) is after the interceptor returned (%v)", occurredAt, after)
}

// TestStream_ContextThreadsToHandler confirms event.Current(ss.Context())
// inside the handler resolves to the same event the interceptor records,
// so handler-side Action/metadata writes make it into the recorded event.
func TestStream_ContextThreadsToHandler(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}

	err := invokeStream(t, spy, func(srv any, stream googlegrpc.ServerStream) error {
		e := event.Current(stream.Context())
		e.Action = "tail.watch"
		e.WithField("cursor", "abc123")
		return nil
	})

	require.NoError(t, err)
	got := spy.events()
	require.Len(t, got, 1)
	require.Equal(t, "tail.watch", got[0].Action)
	require.Equal(t, "abc123", got[0].Metadata["cursor"])
}

// TestStream_CloneInsideHandlerStaysUnnamed is the I5 falsification for
// the stream path: EventStreamInterceptor used to set Action:
// info.FullMethod on the template too, so a FromContext clone made inside
// the stream handler inherited the RPC method name instead of coming back
// unnamed.
func TestStream_CloneInsideHandlerStaysUnnamed(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	var clone *event.Event

	err := invokeStream(t, spy, func(srv any, stream googlegrpc.ServerStream) error {
		clone = event.FromContext(stream.Context())
		return nil
	})

	require.NoError(t, err)
	require.NotNil(t, clone)
	require.Empty(t, clone.Action,
		"a FromContext clone must not inherit the RPC method name from the template")

	got := spy.events()
	require.Len(t, got, 1, "only the primary event auto-records")
	require.Equal(t, "/everscribe.v1.Tail/Watch", got[0].Action)
}

// TestStream_ErrorRecordsHTTPEquivalentCode covers outcome mapping: a
// stream handler's returned gRPC error records the same HTTP-equivalent
// code the unary path does.
func TestStream_ErrorRecordsHTTPEquivalentCode(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}

	err := invokeStream(t, spy, func(srv any, stream googlegrpc.ServerStream) error {
		return status.Error(codes.PermissionDenied, "nope")
	})

	require.Error(t, err)
	got := spy.events()
	require.Len(t, got, 1)
	require.Equal(t, "denied", got[0].Result.Status)
	require.Equal(t, 403, got[0].Result.Code)
	require.Equal(t, "nope", got[0].Result.Message)
}
