package event_test

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

const streamFullMethod = "/everscribe.v1.Tail/Watch"

// invokeStream wires spy up behind event.StreamInterceptor and runs handler
// through it, the same way grpc.ChainStreamInterceptor would.
func invokeStream(t *testing.T, spy *spyRecorder, handler googlegrpc.StreamHandler) error {
	t.Helper()
	ic := event.StreamInterceptor(event.Options{Recorder: spy, Logger: nopLogger{}})
	info := &googlegrpc.StreamServerInfo{FullMethod: streamFullMethod}
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(
		"user-agent", "grpc-go/1.68",
		"x-request-id", "req-stream",
	))
	ss := &fakeServerStream{ctx: ctx}
	return ic(nil, ss, info, handler)
}

func TestStream(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		handler     func(t *testing.T, stream googlegrpc.ServerStream) error
		wantErr     bool
		wantAction  string
		wantStatus  string
		wantCode    int
		wantMessage any
		wantMeta    map[string]any
	}{
		{
			// The headline contract: a stream that exchanges several
			// messages still records exactly once, at stream close, not
			// once per SendMsg/RecvMsg.
			name: "one event per stream not per message",
			handler: func(t *testing.T, stream googlegrpc.ServerStream) error {
				for i := 0; i < 5; i++ {
					require.NoError(t, stream.SendMsg(struct{}{}))
					require.NoError(t, stream.RecvMsg(new(struct{})))
				}
				return nil
			},
			wantAction: streamFullMethod, wantStatus: "ok", wantCode: 200,
		},
		{
			// event.Current(ss.Context()) inside the handler must resolve
			// to the same event the interceptor records, so handler-side
			// Action/metadata writes make it into the recorded event.
			name: "context threads to handler",
			handler: func(t *testing.T, stream googlegrpc.ServerStream) error {
				e := event.Current(stream.Context())
				e.Action = "tail.watch"
				e.WithField("cursor", "abc123")
				return nil
			},
			wantAction: "tail.watch", wantStatus: "ok", wantCode: 200,
			wantMeta: map[string]any{"cursor": "abc123"},
		},
		{
			// Outcome mapping: a stream handler's returned gRPC error
			// records the same HTTP-equivalent code the unary path does.
			name: "error records http equivalent code",
			handler: func(t *testing.T, stream googlegrpc.ServerStream) error {
				return status.Error(codes.PermissionDenied, "nope")
			},
			wantErr:    true,
			wantAction: streamFullMethod, wantStatus: "denied", wantCode: 403, wantMessage: "nope",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &spyRecorder{}

			err := invokeStream(t, spy, func(srv any, stream googlegrpc.ServerStream) error {
				return tc.handler(t, stream)
			})
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			got := spy.events()
			require.Len(t, got, 1, "a stream must record exactly one event")
			require.Equal(t, tc.wantAction, got[0].Action)
			require.Equal(t, tc.wantStatus, got[0].Result.Status)
			require.Equal(t, tc.wantCode, got[0].Result.Code)
			require.Equal(t, tc.wantMessage, got[0].Result.Message)
			require.Equal(t, tc.wantMeta, got[0].Metadata)
		})
	}
}

// TestStream_OccurredAtStampedAtCloseNotOpen is the critical case: the
// server's live tail filters occurred_at > since, so a stream stamped at
// open would land behind the caller's cursor and never surface.
//
// The handler sleeps for a measurable interval so an open-time stamp
// (landing only microseconds after `before`) is distinguishable from a
// close-time one (landing at or after `before + sleep`).
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

// TestStream_CloneInsideHandlerStaysUnnamed is the I5 falsification for
// the stream path: StreamInterceptor used to set Action: info.FullMethod
// on the template too, so a NewFromContext clone made inside the stream
// handler inherited the RPC method name instead of coming back unnamed.
func TestStream_CloneInsideHandlerStaysUnnamed(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	var clone *event.Event

	err := invokeStream(t, spy, func(srv any, stream googlegrpc.ServerStream) error {
		clone = event.NewFromContext(stream.Context())
		return nil
	})

	require.NoError(t, err)
	require.NotNil(t, clone)
	require.Empty(t, clone.Action,
		"a NewFromContext clone must not inherit the RPC method name from the template")

	got := spy.events()
	require.Len(t, got, 1, "only the primary event auto-records")
	require.Equal(t, streamFullMethod, got[0].Action)
}
