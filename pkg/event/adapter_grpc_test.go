package event_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/everscribe/sdk-go/pkg/event"
)

func invoke(t *testing.T, spy *spyRecorder, handler googlegrpc.UnaryHandler) {
	t.Helper()
	ic := event.UnaryInterceptor(event.Options{Recorder: spy, Logger: nopLogger{}})
	info := &googlegrpc.UnaryServerInfo{FullMethod: "/everscribe.v1.Ingest/Record"}
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(
		"user-agent", "grpc-go/1.68",
		"x-request-id", "req-abc",
	))
	_, _ = ic(ctx, struct{}{}, info, handler)
}

func TestUnary_ActionDefaultsToFullMethod(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	invoke(t, spy, func(ctx context.Context, req any) (any, error) { return nil, nil })

	got := spy.events()
	require.Len(t, got, 1)
	require.Equal(t, "/everscribe.v1.Ingest/Record", got[0].Action)
}

// TestUnary_OKRecordsAs200 is the case native gRPC codes would break: OK is
// code 0, which omitempty drops on the wire.
func TestUnary_OKRecordsAs200(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	invoke(t, spy, func(ctx context.Context, req any) (any, error) { return nil, nil })

	got := spy.events()[0]
	require.Equal(t, "ok", got.Result.Status)
	require.Equal(t, 200, got.Result.Code)
}

func TestUnary_PermissionDeniedRecordsAs403(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	invoke(t, spy, func(ctx context.Context, req any) (any, error) {
		return nil, status.Error(codes.PermissionDenied, "nope")
	})

	got := spy.events()[0]
	require.Equal(t, "denied", got.Result.Status)
	require.Equal(t, 403, got.Result.Code)
	require.Equal(t, "nope", got.Result.Message)
}

// TestUnary_OutcomeSeenAfterHandlerDefers is coupling point 3. The status
// comes from the returned error, which does not exist until every defer in
// the handler has run, so a handler-side defer could never observe it.
func TestUnary_OutcomeSeenAfterHandlerDefers(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	invoke(t, spy, func(ctx context.Context, req any) (any, error) {
		defer func() { event.Current(ctx).WithField("ran", true) }()
		return nil, status.Error(codes.NotFound, "missing")
	})

	got := spy.events()[0]
	require.Equal(t, 404, got.Result.Code)
	require.Equal(t, true, got.Metadata["ran"], "handler defers still ran before end")
}

func TestUnary_OriginFromMetadataAndPeer(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	invoke(t, spy, func(ctx context.Context, req any) (any, error) { return nil, nil })

	got := spy.events()[0]
	require.Equal(t, "grpc-go/1.68", got.Origin.UserAgent)
	require.Equal(t, "req-abc", got.Origin.RequestID)
}

// TestUnary_CloneInsideHandlerStaysUnnamed guards a regression (I5):
// UnaryInterceptor used to set Action on the Begin template, so every
// NewFromContext clone inherited the RPC method name instead of coming
// back unnamed, and would auto-record instead of being dropped by the
// empty-Action guard. The primary request-scoped event still must record
// under the full method name.
func TestUnary_CloneInsideHandlerStaysUnnamed(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	var clone *event.Event
	invoke(t, spy, func(ctx context.Context, req any) (any, error) {
		clone = event.NewFromContext(ctx)
		return nil, nil
	})

	require.NotNil(t, clone)
	require.Empty(t, clone.Action,
		"a NewFromContext clone must not inherit the RPC method name from the template")

	got := spy.events()
	require.Len(t, got, 1, "only the primary event auto-records")
	require.Equal(t, "/everscribe.v1.Ingest/Record", got[0].Action)
}

func TestUnary_HandlerCanOverrideAction(t *testing.T) {
	t.Parallel()
	spy := &spyRecorder{}
	invoke(t, spy, func(ctx context.Context, req any) (any, error) {
		event.Current(ctx).Action = "user.login"
		return nil, nil
	})

	require.Equal(t, "user.login", spy.events()[0].Action)
}
