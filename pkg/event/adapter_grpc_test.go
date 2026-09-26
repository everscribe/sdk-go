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
	info := &googlegrpc.UnaryServerInfo{FullMethod: unaryFullMethod}
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(
		"user-agent", "grpc-go/1.68",
		"x-request-id", "req-abc",
	))
	_, _ = ic(ctx, struct{}{}, info, handler)
}

const unaryFullMethod = "/everscribe.v1.Ingest/Record"

func TestUnary(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		handler     googlegrpc.UnaryHandler
		wantAction  string
		wantStatus  string
		wantCode    int
		wantMessage any
		wantMeta    map[string]any
	}{
		{
			// OK is the case native gRPC codes would break: it is code 0,
			// which omitempty drops on the wire. Action defaults to
			// info.FullMethod when the handler names nothing.
			name:       "ok defaults action to full method and records as 200",
			handler:    func(ctx context.Context, req any) (any, error) { return nil, nil },
			wantAction: unaryFullMethod, wantStatus: "ok", wantCode: 200,
		},
		{
			name: "permission denied records as 403",
			handler: func(ctx context.Context, req any) (any, error) {
				return nil, status.Error(codes.PermissionDenied, "nope")
			},
			wantAction: unaryFullMethod, wantStatus: "denied", wantCode: 403, wantMessage: "nope",
		},
		{
			// Coupling point 3: the status comes from the returned error,
			// which does not exist until every defer in the handler has
			// run, so a handler-side defer could never observe it - but
			// its writes to the event must still land.
			name: "outcome seen after handler defers",
			handler: func(ctx context.Context, req any) (any, error) {
				defer func() { event.Current(ctx).WithField("ran", true) }()
				return nil, status.Error(codes.NotFound, "missing")
			},
			wantAction: unaryFullMethod, wantStatus: "error", wantCode: 404, wantMessage: "missing",
			wantMeta: map[string]any{"ran": true},
		},
		{
			name: "handler can override action",
			handler: func(ctx context.Context, req any) (any, error) {
				event.Current(ctx).Action = "user.login"
				return nil, nil
			},
			wantAction: "user.login", wantStatus: "ok", wantCode: 200,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &spyRecorder{}
			invoke(t, spy, tc.handler)

			events := spy.events()
			require.Len(t, events, 1)
			got := events[0]
			require.Equal(t, tc.wantAction, got.Action)
			require.Equal(t, tc.wantStatus, got.Result.Status)
			require.Equal(t, tc.wantCode, got.Result.Code)
			require.Equal(t, tc.wantMessage, got.Result.Message)
			require.Equal(t, tc.wantMeta, got.Metadata,
				"handler writes to the event, including from a defer, must reach the recorded copy")

			// Origin comes from the incoming metadata invoke sets, on
			// every call regardless of outcome.
			require.Equal(t, "grpc-go/1.68", got.Origin.UserAgent)
			require.Equal(t, "req-abc", got.Origin.RequestID)
		})
	}
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
	require.Equal(t, unaryFullMethod, got[0].Action)
}
