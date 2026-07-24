package adapters_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/everscribe/sdk-go/pkg/event"
	"github.com/everscribe/sdk-go/pkg/grpc/adapters"
)

func TestHTTPStatusFor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		code codes.Code
		want int
	}{
		{codes.OK, 200},
		{codes.Canceled, 499},
		{codes.Unknown, 500},
		{codes.InvalidArgument, 400},
		{codes.DeadlineExceeded, 504},
		{codes.NotFound, 404},
		{codes.AlreadyExists, 409},
		{codes.PermissionDenied, 403},
		{codes.ResourceExhausted, 429},
		{codes.FailedPrecondition, 400},
		{codes.Aborted, 409},
		{codes.OutOfRange, 400},
		{codes.Unimplemented, 501},
		{codes.Internal, 500},
		{codes.Unavailable, 503},
		{codes.DataLoss, 500},
		{codes.Unauthenticated, 401},
	}
	require.Len(t, tests, 17, "all 17 gRPC codes must be covered")
	for _, tt := range tests {
		t.Run(tt.code.String(), func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, adapters.HTTPStatusFor(tt.code))
		})
	}
}

// TestOKIsNotZero is the whole reason for the mapping. A native gRPC OK is
// code 0, which every SDK drops via omitempty and which the server stores
// as JSONB, so result.code would never match a successful call.
func TestOKIsNotZero(t *testing.T) {
	t.Parallel()
	got := event.ResultFromHTTPStatus(adapters.HTTPStatusFor(codes.OK))
	require.Equal(t, "ok", got.Status)
	require.Equal(t, 200, got.Code)
	require.NotZero(t, got.Code, "a zero code is dropped on the wire")
}

func TestDeniedCodesMapToDenied(t *testing.T) {
	t.Parallel()
	for _, c := range []codes.Code{codes.PermissionDenied, codes.Unauthenticated} {
		got := event.ResultFromHTTPStatus(adapters.HTTPStatusFor(c))
		require.Equal(t, "denied", got.Status, c.String())
	}
}
