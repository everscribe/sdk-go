package grpc

import (
	"net/http"

	"google.golang.org/grpc/codes"
)

// HTTPStatusFor maps a gRPC status code to its canonical HTTP equivalent,
// following the grpc-gateway / Google API design guide mapping.
//
// Result.Code carries this rather than the native gRPC code, deliberately.
// Native codes break three things: code 0 (OK) is dropped by omitempty in
// every SDK and stored as JSONB, so result.code never matches a successful
// gRPC call; result.code >= 400 matches no gRPC error at all, since native
// codes are 1 through 16; and the NLP query layer's priors map "forbidden"
// to 403.
//
// The cost is accepted: InvalidArgument, FailedPrecondition, and OutOfRange
// all collapse to 400, so the exact gRPC code is not recoverable from the
// event. The full status message is preserved in Result.Message.
func HTTPStatusFor(c codes.Code) int {
	switch c {
	case codes.OK:
		return http.StatusOK
	case codes.Canceled:
		return 499 // nginx's client-closed-request; no stdlib constant
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return http.StatusBadRequest
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists, codes.Aborted:
		return http.StatusConflict
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	default: // Unknown, Internal, DataLoss
		return http.StatusInternalServerError
	}
}
