package recorder

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"time"
)

// isRetryable reports whether a failed flush is worth another attempt.
func isRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var tr interface{ Transient() bool }
	if errors.As(err, &tr) && tr.Transient() {
		return true
	}
	var ue *url.Error
	var ne net.Error
	return errors.As(err, &ue) || errors.As(err, &ne) ||
		errors.Is(err, context.DeadlineExceeded)
}

// isSplittable reports whether halving the batch can isolate the cause.
func isSplittable(err error) bool {
	var he *HTTPError
	if errors.As(err, &he) {
		switch he.StatusCode {
		case http.StatusBadRequest, http.StatusConflict, http.StatusRequestEntityTooLarge:
			return true
		}
	}
	var uv *json.UnsupportedValueError
	var ut *json.UnsupportedTypeError
	var me *json.MarshalerError
	return errors.As(err, &uv) || errors.As(err, &ut) || errors.As(err, &me)
}

// backoffCap returns min(max, initial*2^(n-1)) for retry n >= 1.
func backoffCap(n int, initial, max time.Duration) time.Duration {
	d := initial
	for i := 1; i < n; i++ {
		if d >= max || d > max/2 {
			return max
		}
		d *= 2
	}
	if d > max {
		return max
	}
	return d
}

// retryWait is the jittered backoff, raised to any Retry-After, capped at max.
func retryWait(n int, err error, initial, max time.Duration, jitter func(time.Duration) time.Duration) time.Duration {
	w := jitter(backoffCap(n, initial, max))
	var he *HTTPError
	if errors.As(err, &he) && he.RetryAfter > 0 {
		if he.RetryAfter > w {
			w = he.RetryAfter
		}
	}
	if w > max {
		return max
	}
	return w
}
