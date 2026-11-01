package recorder

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type transientErr struct{}

func (transientErr) Error() string   { return "transient" }
func (transientErr) Transient() bool { return true }

func TestIsRetryable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"500", &HTTPError{StatusCode: 500}, true},
		{"503", &HTTPError{StatusCode: 503}, true},
		{"429", &HTTPError{StatusCode: 429}, true},
		{"400", &HTTPError{StatusCode: 400}, false},
		{"401", &HTTPError{StatusCode: 401}, false},
		{"404", &HTTPError{StatusCode: 404}, false},
		{"409", &HTTPError{StatusCode: 409}, false},
		{"413", &HTTPError{StatusCode: 413}, false},
		{"transport", &url.Error{Op: "Post", URL: "x", Err: syscall.ECONNREFUSED}, true},
		{"wrapped transport", fmt.Errorf("recorder: post: %w", &url.Error{Err: syscall.ECONNREFUSED}), true},
		{"url error canceled", &url.Error{Err: context.Canceled}, false},
		{"deadline", context.DeadlineExceeded, true},
		{"canceled", context.Canceled, false},
		{"marshal", fmt.Errorf("recorder: marshal event: %w", &json.UnsupportedTypeError{}), false},
		{"custom transient", fmt.Errorf("wrap: %w", transientErr{}), true},
		{"nil", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, isRetryable(tc.err))
		})
	}
}

func TestIsSplittable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"400", &HTTPError{StatusCode: 400}, true},
		{"409", &HTTPError{StatusCode: 409}, true},
		{"413", &HTTPError{StatusCode: 413}, true},
		{"unsupported value", fmt.Errorf("recorder: marshal batch: %w", &json.UnsupportedValueError{}), true},
		{"unsupported type", fmt.Errorf("recorder: marshal batch: %w", &json.UnsupportedTypeError{}), true},
		{"marshaler", fmt.Errorf("recorder: marshal batch: %w", &json.MarshalerError{}), true},
		{"401", &HTTPError{StatusCode: 401}, false},
		{"403", &HTTPError{StatusCode: 403}, false},
		{"404", &HTTPError{StatusCode: 404}, false},
		{"500", &HTTPError{StatusCode: 500}, false},
		{"nil", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, isSplittable(tc.err))
		})
	}
}

func TestBackoffCap(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		n    int
		want time.Duration
	}{
		{1, 500 * time.Millisecond},
		{2, time.Second},
		{3, 2 * time.Second},
		{7, 30 * time.Second},
		{100, 30 * time.Second},
	} {
		t.Run(fmt.Sprint(tc.n), func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, backoffCap(tc.n, 500*time.Millisecond, 30*time.Second))
		})
	}
}

func TestRetryWait(t *testing.T) {
	t.Parallel()
	id := func(d time.Duration) time.Duration { return d }
	for _, tc := range []struct {
		name string
		err  error
		want time.Duration
	}{
		{"no retry-after", &HTTPError{StatusCode: 503}, 2 * time.Second},
		{"larger retry-after wins", &HTTPError{StatusCode: 429, RetryAfter: 10 * time.Second}, 10 * time.Second},
		{"retry-after capped at max", &HTTPError{StatusCode: 429, RetryAfter: 3600 * time.Second}, 30 * time.Second},
		{"non-http error", context.DeadlineExceeded, 2 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, retryWait(3, tc.err, 500*time.Millisecond, 30*time.Second, id))
		})
	}
}
