package event

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResult_MarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   Result
		want string
	}{
		{
			name: "string message marshals as string",
			in:   Result{Status: "error", Code: 500, Message: "boom"},
			want: `{"status":"error","code":500,"message":"boom"}`,
		},
		{
			name: "error message marshals as .Error()",
			in:   Result{Status: "error", Code: 500, Message: errors.New("db down")},
			want: `{"status":"error","code":500,"message":"db down"}`,
		},
		{
			name: "wrapped error message marshals as full .Error() chain",
			in:   Result{Status: "error", Message: wrapErr()},
			want: `{"status":"error","message":"recorder: wrapped: original"}`,
		},
		{
			name: "nil message omitted",
			in:   Result{Status: "ok", Code: 200},
			want: `{"status":"ok","code":200}`,
		},
		{
			name: "empty string message omitted",
			in:   Result{Status: "ok", Code: 200, Message: ""},
			want: `{"status":"ok","code":200}`,
		},
		{
			name: "non-string non-error marshals as default JSON",
			in:   Result{Status: "ok", Message: map[string]any{"k": 1}},
			want: `{"status":"ok","message":{"k":1}}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := json.Marshal(tc.in)
			require.NoError(t, err)
			require.JSONEq(t, tc.want, string(got))
		})
	}
}

func TestResult_MarshalJSON_RoundTripsThroughEvent(t *testing.T) {
	t.Parallel()

	e := New("user.login")
	e.Result = Result{Status: "error", Code: 500, Message: errors.New("db down")}

	body, err := json.Marshal(e)
	require.NoError(t, err)

	var got Event
	require.NoError(t, json.Unmarshal(body, &got))

	// On the wire, Message is a string. Decoding into any gives us the
	// string back — callers reading it can type-assert to string.
	msg, ok := got.Result.Message.(string)
	require.True(t, ok)
	require.Equal(t, "db down", msg)
}

func wrapErr() error {
	return errors.New("recorder: wrapped: original")
}
