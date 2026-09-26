package event

import "encoding/json"

// Result captures the outcome of the audited action. An empty Status
// means "unrecorded"; Recorder implementations may populate it from the
// HTTP status at record time if e.Result is unset.
//
// Message accepts any value but special-cases error: an error marshals
// as err.Error(), so callers can write Result{Message: err} instead of
// Result{Message: err.Error()}. Strings marshal as themselves; other
// types use their default JSON encoding.
type Result struct {
	Status  string `json:"status,omitempty"` // "ok" | "error" | "denied"
	Code    int    `json:"code,omitempty"`   // HTTP status or app-defined code
	Message any    `json:"message,omitempty"`
}

// MarshalJSON converts an error in Message to its .Error() string
// before encoding so callers can pass errors directly without manually
// calling .Error(). Other Message types are encoded by encoding/json's
// default rules. Empty strings (whether assigned directly or produced
// by an error's .Error()) are omitted, matching the behavior the field
// had before it was widened from string to any.
func (r Result) MarshalJSON() ([]byte, error) {
	type alias Result
	out := alias(r)
	if err, ok := out.Message.(error); ok {
		out.Message = err.Error()
	}
	if s, ok := out.Message.(string); ok && s == "" {
		out.Message = nil
	}
	return json.Marshal(out)
}
