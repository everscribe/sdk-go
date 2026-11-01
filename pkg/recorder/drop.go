package recorder

import (
	"github.com/everscribe/sdk-go/pkg/event"
)

// DropReason says why the recorder discarded events.
type DropReason string

const (
	DropBufferFull       DropReason = "buffer_full"       // buffer was full: drop-newest, or an auto-record that got an overflow error
	DropRejected         DropReason = "rejected"          // non-retryable error, or split budget used up
	DropRetriesExhausted DropReason = "retries_exhausted" // MaxRetryDuration elapsed
	DropShutdown         DropReason = "shutdown"          // Close's drain timeout elapsed
	DropClosed           DropReason = "closed"            // recorded after Close, or released from a block wait by Close
)

var allDropReasons = []DropReason{
	DropBufferFull, DropRejected, DropRetriesExhausted, DropShutdown, DropClosed,
}

// WithOnDrop sets a callback invoked whenever events are discarded.
// err is the last delivery error for rejected and retries_exhausted,
// and nil otherwise. The callback must not block; a panic is recovered
// and logged.
func WithOnDrop(fn func(events []event.Event, reason DropReason, err error)) BufferedOption {
	return func(c *bufferedConfig) { c.onDrop = fn }
}

// reportDrop counts dropped events and calls the onDrop callback. Every
// drop goes through here. It takes no locks.
func (b *BufferedRecorder) reportDrop(events []event.Event, reason DropReason, err error) {
	if len(events) == 0 {
		return
	}
	n := int64(len(events))
	if c, ok := b.dropCounts[reason]; ok {
		c.Add(n)
	}
	if reason == DropBufferFull {
		b.dropped.Add(n)
	}
	if b.cfg.onDrop == nil {
		return
	}
	cp := append([]event.Event(nil), events...)
	defer func() {
		if r := recover(); r != nil {
			b.cfg.logger.Error("recorder onDrop callback panicked", "panic", r, "reason", string(reason))
		}
	}()
	b.cfg.onDrop(cp, reason, err)
}
