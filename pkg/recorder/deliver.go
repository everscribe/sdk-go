package recorder

import (
	"context"
	"time"

	"github.com/everscribe/sdk-go/pkg/event"
)

// splitBudget caps split sends per original batch.
const splitBudget = 32

// delivery is the state shared by one batch and the halves split from it.
type delivery struct {
	b        *BufferedRecorder
	ctx      context.Context
	deadline time.Time
	budget   int
	splitErr error // last splittable error, for parts the budget drops
}

// deliver sends one batch, retrying and splitting it, until every event
// is delivered or reported. If ctx ends first it returns the unresolved
// events, in order, with ctx.Err(), and does not report them.
func (b *BufferedRecorder) deliver(ctx context.Context, batch []event.Event) (remaining []event.Event, err error) {
	if len(batch) == 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return batch, err
	}
	d := &delivery{
		b:        b,
		ctx:      ctx,
		deadline: time.Now().Add(b.cfg.maxRetryDuration),
		budget:   splitBudget,
	}
	return d.send(batch)
}

// send resolves part p with the retry loop.
func (d *delivery) send(p []event.Event) ([]event.Event, error) {
	cfg := &d.b.cfg
	final := false
	for n := 1; ; n++ {
		retry, err := d.attempt(p)
		if err == nil {
			return nil, nil
		}
		if cerr := d.ctx.Err(); cerr != nil {
			return retry, cerr
		}
		d.b.flushErrs.Add(1)
		cfg.logger.Warn("recorder delivery attempt failed",
			"error", err, "batch_size", len(p), "attempt", n)
		switch {
		case len(retry) == 0:
			return nil, nil
		case len(retry) > 1 && isSplittable(err):
			d.splitErr = err
			return d.split(retry)
		case !isRetryable(err):
			d.drop(retry, DropRejected, err)
			return nil, nil
		case final:
			d.drop(retry, DropRetriesExhausted, err)
			return nil, nil
		}
		p = retry
		wait := retryWait(n, err, cfg.initialBackoff, cfg.maxBackoff, cfg.jitter)
		if left := time.Until(d.deadline); wait >= left {
			wait, final = max(left, 0), true
		}
		if !d.sleep(wait) {
			return p, d.ctx.Err()
		}
		d.b.retries.Add(1)
	}
}

// split halves p and resolves the first half fully before the second.
func (d *delivery) split(p []event.Event) ([]event.Event, error) {
	halves := [2][]event.Event{p[:len(p)/2], p[len(p)/2:]}
	for i, h := range halves {
		if d.budget == 0 {
			d.drop(h, DropRejected, d.splitErr)
			continue
		}
		d.budget--
		rest, err := d.send(h)
		if err != nil {
			out := append([]event.Event(nil), rest...)
			if i == 0 {
				out = append(out, halves[1]...)
			}
			return out, err
		}
	}
	return nil, nil
}

// attempt sends p once. It returns the events still unresolved and the
// error behind them; nil error means nothing is left to do.
func (d *delivery) attempt(p []event.Event) ([]event.Event, error) {
	ctx, cancel := context.WithTimeout(d.ctx, d.b.cfg.flushTimeout)
	defer cancel()

	if br, ok := d.b.inner.(BatchRecorder); ok {
		if err := br.RecordBatch(ctx, p); err != nil {
			return p, err
		}
		d.b.flushed.Add(int64(len(p)))
		return nil, nil
	}

	// Serial fallback: keep only retryable failures, reject the rest alone.
	var failed []event.Event
	var lastErr, retryErr error
	for i := range p {
		if err := d.ctx.Err(); err != nil {
			return append(failed, p[i:]...), err
		}
		err := d.b.inner.Record(ctx, &p[i])
		if cerr := d.ctx.Err(); err != nil && cerr != nil {
			return append(failed, p[i:]...), cerr
		}
		switch {
		case err == nil:
			d.b.flushed.Add(1)
			continue
		case isRetryable(err):
			failed = append(failed, p[i])
			retryErr = err
		default:
			d.drop(p[i:i+1], DropRejected, err)
		}
		lastErr = err
	}
	if retryErr != nil {
		return failed, retryErr
	}
	return nil, lastErr
}

// sleep waits w, returning false if ctx ends first.
func (d *delivery) sleep(w time.Duration) bool {
	if w <= 0 {
		return d.ctx.Err() == nil
	}
	t := time.NewTimer(w)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-d.ctx.Done():
		return false
	}
}

func (d *delivery) drop(events []event.Event, reason DropReason, err error) {
	d.b.cfg.logger.Error("recorder dropped events",
		"reason", string(reason), "count", len(events), "error", err)
	d.b.reportDrop(events, reason, err)
}
