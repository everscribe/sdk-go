package recorder

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/everscribe/sdk-go/pkg/event"
)

type dropCall struct {
	events []event.Event
	reason DropReason
	err    error
}

func TestReportDrop_CallsCallbackWithCopy(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var got []dropCall
	b := newTestBuffered(t, &captureRec{}, WithOnDrop(func(evs []event.Event, r DropReason, err error) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, dropCall{evs, r, err})
	}))

	src := []event.Event{{Action: "a.one"}, {Action: "a.two"}}
	b.reportDrop(src, DropRejected, ErrBufferFull)
	src[0].Action = "mutated"

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 1)
	require.Equal(t, DropRejected, got[0].reason)
	require.ErrorIs(t, got[0].err, ErrBufferFull)
	require.Equal(t, "a.one", got[0].events[0].Action)
	require.Equal(t, "a.two", got[0].events[1].Action)
}

func TestReportDrop_RecoversPanic(t *testing.T) {
	t.Parallel()
	inner := &captureBatchRec{}
	b := newTestBuffered(t, inner, WithOnDrop(func([]event.Event, DropReason, error) {
		panic("boom")
	}))

	require.NotPanics(t, func() {
		b.reportDrop([]event.Event{{Action: "a.one"}}, DropClosed, nil)
	})
	require.Equal(t, int64(1), b.Stats().DroppedByReason[DropClosed])

	require.NoError(t, b.Record(t.Context(), event.New("a.later")))
	require.Eventually(t, func() bool {
		return b.Flush(t.Context()) == nil && b.Stats().Flushed == 1
	}, time.Second, 5*time.Millisecond)
}

func TestReportDrop_NoCallback(t *testing.T) {
	t.Parallel()
	b := newTestBuffered(t, &captureRec{})
	reasons := []DropReason{DropBufferFull, DropRejected, DropRetriesExhausted, DropShutdown, DropClosed}
	for _, r := range reasons {
		require.NotPanics(t, func() {
			b.reportDrop([]event.Event{{Action: "a"}, {Action: "b"}}, r, nil)
		})
	}
	st := b.Stats()
	for _, r := range reasons {
		require.Equal(t, int64(2), st.DroppedByReason[r], r)
	}
}

func TestReportDrop_BufferFullMovesDropped(t *testing.T) {
	t.Parallel()
	b := newTestBuffered(t, &captureRec{})
	for range 3 {
		b.reportDrop([]event.Event{{Action: "a"}}, DropBufferFull, nil)
	}
	b.reportDrop([]event.Event{{Action: "a"}}, DropRejected, nil)

	st := b.Stats()
	require.Equal(t, int64(3), st.Dropped)
	require.Equal(t, st.Dropped, st.DroppedByReason[DropBufferFull])
	require.Equal(t, int64(1), st.DroppedByReason[DropRejected])
}
