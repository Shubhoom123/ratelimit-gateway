package resilience

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// ErrBulkheadFull means no slot freed up within the wait budget.
var ErrBulkheadFull = errors.New("bulkhead full")

// Bulkhead caps concurrent calls to one backend. Callers that cannot get a slot
// within MaxWait are shed immediately instead of queueing without bound, which
// keeps latency bounded and stops a slow backend from consuming every goroutine
// and connection in the gateway.
type Bulkhead struct {
	slots   chan struct{}
	maxWait time.Duration
	waiting atomic.Int64
	shed    atomic.Uint64
}

// NewBulkhead allows max concurrent holders and queues for at most maxWait.
func NewBulkhead(max int, maxWait time.Duration) *Bulkhead {
	if max <= 0 {
		max = 1
	}
	return &Bulkhead{slots: make(chan struct{}, max), maxWait: maxWait}
}

// Acquire takes a slot or returns ErrBulkheadFull / the context error.
func (b *Bulkhead) Acquire(ctx context.Context) error {
	select {
	case b.slots <- struct{}{}:
		return nil
	default:
	}
	if b.maxWait <= 0 {
		b.shed.Add(1)
		return ErrBulkheadFull
	}
	b.waiting.Add(1)
	defer b.waiting.Add(-1)
	t := time.NewTimer(b.maxWait)
	defer t.Stop()
	select {
	case b.slots <- struct{}{}:
		return nil
	case <-t.C:
		b.shed.Add(1)
		return ErrBulkheadFull
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release frees a slot taken by Acquire.
func (b *Bulkhead) Release() { <-b.slots }

// InFlight is the number of slots currently held.
func (b *Bulkhead) InFlight() int { return len(b.slots) }

// Waiting is the number of callers queued for a slot.
func (b *Bulkhead) Waiting() int64 { return b.waiting.Load() }

// Capacity is the configured maximum concurrency.
func (b *Bulkhead) Capacity() int { return cap(b.slots) }
