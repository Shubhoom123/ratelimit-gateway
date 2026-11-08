package resilience

import (
	"context"
	"sync"
	"testing"
	"time"
)

type clk struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clk) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clk) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func TestBreakerTripsProbesAndRecovers(t *testing.T) {
	c := &clk{t: time.Unix(0, 0)}
	b := NewBreaker(BreakerConfig{Window: 10, MinRequests: 10, FailureRatio: 0.5, OpenFor: time.Second, HalfOpenProbe: 2}, c.now)

	call := func(fail bool) error {
		if err := b.Acquire(); err != nil {
			return err
		}
		if fail {
			b.Failure()
		} else {
			b.Success()
		}
		return nil
	}

	// Under MinRequests nothing trips, even with all failures.
	for i := 0; i < 9; i++ {
		call(true)
	}
	if b.State() != Closed {
		t.Fatalf("tripped before MinRequests")
	}
	call(true) // 10th outcome: 100% failures
	if b.State() != Open {
		t.Fatalf("want open, got %v", b.State())
	}
	if err := call(false); err != ErrOpen {
		t.Fatalf("open breaker must reject, got %v", err)
	}

	c.add(1100 * time.Millisecond)
	if b.State() != HalfOpen {
		t.Fatalf("want half-open after OpenFor, got %v", b.State())
	}
	// Probe failure re-opens.
	call(true)
	if b.State() != Open {
		t.Fatalf("probe failure should re-open, got %v", b.State())
	}

	c.add(1100 * time.Millisecond)
	// Only HalfOpenProbe calls admitted at once.
	if b.Acquire() != nil || b.Acquire() != nil {
		t.Fatal("two probes should be admitted")
	}
	if b.Acquire() != ErrOpen {
		t.Fatal("third concurrent probe should be rejected")
	}
	b.Success()
	b.Success()
	if b.State() != Closed {
		t.Fatalf("want closed after successful probes, got %v", b.State())
	}
}

func TestBreakerStaysClosedUnderMinorErrors(t *testing.T) {
	b := NewBreaker(BreakerConfig{Window: 20, MinRequests: 10, FailureRatio: 0.5}, nil)
	for i := 0; i < 200; i++ {
		b.Acquire()
		if i%5 == 0 { // 20% failure rate
			b.Failure()
		} else {
			b.Success()
		}
	}
	if b.State() != Closed {
		t.Fatalf("20%% failures should not trip a 50%% breaker, state=%v", b.State())
	}
}

func TestBulkheadShedsWhenFullAndRecovers(t *testing.T) {
	b := NewBulkhead(2, 20*time.Millisecond)
	ctx := context.Background()
	if b.Acquire(ctx) != nil || b.Acquire(ctx) != nil {
		t.Fatal("first two should succeed")
	}
	start := time.Now()
	if err := b.Acquire(ctx); err != ErrBulkheadFull {
		t.Fatalf("want ErrBulkheadFull, got %v", err)
	}
	if d := time.Since(start); d < 15*time.Millisecond || d > 200*time.Millisecond {
		t.Fatalf("should wait about maxWait, waited %v", d)
	}
	b.Release()
	if err := b.Acquire(ctx); err != nil {
		t.Fatalf("slot was released, got %v", err)
	}
}

func TestBulkheadQueuedCallerGetsFreedSlot(t *testing.T) {
	b := NewBulkhead(1, time.Second)
	b.Acquire(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Acquire(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	b.Release()
	if err := <-done; err != nil {
		t.Fatalf("queued acquire should succeed, got %v", err)
	}
}
