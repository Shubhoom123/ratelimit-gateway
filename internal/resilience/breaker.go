// Package resilience holds the failure-containment primitives: a circuit
// breaker and a bulkhead (bounded concurrency with a bounded wait).
package resilience

import (
	"errors"
	"sync"
	"time"
)

// ErrOpen means the breaker is rejecting calls.
var ErrOpen = errors.New("circuit breaker open")

// State of a breaker.
type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	return [...]string{"closed", "open", "half_open"}[s]
}

// BreakerConfig tunes a breaker.
type BreakerConfig struct {
	Window        int           // outcomes kept in the rolling window (default 20)
	MinRequests   int           // don't trip before this many outcomes (default 10)
	FailureRatio  float64       // trip at >= this failure fraction (default 0.5)
	OpenFor       time.Duration // time spent open before probing (default 5s)
	HalfOpenProbe int           // probes admitted while half-open (default 3)
}

func (c *BreakerConfig) defaults() {
	if c.Window <= 0 {
		c.Window = 20
	}
	if c.MinRequests <= 0 {
		c.MinRequests = 10
	}
	if c.FailureRatio <= 0 {
		c.FailureRatio = 0.5
	}
	if c.OpenFor <= 0 {
		c.OpenFor = 5 * time.Second
	}
	if c.HalfOpenProbe <= 0 {
		c.HalfOpenProbe = 3
	}
}

// Breaker trips on a high failure ratio over the last N outcomes, rejects for
// OpenFor, then lets a few probe calls through. Any probe failure re-opens it;
// all probes succeeding closes it.
type Breaker struct {
	cfg BreakerConfig
	now func() time.Time

	mu        sync.Mutex
	state     State
	ring      []bool // true = failure
	pos, n    int
	failures  int
	openedAt  time.Time
	probes    int // admitted
	probeOK   int // succeeded
	transFunc func(from, to State)
}

// NewBreaker builds a breaker; now may be nil.
func NewBreaker(cfg BreakerConfig, now func() time.Time) *Breaker {
	cfg.defaults()
	if now == nil {
		now = time.Now
	}
	return &Breaker{cfg: cfg, now: now, ring: make([]bool, cfg.Window)}
}

// OnTransition registers a state-change callback (called with the lock held, so keep it cheap).
func (b *Breaker) OnTransition(f func(from, to State)) { b.transFunc = f }

func (b *Breaker) set(s State) {
	if b.state == s {
		return
	}
	from := b.state
	b.state = s
	if b.transFunc != nil {
		b.transFunc(from, s)
	}
}

func (b *Breaker) reset() {
	for i := range b.ring {
		b.ring[i] = false
	}
	b.pos, b.n, b.failures = 0, 0, 0
}

// State reports the current state (promoting Open to HalfOpen when due).
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.promote()
	return b.state
}

func (b *Breaker) promote() {
	if b.state == Open && b.now().Sub(b.openedAt) >= b.cfg.OpenFor {
		b.set(HalfOpen)
		b.probes, b.probeOK = 0, 0
	}
}

// Acquire asks permission to call. On nil error the caller must call exactly
// one of Success or Failure.
func (b *Breaker) Acquire() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.promote()
	switch b.state {
	case Open:
		return ErrOpen
	case HalfOpen:
		if b.probes >= b.cfg.HalfOpenProbe {
			return ErrOpen
		}
		b.probes++
	}
	return nil
}

// Cancel returns an Acquire that ended without a verdict (the caller was shed
// by another limiter, or the client went away). It frees a half-open probe slot
// so the breaker cannot get stuck waiting for an outcome that never arrives.
func (b *Breaker) Cancel() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == HalfOpen && b.probes > 0 {
		b.probes--
	}
}

// Success records a successful call.
func (b *Breaker) Success() { b.record(false) }

// Failure records a failed call.
func (b *Breaker) Failure() { b.record(true) }

func (b *Breaker) record(failed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case HalfOpen:
		if failed {
			b.openedAt = b.now()
			b.set(Open)
			return
		}
		b.probeOK++
		if b.probeOK >= b.cfg.HalfOpenProbe {
			b.reset()
			b.set(Closed)
		}
	case Closed:
		if b.n == len(b.ring) && b.ring[b.pos] {
			b.failures--
		}
		b.ring[b.pos] = failed
		if failed {
			b.failures++
		}
		b.pos = (b.pos + 1) % len(b.ring)
		if b.n < len(b.ring) {
			b.n++
		}
		if b.n >= b.cfg.MinRequests && float64(b.failures)/float64(b.n) >= b.cfg.FailureRatio {
			b.openedAt = b.now()
			b.reset()
			b.set(Open)
		}
	}
	// Outcomes that arrive while Open (calls admitted before the trip) are ignored.
}
