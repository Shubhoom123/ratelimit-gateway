// Package limiter implements three rate-limiting algorithms twice: in memory
// (single process, used for the algorithm benchmark and as the Redis-outage
// fallback) and in Redis via atomic Lua scripts (shared across gateway
// instances).
package limiter

import (
	"context"
	"fmt"
	"time"
)

// Algorithm names as they appear in the config file.
const (
	TokenBucket          = "token_bucket"
	SlidingWindowLog     = "sliding_window_log"
	SlidingWindowCounter = "sliding_window_counter"
)

// Rule describes one limit.
//
//   - TokenBucket: refill Limit tokens per Window, bucket holds Burst tokens.
//   - SlidingWindow*: at most Limit requests in any Window (Burst is ignored).
type Rule struct {
	Algorithm string        `json:"algorithm"`
	Limit     int64         `json:"limit"`
	Window    time.Duration `json:"-"`
	Burst     int64         `json:"burst"`
}

// Validate checks the rule is usable.
func (r Rule) Validate() error {
	switch r.Algorithm {
	case TokenBucket, SlidingWindowLog, SlidingWindowCounter:
	default:
		return fmt.Errorf("unknown algorithm %q", r.Algorithm)
	}
	if r.Limit <= 0 {
		return fmt.Errorf("limit must be > 0")
	}
	if r.Window <= 0 {
		return fmt.Errorf("window must be > 0")
	}
	if r.Burst < 0 {
		return fmt.Errorf("burst must be >= 0")
	}
	return nil
}

// Capacity is the bucket size for a token bucket (Burst, defaulting to Limit).
func (r Rule) Capacity() int64 {
	if r.Burst > 0 {
		return r.Burst
	}
	return r.Limit
}

// Decision is the outcome of one Allow call.
type Decision struct {
	Allowed    bool
	Limit      int64
	Remaining  int64
	RetryAfter time.Duration // zero when allowed
}

// Limiter decides whether one request for key may proceed.
type Limiter interface {
	Allow(ctx context.Context, key string, rule Rule) (Decision, error)
}

// Clock lets tests and the accuracy benchmark drive time deterministically.
type Clock func() time.Time
