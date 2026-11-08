package limiter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shubhamkhalkho/ratelimit-gateway/internal/redisc"
)

// Redis enforces limits through atomic Lua scripts, so any number of gateway
// instances sharing one Redis see a single consistent counter per key.
type Redis struct {
	c      *redisc.Client
	nonce  atomic.Uint64
	id     string // random per instance: makes sorted-set members globally unique
	prefix string
	// Calls counts round trips to Redis; the fast-path benchmark reads it.
	Calls atomic.Int64
}

// NewRedis wraps a client. prefix namespaces keys (e.g. "rl").
func NewRedis(c *redisc.Client, prefix string) *Redis {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // the OS entropy source failing is not recoverable
	}
	return &Redis{c: c, prefix: prefix, id: hex.EncodeToString(b[:])}
}

// Key exposes the Redis key prefix used for an identity (for memory benchmarks and ops tooling).
func (r *Redis) Key(id, alg string) string { return r.key(id, alg) }

// key builds "prefix:{identity}:algorithm". The braces are a Redis Cluster
// hash tag, keeping every key a script touches for one identity in one slot.
func (r *Redis) key(id string, alg string) string {
	return r.prefix + ":{" + id + "}:" + alg
}

// Allow implements Limiter.
func (r *Redis) Allow(ctx context.Context, id string, rule Rule) (Decision, error) {
	r.Calls.Add(1)
	k := r.key(id, rule.Algorithm)
	winMs := rule.Window.Milliseconds()
	if winMs < 1 {
		winMs = 1
	}
	var vals []int64
	var err error
	switch rule.Algorithm {
	case TokenBucket:
		rate := float64(rule.Limit) / rule.Window.Seconds()
		vals, err = redisc.Ints(tokenBucketScript.Run(ctx, r.c, []string{k}, rate, rule.Capacity(), 1, 0))
	case SlidingWindowLog:
		// Members must be unique across *all* gateway instances. A per-process
		// counter alone collides (two instances, same millisecond, same count),
		// ZADD then overwrites instead of adding, and the log undercounts.
		nonce := r.id + "-" + strconv.FormatUint(r.nonce.Add(1), 36)
		vals, err = redisc.Ints(slidingLogScript.Run(ctx, r.c, []string{k}, rule.Limit, winMs, nonce))
	case SlidingWindowCounter:
		vals, err = redisc.Ints(slidingCounterScript.Run(ctx, r.c, []string{k}, rule.Limit, winMs))
	default:
		return Decision{}, fmt.Errorf("unknown algorithm %q", rule.Algorithm)
	}
	if err != nil {
		return Decision{}, err
	}
	if len(vals) != 3 {
		return Decision{}, fmt.Errorf("limiter: script returned %d values", len(vals))
	}
	return Decision{
		Allowed:    vals[0] > 0,
		Limit:      rule.Limit,
		Remaining:  vals[1],
		RetryAfter: time.Duration(vals[2]) * time.Millisecond,
	}, nil
}

// lease asks Redis for up to want tokens from a token bucket.
func (r *Redis) lease(ctx context.Context, id string, rule Rule, want int64) (granted int64, retry time.Duration, err error) {
	r.Calls.Add(1)
	rate := float64(rule.Limit) / rule.Window.Seconds()
	vals, err := redisc.Ints(tokenBucketScript.Run(ctx, r.c, []string{r.key(id, TokenBucket)}, rate, rule.Capacity(), want, 1))
	if err != nil {
		return 0, 0, err
	}
	if len(vals) != 3 {
		return 0, 0, fmt.Errorf("limiter: script returned %d values", len(vals))
	}
	return vals[0], time.Duration(vals[2]) * time.Millisecond, nil
}

// ---------------------------------------------------------------------------
// Local fast path
// ---------------------------------------------------------------------------

// FastPath wraps Redis with two local caches that cut round trips at a small,
// bounded accuracy cost:
//
//  1. Token leases (token bucket only): take a batch of tokens from Redis and
//     spend them locally. Worst case, every gateway instance holds one unspent
//     batch, so the cluster can admit up to instances*batch extra requests
//     over the true limit within one lease TTL. Unspent tokens are discarded at
//     lease expiry, so the limiter errs slightly *low* under light load.
//  2. Deny cache (all algorithms): once Redis says "no, retry in X", answer
//     "no" locally for min(X, MaxDenyTTL) instead of asking again. A client
//     hammering a limit costs one Redis call per retry window, not one per
//     request.
type FastPath struct {
	R          *Redis
	Now        Clock
	LeaseTTL   time.Duration // how long leased tokens stay spendable (default 100ms)
	MaxBatch   int64         // cap on batch size (default 50)
	MaxDenyTTL time.Duration // cap on local denial caching (default 250ms)

	shards [64]fpShard
}

type fpShard struct {
	mu     sync.Mutex
	leases map[string]*lease
	denies map[string]time.Time
}

type lease struct {
	tokens  int64
	expires time.Time
}

// sweep drops expired entries once a shard grows large, so a stream of
// one-off identities cannot grow the caches without bound. Caller holds mu.
func (s *fpShard) sweep(now time.Time) {
	if len(s.leases)+len(s.denies) < 4096 {
		return
	}
	for k, l := range s.leases {
		if !now.Before(l.expires) {
			delete(s.leases, k)
		}
	}
	for k, t := range s.denies {
		if !now.Before(t) {
			delete(s.denies, k)
		}
	}
}

// NewFastPath builds the wrapper.
func NewFastPath(r *Redis, now Clock) *FastPath {
	if now == nil {
		now = time.Now
	}
	f := &FastPath{R: r, Now: now, LeaseTTL: 100 * time.Millisecond, MaxBatch: 50, MaxDenyTTL: 250 * time.Millisecond}
	for i := range f.shards {
		f.shards[i].leases = map[string]*lease{}
		f.shards[i].denies = map[string]time.Time{}
	}
	return f
}

func (f *FastPath) shard(key string) *fpShard {
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h = (h ^ uint32(key[i])) * 16777619
	}
	return &f.shards[h%uint32(len(f.shards))]
}

// batchSize picks how many tokens to lease: a twentieth of the per-second rate
// (so a lease is worth about 50ms of traffic), at least 1, at most MaxBatch.
func (f *FastPath) batchSize(rule Rule) int64 {
	perSec := float64(rule.Limit) / rule.Window.Seconds()
	b := int64(math.Floor(perSec / 20))
	if b < 1 {
		b = 1
	}
	if b > f.MaxBatch {
		b = f.MaxBatch
	}
	if c := rule.Capacity(); b > c {
		b = c
	}
	return b
}

// Allow implements Limiter.
func (f *FastPath) Allow(ctx context.Context, id string, rule Rule) (Decision, error) {
	now := f.Now()
	cacheKey := id + "|" + rule.Algorithm
	s := f.shard(cacheKey)

	s.mu.Lock()
	if until, ok := s.denies[cacheKey]; ok {
		if now.Before(until) {
			s.mu.Unlock()
			return Decision{Limit: rule.Limit, RetryAfter: until.Sub(now)}, nil
		}
		delete(s.denies, cacheKey)
	}
	if rule.Algorithm == TokenBucket {
		if l := s.leases[cacheKey]; l != nil && l.tokens > 0 && now.Before(l.expires) {
			l.tokens--
			rem := l.tokens
			s.mu.Unlock()
			return Decision{Allowed: true, Limit: rule.Limit, Remaining: rem}, nil
		}
	}
	s.mu.Unlock()

	if rule.Algorithm == TokenBucket {
		granted, retry, err := f.R.lease(ctx, id, rule, f.batchSize(rule))
		if err != nil {
			return Decision{}, err
		}
		if granted > 0 {
			s.mu.Lock()
			s.sweep(now)
			s.leases[cacheKey] = &lease{tokens: granted - 1, expires: now.Add(f.LeaseTTL)}
			s.mu.Unlock()
			return Decision{Allowed: true, Limit: rule.Limit, Remaining: granted - 1}, nil
		}
		f.cacheDeny(s, cacheKey, now, retry)
		return Decision{Limit: rule.Limit, RetryAfter: retry}, nil
	}

	d, err := f.R.Allow(ctx, id, rule)
	if err != nil {
		return d, err
	}
	if !d.Allowed {
		f.cacheDeny(s, cacheKey, now, d.RetryAfter)
	}
	return d, nil
}

func (f *FastPath) cacheDeny(s *fpShard, key string, now time.Time, retry time.Duration) {
	ttl := retry
	if ttl > f.MaxDenyTTL {
		ttl = f.MaxDenyTTL
	}
	if ttl <= 0 {
		return
	}
	s.mu.Lock()
	s.sweep(now)
	s.denies[key] = now.Add(ttl)
	s.mu.Unlock()
}
