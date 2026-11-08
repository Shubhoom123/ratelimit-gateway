package limiter

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// Local is an in-process limiter. It is correct for one gateway instance only.
// Keys are sharded across mutexes so unrelated keys do not contend.
type Local struct {
	now    Clock
	shards [64]localShard
}

type localShard struct {
	mu      sync.Mutex
	buckets map[string]*bucketState
	logs    map[string]*logState
	counts  map[string]*counterState
	_       [24]byte // keep neighbouring shards off the same cache line
}

type bucketState struct {
	tokens float64
	last   time.Time
}

type logState struct{ stamps []time.Time } // ascending

type counterState struct {
	curStart  int64 // window index (unix nanos / window)
	cur, prev int64
}

// NewLocal returns an in-memory limiter; pass nil for the wall clock.
func NewLocal(now Clock) *Local {
	if now == nil {
		now = time.Now
	}
	l := &Local{now: now}
	for i := range l.shards {
		l.shards[i] = localShard{
			buckets: map[string]*bucketState{},
			logs:    map[string]*logState{},
			counts:  map[string]*counterState{},
		}
	}
	return l
}

func (l *Local) shard(key string) *localShard {
	var h uint32 = 2166136261 // FNV-1a
	for i := 0; i < len(key); i++ {
		h = (h ^ uint32(key[i])) * 16777619
	}
	return &l.shards[h%uint32(len(l.shards))]
}

// Allow implements Limiter.
func (l *Local) Allow(_ context.Context, key string, r Rule) (Decision, error) {
	now := l.now()
	s := l.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	switch r.Algorithm {
	case TokenBucket:
		return s.tokenBucket(key, r, now), nil
	case SlidingWindowLog:
		return s.slidingLog(key, r, now), nil
	case SlidingWindowCounter:
		return s.slidingCounter(key, r, now), nil
	}
	return Decision{}, fmt.Errorf("unknown algorithm %q", r.Algorithm)
}

func (s *localShard) tokenBucket(key string, r Rule, now time.Time) Decision {
	capacity := float64(r.Capacity())
	rate := float64(r.Limit) / r.Window.Seconds() // tokens per second
	b := s.buckets[key]
	if b == nil {
		b = &bucketState{tokens: capacity, last: now}
		s.buckets[key] = b
	}
	if el := now.Sub(b.last).Seconds(); el > 0 {
		b.tokens = math.Min(capacity, b.tokens+el*rate)
		b.last = now
	}
	if b.tokens >= 1 {
		b.tokens--
		return Decision{Allowed: true, Limit: r.Limit, Remaining: int64(b.tokens)}
	}
	wait := time.Duration((1 - b.tokens) / rate * float64(time.Second))
	return Decision{Limit: r.Limit, RetryAfter: wait}
}

func (s *localShard) slidingLog(key string, r Rule, now time.Time) Decision {
	st := s.logs[key]
	if st == nil {
		st = &logState{}
		s.logs[key] = st
	}
	cutoff := now.Add(-r.Window)
	i := 0
	for i < len(st.stamps) && !st.stamps[i].After(cutoff) {
		i++
	}
	st.stamps = st.stamps[i:]
	if int64(len(st.stamps)) < r.Limit {
		st.stamps = append(st.stamps, now)
		return Decision{Allowed: true, Limit: r.Limit, Remaining: r.Limit - int64(len(st.stamps))}
	}
	return Decision{Limit: r.Limit, RetryAfter: st.stamps[0].Add(r.Window).Sub(now)}
}

func (s *localShard) slidingCounter(key string, r Rule, now time.Time) Decision {
	w := int64(r.Window)
	idx := now.UnixNano() / w
	st := s.counts[key]
	if st == nil {
		st = &counterState{curStart: idx}
		s.counts[key] = st
	}
	switch gap := idx - st.curStart; {
	case gap == 1:
		st.prev, st.cur = st.cur, 0
	case gap > 1:
		st.prev, st.cur = 0, 0
	}
	st.curStart = idx
	frac := float64(now.UnixNano()%w) / float64(w)
	est := float64(st.prev)*(1-frac) + float64(st.cur)
	if est+1 <= float64(r.Limit) {
		st.cur++
		return Decision{Allowed: true, Limit: r.Limit, Remaining: int64(float64(r.Limit) - est - 1)}
	}
	return Decision{Limit: r.Limit, RetryAfter: time.Duration(w - now.UnixNano()%w)}
}

// Len reports tracked keys (for memory tests).
func (l *Local) Len() int {
	n := 0
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.Lock()
		n += len(s.buckets) + len(s.logs) + len(s.counts)
		s.mu.Unlock()
	}
	return n
}
