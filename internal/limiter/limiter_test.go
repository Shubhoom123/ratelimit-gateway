package limiter

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubhamkhalkho/ratelimit-gateway/internal/redisc"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func count(l Limiter, key string, r Rule, n int) int {
	ok := 0
	for i := 0; i < n; i++ {
		d, err := l.Allow(context.Background(), key, r)
		if err != nil {
			panic(err)
		}
		if d.Allowed {
			ok++
		}
	}
	return ok
}

// A token bucket lets a full burst through at once and then refills at the
// steady rate. This is the behavioural difference interviewers ask about.
func TestTokenBucketAllowsBurstThenRefills(t *testing.T) {
	clk := newFakeClock()
	l := NewLocal(clk.Now)
	r := Rule{Algorithm: TokenBucket, Limit: 10, Window: time.Second, Burst: 30}

	if got := count(l, "k", r, 100); got != 30 {
		t.Fatalf("burst: allowed %d, want 30", got)
	}
	clk.Advance(500 * time.Millisecond) // refill 5 tokens
	if got := count(l, "k", r, 100); got != 5 {
		t.Fatalf("after 500ms: allowed %d, want 5", got)
	}
}

// A sliding window never admits more than Limit in any window, bursts included.
func TestSlidingLogIsExact(t *testing.T) {
	clk := newFakeClock()
	l := NewLocal(clk.Now)
	r := Rule{Algorithm: SlidingWindowLog, Limit: 10, Window: time.Second}

	if got := count(l, "k", r, 100); got != 10 {
		t.Fatalf("allowed %d, want 10", got)
	}
	clk.Advance(999 * time.Millisecond)
	if got := count(l, "k", r, 5); got != 0 {
		t.Fatalf("999ms later allowed %d, want 0", got)
	}
	clk.Advance(2 * time.Millisecond)
	if got := count(l, "k", r, 100); got != 10 {
		t.Fatalf("after window allowed %d, want 10", got)
	}
}

// The fixed-window boundary problem: 2x the limit across a boundary. The
// sliding-window counter should hold the line to roughly the limit.
func TestSlidingCounterFixesBoundaryBurst(t *testing.T) {
	clk := newFakeClock()
	// Align to the start of a window.
	clk.Advance(time.Second - time.Duration(clk.Now().UnixNano()%int64(time.Second)))
	l := NewLocal(clk.Now)
	r := Rule{Algorithm: SlidingWindowCounter, Limit: 100, Window: time.Second}

	clk.Advance(900 * time.Millisecond)
	first := count(l, "k", r, 500)
	clk.Advance(200 * time.Millisecond) // 100ms into the next window
	second := count(l, "k", r, 500)
	// A naive fixed window would allow 100 + 100 = 200 within 200ms.
	if first != 100 {
		t.Fatalf("first burst allowed %d, want 100", first)
	}
	if second > 15 {
		t.Fatalf("second burst allowed %d across the boundary, want <= 15 (naive fixed window would allow 100)", second)
	}
}

func TestRuleValidate(t *testing.T) {
	bad := []Rule{
		{Algorithm: "nope", Limit: 1, Window: time.Second},
		{Algorithm: TokenBucket, Limit: 0, Window: time.Second},
		{Algorithm: TokenBucket, Limit: 1, Window: 0},
	}
	for _, r := range bad {
		if r.Validate() == nil {
			t.Errorf("expected %+v to be invalid", r)
		}
	}
}

// ---------------------------------------------------------------------------
// Redis integration tests. They need a real Redis (the point is to exercise the
// Lua scripts). Set REDIS_ADDR, or start one on 127.0.0.1:6379; otherwise skip.
// ---------------------------------------------------------------------------

func redisClient(t testing.TB) *redisc.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	c := redisc.New(redisc.Options{Addr: addr, PoolSize: 128})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.Do(ctx, "PING"); err != nil {
		t.Skipf("redis not reachable at %s: %v", addr, err)
	}
	t.Cleanup(c.Close)
	return c
}

func uniq(t testing.TB) string {
	return fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
}

func TestRedisAlgorithmsBasicBehaviour(t *testing.T) {
	c := redisClient(t)
	l := NewRedis(c, "test")
	rules := map[string]Rule{
		TokenBucket:          {Algorithm: TokenBucket, Limit: 20, Window: time.Second, Burst: 20},
		SlidingWindowLog:     {Algorithm: SlidingWindowLog, Limit: 20, Window: time.Second},
		SlidingWindowCounter: {Algorithm: SlidingWindowCounter, Limit: 20, Window: time.Second},
	}
	for name, r := range rules {
		t.Run(name, func(t *testing.T) {
			id := uniq(t)
			got := count(l, id, r, 100)
			// Rate-per-ms refill during the loop can add a token or two for
			// the bucket; the windows are strict.
			if got < 20 || got > 22 {
				t.Fatalf("allowed %d of 100, want ~20", got)
			}
			d, _ := l.Allow(context.Background(), id, r)
			if d.Allowed || d.RetryAfter <= 0 {
				t.Fatalf("expected denial with RetryAfter, got %+v", d)
			}
		})
	}
}

// The core correctness claim: many independent gateway instances (separate
// clients, goroutines) hammering one key never exceed the limit.
func TestRedisNeverOversubscribesAcrossInstances(t *testing.T) {
	c := redisClient(t)
	const instances, perInstance = 8, 400
	for _, alg := range []string{TokenBucket, SlidingWindowLog} {
		t.Run(alg, func(t *testing.T) {
			r := Rule{Algorithm: alg, Limit: 100, Window: 5 * time.Second, Burst: 100}
			id := uniq(t)
			var allowed atomic.Int64
			var wg sync.WaitGroup
			start := time.Now()
			for i := 0; i < instances; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					l := NewRedis(c, "test") // each goroutine = one gateway instance
					for j := 0; j < perInstance; j++ {
						d, err := l.Allow(context.Background(), id, r)
						if err != nil {
							t.Error(err)
							return
						}
						if d.Allowed {
							allowed.Add(1)
						}
					}
				}()
			}
			wg.Wait()
			elapsed := time.Since(start)
			// Bucket: burst + refill during the run. Log: exactly the limit.
			max := int64(100)
			if alg == TokenBucket {
				max += int64(elapsed.Seconds()*20) + 1
			}
			if got := allowed.Load(); got > max || got < 100 {
				t.Fatalf("allowed %d (elapsed %v), want in [100,%d]", got, elapsed, max)
			}
		})
	}
}

// naiveCounter is the classic broken design: GET, compare, then INCR as three
// separate round trips. It exists only to show the race in a test.
func naiveAllow(ctx context.Context, c *redisc.Client, key string, limit int64) bool {
	v, _ := c.Do(ctx, "GET", key)
	var n int64
	if s, ok := v.(string); ok {
		fmt.Sscan(s, &n)
	}
	if n >= limit {
		return false
	}
	c.Do(ctx, "INCR", key)
	return true
}

func TestNaiveGetThenIncrOversubscribes(t *testing.T) {
	c := redisClient(t)
	key := "naive:" + uniq(t)
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if naiveAllow(context.Background(), c, key, 100) {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	c.Do(context.Background(), "DEL", key)
	got := allowed.Load()
	t.Logf("naive GET+INCR admitted %d with a limit of 100", got)
	if got <= 100 {
		t.Skipf("race did not manifest this run (admitted %d); it is probabilistic", got)
	}
}

// Fast path: far fewer Redis calls, and the overshoot stays within the bound
// documented on FastPath (instances * batch).
func TestFastPathCutsRoundTripsWithBoundedError(t *testing.T) {
	c := redisClient(t)
	const instances = 4
	r := Rule{Algorithm: TokenBucket, Limit: 1000, Window: 10 * time.Second, Burst: 1000}
	id := uniq(t)

	var allowed, calls atomic.Int64
	var total atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < instances; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rl := NewRedis(c, "test")
			fp := NewFastPath(rl, nil)
			for j := 0; j < 2000; j++ {
				d, err := fp.Allow(context.Background(), id, r)
				if err != nil {
					t.Error(err)
					return
				}
				total.Add(1)
				if d.Allowed {
					allowed.Add(1)
				}
			}
			calls.Add(rl.Calls.Load())
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	batch := int64(50) // limit 1000/10s = 100/s, /20 = 5 -> see batchSize
	batch = NewFastPath(NewRedis(c, "x"), nil).batchSize(r)
	refill := int64(elapsed.Seconds()*100) + 1
	ceiling := int64(1000) + refill + int64(instances)*batch
	t.Logf("requests=%d allowed=%d redis_calls=%d batch=%d ceiling=%d", total.Load(), allowed.Load(), calls.Load(), batch, ceiling)
	if allowed.Load() > ceiling {
		t.Fatalf("allowed %d exceeds documented bound %d", allowed.Load(), ceiling)
	}
	if calls.Load()*2 >= total.Load() {
		t.Fatalf("expected the fast path to at least halve Redis calls: %d calls for %d requests", calls.Load(), total.Load())
	}
}

func TestRedisScriptReloadsAfterFlush(t *testing.T) {
	c := redisClient(t)
	l := NewRedis(c, "test")
	r := Rule{Algorithm: SlidingWindowCounter, Limit: 5, Window: time.Second}
	if _, err := l.Allow(context.Background(), uniq(t), r); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do(context.Background(), "SCRIPT", "FLUSH"); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Allow(context.Background(), uniq(t), r); err != nil {
		t.Fatalf("expected NOSCRIPT fallback to EVAL, got %v", err)
	}
}
