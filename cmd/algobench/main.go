// Command algobench compares the three algorithms on accuracy, memory and
// latency and prints a markdown report.
//
//	algobench -redis 127.0.0.1:6379 > docs/algorithm-benchmark.md
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubhamkhalkho/ratelimit-gateway/internal/limiter"
	"github.com/shubhamkhalkho/ratelimit-gateway/internal/redisc"
)

var algs = []string{limiter.TokenBucket, limiter.SlidingWindowLog, limiter.SlidingWindowCounter}

const (
	limit  = 100
	window = time.Second
)

func rule(alg string) limiter.Rule {
	return limiter.Rule{Algorithm: alg, Limit: limit, Window: window, Burst: limit}
}

// maxInAnyWindow returns the most admitted requests inside any [t, t+window).
func maxInAnyWindow(ts []time.Time) int {
	best, j := 0, 0
	for i := range ts {
		for ts[i].Sub(ts[j]) >= window {
			j++
		}
		if n := i - j + 1; n > best {
			best = n
		}
	}
	return best
}

type accuracy struct{ boundaryMax, overloadTotal, overloadMax int }

// accuracyRun drives the algorithm with a virtual clock (no sleeping, no noise).
func accuracyRun(alg string) accuracy {
	var now time.Time
	l := limiter.NewLocal(func() time.Time { return now })
	ctx := context.Background()
	r := rule(alg)
	var a accuracy

	// 1. Boundary burst: 300 requests just before a window edge, 300 just after.
	// A fixed-window counter would admit 2x the limit across the edge.
	base := time.Unix(1_700_000_000, 0).Truncate(window).Add(10 * window) // aligned, bucket is full
	var adm []time.Time
	for _, off := range []time.Duration{window - time.Millisecond, window + time.Millisecond} {
		now = base.Add(off)
		for i := 0; i < 300; i++ {
			if d, _ := l.Allow(ctx, "boundary", r); d.Allowed {
				adm = append(adm, now)
			}
		}
	}
	a.boundaryMax = maxInAnyWindow(adm)

	// 2. Sustained 5x overload for 10s (500 req/s), evenly spaced, fresh key.
	adm = adm[:0]
	start := base.Add(20 * window)
	for i := 0; i < 5000; i++ {
		now = start.Add(time.Duration(i) * (window / 500))
		if d, _ := l.Allow(ctx, "overload", r); d.Allowed {
			adm = append(adm, now)
		}
	}
	a.overloadTotal = len(adm)
	a.overloadMax = maxInAnyWindow(adm)
	return a
}

func heapAfterGC() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// localBytesPerKey measures in-process state for keys that each received limit requests.
func localBytesPerKey(alg string) float64 {
	const keys = 20000
	before := heapAfterGC()
	l := limiter.NewLocal(nil)
	r := rule(alg)
	ctx := context.Background()
	for k := 0; k < keys; k++ {
		id := fmt.Sprintf("key-%d", k)
		for i := 0; i < limit; i++ {
			l.Allow(ctx, id, r)
		}
	}
	after := heapAfterGC()
	runtime.KeepAlive(l)
	return float64(after-before) / keys
}

func redisBytesPerKey(c *redisc.Client, alg string) (float64, error) {
	const keys = 200
	ctx := context.Background()
	rl := limiter.NewRedis(c, "bench")
	r := rule(alg)
	var total int64
	for k := 0; k < keys; k++ {
		id := fmt.Sprintf("mem-%s-%d", alg, k)
		for i := 0; i < limit; i++ {
			if _, err := rl.Allow(ctx, id, r); err != nil {
				return 0, err
			}
		}
		reply, err := c.Do(ctx, "KEYS", "bench:{"+id+"}:*")
		if err != nil {
			return 0, err
		}
		for _, name := range reply.([]any) {
			u, err := c.Do(ctx, "MEMORY", "USAGE", name)
			if err != nil {
				return 0, err
			}
			total += u.(int64)
		}
	}
	return float64(total) / keys, nil
}

func localNsPerOp(alg string) float64 {
	r := rule(alg)
	r.Limit = 1 << 40 // never deny: measure the decision path, not the reject path
	r.Burst = 1 << 40
	res := testing.Benchmark(func(b *testing.B) {
		l := limiter.NewLocal(nil)
		ctx := context.Background()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				l.Allow(ctx, fmt.Sprintf("k%d", i&1023), r)
				i++
			}
		})
	})
	return float64(res.NsPerOp())
}

type redisLat struct{ p50, p99 time.Duration }

// redisLatency measures single-caller round-trip latency to Redis.
func redisLatency(c *redisc.Client, alg string) redisLat {
	rl := limiter.NewRedis(c, "bench")
	r := rule(alg)
	r.Limit = 1 << 30
	r.Burst = 1 << 30
	ctx := context.Background()
	const n = 5000
	lat := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		t := time.Now()
		rl.Allow(ctx, fmt.Sprintf("lat-%s-%d", alg, i%64), r)
		lat = append(lat, time.Since(t))
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	return redisLat{lat[n/2], lat[n*99/100]}
}

func redisThroughput(c *redisc.Client, alg string) float64 {
	rl := limiter.NewRedis(c, "bench")
	r := rule(alg)
	r.Limit = 1 << 30
	r.Burst = 1 << 30
	var ctr atomic.Int64
	res := testing.Benchmark(func(b *testing.B) {
		ctx := context.Background()
		b.SetParallelism(8)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				rl.Allow(ctx, fmt.Sprintf("tp-%s-%d", alg, ctr.Add(1)&1023), r)
			}
		})
	})
	return float64(res.N) / res.T.Seconds()
}

func main() {
	addr := flag.String("redis", "127.0.0.1:6379", "Redis address (skips Redis sections if unreachable)")
	flag.Parse()
	var w strings.Builder
	fmt.Fprintf(&w, "# Algorithm benchmark\n\nGenerated by `cmd/algobench` on %s (%s/%s, %d CPUs, %s).\n", time.Now().Format("2006-01-02"), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	fmt.Fprintf(&w, "Rule under test: %d requests per %s (token bucket: refill %d/s, capacity %d).\n\n", limit, window, limit, limit)

	fmt.Fprintf(&w, "## Accuracy (virtual clock, single process)\n\n")
	fmt.Fprintf(&w, "| Algorithm | Max admitted in any 1s window, boundary burst (limit %d) | Admitted over 10s at 5x overload (ideal %d + burst) | Max in any 1s window, steady overload |\n|---|---|---|---|\n", limit, limit*10)
	for _, a := range algs {
		r := accuracyRun(a)
		fmt.Fprintf(&w, "| `%s` | %d (%.2fx) | %d | %d (%.2fx) |\n", a, r.boundaryMax, float64(r.boundaryMax)/limit, r.overloadTotal, r.overloadMax, float64(r.overloadMax)/limit)
	}
	fmt.Fprintf(&w, "\nBoundary burst: 300 requests 1ms before a window edge and 300 more 1ms after, on an idle key.\n"+
		"Steady overload: 500 requests/second for 10s on a fresh key.\n\n")

	fmt.Fprintf(&w, "## Memory per key (each key received %d requests)\n\n| Algorithm | In-process bytes/key | Redis `MEMORY USAGE` bytes/key |\n|---|---|---|\n", limit)
	c := redisc.New(redisc.Options{Addr: *addr, PoolSize: 64})
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, perr := c.Do(ctx, "PING")
	cancel()
	redisUp := perr == nil
	for _, a := range algs {
		rb := "n/a (Redis unreachable)"
		if redisUp {
			if v, err := redisBytesPerKey(c, a); err == nil {
				rb = fmt.Sprintf("%.0f", v)
			} else {
				rb = "error: " + err.Error()
			}
		}
		fmt.Fprintf(&w, "| `%s` | %.0f | %s |\n", a, localBytesPerKey(a), rb)
	}

	fmt.Fprintf(&w, "\n## Latency\n\n| Algorithm | In-process decision (ns/op, parallel) |")
	if redisUp {
		fmt.Fprintf(&w, " Redis round trip p50 | Redis round trip p99 | Redis throughput (8 callers per core) |")
	}
	fmt.Fprintf(&w, "\n|---|---|")
	if redisUp {
		fmt.Fprintf(&w, "---|---|---|")
	}
	fmt.Fprintln(&w)
	for _, a := range algs {
		fmt.Fprintf(&w, "| `%s` | %.0f |", a, localNsPerOp(a))
		if redisUp {
			l := redisLatency(c, a)
			fmt.Fprintf(&w, " %s | %s | %.0f ops/s |", l.p50.Round(time.Microsecond), l.p99.Round(time.Microsecond), redisThroughput(c, a))
		}
		fmt.Fprintln(&w)
	}
	if redisUp {
		fmt.Fprintf(&w, "\nRedis numbers are loopback to a local single-node Redis, so they measure script cost and client overhead, not network distance. Expect a real AWS same-AZ round trip to add roughly the network RTT on top.\n")
	}
	fmt.Fprint(os.Stdout, w.String())
}
