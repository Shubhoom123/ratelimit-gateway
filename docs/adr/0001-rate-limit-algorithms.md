# 0001. Three rate-limit algorithms behind one interface

Status: accepted

## Context

Different routes want different things from a limiter. A public read API can tolerate a short burst. A login endpoint cannot. Memory per key also matters when there are millions of API keys.

## Decision

Implement token bucket, sliding-window log and sliding-window counter behind one `Limiter` interface (`internal/limiter/limiter.go`). Each exists twice: in memory (`local.go`, used for the benchmark and the Redis-outage fallback) and as a Redis Lua script (`scripts.go`). The algorithm is chosen per route or per API key in config. The default is token bucket.

## How they differ, with measurements

Measured by `cmd/algobench` with a limit of 100 per second (full tables in `docs/algorithm-benchmark.md`).

| | Max admitted in any 1 s under steady overload | Redis memory per key (100 requests seen) | In-process cost |
|---|---|---|---|
| token bucket | 199 (1.99x the limit) | 135 B | 141 ns/op |
| sliding-window log | 100 | 5224 B | 376 ns/op |
| sliding-window counter | 100 | 121 B | 187 ns/op |

**Why a token bucket allows bursts.** The bucket holds up to `burst` tokens and refills at the limit rate. A key that has been idle is full, so it can spend the whole bucket at once, then continue at the refill rate. In any one second it can admit up to the capacity plus one second of refill, which is why steady overload showed 199 against a limit of 100. That is the intended behavior, not a bug. Set `burst` equal to `limit` for a bucket of one second.

**Why the sliding-window counter does not.** It keeps two fixed-window counters and estimates the load in the trailing window as `previous * (1 - fraction_elapsed) + current`. It refuses a request when the estimate would pass the limit. A burst right after a window edge still sees most of the previous window's count, so the classic fixed-window problem (up to 2x the limit across a boundary) does not appear. In our boundary test every algorithm admitted exactly 100.

**What the counter gives up.** The estimate assumes the previous window's requests were spread evenly. If they were all bunched at the end of that window, the estimate is too low and it admits slightly too many. Over a 10 s run against a limit of 200/s it admitted 206 and 202 per second in the two clean runs (1% to 3% over). The log is exact because it stores one timestamp per request, but its memory grows with the limit.

## Consequences

- Use the token bucket where bursts are fine. Use the sliding-window counter when the limit must hold in every window and memory matters. Use the log only when exactness matters and the limit is small.
- Three implementations of each algorithm (memory and Redis) mean three places for behavior to drift. The tests exercise both and the benchmark compares them.
