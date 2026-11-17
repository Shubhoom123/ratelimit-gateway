# Interview notes

How to talk about this project: a short pitch, the questions it tends to prompt, and honest answers. Every number is from this repo's own runs ([load-test report](loadtest-report.md), [algorithm benchmark](algorithm-benchmark.md)) on one 2-CPU machine, so describe them as behavior, not capacity.

## 30-second pitch

I built a rate-limiting API gateway in Go. Several gateway instances share limits through Redis, using atomic Lua scripts and Redis's own clock so they can't race or disagree on time. It also protects backends: circuit breakers, bulkheads, deadlines and load shedding. When I slowed a backend to 400 ms, p99 for served requests stayed around 10 ms instead of about 4.9 seconds, at the cost of shedding 44% of requests. It reloads config without a restart, exports Prometheus metrics with alerts, and runs entirely in Docker Compose.

## Questions and answers

**Why does a token bucket allow bursts but a sliding-window counter doesn't?** A bucket that has been idle is full, so it can pay out its whole capacity at once and then refill. In my benchmark with a limit of 100 per second, steady overload let 199 through in the worst 1 s window. The sliding-window counter estimates the rolling count from the current and previous windows, so it held at 100 with 1-3% error in the cluster test. The log is exact but stored 5,224 bytes per key against about 130 for the other two. ([ADR 0001](adr/0001-rate-limit-algorithms.md))

**How do you avoid races across gateway instances?** Each decision is one Lua script, which Redis runs atomically, so read-modify-write can't interleave. The script reads `TIME` from Redis instead of using the gateway's clock, so clock skew between instances can't matter. ([ADR 0002](adr/0002-atomic-lua-and-redis-time.md))

**What does the local fast path cost in accuracy?** For the token bucket, instances take small batches of tokens (leases) and spend them locally; for every algorithm they cache a denial for the retry hint, capped at 250 ms. Most requests never reach Redis: 474-477 calls instead of 20,000 at 2,000 req/s, with the same latency. The price is a bounded over-admission of up to `instances x batch` requests (about 20 extra for a 200/s rule on 2 instances), because each instance can hold one unspent batch for up to 100 ms. I couldn't measure the over-admission in my runs, but that is the worst case. ([ADR 0003](adr/0003-local-fast-path.md))

**Fail open or fail closed when Redis is down?** It depends on the route, so it's per-route config. A read endpoint can fail open. A payment or login route should fail closed. `local_fallback` is the middle option: each instance enforces `limit / fallback_divisor` in memory. In my test (1500 req/s against a 500/s route) the three policies admitted about 900, 600 and 350 per second. A breaker on Redis itself means requests don't each wait for the 25 ms timeout. ([ADR 0004](adr/0004-redis-outage-policy.md))

**What happened to p99 when you overloaded it?** With no protection, 90.6% of requests succeeded but p99 was about 4.9 s, because the queue behind the slow backend grew until requests timed out. With breaker, bulkhead and deadline, 54.8% succeeded at a p99 of 10-11 ms, and 44.3% got a fast 503. It trades throughput for latency on purpose: a fast rejection lets the client retry elsewhere. ([ADR 0005](adr/0005-resilience-breaker-bulkhead-shedding.md))

**How do you know your alerts work?** I tested one. I stopped Redis, sent traffic, and watched `RedisBreakerOpen` fire. That test also found a bug in my first version: a failing dependency cycles between open and half-open every second or two, so `state == 1 for 15s` never held. I changed the rules to look at the maximum over the last 30 s.

**Why no cloud deployment?** I wanted the project to cost nothing to run or test, ever. Compose reproduces the multi-instance setup with Redis, and the results are repeatable on any laptop. The trade-off, which I state in the README, is that there is no real network between gateway and Redis in the numbers. ([ADR 0006](adr/0006-local-only-zero-cost.md))

**What would you do next?** Make the sliding-window counter script Cluster-safe by passing both window keys in `KEYS`, test against a Redis Cluster, add a request-level trace ID, and run longer load tests with confidence intervals on a machine that isn't also running the load generator.

## Things I'd say honestly if asked

- Load numbers come from two short runs on one machine with everything sharing 2 CPUs. They show ordering and behavior, not capacity.
- The Redis-down numbers vary run to run, because timing against the breaker shifts them. The ordering (fail open > local fallback > fail closed) repeated.
- The fast path is an accuracy trade-off, not a free win.
- I measured behavior with my own tests and k6 scripts, but I did not run a long soak test.
- I wrote a first version of the alert rules that didn't work. I found it by testing them against a real Redis outage.

## Concepts this project touches

Rate-limiting algorithms; atomic operations and Lua in Redis; distributed clocks; cache and lease design; circuit breaker, bulkhead and timeout patterns; load shedding and backpressure; graceful degradation; observability (metrics, dashboards, alerting); hot config reload; testing with a race detector and real dependencies; CI.
