# Design decisions

Short records of why the gateway works the way it does, each with the code that implements it and the measurements that back it.

| # | Decision |
|---|---|
| [0001](0001-rate-limit-algorithms.md) | Three algorithms behind one interface; why a token bucket allows bursts and a sliding-window counter does not |
| [0002](0002-atomic-lua-and-redis-time.md) | Atomic Lua scripts and Redis as the only clock; how races across instances are avoided |
| [0003](0003-local-fast-path.md) | Optional local fast path: fewer Redis calls for a bounded accuracy loss |
| [0004](0004-redis-outage-policy.md) | Fail open, fail closed, or local fallback when Redis is down |
| [0005](0005-resilience-breaker-bulkhead-shedding.md) | Circuit breaker, bulkhead, deadlines and load shedding |
| [0006](0006-local-only-zero-cost.md) | Why the project runs locally with no cloud deployment |

## Questions these answer

- *Why does a token bucket allow bursts when a sliding-window counter doesn't?* 0001.
- *How did you avoid race conditions across gateway instances?* 0002.
- *Fail open or fail closed when Redis is down?* 0004.
- *What happened to p99 when you overloaded it?* 0005, with the table of the slow-backend run.
