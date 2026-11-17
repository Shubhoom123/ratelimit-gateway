# 0002. Atomic Lua scripts and Redis as the only clock

Status: accepted

## Context

Several gateway instances share limits through Redis. A limiter step is read, compute, write. If two instances do that as separate commands they can both read the same count and both admit a request the limit should have refused. Instances also have independent clocks, so "now" differs between them.

## Options considered

1. `GET` then `SET` from the gateway. Racy by construction.
2. `WATCH` / `MULTI` / `EXEC` with retries. Correct, but retries pile up under contention on a hot key.
3. A Lua script. Redis runs one script at a time, so the read-modify-write cannot interleave with another gateway's call.

## Decision

Use one Lua script per algorithm (`internal/limiter/scripts.go`) and read time with `redis.call('TIME')` inside the script instead of trusting the caller. Every script returns `{allowed, remaining, retry_after_ms}`. Scripts are called with `EVALSHA`, falling back to `EVAL` once if Redis reports `NOSCRIPT` (for example after a restart); this is covered by tests in `internal/redisc`.

Using Redis `TIME` gives every decision the same clock, so instance A and instance B cannot disagree about which window a request belongs to.

## A bug the design record explains

The sliding-window log adds each request to a sorted set with a unique member. A per-process counter alone is not unique across instances: two instances in the same millisecond with the same count would produce the same member, `ZADD` would overwrite instead of add, and the log would undercount. The members now include a random per-instance id (`redis.go`, comment on the `SlidingWindowLog` case).

## Consequences

- Every limiter decision costs one Redis round trip, which is why ADR 0003 exists.
- Redis is a single point of failure for limiting. ADR 0004 covers what the gateway does then.
- Keys are built as `prefix:{identity}:algorithm`. The braces are a Redis Cluster hash tag, so keys for one identity share a slot.

## Known limitation

The sliding-window counter script builds its per-window keys inside the script (`KEYS[1] .. ':' .. window_index`) instead of receiving them in `KEYS`. They share the hash tag, so they land in the same slot, but Redis documents that scripts should declare every key they touch to be Cluster-safe. This has only been run against a single Redis node, not a Cluster. Fixing it means passing the current and previous window keys as `KEYS[1]` and `KEYS[2]`.
