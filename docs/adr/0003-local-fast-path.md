# 0003. Optional local fast path

Status: accepted

## Context

Calling Redis for every request puts a network round trip on the hot path. On loopback it is cheap (p50 33 to 59 microseconds in the benchmark), but a real network adds its own latency, and Redis becomes the throughput limit.

## Decision

An optional wrapper (`FastPath` in `internal/limiter/redis.go`, enabled with `fast_path.enabled`) trades a small, bounded accuracy loss for fewer round trips:

1. **Token leases** (token bucket only). Take a batch of tokens from Redis and spend them locally. The batch is one twentieth of the per-second rate, at least 1 and at most 50, and unspent tokens expire after 100 ms.
2. **Deny cache** (all algorithms). After Redis says no, answer no locally for the retry hint, capped at 250 ms.

## Accuracy bound

Each instance can hold one unspent batch, so across the cluster up to `instances x batch` requests above the limit can be admitted within one lease lifetime. For a 200/s rule the batch is 10, so with 2 instances that is at most 20 extra. Unspent tokens are thrown away at expiry, so under light load the limiter errs slightly low.

## Measured effect

From the two clean local runs:

- At 2000 requests/s with no limit hit: 474 to 477 Redis calls instead of 20,000, with the same latency (p99 9.0 to 9.3 ms against 9.1 to 10.6 ms).
- Accuracy scenario (600/s offered against a 200/s limit): both runs allowed 220/s with the fast path on and off, so the over-admission was not measurable here, though the worst case above still applies.
- Under heavy overload it saves much less: 5,273 to 5,501 of 6,000 calls. The deny cache lasts only as long as the retry hint, which for a token bucket at 200/s is about 5 ms, so denials are re-checked quickly.

## Consequences

- Lower Redis load and latency in the common case, at the price of a documented over-admission bound.
- Only the token bucket gets leases; the sliding-window algorithms get only the deny cache.
- Leases are per instance and per key, so very many keys means many small leases. Idle entries are swept when a shard grows large, so one-off identities cannot grow the cache without bound.
