# 0004. What happens when Redis is down: fail open, fail closed, or fall back

Status: accepted

## Context

If the limiter cannot reach Redis, the gateway must choose between two bad outcomes. Rejecting everything turns a Redis outage into a total outage. Admitting everything removes the protection the limiter exists to provide.

## Decision

Make it a per-route policy, with a global default (`on_redis_error`, overridable per route):

| Policy | Behavior while Redis is unavailable |
|---|---|
| `fail_open` | Admit every request. |
| `fail_closed` | Reject with 503 (`X-Shed-Reason: limiter_unavailable`). |
| `local_fallback` (default) | Enforce `limit / fallback_divisor` per instance in memory. |

Two things keep an outage cheap. Redis calls have a short timeout (25 ms by default), and a circuit breaker on Redis stops requests from paying that timeout once failures pile up. The breaker's state is exposed in `/readyz` and metrics. Redis being down does not make the gateway unready, because the policy handles it.

## Measured behavior

Clean local runs, 1500 requests/s against a 500/s route, Redis killed for 7 s of a 20 s run:

| Policy | Allowed per second (run 1, run 2) |
|---|---|
| `fail_open` | 900, 901 |
| `local_fallback` | 575, 625 |
| `fail_closed` | 324, 374 |

The ordering held in both runs. The exact numbers did not for `fail_closed` (503 counts were 18,021 and 10,528), because the outage timing against the breaker moves the counts, so compare policies qualitatively.

## Consequences

- Choose per route. A login or payment route should fail closed, because an outage is when brute-force attempts are cheapest. A read-heavy route can fail open.
- Fail open is less dangerous than it sounds, because the per-backend bulkheads and breakers (ADR 0005) still cap what reaches a backend.
- `local_fallback` assumes `fallback_divisor` matches the number of instances. Running more instances than the divisor means the combined limit is higher than intended; running fewer means it is lower.
- Fixed `fallback_divisor` is a simplification. Instances do not know about each other when Redis is gone.
