# 0005. Protecting backends: breaker, bulkhead, deadline and load shedding

Status: accepted

## Context

When a backend slows down, requests pile up inside the gateway. Each waits on a slow call, memory and goroutines grow, latency for everything rises, and the gateway can collapse along with the backend.

## Decision

Layer several limits, from the outside in (`internal/gateway/gateway.go`, `internal/resilience`):

1. **Gateway-wide in-flight cap** (`max_in_flight`). The outermost guard. Over the cap, return 503 with `Retry-After`.
2. **Rate limit** (ADR 0001 to 0004).
3. **Per-backend circuit breaker.** A rolling window of outcomes trips at a failure ratio, rejects for a set time, then admits a few probe requests. A 5xx or a deadline counts as a failure.
4. **Per-backend bulkhead.** Bounded concurrency with a bounded queue wait. When it is full, shed immediately instead of queueing.
5. **Per-route deadline.** The whole backend call gets a timeout.

Shed responses use 503 with `Retry-After: 1` and an `X-Shed-Reason` header (`breaker_open`, `bulkhead_full`, `gateway_overloaded`, `limiter_unavailable`) so clients and dashboards can tell them apart.

Outcomes the backend did not cause are not counted against it. A client cancelling, or the gateway shedding locally, calls `breaker.Cancel()` so shedding cannot trip the breaker and feed back on itself.

## Measured behavior

Clean local runs, backend latency jumps from 5 ms to 400 ms mid-run, 800 requests/s for 20 s. Read both rows, because protection trades throughput for latency:

| | Served successfully | p99 of served requests | Failed or shed |
|---|---|---|---|
| No protection | 14,496 (90.6%) | about 4,856 ms | 9.4% (client timeouts and 504s) |
| Bulkhead, deadline, breaker | 8,770 (54.8%) | 10 to 11 ms | 44.3% shed with 503, 0.9% 504 |

Without protection most requests eventually succeed, but the queue behind the slow backend grows until requests hit the 5 s timeout. With protection the gateway answers 55% fast and says no to the rest right away.

When the backend is killed for 7 s, about 40% of requests get a fast 503 from the open breaker and only 0.3% to 0.4% get a 502, with p99 of 10 to 13 ms.

## Consequences

- Clients see 503s during a backend problem. That is deliberate: a fast refusal is better than a slow timeout for most callers, and `Retry-After` tells them when to try again.
- Thresholds are config, per backend. The values in the demo config are tuned for the local tests, not for production traffic.
- Backend latency in the tests is injected, so these results show the mechanism works, not how it behaves against a real service.
