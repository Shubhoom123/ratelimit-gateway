# Local load-test report

Two complete runs of `scripts/loadtest_local.py` on the same machine (2-CPU Linux container; load generator, Redis, backend and both gateways share those CPUs). Treat the numbers as a floor for the code and as evidence of behavior, not as a capacity claim. Raw per-scenario data for the latest run is in [`../results/`](../results/); run 1 is summarized below.

Setup: 2 gateway instances sharing one Redis, one backend with 5 ms base latency and 64 workers. Each scenario sends a fixed request rate through both gateways. Redis and backend failures are injected mid-run by the harness.

| Scenario | Allowed/s (run 1 / run 2) | p50 ms | p99 ms | p99 of successes ms | Redis calls | Status codes (run 2) |
|---|---|---|---|---|---|---|
| Baseline 1000 req/s, no limit | 999 / 999 | 6.6 / 6.7 | 7.9 / 8.4 | 7.9 / 8.4 | 10000 / 10000 | 200: 10000 |
| Baseline 2000 req/s, no limit | 1999 / 1999 | 6.4 / 6.6 | 9.1 / 10.6 | 9.1 / 10.6 | 20000 / 20000 | 200: 20000 |
| Baseline 2000 req/s, fast path on | 1999 / 1999 | 6.3 / 6.4 | 9.0 / 9.3 | 9.0 / 9.3 | 474 / 477 | 200: 20000 |
| Accuracy: token bucket | 220 / 220 | 1.1 / 1.2 | 7.7 / 7.7 | 8.0 / 7.9 | 6000 / 6000 | 200: 2199, 429: 3801 |
| Accuracy: token bucket + fast path | 220 / 220 | 1.2 / 1.2 | 7.7 / 7.8 | 7.8 / 8.2 | 5273 / 5501 | 200: 2199, 429: 3801 |
| Accuracy: sliding-window log | 200 / 200 | 1.2 / 1.2 | 7.6 / 7.7 | 8.2 / 8.2 | 6000 / 6000 | 200: 2000, 429: 4000 |
| Accuracy: sliding-window counter | 206 / 202 | 1.1 / 1.1 | 7.5 / 7.7 | 7.7 / 7.9 | 6000 / 6000 | 200: 2021, 429: 3979 |
| Backend slows to 400 ms, no protection | 725 / 725 | 7.4 / 7.3 | 5001.9 / 5001.8 | 4855.8 / 4855.8 | 16000 / 16000 | 200: 14496, 504: 92, error: 1412 |
| Backend slows to 400 ms, bulkhead+deadline+breaker | 438 / 438 | 6.0 / 6.0 | 15.2 / 14.0 | 10.6 / 10.0 | 16000 / 16000 | 200: 8771, 503: 7085, 504: 144 |
| Backend killed 7 s | 476 / 477 | 6.1 / 6.1 | 10.1 / 12.6 | 10.1 / 14.2 | 16000 / 16000 | 200: 9533, 502: 55, 503: 6412 |
| Redis down 7 s, fail_open | 900 / 901 | 6.0 / 6.0 | 9.8 / 9.5 | 10.5 / 10.1 | 19506 / 19505 | 200: 18017, 429: 11983 |
| Redis down 7 s, local_fallback | 575 / 625 | 1.3 / 1.3 | 8.9 / 9.2 | 10.2 / 10.7 | 19506 / 16508 | 200: 12496, 429: 17504 |
| Redis down 7 s, fail_closed | 324 / 374 | 1.0 / 1.0 | 9.4 / 8.3 | 13.8 / 10.3 | 12027 / 19504 | 200: 7489, 429: 11983, 503: 10528 |

## What repeated and what did not

- Deterministic across both runs: token bucket 220/s and sliding-window log 200/s against a 200/s limit. The sliding-window counter admitted 206/s then 202/s (about 1-3% over). The token-bucket figure is exact because 200 burst plus 200/s refill over 10 s is 2200 requests.
- Stable: baseline latency, backend-slow and backend-killed results (status counts within about 1% between runs).
- Noisy: the Redis-outage scenarios. The timing of the outage against the Redis breaker shifts how many requests are rejected, so `fail_closed` allowed 324/s then 374/s and its 503 count moved between 18,021 and 10,528. Compare the policies qualitatively (fail_open admits most, then local_fallback, then fail_closed), not by exact numbers.

## Limitations

Single machine, 10-20 s runs, two trials, no confidence intervals. Backend latency is injected, not a real service. Not a cloud deployment: no network distance between gateway and Redis.
