# k6 load tests

Run against the local Compose stack (`make up`). They need only Docker; k6 itself runs in a container.

| Script | What it checks | Passes when |
|---|---|---|
| `smoke.js` | Baseline at 500 req/s through both gateways on `/api` (no rate limit, so it never touches Redis) | under 1% errors, p95 under 50 ms, p99 under 100 ms |
| `accuracy.js` | One identity at 600 req/s against a 100/s token bucket (burst 100) | allowed total is between 1000 and 1250 (ideal about 1100) |
| `slow_backend.js` | Backend goes slow at t=5 s and recovers at t=15 s | served requests stay under 150 ms at p99, and nothing hangs |

```bash
bash loadtest/k6/run.sh smoke
bash loadtest/k6/run.sh accuracy       # wait 2 s between repeat runs so the bucket refills
bash loadtest/k6/run.sh slow_backend
```

Redis-down testing needs a container stop, so it is manual. Only the rate-limited routes
(`/limited`, `/strict`) call Redis, so `smoke` won't exercise it. Run `accuracy`, or loop
`curl localhost:18081/limited` for about 30 s, after
`docker compose -f deploy/compose/docker-compose.yml stop redis`, and watch the dashboard and
`/alerts` (http://localhost:9099/alerts). `RedisBreakerOpen` should reach firing. With the demo config's
`local_fallback` policy and 2 gateways, `accuracy` still admits about 1,100 requests, because each
gateway enforces half the limit on its own. Start Redis again with `start redis`.

URLs default to `host.docker.internal:18081/18082`. Override with `GATEWAYS=http://a,http://b`.
The older Go load generator (`scripts/loadtest_local.py`) produced the numbers in the README and
`docs/loadtest-report.md`; these k6 scripts are an independent cross-check, not a replacement.

## If `accuracy` shows about double the expected total

Both gateways must see the same client IP, because the limiter charges unknown callers by IP. If your
Docker setup gives each gateway a different source address, each gets its own bucket and the total
doubles. That is a test setup issue, not a limiter bug. Send an `X-API-Key` header that both gateways
know (add one under `api_keys` in `deploy/compose/config.json`) to charge a fixed identity instead.
