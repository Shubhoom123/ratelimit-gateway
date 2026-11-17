// Overload and recovery: backend2 is made slow mid-run through its control
// endpoint, then restored. Route /flaky has a bulkhead, a 250 ms deadline and a
// circuit breaker. Expect fast answers (200) or fast refusals (503), not a pile-up.
//   t=0-5 s    normal
//   t=5 s      backend2 latency -> 400 ms (longer than the 250 ms deadline)
//   t=15 s     backend2 latency -> 5 ms
// Custom metrics: ok_latency (only 200s), shed (503), timeouts (504).
import http from 'k6/http';
import { Trend, Counter } from 'k6/metrics';
import exec from 'k6/execution';

const BASES = (__ENV.GATEWAYS || 'http://host.docker.internal:18081,http://host.docker.internal:18082').split(',');
const CONTROL = __ENV.BACKEND2_CONTROL || 'http://host.docker.internal:9002/__control';

http.setResponseCallback(http.expectedStatuses(200, 503, 504));

const okLatency = new Trend('ok_latency', true);
const shed = new Counter('shed');
const timeouts = new Counter('timeouts');
const served = new Counter('served');

export const options = {
  scenarios: {
    load: { executor: 'constant-arrival-rate', rate: 400, timeUnit: '1s', duration: '25s',
            preAllocatedVUs: 100, maxVUs: 600, exec: 'load' },
    slow: { executor: 'shared-iterations', vus: 1, iterations: 1, startTime: '5s', exec: 'slowDown' },
    recover: { executor: 'shared-iterations', vus: 1, iterations: 1, startTime: '15s', exec: 'recover' },
  },
  thresholds: {
    // Requests that succeed must stay fast, even while the backend is slow.
    ok_latency: ['p(99)<150'],
    // Requests that fail are refused or time out quickly, never hang.
    http_req_duration: ['p(99)<400'],
  },
};

export function load() {
  const base = BASES[exec.scenario.iterationInTest % BASES.length];
  const res = http.get(`${base}/flaky/x`);
  if (res.status === 200) { okLatency.add(res.timings.duration); served.add(1); }
  else if (res.status === 503) shed.add(1);
  else if (res.status === 504) timeouts.add(1);
}

export function slowDown() { http.get(`${CONTROL}?latency=400ms`); }
export function recover() { http.get(`${CONTROL}?latency=5ms`); }

// Always put the backend back, even if the run is interrupted late.
export function teardown() { http.get(`${CONTROL}?latency=5ms`); }
