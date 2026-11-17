// Baseline: steady traffic through both gateways on an unlimited route.
// Expect: no errors and low latency.
import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

const BASES = (__ENV.GATEWAYS || 'http://host.docker.internal:18081,http://host.docker.internal:18082').split(',');

http.setResponseCallback(http.expectedStatuses(200));

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: 500, timeUnit: '1s', duration: '20s',
      preAllocatedVUs: 50, maxVUs: 300,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<50', 'p(99)<100'],
  },
};

export default function () {
  const base = BASES[exec.scenario.iterationInTest % BASES.length];
  const res = http.get(`${base}/api/hello`);
  check(res, { 'status is 200': (r) => r.status === 200 });
}
