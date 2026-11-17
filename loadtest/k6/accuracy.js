// Limiter accuracy across two gateway instances. One identity (this machine's
// IP) is hit on /limited, which is a token bucket of 100/s with a burst of 100.
// Expected allowed total over 10 s: burst 100 + 100/s * 10 s = about 1100.
// The fast path can add a little (see docs/adr/0003), so the upper bound has slack.
// Run it twice in a row and wait 2 s between runs so the bucket starts full each time.
import http from 'k6/http';
import { Counter } from 'k6/metrics';
import exec from 'k6/execution';

const BASES = (__ENV.GATEWAYS || 'http://host.docker.internal:18081,http://host.docker.internal:18082').split(',');
const ROUTE = __ENV.ROUTE || '/limited';

http.setResponseCallback(http.expectedStatuses(200, 429));

const allowed = new Counter('allowed');
const denied = new Counter('denied');

export const options = {
  scenarios: {
    offered: {
      executor: 'constant-arrival-rate',
      rate: 600, timeUnit: '1s', duration: '10s',
      preAllocatedVUs: 100, maxVUs: 400,
    },
  },
  thresholds: {
    allowed: ['count>=1000', 'count<=1250'],
  },
};

export default function () {
  const base = BASES[exec.scenario.iterationInTest % BASES.length];
  const res = http.get(`${base}${ROUTE}`);
  if (res.status === 200) allowed.add(1); else if (res.status === 429) denied.add(1);
}
