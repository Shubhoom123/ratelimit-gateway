package limiter

import "github.com/shubhamkhalkho/ratelimit-gateway/internal/redisc"

// All scripts read time from Redis (TIME) instead of trusting the caller.
// Gateway instances have independent clocks, so using the caller's clock would
// make instance A's "now" disagree with instance B's and let requests slip
// through at window edges. One clock (Redis) makes the decision linearizable.
//
// Each script runs atomically: Redis executes one script at a time, so the
// read-modify-write below cannot interleave with another gateway's call.
// Every script returns {allowed, remaining, retry_after_ms}.

const luaNow = `
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000) -- milliseconds
`

// KEYS[1]=bucket hash. ARGV: rate_per_sec, capacity, want, partial(0|1)
// partial=1 grants min(floor(tokens), want) so a gateway can lease a batch of
// tokens for its local fast path; partial=0 is all-or-nothing.
// Returns {granted, remaining, retry_after_ms}.
var tokenBucketScript = redisc.NewScript(luaNow + `
local rate = tonumber(ARGV[1])
local cap = tonumber(ARGV[2])
local want = tonumber(ARGV[3])
local partial = tonumber(ARGV[4])
local d = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(d[1])
local ts = tonumber(d[2])
if tokens == nil then tokens = cap; ts = now end
local elapsed = now - ts
if elapsed > 0 then tokens = math.min(cap, tokens + elapsed * rate / 1000) end
local granted = 0
if partial == 1 then
  granted = math.min(math.floor(tokens), want)
elseif tokens >= want then
  granted = want
end
local retry = 0
if granted == 0 then
  retry = math.ceil((1 - tokens) * 1000 / rate)
  if retry < 1 then retry = 1 end
end
tokens = tokens - granted
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', now)
-- idle buckets are full again after cap/rate seconds, so they can be forgotten
redis.call('PEXPIRE', KEYS[1], math.ceil(cap * 1000 / rate) + 1000)
return {granted, math.floor(tokens), retry}
`)

// KEYS[1]=zset. ARGV: limit, window_ms, member_nonce
// The member is unique per request (timestamp + nonce) so two requests in the
// same millisecond are both counted.
var slidingLogScript = redisc.NewScript(luaNow + `
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
local count = redis.call('ZCARD', KEYS[1])
if count < limit then
  redis.call('ZADD', KEYS[1], now, now .. '-' .. ARGV[3])
  redis.call('PEXPIRE', KEYS[1], window)
  return {1, limit - count - 1, 0}
end
local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
local retry = tonumber(oldest[2]) + window - now
if retry < 1 then retry = 1 end
return {0, 0, retry}
`)

// KEYS[1]=counter prefix. ARGV: limit, window_ms
// Two fixed-window counters (current and previous) are blended by how far we
// are into the current window. O(1) memory, approximate at boundaries.
var slidingCounterScript = redisc.NewScript(luaNow + `
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cur = math.floor(now / window)
local frac = (now % window) / window
local curKey = KEYS[1] .. ':' .. cur
local c = tonumber(redis.call('GET', curKey) or '0')
local p = tonumber(redis.call('GET', KEYS[1] .. ':' .. (cur - 1)) or '0')
local est = p * (1 - frac) + c
if est + 1 <= limit then
  redis.call('INCR', curKey)
  redis.call('PEXPIRE', curKey, window * 2)
  return {1, math.floor(limit - est - 1), 0}
end
local retry = window - (now % window)
return {0, 0, retry}
`)
