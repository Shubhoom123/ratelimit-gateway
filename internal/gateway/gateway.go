// Package gateway wires routing, rate limiting and resilience into an
// http.Handler. Configuration is an immutable snapshot swapped atomically, so
// a reload never pauses or tears an in-flight request.
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/shubhamkhalkho/ratelimit-gateway/internal/config"
	"github.com/shubhamkhalkho/ratelimit-gateway/internal/limiter"
	"github.com/shubhamkhalkho/ratelimit-gateway/internal/metrics"
	"github.com/shubhamkhalkho/ratelimit-gateway/internal/redisc"
	"github.com/shubhamkhalkho/ratelimit-gateway/internal/resilience"
)

// Gateway is the reverse proxy.
type Gateway struct {
	log  *slog.Logger
	reg  *metrics.Registry
	snap atomic.Pointer[snapshot]

	local    *limiter.Local // in-memory limiter used by local_fallback
	inflight atomic.Int64

	mRequests  *metrics.CounterVec
	mLatency   *metrics.HistogramVec
	mDecisions *metrics.CounterVec
	mRedisErr  *metrics.CounterVec
	mShed      *metrics.CounterVec
	mReloads   *metrics.CounterVec
}

type snapshot struct {
	version  string
	cfg      *config.File
	routes   []*routeRT // longest prefix first
	backends map[string]*backendRT
	redis    *redisRT
}

type routeRT struct {
	cfg     config.Route
	backend *backendRT
	base    config.LimitCfg // defaults merged with the route override
	policy  string
}

type redisRT struct {
	key     redisKey
	client  *redisc.Client
	direct  *limiter.Redis
	fast    *limiter.FastPath // nil when the fast path is off
	breaker *resilience.Breaker
	timeout time.Duration
}

type redisKey struct {
	addr   string
	prefix string
	pool   int
	fp     config.FastPathCfg
	br     config.Breaker
}

type backendRT struct {
	name     string
	cfg      config.Backend
	proxy    *httputil.ReverseProxy
	bulkhead *resilience.Bulkhead
	breaker  *resilience.Breaker
}

// New creates a gateway with no config; call Reload before serving.
func New(log *slog.Logger, reg *metrics.Registry) *Gateway {
	g := &Gateway{log: log, reg: reg, local: limiter.NewLocal(nil)}
	g.mRequests = reg.Counter("gateway_requests_total", "Requests by route and status code.", "route", "code")
	g.mLatency = reg.Histogram("gateway_request_duration_seconds", "End-to-end request latency.", metrics.DefaultLatencyBuckets, "route")
	g.mDecisions = reg.Counter("gateway_ratelimit_decisions_total", "Limiter decisions by source (redis, local_fallback, fail_open, fail_closed).", "route", "source", "result")
	g.mRedisErr = reg.Counter("gateway_redis_errors_total", "Failed limiter calls to Redis.", "kind")
	g.mShed = reg.Counter("gateway_shed_total", "Requests rejected to protect the gateway or a backend.", "route", "reason")
	g.mReloads = reg.Counter("gateway_config_reloads_total", "Config reload attempts.", "result")
	reg.GaugeFunc("gateway_inflight_requests", "Requests currently being served.", func() []metrics.GaugeSample {
		return []metrics.GaugeSample{{Value: float64(g.inflight.Load())}}
	})
	reg.GaugeFunc("gateway_breaker_state", "Circuit breaker state (0 closed, 1 open, 2 half-open).", g.breakerGauge)
	reg.GaugeFunc("gateway_bulkhead_in_flight", "Slots held per backend bulkhead.", func() []metrics.GaugeSample {
		s := g.snap.Load()
		if s == nil {
			return nil
		}
		var out []metrics.GaugeSample
		for n, b := range s.backends {
			out = append(out, metrics.GaugeSample{Labels: map[string]string{"backend": n}, Value: float64(b.bulkhead.InFlight())})
		}
		return out
	})
	reg.GaugeFunc("gateway_redis_calls", "Limiter round trips to Redis since start (monotonic).", func() []metrics.GaugeSample {
		s := g.snap.Load()
		if s == nil {
			return nil
		}
		return []metrics.GaugeSample{{Value: float64(s.redis.direct.Calls.Load())}}
	})
	reg.GaugeFunc("gateway_config_info", "Live config version (value is always 1).", func() []metrics.GaugeSample {
		s := g.snap.Load()
		if s == nil {
			return nil
		}
		return []metrics.GaugeSample{{Labels: map[string]string{"version": s.version}, Value: 1}}
	})
	return g
}

func (g *Gateway) breakerGauge() []metrics.GaugeSample {
	s := g.snap.Load()
	if s == nil {
		return nil
	}
	out := []metrics.GaugeSample{{Labels: map[string]string{"name": "redis", "kind": "redis"}, Value: float64(s.redis.breaker.State())}}
	for n, b := range s.backends {
		out = append(out, metrics.GaugeSample{Labels: map[string]string{"name": n, "kind": "backend"}, Value: float64(b.breaker.State())})
	}
	return out
}

func breakerCfg(b config.Breaker) resilience.BreakerConfig {
	return resilience.BreakerConfig{
		Window: b.Window, MinRequests: b.MinRequests, FailureRatio: b.FailureRatio,
		OpenFor: b.OpenFor.Std(), HalfOpenProbe: b.Probes,
	}
}

// Reload builds a new snapshot from f and swaps it in. Runtime state that has
// not changed (circuit breakers, bulkheads, Redis connections, leases) is
// carried over, so a reload that only edits a rate limit does not reset a
// breaker that is currently open.
func (g *Gateway) Reload(f *config.File, version string) error {
	old := g.snap.Load()
	s := &snapshot{version: version, cfg: f, backends: map[string]*backendRT{}}

	rk := redisKey{addr: f.Redis.Addr, prefix: f.Redis.Prefix, pool: f.Redis.PoolSize, fp: f.FastPath, br: f.Redis.Breaker}
	if old != nil && old.redis.key == rk && old.redis.timeout == f.Redis.Timeout.Std() {
		s.redis = old.redis
	} else {
		client := redisc.New(redisc.Options{Addr: f.Redis.Addr, PoolSize: f.Redis.PoolSize})
		direct := limiter.NewRedis(client, f.Redis.Prefix)
		rt := &redisRT{key: rk, client: client, direct: direct, timeout: f.Redis.Timeout.Std(),
			breaker: resilience.NewBreaker(breakerCfg(f.Redis.Breaker), nil)}
		if f.FastPath.Enabled {
			fp := limiter.NewFastPath(direct, nil)
			if f.FastPath.LeaseTTL > 0 {
				fp.LeaseTTL = f.FastPath.LeaseTTL.Std()
			}
			if f.FastPath.MaxBatch > 0 {
				fp.MaxBatch = f.FastPath.MaxBatch
			}
			if f.FastPath.MaxDenyTTL > 0 {
				fp.MaxDenyTTL = f.FastPath.MaxDenyTTL.Std()
			}
			rt.fast = fp
		}
		s.redis = rt
		if old != nil {
			// Let in-flight requests finish on the old client, then close it.
			time.AfterFunc(30*time.Second, old.redis.client.Close)
		}
	}

	for name, bc := range f.Backends {
		if old != nil {
			if ob, ok := old.backends[name]; ok && ob.cfg == bc {
				s.backends[name] = ob
				continue
			}
		}
		b, err := g.newBackend(name, bc)
		if err != nil {
			g.mReloads.Inc("rejected")
			return err
		}
		s.backends[name] = b
	}

	for _, rc := range f.Routes {
		base := f.Defaults
		if rc.Limit != nil {
			base = rc.Limit.Merge(f.Defaults)
		}
		policy := f.OnRedisError
		if rc.OnRedisError != "" {
			policy = rc.OnRedisError
		}
		s.routes = append(s.routes, &routeRT{cfg: rc, backend: s.backends[rc.Backend], base: base, policy: policy})
	}
	sort.SliceStable(s.routes, func(i, j int) bool { return len(s.routes[i].cfg.Prefix) > len(s.routes[j].cfg.Prefix) })

	g.snap.Store(s)
	g.mReloads.Inc("applied")
	g.log.Info("config applied", "version", version, "routes", len(s.routes), "backends", len(s.backends))
	return nil
}

func (g *Gateway) newBackend(name string, bc config.Backend) (*backendRT, error) {
	target, err := url.Parse(bc.URL)
	if err != nil {
		return nil, fmt.Errorf("backend %q: %w", name, err)
	}
	tr := &http.Transport{
		Proxy:               nil,
		DialContext:         (&net.Dialer{Timeout: time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        bc.MaxConcurrent * 2,
		MaxIdleConnsPerHost: bc.MaxConcurrent,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true, // pass bytes through untouched
	}
	b := &backendRT{
		name:     name,
		cfg:      bc,
		bulkhead: resilience.NewBulkhead(bc.MaxConcurrent, bc.QueueWait.Std()),
		breaker:  resilience.NewBreaker(breakerCfg(bc.Breaker), nil),
	}
	b.breaker.OnTransition(func(from, to resilience.State) {
		g.log.Warn("circuit breaker transition", "backend", name, "from", from.String(), "to", to.String())
	})
	b.proxy = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.SetXForwarded()
		},
		Transport: tr,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				writeError(w, http.StatusGatewayTimeout, "backend timed out")
			case errors.Is(err, context.Canceled):
				// Client went away; nothing useful to write.
			default:
				g.log.Warn("backend error", "backend", name, "err", err)
				writeError(w, http.StatusBadGateway, "backend unavailable")
			}
		},
	}
	return b, nil
}

// ---------------------------------------------------------------------------
// Request handling
// ---------------------------------------------------------------------------

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func retryAfterSeconds(d time.Duration) string {
	s := int(math.Ceil(d.Seconds()))
	if s < 1 {
		s = 1
	}
	return strconv.Itoa(s)
}

// ServeHTTP implements http.Handler.
func (g *Gateway) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	start := time.Now()
	w := &statusWriter{ResponseWriter: rw}
	s := g.snap.Load()
	routeName := "none"
	defer func() {
		code := w.status
		if code == 0 {
			code = http.StatusOK
		}
		g.mRequests.Inc(routeName, strconv.Itoa(code))
		g.mLatency.Observe(time.Since(start).Seconds(), routeName)
	}()

	if s == nil {
		writeError(w, http.StatusServiceUnavailable, "gateway not configured")
		return
	}
	route := s.match(r.URL.Path)
	if route == nil {
		writeError(w, http.StatusNotFound, "no route")
		return
	}
	routeName = route.cfg.Name

	// Gateway-wide cap: the outermost protection against collapse.
	if max := int64(s.cfg.MaxInFlight); max > 0 {
		if g.inflight.Add(1) > max {
			g.inflight.Add(-1)
			g.shed(w, route, "gateway_overloaded")
			return
		}
	} else {
		g.inflight.Add(1)
	}
	defer g.inflight.Add(-1)

	identity, rule, ok := g.identify(s, route, r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "valid API key required")
		return
	}

	d, source, err := g.decide(r.Context(), s, route, identity, rule)
	if err != nil {
		// Only reachable for fail_closed: the limiter is down and policy says reject.
		g.shed(w, route, "limiter_unavailable")
		return
	}
	g.mDecisions.Inc(routeName, source, map[bool]string{true: "allowed", false: "denied"}[d.Allowed])
	if d.Limit > 0 {
		w.Header().Set("X-RateLimit-Limit", strconv.FormatInt(d.Limit, 10))
	}
	if d.Remaining >= 0 && source != "fail_open" {
		w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(d.Remaining, 10))
	}
	if !d.Allowed {
		w.Header().Set("Retry-After", retryAfterSeconds(d.RetryAfter))
		writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
		return
	}

	g.forward(w, r, route)
}

func (s *snapshot) match(path string) *routeRT {
	for _, rt := range s.routes {
		p := rt.cfg.Prefix
		if path == p || strings.HasPrefix(path, strings.TrimSuffix(p, "/")+"/") || (strings.HasSuffix(p, "/") && strings.HasPrefix(path, p)) {
			return rt
		}
	}
	return nil
}

// identify picks who the request is charged to and which limit applies.
// Precedence: per-key per-route override > per-key override > route > defaults.
func (g *Gateway) identify(s *snapshot, route *routeRT, r *http.Request) (string, limiter.Rule, bool) {
	rule := route.base
	key := r.Header.Get("X-API-Key")
	if kc, ok := s.cfg.APIKeys[key]; ok && key != "" {
		if kc.Limit != nil {
			rule = kc.Limit.Merge(rule)
		}
		if o := kc.Routes[route.cfg.Name]; o != nil {
			rule = o.Merge(rule)
		}
		return route.cfg.Name + ":key:" + kc.Name, rule.Rule(), true
	}
	if route.cfg.Auth == "required" {
		return "", limiter.Rule{}, false
	}
	// Unknown keys are charged to the client IP, not to the key string: otherwise
	// a caller could mint unlimited identities by sending random X-API-Key values.
	return route.cfg.Name + ":ip:" + clientIP(r, s.cfg.TrustXFF), rule.Rule(), true
}

func clientIP(r *http.Request, trustXFF bool) string {
	if trustXFF {
		// Behind an ALB the rightmost hop is the address the ALB itself saw;
		// anything to its left is client-supplied and spoofable.
		if xs := r.Header.Get("X-Forwarded-For"); xs != "" {
			parts := strings.Split(xs, ",")
			return strings.TrimSpace(parts[len(parts)-1])
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// decide asks the limiter; if Redis is unavailable it applies the route's
// outage policy. The returned error is non-nil only for fail_closed.
func (g *Gateway) decide(ctx context.Context, s *snapshot, route *routeRT, id string, rule limiter.Rule) (limiter.Decision, string, error) {
	rt := s.redis
	if rt.breaker.Acquire() == nil {
		cctx, cancel := context.WithTimeout(ctx, rt.timeout)
		var lim limiter.Limiter = rt.direct
		if rt.fast != nil {
			lim = rt.fast
		}
		d, err := lim.Allow(cctx, id, rule)
		cancel()
		if err == nil {
			rt.breaker.Success()
			return d, "redis", nil
		}
		if ctx.Err() != nil { // the client left; that says nothing about Redis
			rt.breaker.Cancel()
			return limiter.Decision{}, "", ctx.Err()
		}
		rt.breaker.Failure()
		g.mRedisErr.Inc("call")
	} else {
		g.mRedisErr.Inc("breaker_open")
	}

	// Redis is unavailable (error, timeout, or breaker open): apply policy.
	switch route.policy {
	case config.FailClosed:
		return limiter.Decision{}, "fail_closed", errors.New("limiter unavailable")
	case config.LocalFallback:
		div := int64(s.cfg.FallbackDivisor)
		fr := rule
		fr.Limit = max(1, rule.Limit/div)
		if fr.Burst > 0 {
			fr.Burst = max(1, fr.Burst/div)
		}
		d, _ := g.local.Allow(ctx, id, fr)
		return d, "local_fallback", nil
	default: // fail_open
		return limiter.Decision{Allowed: true, Limit: rule.Limit, Remaining: -1}, "fail_open", nil
	}
}

func (g *Gateway) shed(w http.ResponseWriter, route *routeRT, reason string) {
	g.mShed.Inc(route.cfg.Name, reason)
	w.Header().Set("Retry-After", "1")
	w.Header().Set("X-Shed-Reason", reason)
	writeError(w, http.StatusServiceUnavailable, "overloaded: "+reason)
}

func (g *Gateway) forward(w *statusWriter, r *http.Request, route *routeRT) {
	b := route.backend
	if err := b.breaker.Acquire(); err != nil {
		g.shed(w, route, "breaker_open")
		return
	}
	if err := b.bulkhead.Acquire(r.Context()); err != nil {
		b.breaker.Cancel() // shed locally: says nothing about backend health
		if errors.Is(err, resilience.ErrBulkheadFull) {
			g.shed(w, route, "bulkhead_full")
		}
		return
	}
	defer b.bulkhead.Release()

	ctx, cancel := context.WithTimeout(r.Context(), route.cfg.Timeout.Std())
	defer cancel()
	r = r.WithContext(ctx)
	if route.cfg.StripPrefix {
		r = r.Clone(ctx)
		r.URL.Path = "/" + strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, strings.TrimSuffix(route.cfg.Prefix, "/")), "/")
		r.URL.RawPath = ""
	}

	b.proxy.ServeHTTP(w, r)

	switch {
	case r.Context().Err() != nil && !errors.Is(r.Context().Err(), context.DeadlineExceeded):
		b.breaker.Cancel() // client cancelled
	case w.status >= 500 || errors.Is(r.Context().Err(), context.DeadlineExceeded):
		b.breaker.Failure()
	default:
		b.breaker.Success()
	}
}

// ---------------------------------------------------------------------------
// Admin
// ---------------------------------------------------------------------------

// AdminHandler serves /healthz, /readyz, /metrics and /admin/config.
func (g *Gateway) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		s := g.snap.Load()
		if s == nil {
			writeError(w, http.StatusServiceUnavailable, "no config loaded")
			return
		}
		// Redis being down does not make the gateway unready: the outage policy
		// handles it. We report it so operators can see it.
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ready", "config": s.version, "redis_breaker": s.redis.breaker.State().String()})
	})
	mux.Handle("/metrics", g.reg.Handler())
	mux.HandleFunc("/admin/config", func(w http.ResponseWriter, _ *http.Request) {
		s := g.snap.Load()
		if s == nil {
			writeError(w, http.StatusServiceUnavailable, "no config loaded")
			return
		}
		type routeView struct {
			Name, Prefix, Backend string
			Limit                 limiter.Rule
			Window                string
		}
		var rs []routeView
		for _, r := range s.routes {
			rs = append(rs, routeView{r.cfg.Name, r.cfg.Prefix, r.cfg.Backend, r.base.Rule(), r.base.Window.Std().String()})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"version": s.version, "routes": rs, "on_redis_error": s.cfg.OnRedisError})
	})
	return mux
}
