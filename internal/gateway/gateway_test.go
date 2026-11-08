package gateway

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubhamkhalkho/ratelimit-gateway/internal/config"
	"github.com/shubhamkhalkho/ratelimit-gateway/internal/metrics"
	"github.com/shubhamkhalkho/ratelimit-gateway/internal/redisc"
)

func redisAddr(t testing.TB) string {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	c := redisc.New(redisc.Options{Addr: addr})
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.Do(ctx, "PING"); err != nil {
		t.Skipf("redis not reachable at %s: %v", addr, err)
	}
	return addr
}

type harness struct {
	gw  *Gateway
	reg *metrics.Registry
	srv *httptest.Server
}

func newHarness(t *testing.T, cfgJSON string) *harness {
	t.Helper()
	reg := metrics.NewRegistry()
	gw := New(slog.New(slog.NewTextHandler(io.Discard, nil)), reg)
	f, err := config.Parse([]byte(cfgJSON))
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	if err := gw.Reload(f, "t1"); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(gw)
	t.Cleanup(srv.Close)
	return &harness{gw: gw, reg: reg, srv: srv}
}

func (h *harness) get(path string, headers ...string) *http.Response {
	req, _ := http.NewRequest("GET", h.srv.URL+path, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
}

func codes(h *harness, n int, path string, headers ...string) map[int]int {
	out := map[int]int{}
	for i := 0; i < n; i++ {
		out[h.get(path, headers...).StatusCode]++
	}
	return out
}

func uniqueRoute(t *testing.T) string {
	return fmt.Sprintf("/%s-%d", strings.ToLower(t.Name()), time.Now().UnixNano())
}

type cfgOpts struct {
	Redis, Backend, Prefix, Limit, Algorithm, Policy, Extra, BackendExtra, RouteExtra string
	Burst                                                                             int
}

func cfg(o cfgOpts) string {
	if o.Algorithm == "" {
		o.Algorithm = "sliding_window_log"
	}
	if o.Policy == "" {
		o.Policy = "local_fallback"
	}
	if o.Limit == "" {
		o.Limit = "10"
	}
	if o.BackendExtra != "" {
		o.BackendExtra = "," + o.BackendExtra
	}
	if o.RouteExtra != "" {
		o.RouteExtra = "," + o.RouteExtra
	}
	return fmt.Sprintf(`{
	  "redis": {"addr": %q, "prefix": "t%d", "timeout": "50ms", "breaker": {"window": 10, "min_requests": 5, "failure_ratio": 0.5, "open_for": "300ms", "probes": 1}},
	  "defaults": {"algorithm": %q, "limit": %s, "window": "1m", "burst": %d},
	  "on_redis_error": %q, "fallback_divisor": 2
	  %s,
	  "backends": {"svc": {"url": %q %s}},
	  "routes": [{"name": "r", "prefix": %q, "backend": "svc"%s}]
	}`, o.Redis, time.Now().UnixNano(), o.Algorithm, o.Limit, o.Burst, o.Policy, o.Extra, o.Backend, o.BackendExtra, o.Prefix, o.RouteExtra)
}

func okBackend(t *testing.T) *httptest.Server {
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "ok") }))
	t.Cleanup(b.Close)
	return b
}

func TestEnforcesLimitAndSetsHeaders(t *testing.T) {
	addr := redisAddr(t)
	be := okBackend(t)
	p := uniqueRoute(t)
	h := newHarness(t, cfg(cfgOpts{Redis: addr, Backend: be.URL, Prefix: p, Limit: "5"}))

	got := codes(h, 12, p+"/x")
	if got[200] != 5 || got[429] != 7 {
		t.Fatalf("want 5x200 and 7x429, got %v", got)
	}
	resp := h.get(p + "/x")
	if resp.StatusCode != 429 || resp.Header.Get("Retry-After") == "" || resp.Header.Get("X-RateLimit-Limit") != "5" {
		t.Fatalf("bad 429 response: %d %v", resp.StatusCode, resp.Header)
	}
}

func TestPerAPIKeyOverrideAndIsolation(t *testing.T) {
	addr := redisAddr(t)
	be := okBackend(t)
	p := uniqueRoute(t)
	name := fmt.Sprintf("acme%d", time.Now().UnixNano())
	h := newHarness(t, cfg(cfgOpts{Redis: addr, Backend: be.URL, Prefix: p, Limit: "3",
		Extra: fmt.Sprintf(`, "api_keys": {"k-premium": {"name": %q, "limit": {"limit": 20}}}`, name)}))

	if got := codes(h, 10, p, "X-API-Key", "k-premium"); got[200] != 10 {
		t.Fatalf("premium key should have a limit of 20, got %v", got)
	}
	if got := codes(h, 10, p); got[200] != 3 {
		t.Fatalf("anonymous should be capped at 3, got %v", got)
	}
	// A random key must not mint a fresh identity.
	if got := codes(h, 5, p, "X-API-Key", "made-up"); got[200] != 0 {
		t.Fatalf("unknown keys share the caller's IP bucket, which is already exhausted: %v", got)
	}
}

func TestRequiredAuthRejectsUnknownKey(t *testing.T) {
	addr := redisAddr(t)
	be := okBackend(t)
	p := uniqueRoute(t)
	h := newHarness(t, cfg(cfgOpts{Redis: addr, Backend: be.URL, Prefix: p, RouteExtra: `"auth": "required"`}))
	if c := h.get(p).StatusCode; c != 401 {
		t.Fatalf("want 401, got %d", c)
	}
}

func TestHotReloadChangesLimitWithoutRestart(t *testing.T) {
	addr := redisAddr(t)
	be := okBackend(t)
	p := uniqueRoute(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	write := func(limit string) {
		if err := os.WriteFile(path, []byte(cfg(cfgOpts{Redis: addr, Backend: be.URL, Prefix: p, Limit: limit})), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("3")

	reg := metrics.NewRegistry()
	gw := New(slog.New(slog.NewTextHandler(io.Discard, nil)), reg)
	w := config.NewWatcher(config.FileSource(path), time.Hour, gw.Reload, nil)
	if _, err := w.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(gw)
	defer srv.Close()
	h := &harness{gw: gw, reg: reg, srv: srv}

	if got := codes(h, 10, p); got[200] != 3 {
		t.Fatalf("before reload: %v", got)
	}

	// An invalid edit is rejected and the old config keeps serving.
	os.WriteFile(path, []byte(`{"redis": {"addr": "x"}, "defalts": {}}`), 0o644)
	if applied, err := w.Check(context.Background()); applied || err == nil {
		t.Fatalf("invalid config must be rejected, applied=%v err=%v", applied, err)
	}
	if c := h.get(p).StatusCode; c != 429 {
		t.Fatalf("old config should still be live (429), got %d", c)
	}

	// A valid edit takes effect with no restart. Same identity, higher limit.
	write("1000")
	if applied, err := w.Check(context.Background()); !applied || err != nil {
		t.Fatalf("valid config should apply: applied=%v err=%v", applied, err)
	}
	if got := codes(h, 10, p); got[200] != 10 {
		t.Fatalf("after reload the new limit should apply: %v", got)
	}
}

func TestReloadKeepsBreakerState(t *testing.T) {
	addr := redisAddr(t)
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
	defer bad.Close()
	p := uniqueRoute(t)
	br := `"breaker": {"window": 10, "min_requests": 5, "failure_ratio": 0.5, "open_for": "1h", "probes": 1}`
	h := newHarness(t, cfg(cfgOpts{Redis: addr, Backend: bad.URL, Prefix: p, Limit: "1000", BackendExtra: br}))
	codes(h, 10, p)
	if c := h.get(p).StatusCode; c != 503 {
		t.Fatalf("breaker should be open, got %d", c)
	}
	f, _ := config.Parse([]byte(cfg(cfgOpts{Redis: addr, Backend: bad.URL, Prefix: p, Limit: "2000", BackendExtra: br})))
	if err := h.gw.Reload(f, "t2"); err != nil {
		t.Fatal(err)
	}
	if c := h.get(p).StatusCode; c != 503 {
		t.Fatalf("a reload that only changes a limit must not reset an open breaker, got %d", c)
	}
}

// ---------------------------------------------------------------------------
// Redis outage policy
// ---------------------------------------------------------------------------

const deadRedis = "127.0.0.1:1" // connection refused immediately

func TestRedisDownFailOpenAdmitsEverything(t *testing.T) {
	be := okBackend(t)
	p := uniqueRoute(t)
	h := newHarness(t, cfg(cfgOpts{Redis: deadRedis, Backend: be.URL, Prefix: p, Limit: "3", Policy: "fail_open"}))
	if got := codes(h, 50, p); got[200] != 50 {
		t.Fatalf("fail_open should admit all: %v", got)
	}
}

func TestRedisDownFailClosedRejectsWith503(t *testing.T) {
	be := okBackend(t)
	p := uniqueRoute(t)
	h := newHarness(t, cfg(cfgOpts{Redis: deadRedis, Backend: be.URL, Prefix: p, Limit: "1000", Policy: "fail_closed"}))
	resp := h.get(p)
	if resp.StatusCode != 503 || resp.Header.Get("X-Shed-Reason") != "limiter_unavailable" {
		t.Fatalf("want 503 limiter_unavailable, got %d %v", resp.StatusCode, resp.Header)
	}
}

func TestRedisDownLocalFallbackEnforcesPerInstanceShare(t *testing.T) {
	be := okBackend(t)
	p := uniqueRoute(t)
	// limit 10, fallback_divisor 2 => 5 per instance.
	h := newHarness(t, cfg(cfgOpts{Redis: deadRedis, Backend: be.URL, Prefix: p, Limit: "10", Policy: "local_fallback", Algorithm: "token_bucket", Burst: 10}))
	got := codes(h, 30, p)
	if got[200] < 5 || got[200] > 6 {
		t.Fatalf("local fallback should admit ~5 (limit/divisor), got %v", got)
	}
}

func TestRedisBreakerStopsPayingTimeoutAndRecovers(t *testing.T) {
	// A listener that accepts and never answers makes every call hit the 50ms
	// timeout, the expensive failure mode a breaker exists to avoid.
	ln := newBlackhole(t)
	be := okBackend(t)
	p := uniqueRoute(t)
	h := newHarness(t, cfg(cfgOpts{Redis: ln.Addr().String(), Backend: be.URL, Prefix: p, Limit: "1000", Policy: "fail_open"}))

	for i := 0; i < 8; i++ { // first calls time out and trip the breaker
		h.get(p)
	}
	start := time.Now()
	for i := 0; i < 50; i++ {
		if c := h.get(p).StatusCode; c != 200 {
			t.Fatalf("fail_open should keep serving, got %d", c)
		}
	}
	if d := time.Since(start); d > 400*time.Millisecond {
		t.Fatalf("with the breaker open 50 requests should be fast, took %v", d)
	}
	if v := h.gw.mRedisErr.Value("breaker_open"); v == 0 {
		t.Fatal("expected breaker_open to be counted")
	}
}

// newBlackhole listens and accepts connections but never replies.
func newBlackhole(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		for _, c := range conns {
			c.Close()
		}
		mu.Unlock()
	})
	return ln
}

// ---------------------------------------------------------------------------
// Backend protection
// ---------------------------------------------------------------------------

func TestSlowBackendIsShedByBulkheadNotQueuedForever(t *testing.T) {
	addr := redisAddr(t)
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer slow.Close()
	p := uniqueRoute(t)
	h := newHarness(t, cfg(cfgOpts{Redis: addr, Backend: slow.URL, Prefix: p, Limit: "100000",
		BackendExtra: `"max_concurrent": 4, "queue_wait": "10ms"`, RouteExtra: `"timeout": "2s"`}))

	var wg sync.WaitGroup
	var mu sync.Mutex
	got := map[int]int{}
	var maxShedLatency time.Duration
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st := time.Now()
			c := h.get(p).StatusCode
			mu.Lock()
			got[c]++
			if c == 503 && time.Since(st) > maxShedLatency {
				maxShedLatency = time.Since(st)
			}
			mu.Unlock()
		}()
		time.Sleep(time.Millisecond)
	}
	// Wait for sheds to finish; the 4 admitted requests stay stuck on the slow backend.
	time.Sleep(300 * time.Millisecond)
	if v := h.gw.mShed.Value("r", "bulkhead_full"); v < 30 {
		t.Fatalf("expected most of the 40 requests shed by the bulkhead, got %d (codes so far %v)", v, got)
	}
	mu.Lock()
	ml := maxShedLatency
	mu.Unlock()
	if ml > time.Second {
		t.Fatalf("shed responses should be fast, slowest was %v", ml)
	}
	close(release) // unblock the admitted requests so the test can finish
	wg.Wait()
}

func TestRequestTimeoutReturns504(t *testing.T) {
	addr := redisAddr(t)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer slow.Close()
	p := uniqueRoute(t)
	h := newHarness(t, cfg(cfgOpts{Redis: addr, Backend: slow.URL, Prefix: p, Limit: "1000", RouteExtra: `"timeout": "80ms"`}))
	start := time.Now()
	c := h.get(p).StatusCode
	if c != 504 {
		t.Fatalf("want 504, got %d", c)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("timeout should fire near 80ms, took %v", d)
	}
}

func TestBackendBreakerOpensThenRecovers(t *testing.T) {
	addr := redisAddr(t)
	var failing atomic.Bool
	failing.Store(true)
	var hits atomic.Int64
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if failing.Load() {
			w.WriteHeader(500)
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer be.Close()
	p := uniqueRoute(t)
	br := `"breaker": {"window": 10, "min_requests": 5, "failure_ratio": 0.5, "open_for": "200ms", "probes": 2}`
	h := newHarness(t, cfg(cfgOpts{Redis: addr, Backend: be.URL, Prefix: p, Limit: "100000", BackendExtra: br}))

	for i := 0; i < 20; i++ {
		h.get(p)
	}
	hitsAtTrip := hits.Load()
	if hitsAtTrip > 6 {
		t.Fatalf("breaker should open after ~5 failures; backend saw %d calls", hitsAtTrip)
	}
	resp := h.get(p)
	if resp.StatusCode != 503 || resp.Header.Get("X-Shed-Reason") != "breaker_open" {
		t.Fatalf("want fast 503 breaker_open, got %d %v", resp.StatusCode, resp.Header)
	}
	if hits.Load() != hitsAtTrip {
		t.Fatal("an open breaker must not reach the backend")
	}

	failing.Store(false)
	time.Sleep(300 * time.Millisecond)
	for i := 0; i < 4; i++ {
		h.get(p)
	}
	if c := h.get(p).StatusCode; c != 200 {
		t.Fatalf("breaker should have closed after healthy probes, got %d", c)
	}
}

func TestGatewayWideInFlightCap(t *testing.T) {
	addr := redisAddr(t)
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer slow.Close()
	p := uniqueRoute(t)
	h := newHarness(t, cfg(cfgOpts{Redis: addr, Backend: slow.URL, Prefix: p, Limit: "100000", Extra: `, "max_in_flight": 3`,
		BackendExtra: `"max_concurrent": 100`, RouteExtra: `"timeout": "3s"`}))

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); h.get(p) }()
	}
	time.Sleep(100 * time.Millisecond)
	resp := h.get(p)
	if resp.StatusCode != 503 || resp.Header.Get("X-Shed-Reason") != "gateway_overloaded" {
		t.Fatalf("4th concurrent request should be shed, got %d %v", resp.StatusCode, resp.Header)
	}
	close(release)
	wg.Wait()
}

func TestUnknownRoute404(t *testing.T) {
	addr := redisAddr(t)
	be := okBackend(t)
	h := newHarness(t, cfg(cfgOpts{Redis: addr, Backend: be.URL, Prefix: uniqueRoute(t)}))
	if c := h.get("/nope").StatusCode; c != 404 {
		t.Fatalf("want 404, got %d", c)
	}
}
