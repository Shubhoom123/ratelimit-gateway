// Package config parses, validates and hot-reloads the gateway configuration.
package config

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/shubhamkhalkho/ratelimit-gateway/internal/limiter"
)

// Duration is a time.Duration that reads as a string ("250ms") in JSON.
type Duration time.Duration

// UnmarshalJSON accepts "1s" style strings.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"250ms\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// MarshalJSON writes the string form.
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

// Std converts to time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// Redis outage policies.
const (
	FailOpen      = "fail_open"      // admit everything while Redis is unavailable
	FailClosed    = "fail_closed"    // reject everything (429/503)
	LocalFallback = "local_fallback" // enforce limit/FallbackDivisor per instance, in memory
)

// File is the top-level config document.
type File struct {
	Redis           RedisCfg           `json:"redis"`
	Defaults        LimitCfg           `json:"defaults"`
	OnRedisError    string             `json:"on_redis_error"`   // default local_fallback
	FallbackDivisor int                `json:"fallback_divisor"` // expected instance count for local_fallback (default 2)
	FastPath        FastPathCfg        `json:"fast_path"`
	MaxInFlight     int                `json:"max_in_flight"` // gateway-wide cap, 0 = unlimited
	TrustXFF        bool               `json:"trust_xff"`     // use the last X-Forwarded-For hop as client IP (set behind an ALB)
	Backends        map[string]Backend `json:"backends"`
	Routes          []Route            `json:"routes"`
	APIKeys         map[string]KeyCfg  `json:"api_keys"`
}

// RedisCfg locates Redis and bounds how long we wait for it.
type RedisCfg struct {
	Addr     string   `json:"addr"`
	Prefix   string   `json:"prefix"` // key namespace; lets environments or tenants share one Redis (default "rl")
	PoolSize int      `json:"pool_size"`
	Timeout  Duration `json:"timeout"` // per limiter call; default 25ms
	Breaker  Breaker  `json:"breaker"`
}

// FastPathCfg switches the local lease/deny cache on.
type FastPathCfg struct {
	Enabled    bool     `json:"enabled"`
	LeaseTTL   Duration `json:"lease_ttl"`
	MaxBatch   int64    `json:"max_batch"`
	MaxDenyTTL Duration `json:"max_deny_ttl"`
}

// LimitCfg is a (possibly partial) rate limit. Zero fields inherit.
type LimitCfg struct {
	Algorithm string   `json:"algorithm"`
	Limit     int64    `json:"limit"`
	Window    Duration `json:"window"`
	Burst     int64    `json:"burst"`
}

// Breaker mirrors resilience.BreakerConfig in file form.
type Breaker struct {
	Window       int      `json:"window"`
	MinRequests  int      `json:"min_requests"`
	FailureRatio float64  `json:"failure_ratio"`
	OpenFor      Duration `json:"open_for"`
	Probes       int      `json:"probes"`
}

// Backend is one upstream service.
type Backend struct {
	URL           string   `json:"url"`
	MaxConcurrent int      `json:"max_concurrent"`
	QueueWait     Duration `json:"queue_wait"`
	Breaker       Breaker  `json:"breaker"`
}

// Route maps a path prefix to a backend.
type Route struct {
	Name         string    `json:"name"`
	Prefix       string    `json:"prefix"`
	Backend      string    `json:"backend"`
	Limit        *LimitCfg `json:"limit"`
	Timeout      Duration  `json:"timeout"`        // whole-request deadline, default 1s
	Auth         string    `json:"auth"`           // "optional" (default) or "required"
	OnRedisError string    `json:"on_redis_error"` // overrides the global policy for this route
	StripPrefix  bool      `json:"strip_prefix"`
}

// KeyCfg overrides limits for one API key.
type KeyCfg struct {
	Name   string               `json:"name"`
	Limit  *LimitCfg            `json:"limit"`  // applies to every route
	Routes map[string]*LimitCfg `json:"routes"` // per-route override (by route name); wins over Limit
}

// Merge returns base with every non-zero field of o applied on top.
func (o LimitCfg) Merge(base LimitCfg) LimitCfg {
	if o.Algorithm != "" {
		base.Algorithm = o.Algorithm
	}
	if o.Limit != 0 {
		base.Limit = o.Limit
	}
	if o.Window != 0 {
		base.Window = o.Window
	}
	if o.Burst != 0 {
		base.Burst = o.Burst
	}
	return base
}

// Rule converts to a limiter.Rule.
func (l LimitCfg) Rule() limiter.Rule {
	return limiter.Rule{Algorithm: l.Algorithm, Limit: l.Limit, Window: l.Window.Std(), Burst: l.Burst}
}

// Parse decodes and validates a config document. Unknown fields are errors so
// a typo like "limt" fails the reload instead of silently doing nothing.
func Parse(data []byte) (*File, error) {
	var f File
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	f.applyDefaults()
	if err := f.Validate(); err != nil {
		return nil, err
	}
	return &f, nil
}

func (f *File) applyDefaults() {
	if f.Redis.Prefix == "" {
		f.Redis.Prefix = "rl"
	}
	if f.Redis.Timeout == 0 {
		f.Redis.Timeout = Duration(25 * time.Millisecond)
	}
	if f.OnRedisError == "" {
		f.OnRedisError = LocalFallback
	}
	if f.FallbackDivisor <= 0 {
		f.FallbackDivisor = 2
	}
	if f.Defaults.Algorithm == "" {
		f.Defaults.Algorithm = limiter.TokenBucket
	}
	if f.Defaults.Window == 0 {
		f.Defaults.Window = Duration(time.Second)
	}
	for i := range f.Routes {
		r := &f.Routes[i]
		if r.Timeout == 0 {
			r.Timeout = Duration(time.Second)
		}
		if r.Auth == "" {
			r.Auth = "optional"
		}
		if r.Name == "" {
			r.Name = strings.Trim(strings.ReplaceAll(r.Prefix, "/", "_"), "_")
		}
	}
	for n, b := range f.Backends {
		if b.MaxConcurrent == 0 {
			b.MaxConcurrent = 100
		}
		if b.QueueWait == 0 {
			b.QueueWait = Duration(25 * time.Millisecond)
		}
		f.Backends[n] = b
	}
}

func validPolicy(p string) bool {
	return p == "" || p == FailOpen || p == FailClosed || p == LocalFallback
}

// Validate checks cross-references and values.
func (f *File) Validate() error {
	var errs []error
	if f.Redis.Addr == "" {
		errs = append(errs, errors.New("redis.addr is required"))
	}
	if !validPolicy(f.OnRedisError) {
		errs = append(errs, fmt.Errorf("on_redis_error %q invalid", f.OnRedisError))
	}
	if err := f.Defaults.Rule().Validate(); err != nil {
		errs = append(errs, fmt.Errorf("defaults: %w", err))
	}
	if len(f.Routes) == 0 {
		errs = append(errs, errors.New("at least one route is required"))
	}
	seen := map[string]bool{}
	for _, r := range f.Routes {
		if !strings.HasPrefix(r.Prefix, "/") {
			errs = append(errs, fmt.Errorf("route %q: prefix must start with /", r.Name))
		}
		if seen[r.Name] {
			errs = append(errs, fmt.Errorf("duplicate route name %q", r.Name))
		}
		seen[r.Name] = true
		if _, ok := f.Backends[r.Backend]; !ok {
			errs = append(errs, fmt.Errorf("route %q: unknown backend %q", r.Name, r.Backend))
		}
		if r.Auth != "optional" && r.Auth != "required" {
			errs = append(errs, fmt.Errorf("route %q: auth must be optional or required", r.Name))
		}
		if !validPolicy(r.OnRedisError) {
			errs = append(errs, fmt.Errorf("route %q: on_redis_error %q invalid", r.Name, r.OnRedisError))
		}
		if r.Limit != nil {
			if err := r.Limit.Merge(f.Defaults).Rule().Validate(); err != nil {
				errs = append(errs, fmt.Errorf("route %q: %w", r.Name, err))
			}
		}
	}
	for n, b := range f.Backends {
		if !strings.HasPrefix(b.URL, "http://") && !strings.HasPrefix(b.URL, "https://") {
			errs = append(errs, fmt.Errorf("backend %q: url must be http(s)", n))
		}
	}
	for k, kc := range f.APIKeys {
		if kc.Limit != nil {
			if err := kc.Limit.Merge(f.Defaults).Rule().Validate(); err != nil {
				errs = append(errs, fmt.Errorf("api key %q: %w", kc.Name, err))
			}
		}
		for rn, l := range kc.Routes {
			if !seen[rn] {
				errs = append(errs, fmt.Errorf("api key %q: unknown route %q", k, rn))
				continue
			}
			if err := l.Merge(f.Defaults).Rule().Validate(); err != nil {
				errs = append(errs, fmt.Errorf("api key %q route %q: %w", k, rn, err))
			}
		}
	}
	return errors.Join(errs...)
}

// ---------------------------------------------------------------------------
// Sources and watching
// ---------------------------------------------------------------------------

// Source loads raw config bytes.
type Source interface {
	Load(ctx context.Context) ([]byte, error)
	String() string
}

// FileSource reads a local file.
type FileSource string

// Load implements Source.
func (s FileSource) Load(context.Context) ([]byte, error) { return os.ReadFile(string(s)) }
func (s FileSource) String() string                       { return string(s) }

// HTTPSource GETs a URL (an internal config service, a pre-signed object URL, ...).
type HTTPSource struct {
	URL    string
	Client *http.Client
}

// Load implements Source.
func (s HTTPSource) Load(ctx context.Context) ([]byte, error) {
	c := s.Client
	if c == nil {
		c = &http.Client{Timeout: 5 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("config source returned %s", resp.Status)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 4<<20))
}
func (s HTTPSource) String() string { return s.URL }

// NewSource picks a source from a flag value: http(s):// URLs poll over HTTP,
// anything else is a file path.
func NewSource(spec string) Source {
	if strings.HasPrefix(spec, "http://") || strings.HasPrefix(spec, "https://") {
		return HTTPSource{URL: spec}
	}
	return FileSource(spec)
}

// Watcher polls a source and calls apply for each *new, valid* document. An
// invalid document is reported to onError and the previous config stays live:
// a bad edit must never take the gateway down.
type Watcher struct {
	Src      Source
	Interval time.Duration
	Apply    func(*File, string) error // version is a short content hash
	OnError  func(error)
	last     string
	trigger  chan struct{}
}

// NewWatcher builds a watcher.
func NewWatcher(src Source, interval time.Duration, apply func(*File, string) error, onErr func(error)) *Watcher {
	return &Watcher{Src: src, Interval: interval, Apply: apply, OnError: onErr, trigger: make(chan struct{}, 1)}
}

// Reload requests an immediate check (wired to SIGHUP).
func (w *Watcher) Reload() {
	select {
	case w.trigger <- struct{}{}:
	default:
	}
}

// Check loads once; returns whether a new config was applied.
func (w *Watcher) Check(ctx context.Context) (bool, error) {
	data, err := w.Src.Load(ctx)
	if err != nil {
		return false, err
	}
	sum := sha256.Sum256(data)
	ver := hex.EncodeToString(sum[:])[:12]
	if ver == w.last {
		return false, nil
	}
	f, err := Parse(data)
	if err != nil {
		w.last = ver // don't re-report the same bad document every tick
		return false, err
	}
	if err := w.Apply(f, ver); err != nil {
		return false, err
	}
	w.last = ver
	return true, nil
}

// Run checks immediately and then on every tick or Reload until ctx ends.
func (w *Watcher) Run(ctx context.Context) {
	t := time.NewTicker(w.Interval)
	defer t.Stop()
	for {
		if _, err := w.Check(ctx); err != nil && w.OnError != nil {
			w.OnError(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-w.trigger:
		}
	}
}
