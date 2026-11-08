package config

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// valid is a minimal config that passes validation; tests mutate copies of it.
const valid = `{
  "redis": {"addr": "127.0.0.1:6379"},
  "defaults": {"limit": 100},
  "backends": {"svc": {"url": "http://127.0.0.1:9001"}},
  "routes": [{"prefix": "/api", "backend": "svc"}]
}`

func mustParse(t *testing.T, doc string) *File {
	t.Helper()
	f, err := Parse([]byte(doc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return f
}

func TestParseAppliesDefaults(t *testing.T) {
	f := mustParse(t, valid)
	if f.Redis.Prefix != "rl" {
		t.Errorf("redis prefix = %q, want rl", f.Redis.Prefix)
	}
	if f.Redis.Timeout.Std() != 25*time.Millisecond {
		t.Errorf("redis timeout = %v, want 25ms", f.Redis.Timeout.Std())
	}
	if f.OnRedisError != LocalFallback {
		t.Errorf("on_redis_error = %q, want %q", f.OnRedisError, LocalFallback)
	}
	if f.FallbackDivisor != 2 {
		t.Errorf("fallback_divisor = %d, want 2", f.FallbackDivisor)
	}
	if f.Defaults.Algorithm != "token_bucket" || f.Defaults.Window.Std() != time.Second {
		t.Errorf("defaults = %+v", f.Defaults)
	}
	r := f.Routes[0]
	if r.Name != "api" {
		t.Errorf("route name derived from prefix = %q, want api", r.Name)
	}
	if r.Timeout.Std() != time.Second || r.Auth != "optional" {
		t.Errorf("route defaults = timeout %v auth %q", r.Timeout.Std(), r.Auth)
	}
	b := f.Backends["svc"]
	if b.MaxConcurrent != 100 || b.QueueWait.Std() != 25*time.Millisecond {
		t.Errorf("backend defaults = %+v", b)
	}
}

func TestParseDerivesRouteNameFromNestedPrefix(t *testing.T) {
	doc := strings.Replace(valid, `"/api"`, `"/v1/users/"`, 1)
	f := mustParse(t, doc)
	if got := f.Routes[0].Name; got != "v1_users" {
		t.Errorf("name = %q, want v1_users", got)
	}
}

func TestParseRejectsBadDocuments(t *testing.T) {
	cases := []struct {
		name    string
		doc     string
		wantErr string
	}{
		{"unknown field (typo)", strings.Replace(valid, `"limit": 100`, `"limt": 100`, 1), "unknown field"},
		{"not json", `{`, "config:"},
		{"duration as number", strings.Replace(valid, `"redis": {"addr": "127.0.0.1:6379"}`, `"redis": {"addr": "x:1", "timeout": 25}`, 1), "duration must be a string"},
		{"bad duration string", strings.Replace(valid, `"redis": {"addr": "127.0.0.1:6379"}`, `"redis": {"addr": "x:1", "timeout": "soon"}`, 1), "invalid duration"},
		{"missing redis addr", strings.Replace(valid, `"addr": "127.0.0.1:6379"`, `"addr": ""`, 1), "redis.addr is required"},
		{"bad global policy", strings.Replace(valid, `"defaults"`, `"on_redis_error": "panic", "defaults"`, 1), "on_redis_error"},
		{"no routes", `{"redis":{"addr":"x:1"},"defaults":{"limit":1},"backends":{}}`, "at least one route"},
		{"unknown backend", strings.Replace(valid, `"backend": "svc"`, `"backend": "nope"`, 1), "unknown backend"},
		{"prefix without slash", strings.Replace(valid, `"/api"`, `"api"`, 1), "prefix must start with /"},
		{"backend url without scheme", strings.Replace(valid, `http://127.0.0.1:9001`, `127.0.0.1:9001`, 1), "url must be http(s)"},
		{"zero default limit", strings.Replace(valid, `"limit": 100`, `"limit": 0`, 1), "limit must be > 0"},
		{"unknown algorithm", strings.Replace(valid, `"defaults": {`, `"defaults": {"algorithm": "leaky", `, 1), "unknown algorithm"},
		{"bad route auth", strings.Replace(valid, `"backend": "svc"}`, `"backend": "svc", "auth": "maybe"}`, 1), "auth must be"},
		{"bad route policy", strings.Replace(valid, `"backend": "svc"}`, `"backend": "svc", "on_redis_error": "shrug"}`, 1), "on_redis_error"},
		{"route limit invalid after merge", strings.Replace(valid, `"backend": "svc"}`, `"backend": "svc", "limit": {"algorithm": "nope"}}`, 1), "unknown algorithm"},
		{"duplicate route names", strings.Replace(valid, `"routes": [{"prefix": "/api", "backend": "svc"}]`,
			`"routes": [{"name":"a","prefix": "/a", "backend": "svc"},{"name":"a","prefix": "/b", "backend": "svc"}]`, 1), "duplicate route name"},
		{"api key unknown route", strings.Replace(valid, `"routes":`, `"api_keys": {"k1": {"name":"k1","routes": {"ghost": {"limit": 5}}}}, "routes":`, 1), "unknown route"},
		{"api key invalid limit", strings.Replace(valid, `"routes":`, `"api_keys": {"k1": {"name":"k1","limit": {"algorithm": "bogus"}}}, "routes":`, 1), "unknown algorithm"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.doc))
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateReportsEveryProblemAtOnce(t *testing.T) {
	doc := `{"redis":{"addr":""},"defaults":{"limit":0},"backends":{"b":{"url":"nope"}},"routes":[{"prefix":"x","backend":"zzz"}]}`
	_, err := Parse([]byte(doc))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"redis.addr", "limit must be > 0", "prefix must start", "unknown backend", "url must be http(s)"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("joined error missing %q:\n%v", want, err)
		}
	}
}

func TestMergeOverridesOnlyNonZeroFields(t *testing.T) {
	base := LimitCfg{Algorithm: "token_bucket", Limit: 100, Window: Duration(time.Second), Burst: 100}
	got := LimitCfg{Limit: 5}.Merge(base)
	if got.Limit != 5 || got.Algorithm != "token_bucket" || got.Window != base.Window || got.Burst != 100 {
		t.Errorf("merge = %+v", got)
	}
	got = LimitCfg{Algorithm: "sliding_window_log", Window: Duration(2 * time.Second), Burst: 7}.Merge(base)
	if got.Algorithm != "sliding_window_log" || got.Window.Std() != 2*time.Second || got.Burst != 7 || got.Limit != 100 {
		t.Errorf("merge = %+v", got)
	}
}

func TestRuleConversion(t *testing.T) {
	r := LimitCfg{Algorithm: "token_bucket", Limit: 10, Window: Duration(500 * time.Millisecond), Burst: 20}.Rule()
	if r.Algorithm != "token_bucket" || r.Limit != 10 || r.Window != 500*time.Millisecond || r.Burst != 20 {
		t.Errorf("rule = %+v", r)
	}
}

func TestDurationJSONRoundTrip(t *testing.T) {
	b, err := Duration(1500 * time.Millisecond).MarshalJSON()
	if err != nil || string(b) != `"1.5s"` {
		t.Fatalf("marshal = %s, %v", b, err)
	}
	var d Duration
	if err := d.UnmarshalJSON(b); err != nil || d.Std() != 1500*time.Millisecond {
		t.Fatalf("unmarshal = %v, %v", d, err)
	}
}

func TestAPIKeyRouteOverrideValidates(t *testing.T) {
	doc := strings.Replace(valid, `"routes":`,
		`"api_keys": {"k1": {"name":"gold","limit": {"limit": 1000}, "routes": {"api": {"limit": 5, "algorithm": "sliding_window_counter"}}}}, "routes":`, 1)
	f := mustParse(t, doc)
	kc := f.APIKeys["k1"]
	if kc.Limit.Limit != 1000 || kc.Routes["api"].Algorithm != "sliding_window_counter" {
		t.Errorf("api key cfg = %+v", kc)
	}
}

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

func TestNewSourcePicksByScheme(t *testing.T) {
	if _, ok := NewSource("http://x/cfg").(HTTPSource); !ok {
		t.Error("http:// should give HTTPSource")
	}
	if _, ok := NewSource("https://x/cfg").(HTTPSource); !ok {
		t.Error("https:// should give HTTPSource")
	}
	if _, ok := NewSource("/etc/gw/config.json").(FileSource); !ok {
		t.Error("path should give FileSource")
	}
}

func TestFileSourceMissingFile(t *testing.T) {
	if _, err := FileSource(filepath.Join(t.TempDir(), "nope.json")).Load(context.Background()); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestHTTPSource(t *testing.T) {
	var status atomic.Int32
	status.Store(200)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s := int(status.Load()); s != 200 {
			http.Error(w, "boom", s)
			return
		}
		w.Write([]byte(valid))
	}))
	defer srv.Close()

	src := NewSource(srv.URL)
	data, err := src.Load(context.Background())
	if err != nil || string(data) != valid {
		t.Fatalf("Load = %q, %v", data, err)
	}
	status.Store(500)
	if _, err := src.Load(context.Background()); err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("expected 500 error, got %v", err)
	}
}

func TestHTTPSourceHonoursContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := (HTTPSource{URL: srv.URL}).Load(ctx); err == nil {
		t.Fatal("expected context error")
	}
	if time.Since(start) > time.Second {
		t.Fatal("Load ignored the context deadline")
	}
}

// ---------------------------------------------------------------------------
// Watcher
// ---------------------------------------------------------------------------

// memSource is a Source whose content tests swap at will.
type memSource struct {
	mu   sync.Mutex
	data string
	err  error
}

func (m *memSource) set(d string)   { m.mu.Lock(); m.data = d; m.err = nil; m.mu.Unlock() }
func (m *memSource) fail(e error)   { m.mu.Lock(); m.err = e; m.mu.Unlock() }
func (m *memSource) String() string { return "mem" }
func (m *memSource) Load(context.Context) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return []byte(m.data), m.err
}

type applied struct {
	mu       sync.Mutex
	versions []string
	failNext bool
}

func (a *applied) apply(_ *File, v string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failNext {
		a.failNext = false
		return errors.New("apply failed")
	}
	a.versions = append(a.versions, v)
	return nil
}
func (a *applied) count() int { a.mu.Lock(); defer a.mu.Unlock(); return len(a.versions) }

func TestWatcherAppliesOnlyChanges(t *testing.T) {
	src := &memSource{data: valid}
	ap := &applied{}
	w := NewWatcher(src, time.Hour, ap.apply, nil)

	if ok, err := w.Check(context.Background()); !ok || err != nil {
		t.Fatalf("first check = %v, %v; want applied", ok, err)
	}
	if ok, err := w.Check(context.Background()); ok || err != nil {
		t.Fatalf("unchanged check = %v, %v; want no-op", ok, err)
	}
	src.set(strings.Replace(valid, `"limit": 100`, `"limit": 200`, 1))
	if ok, err := w.Check(context.Background()); !ok || err != nil {
		t.Fatalf("changed check = %v, %v; want applied", ok, err)
	}
	if ap.count() != 2 {
		t.Fatalf("apply called %d times, want 2", ap.count())
	}
	if ap.versions[0] == ap.versions[1] || len(ap.versions[0]) != 12 {
		t.Errorf("versions should be distinct 12-char hashes: %v", ap.versions)
	}
}

func TestWatcherKeepsPreviousConfigOnBadDocument(t *testing.T) {
	src := &memSource{data: valid}
	ap := &applied{}
	w := NewWatcher(src, time.Hour, ap.apply, nil)
	if _, err := w.Check(context.Background()); err != nil {
		t.Fatal(err)
	}

	src.set(`{"redis": {"addr": ""}}`) // invalid
	if ok, err := w.Check(context.Background()); ok || err == nil {
		t.Fatalf("bad doc = %v, %v; want error and not applied", ok, err)
	}
	if ap.count() != 1 {
		t.Fatalf("a bad document must not be applied (apply count %d)", ap.count())
	}
	// The same bad document is not reported again on the next tick.
	if ok, err := w.Check(context.Background()); ok || err != nil {
		t.Fatalf("repeat of same bad doc = %v, %v; want silent no-op", ok, err)
	}
	// Fixing it applies again.
	src.set(valid)
	if ok, err := w.Check(context.Background()); !ok || err != nil {
		t.Fatalf("recovery = %v, %v; want applied", ok, err)
	}
}

func TestWatcherRetriesAfterApplyFailure(t *testing.T) {
	src := &memSource{data: valid}
	ap := &applied{failNext: true}
	w := NewWatcher(src, time.Hour, ap.apply, nil)
	if ok, err := w.Check(context.Background()); ok || err == nil {
		t.Fatalf("apply failure = %v, %v; want error", ok, err)
	}
	// Same content must be retried (it was never applied).
	if ok, err := w.Check(context.Background()); !ok || err != nil {
		t.Fatalf("retry = %v, %v; want applied", ok, err)
	}
}

func TestWatcherSourceErrorDoesNotChangeState(t *testing.T) {
	src := &memSource{data: valid}
	ap := &applied{}
	w := NewWatcher(src, time.Hour, ap.apply, nil)
	w.Check(context.Background())
	src.fail(errors.New("s3 down"))
	if _, err := w.Check(context.Background()); err == nil {
		t.Fatal("expected source error")
	}
	src.set(valid)
	if ok, err := w.Check(context.Background()); ok || err != nil {
		t.Fatalf("same content after outage = %v, %v; want no-op", ok, err)
	}
	if ap.count() != 1 {
		t.Fatalf("apply count = %d, want 1", ap.count())
	}
}

func TestWatcherRunPicksUpChangeOnTickAndReload(t *testing.T) {
	src := &memSource{data: valid}
	ap := &applied{}
	var errMu sync.Mutex
	var errs []error
	w := NewWatcher(src, 20*time.Millisecond, ap.apply, func(e error) { errMu.Lock(); errs = append(errs, e); errMu.Unlock() })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()

	waitFor(t, func() bool { return ap.count() == 1 }, "initial apply")

	src.set(strings.Replace(valid, `"limit": 100`, `"limit": 300`, 1))
	waitFor(t, func() bool { return ap.count() == 2 }, "apply on tick")

	src.set(`not json`)
	waitFor(t, func() bool { errMu.Lock(); defer errMu.Unlock(); return len(errs) == 1 }, "error reported once")

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

func TestWatcherReloadTriggersImmediateCheck(t *testing.T) {
	src := &memSource{data: valid}
	ap := &applied{}
	w := NewWatcher(src, time.Hour, ap.apply, nil) // tick never fires
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)
	waitFor(t, func() bool { return ap.count() == 1 }, "initial apply")

	src.set(strings.Replace(valid, `"limit": 100`, `"limit": 400`, 1))
	w.Reload()
	w.Reload() // a second request while one is pending must not block
	waitFor(t, func() bool { return ap.count() == 2 }, "apply after Reload")
}

func TestFileSourceWatcherEndToEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(valid), 0o600); err != nil {
		t.Fatal(err)
	}
	ap := &applied{}
	w := NewWatcher(FileSource(path), time.Hour, ap.apply, nil)
	if ok, err := w.Check(context.Background()); !ok || err != nil {
		t.Fatalf("first = %v, %v", ok, err)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(valid, `"limit": 100`, `"limit": 9`, 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if ok, err := w.Check(context.Background()); !ok || err != nil {
		t.Fatalf("after edit = %v, %v", ok, err)
	}
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}
