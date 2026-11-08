// Package metrics is a minimal Prometheus-text-format registry (counters,
// gauges, histograms) so the gateway needs no third-party dependency.
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Registry holds all metrics.
type Registry struct {
	mu       sync.RWMutex
	counters map[string]*CounterVec
	hists    map[string]*HistogramVec
	gauges   map[string]func() []GaugeSample
	help     map[string]string
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		counters: map[string]*CounterVec{},
		hists:    map[string]*HistogramVec{},
		gauges:   map[string]func() []GaugeSample{},
		help:     map[string]string{},
	}
}

// CounterVec is a counter keyed by label values.
type CounterVec struct {
	labels []string
	mu     sync.RWMutex
	vals   map[string]*atomic.Uint64
}

// Counter registers (or returns) a counter family.
func (r *Registry) Counter(name, help string, labels ...string) *CounterVec {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.counters[name]; ok {
		return c
	}
	c := &CounterVec{labels: labels, vals: map[string]*atomic.Uint64{}}
	r.counters[name] = c
	r.help[name] = help
	return c
}

// Inc adds one for the given label values.
func (c *CounterVec) Inc(values ...string) { c.Add(1, values...) }

// Add adds n.
func (c *CounterVec) Add(n uint64, values ...string) {
	k := strings.Join(values, "\xff")
	c.mu.RLock()
	v := c.vals[k]
	c.mu.RUnlock()
	if v == nil {
		c.mu.Lock()
		if v = c.vals[k]; v == nil {
			v = new(atomic.Uint64)
			c.vals[k] = v
		}
		c.mu.Unlock()
	}
	v.Add(n)
}

// Value reads one counter (used by tests).
func (c *CounterVec) Value(values ...string) uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if v := c.vals[strings.Join(values, "\xff")]; v != nil {
		return v.Load()
	}
	return 0
}

// GaugeSample is one labelled gauge value.
type GaugeSample struct {
	Labels map[string]string
	Value  float64
}

// GaugeFunc registers a gauge computed at scrape time.
func (r *Registry) GaugeFunc(name, help string, f func() []GaugeSample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges[name] = f
	r.help[name] = help
}

// HistogramVec is a fixed-bucket histogram keyed by label values.
type HistogramVec struct {
	labels  []string
	bounds  []float64
	mu      sync.RWMutex
	entries map[string]*histogram
}

type histogram struct {
	buckets []atomic.Uint64 // per-bucket (non-cumulative) counts, last is +Inf
	sumNs   atomic.Uint64   // sum stored as microseconds*1000 would lose precision; keep seconds*1e9
	count   atomic.Uint64
}

// DefaultLatencyBuckets are in seconds, 0.5ms .. 10s.
var DefaultLatencyBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Histogram registers (or returns) a histogram family.
func (r *Registry) Histogram(name, help string, bounds []float64, labels ...string) *HistogramVec {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h, ok := r.hists[name]; ok {
		return h
	}
	h := &HistogramVec{labels: labels, bounds: bounds, entries: map[string]*histogram{}}
	r.hists[name] = h
	r.help[name] = help
	return h
}

// Observe records seconds for the given label values.
func (h *HistogramVec) Observe(seconds float64, values ...string) {
	k := strings.Join(values, "\xff")
	h.mu.RLock()
	e := h.entries[k]
	h.mu.RUnlock()
	if e == nil {
		h.mu.Lock()
		if e = h.entries[k]; e == nil {
			e = &histogram{buckets: make([]atomic.Uint64, len(h.bounds)+1)}
			h.entries[k] = e
		}
		h.mu.Unlock()
	}
	i := sort.SearchFloat64s(h.bounds, seconds)
	e.buckets[i].Add(1)
	e.sumNs.Add(uint64(seconds * 1e9))
	e.count.Add(1)
}

func labelString(names []string, joined string, extra ...string) string {
	vals := strings.Split(joined, "\xff")
	var parts []string
	for i, n := range names {
		v := ""
		if i < len(vals) {
			v = vals[i]
		}
		parts = append(parts, fmt.Sprintf(`%s=%q`, n, v))
	}
	parts = append(parts, extra...)
	if len(parts) == 0 {
		return ""
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// Write renders the Prometheus exposition format.
func (r *Registry) Write(w io.Writer) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.counters))
	for n := range r.counters {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		c := r.counters[n]
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", n, r.help[n], n)
		c.mu.RLock()
		keys := make([]string, 0, len(c.vals))
		for k := range c.vals {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(w, "%s%s %d\n", n, labelString(c.labels, k), c.vals[k].Load())
		}
		c.mu.RUnlock()
	}

	names = names[:0]
	for n := range r.gauges {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", n, r.help[n], n)
		for _, s := range r.gauges[n]() {
			var ks []string
			for k := range s.Labels {
				ks = append(ks, k)
			}
			sort.Strings(ks)
			var parts []string
			for _, k := range ks {
				parts = append(parts, fmt.Sprintf("%s=%q", k, s.Labels[k]))
			}
			ls := ""
			if len(parts) > 0 {
				ls = "{" + strings.Join(parts, ",") + "}"
			}
			fmt.Fprintf(w, "%s%s %g\n", n, ls, s.Value)
		}
	}

	names = names[:0]
	for n := range r.hists {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		h := r.hists[n]
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", n, r.help[n], n)
		h.mu.RLock()
		keys := make([]string, 0, len(h.entries))
		for k := range h.entries {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			e := h.entries[k]
			var cum uint64
			for i, b := range h.bounds {
				cum += e.buckets[i].Load()
				fmt.Fprintf(w, "%s_bucket%s %d\n", n, labelString(h.labels, k, fmt.Sprintf(`le="%g"`, b)), cum)
			}
			cum += e.buckets[len(h.bounds)].Load()
			fmt.Fprintf(w, "%s_bucket%s %d\n", n, labelString(h.labels, k, `le="+Inf"`), cum)
			fmt.Fprintf(w, "%s_sum%s %g\n", n, labelString(h.labels, k), float64(e.sumNs.Load())/1e9)
			fmt.Fprintf(w, "%s_count%s %d\n", n, labelString(h.labels, k), e.count.Load())
		}
		h.mu.RUnlock()
	}
}

// Handler serves the registry.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		r.Write(w)
	})
}
