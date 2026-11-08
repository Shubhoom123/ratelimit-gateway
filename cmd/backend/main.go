// Command backend is a fake upstream service whose latency and error rate can
// be changed while it runs, so load tests can slow it down or break it on cue.
//
//	curl 'localhost:9001/__control?latency=300ms&fail=0.5'
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

func main() {
	addr := flag.String("listen", ":9001", "listen address")
	name := flag.String("name", "backend", "name echoed in responses")
	latency := flag.Duration("latency", 5*time.Millisecond, "initial per-request latency")
	workers := flag.Int("workers", 64, "concurrent requests served; extra requests queue, like a fixed thread pool")
	flag.Parse()
	slots := make(chan struct{}, *workers)

	var latencyNs atomic.Int64
	var failPermille atomic.Int64
	var served atomic.Int64
	latencyNs.Store(int64(*latency))

	mux := http.NewServeMux()
	mux.HandleFunc("/__control", func(w http.ResponseWriter, r *http.Request) {
		if v := r.URL.Query().Get("latency"); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			latencyNs.Store(int64(d))
		}
		if v := r.URL.Query().Get("fail"); v != "" {
			var f float64
			if _, err := fmt.Sscan(v, &f); err != nil || f < 0 || f > 1 {
				http.Error(w, "fail must be in [0,1]", 400)
				return
			}
			failPermille.Store(int64(f * 1000))
		}
		json.NewEncoder(w).Encode(map[string]any{
			"latency": time.Duration(latencyNs.Load()).String(),
			"fail":    float64(failPermille.Load()) / 1000,
			"served":  served.Load(),
		})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Queue for a worker. A request whose caller has already given up leaves
		// the queue instead of wasting a worker, as a well-behaved service would.
		select {
		case slots <- struct{}{}:
		case <-r.Context().Done():
			return
		}
		defer func() { <-slots }()
		served.Add(1)
		select {
		case <-time.After(time.Duration(latencyNs.Load())):
		case <-r.Context().Done():
			return
		}
		if rand.Int63n(1000) < failPermille.Load() {
			http.Error(w, `{"error":"injected failure"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"backend":%q,"path":%q}`+"\n", *name, strings.TrimSpace(r.URL.Path))
	})
	log.Printf("backend %q listening on %s", *name, *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
