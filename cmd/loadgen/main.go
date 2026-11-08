// Command loadgen is an open-model HTTP load generator.
//
// Requests are launched on a fixed schedule (constant arrival rate) no matter
// how slowly earlier ones complete, and latency is measured from the moment a
// request was *supposed* to start. A closed-loop tool (N workers each waiting
// for a reply) slows down when the target slows down and so hides exactly the
// tail latency we want to see; this is the coordinated-omission problem.
//
//	loadgen -url http://localhost:8080/api -rate 2000 -duration 10s -out result.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	Name        string         `json:"name"`
	TargetRate  float64        `json:"target_rps"`
	DurationSec float64        `json:"duration_s"`
	Sent        int64          `json:"sent"`
	Completed   int64          `json:"completed"`
	Dropped     int64          `json:"dropped_client_side"`
	Throughput  float64        `json:"achieved_rps"`
	Codes       map[string]int `json:"status_codes"`
	AllowedRPS  float64        `json:"allowed_rps"` // 2xx per second
	P50ms       float64        `json:"p50_ms"`
	P95ms       float64        `json:"p95_ms"`
	P99ms       float64        `json:"p99_ms"`
	MaxMs       float64        `json:"max_ms"`
	// Latency of the requests the gateway accepted (2xx) vs rejected (429/503).
	OKP99ms   float64  `json:"ok_p99_ms"`
	ShedP99ms float64  `json:"shed_p99_ms"`
	Timeline  []bucket `json:"timeline,omitempty"`
}

type bucket struct {
	Second int            `json:"t"`
	Codes  map[string]int `json:"codes"`
	P99ms  float64        `json:"p99_ms"`
}

type sample struct {
	atSec int
	code  int
	lat   time.Duration
}

func pct(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return float64(sorted[i]) / float64(time.Millisecond)
}

func main() {
	var (
		urls     = flag.String("url", "http://127.0.0.1:8080/", "target URL(s), comma separated; requests round-robin across them")
		rate     = flag.Float64("rate", 1000, "arrival rate (requests/second)")
		dur      = flag.Duration("duration", 10*time.Second, "test length")
		maxOut   = flag.Int("max-outstanding", 20000, "client-side cap on in-flight requests (drops are counted and reported)")
		timeout  = flag.Duration("timeout", 5*time.Second, "per-request client timeout")
		apiKey   = flag.String("api-key", "", "X-API-Key header")
		keys     = flag.Int("keys", 0, "if > 0, spread requests over this many distinct X-Forwarded-For identities")
		name     = flag.String("name", "", "scenario name for the report")
		out      = flag.String("out", "", "write JSON result here")
		timeline = flag.Bool("timeline", false, "include a per-second breakdown")
	)
	flag.Parse()
	targets := strings.Split(*urls, ",")

	client := &http.Client{
		Timeout: *timeout,
		Transport: &http.Transport{
			MaxIdleConns:        *maxOut,
			MaxIdleConnsPerHost: *maxOut,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		samples   = make([]sample, 0, int(*rate*(*dur).Seconds())+16)
		sent      atomic.Int64
		dropped   atomic.Int64
		outstand  atomic.Int64
		completed atomic.Int64
	)

	interval := time.Duration(float64(time.Second) / (*rate))
	start := time.Now()
	total := int64(*rate * (*dur).Seconds())
	for i := int64(0); i < total; i++ {
		intended := start.Add(time.Duration(i) * interval)
		if d := time.Until(intended); d > 0 {
			time.Sleep(d)
		}
		sent.Add(1)
		if outstand.Load() >= int64(*maxOut) {
			dropped.Add(1)
			continue
		}
		outstand.Add(1)
		wg.Add(1)
		target := targets[int(i)%len(targets)]
		go func(i int64, intended time.Time) {
			defer wg.Done()
			defer outstand.Add(-1)
			req, _ := http.NewRequest(http.MethodGet, target, nil)
			if *apiKey != "" {
				req.Header.Set("X-API-Key", *apiKey)
			}
			if *keys > 0 {
				req.Header.Set("X-Forwarded-For", fmt.Sprintf("10.%d.%d.%d", (i%int64(*keys))>>16&255, (i%int64(*keys))>>8&255, (i%int64(*keys))&255))
			}
			code := 0
			resp, err := client.Do(req)
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				code = resp.StatusCode
			}
			// Latency counts from the intended send time, not from when this
			// goroutine happened to run.
			lat := time.Since(intended)
			completed.Add(1)
			mu.Lock()
			samples = append(samples, sample{atSec: int(intended.Sub(start).Seconds()), code: code, lat: lat})
			mu.Unlock()
		}(i, intended)
	}
	wg.Wait()
	elapsed := time.Since(start)

	res := result{
		Name: *name, TargetRate: *rate, DurationSec: elapsed.Seconds(),
		Sent: sent.Load(), Completed: completed.Load(), Dropped: dropped.Load(),
		Codes: map[string]int{},
	}
	var all, ok, shed []time.Duration
	byTime := map[int][]sample{}
	for _, s := range samples {
		key := fmt.Sprint(s.code)
		if s.code == 0 {
			key = "error"
		}
		res.Codes[key]++
		all = append(all, s.lat)
		switch {
		case s.code >= 200 && s.code < 300:
			ok = append(ok, s.lat)
		case s.code == 429 || s.code == 503:
			shed = append(shed, s.lat)
		}
		byTime[s.atSec] = append(byTime[s.atSec], s)
	}
	for _, s := range [][]time.Duration{all, ok, shed} {
		sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	}
	res.Throughput = float64(res.Completed) / elapsed.Seconds()
	res.AllowedRPS = float64(len(ok)) / elapsed.Seconds()
	res.P50ms, res.P95ms, res.P99ms = pct(all, .50), pct(all, .95), pct(all, .99)
	res.MaxMs = pct(all, 1)
	res.OKP99ms, res.ShedP99ms = pct(ok, .99), pct(shed, .99)

	if *timeline {
		var secs []int
		for s := range byTime {
			secs = append(secs, s)
		}
		sort.Ints(secs)
		for _, sec := range secs {
			b := bucket{Second: sec, Codes: map[string]int{}}
			var ls []time.Duration
			for _, s := range byTime[sec] {
				k := fmt.Sprint(s.code)
				if s.code == 0 {
					k = "error"
				}
				b.Codes[k]++
				ls = append(ls, s.lat)
			}
			sort.Slice(ls, func(i, j int) bool { return ls[i] < ls[j] })
			b.P99ms = pct(ls, .99)
			res.Timeline = append(res.Timeline, b)
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer f.Close()
		e := json.NewEncoder(f)
		e.SetIndent("", "  ")
		e.Encode(res)
	}
	enc.Encode(res)
}
