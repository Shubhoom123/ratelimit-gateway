// Command gateway runs the rate-limiting API gateway.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shubhamkhalkho/ratelimit-gateway/internal/config"
	"github.com/shubhamkhalkho/ratelimit-gateway/internal/gateway"
	"github.com/shubhamkhalkho/ratelimit-gateway/internal/metrics"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	var (
		listen   = flag.String("listen", env("GATEWAY_LISTEN", ":8080"), "proxy listen address")
		admin    = flag.String("admin", env("GATEWAY_ADMIN", ":9090"), "admin listen address (health, metrics)")
		cfgSpec  = flag.String("config", env("GATEWAY_CONFIG", "config.json"), "config file path or http(s) URL")
		interval = flag.Duration("reload-interval", 2*time.Second, "how often to poll the config source")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	reg := metrics.NewRegistry()
	gw := gateway.New(log, reg)

	// Fail fast: the first config must load and validate, or we refuse to start.
	src := config.NewSource(*cfgSpec)
	watcher := config.NewWatcher(src, *interval, gw.Reload, func(err error) {
		// After startup a bad config is logged and ignored; the old one stays live.
		log.Error("config reload failed; keeping previous config", "source", src.String(), "err", err)
	})
	if _, err := watcher.Check(context.Background()); err != nil {
		log.Error("initial config invalid", "source", src.String(), "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go watcher.Run(ctx)

	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			log.Info("SIGHUP: reloading config")
			watcher.Reload()
		}
	}()

	proxySrv := &http.Server{
		Addr:              *listen,
		Handler:           gw,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
	adminSrv := &http.Server{Addr: *admin, Handler: gw.AdminHandler(), ReadHeaderTimeout: 5 * time.Second}

	errc := make(chan error, 2)
	go func() { errc <- proxySrv.ListenAndServe() }()
	go func() { errc <- adminSrv.ListenAndServe() }()
	log.Info("gateway started", "listen", *listen, "admin", *admin, "config", src.String())

	select {
	case <-ctx.Done():
		log.Info("shutting down: draining in-flight requests")
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Error("server error", "err", err)
		}
	}
	// Graceful drain: stop accepting, let in-flight finish (ECS sends SIGTERM,
	// then SIGKILL after stopTimeout).
	sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = proxySrv.Shutdown(sctx)
	_ = adminSrv.Shutdown(sctx)
}
