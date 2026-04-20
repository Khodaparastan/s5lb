// Package admin exposes /metrics, /healthz, /readyz, /version, /debug/pprof.
package admin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // side-effect: registers /debug/pprof on DefaultServeMux
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ReadinessProber lets the admin server check data-path readiness without a
// hard dependency on the balancer package.
type ReadinessProber interface {
	AnyHealthy() bool
}

// Server is the HTTP admin endpoint.
type Server struct {
	srv *http.Server
	log *slog.Logger
}

// New constructs an admin server bound to `addr`.
func New(addr string, prober ReadinessProber, reg *prometheus.Registry,
	log *slog.Logger, version, commit, buildDate string) *Server {

	mux := http.NewServeMux()

	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if prober.AnyHealthy() {
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "ready")
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "no healthy upstream")
	})
	mux.HandleFunc("/version", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"version":%q,"commit":%q,"go":%q,"build_date":%q}`,
			version, commit, runtime.Version(), buildDate)
	})
	// Re-route pprof off of DefaultServeMux to the admin port only.
	mux.Handle("/debug/", http.DefaultServeMux)

	return &Server{
		srv: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		log: log,
	}
}

// Start listens and serves in a goroutine; errors other than ErrServerClosed
// are logged.
func (a *Server) Start() {
	go func() {
		a.log.Info("admin_listening", "addr", a.srv.Addr)
		if err := a.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.log.Error("admin_server_error", "err", err.Error())
		}
	}()
}

// Stop gracefully shuts down the admin HTTP server.
func (a *Server) Stop(ctx context.Context) { _ = a.srv.Shutdown(ctx) }
