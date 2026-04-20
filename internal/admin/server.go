// Package admin exposes /metrics, /healthz, /readyz, /version, and
// /debug/pprof endpoints on a private mux (not the global DefaultServeMux).
package admin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ReadinessProber lets the admin server check data-path readiness without
// a hard dependency on the balancer package.
type ReadinessProber interface {
	AnyHealthy() bool
}

// Reloader is optionally invoked from POST /admin/reload.
type Reloader interface {
	Reload() error
}

// Server is the admin HTTP endpoint.
type Server struct {
	srv *http.Server
	log *slog.Logger
}

// New constructs an admin server. `reloader` may be nil to disable
// POST /admin/reload.
func New(
	addr string,
	prober ReadinessProber,
	reloader Reloader,
	reg *prometheus.Registry,
	log *slog.Logger,
	version, commit, buildDate string,
) *Server {
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

	// Explicit pprof registration on our private mux (no DefaultServeMux leak).
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	if reloader != nil {
		mux.HandleFunc("/admin/reload", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "POST required", http.StatusMethodNotAllowed)
				return
			}
			if err := reloader.Reload(); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_, _ = io.WriteString(w, "reloaded\n")
		})
	}

	return &Server{
		srv: &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		log: log,
	}
}

// Start runs the admin server in a goroutine.
func (a *Server) Start() {
	go func() {
		a.log.Info("admin_listening", "addr", a.srv.Addr)
		if err := a.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.log.Error("admin_server_error", "err", err.Error())
		}
	}()
}

// Stop gracefully shuts down the admin server.
func (a *Server) Stop(ctx context.Context) { _ = a.srv.Shutdown(ctx) }
