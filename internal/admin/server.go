// Package admin exposes /metrics, /healthz, /readyz, /version, and
// optionally /debug/pprof endpoints on a private mux (not the global DefaultServeMux).
// It also exposes a multi-group REST API and a rich single-page UI.
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"strconv"
	"strings"
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

// GroupInfoProvider exposes per-group state for the admin API.
// It is implemented by balancer.Manager.
// Methods return interface{} so that the admin package can marshal any concrete
// type without creating an import cycle.
type GroupInfoProvider interface {
	// GroupInfos returns a JSON-serialisable snapshot of all groups.
	GroupInfos() interface{}
	// GroupInfoByName returns the GroupInfo for a named group and whether it was found.
	GroupInfoByName(name string) (interface{}, bool)
}

// GroupReloader can reload a single named group.
type GroupReloader interface {
	ReloadGroup(name string) error
}

// StateProvider exposes a flat single-group state snapshot for the admin UI.
type StateProvider interface {
	// State returns a JSON-serialisable snapshot of the primary group state.
	State() interface{}
}

// SessionsProvider exposes active session snapshots.
type SessionsProvider interface {
	// ActiveSessions returns a JSON-serialisable list of active sessions.
	ActiveSessions() interface{}
}

// DrainController can drain or resume an individual upstream by ID.
type DrainController interface {
	// SetUpstreamDrain sets or clears the drain flag. Returns false if not found.
	SetUpstreamDrain(id string, drain bool) bool
}

// Options configures optional admin server features.
type Options struct {
	// BearerToken, when non-empty, requires Authorization: Bearer <token> on
	// every request. Required when the admin listen address is not loopback.
	BearerToken string
	// EnablePprof registers /debug/pprof/* handlers. Disabled by default.
	EnablePprof bool
	// GroupProvider, if set, enables /admin/api/groups/* endpoints.
	GroupProvider GroupInfoProvider
	// GroupReloader, if set, enables POST /admin/api/groups/{name}/reload.
	GroupReloader GroupReloader
	// StateProvider, if set, enables GET /admin/api/state.
	StateProvider StateProvider
	// SessionsProvider, if set, enables GET /admin/api/sessions.
	SessionsProvider SessionsProvider
	// DrainController, if set, enables POST /admin/api/upstreams/{id}/drain.
	DrainController DrainController
}

// Server is the admin HTTP endpoint.
type Server struct {
	srv      *http.Server
	listener net.Listener
	log      *slog.Logger
}

// New constructs an admin server. Returns an error when addr is non-loopback
// and opts.BearerToken is empty (required for security). `reloader` may be nil
// to disable POST /admin/reload.
func New(
	addr string,
	prober ReadinessProber,
	reloader Reloader,
	reg *prometheus.Registry,
	log *slog.Logger,
	version, commit, buildDate string,
	opts Options,
) (*Server, error) {
	if opts.BearerToken == "" && !isLoopbackListenAddr(addr) {
		return nil, fmt.Errorf("admin bearer token is required for non-loopback addr %q", addr)
	}

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
		_ = json.NewEncoder(w).Encode(struct {
			Version   string `json:"version"`
			Commit    string `json:"commit"`
			Go        string `json:"go"`
			BuildDate string `json:"build_date"`
		}{
			Version:   version,
			Commit:    commit,
			Go:        runtime.Version(),
			BuildDate: buildDate,
		})
	})

	if opts.EnablePprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

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

	// --- Multi-group API ---
	if opts.GroupProvider != nil {
		gp := opts.GroupProvider
		gr := opts.GroupReloader

		// GET /admin/api/groups → JSON array of all group summaries.
		mux.HandleFunc("/admin/api/groups", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				http.Error(w, "GET required", http.StatusMethodNotAllowed)
				return
			}
			writeJSON(w, gp.GroupInfos())
		})

		// GET /admin/api/groups/{name} → JSON detail for one group.
		// POST /admin/api/groups/{name}/reload → reload a single group.
		mux.HandleFunc("/admin/api/groups/", func(w http.ResponseWriter, r *http.Request) {
			// Path: /admin/api/groups/{name}  or  /admin/api/groups/{name}/reload
			tail := strings.TrimPrefix(r.URL.Path, "/admin/api/groups/")
			parts := strings.SplitN(tail, "/", 2)
			name := parts[0]
			if name == "" {
				http.Error(w, "group name required", http.StatusBadRequest)
				return
			}

			// POST .../reload
			if len(parts) == 2 && parts[1] == "reload" {
				if r.Method != http.MethodPost {
					http.Error(w, "POST required", http.StatusMethodNotAllowed)
					return
				}
				if gr == nil {
					http.Error(w, "reload not configured", http.StatusNotImplemented)
					return
				}
				if err := gr.ReloadGroup(name); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"status":"reloaded"}`+"\n")
				return
			}

			// GET /{name}
			if r.Method != http.MethodGet {
				http.Error(w, "GET required", http.StatusMethodNotAllowed)
				return
			}
			info, ok := gp.GroupInfoByName(name)
			if !ok {
				http.Error(w, fmt.Sprintf("group %q not found", name), http.StatusNotFound)
				return
			}
			writeJSON(w, info)
		})
	}

	// --- New single-group API (used by ui.html) ---

	// GET /admin/api/state
	if opts.StateProvider != nil {
		sp := opts.StateProvider
		mux.HandleFunc("/admin/api/state", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				http.Error(w, "GET required", http.StatusMethodNotAllowed)
				return
			}
			writeJSON(w, sp.State())
		})

		// GET /admin/api/events — SSE stream that pushes state updates.
		mux.HandleFunc("/admin/api/events", func(w http.ResponseWriter, r *http.Request) {
			fl, ok := w.(http.Flusher)
			if !ok {
				http.Error(w, "streaming not supported", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("Connection", "keep-alive")
			w.Header().Set("X-Accel-Buffering", "no")
			tick := time.NewTicker(2 * time.Second)
			defer tick.Stop()
			for {
				select {
				case <-r.Context().Done():
					return
				case <-tick.C:
					data, err := json.Marshal(sp.State())
					if err != nil {
						continue
					}
					_, _ = fmt.Fprintf(w, "event: state\ndata: %s\n\n", data)
					fl.Flush()
				}
			}
		})
	}

	// GET /admin/api/config — returns current running config as JSON.
	if opts.GroupProvider != nil {
		gp2 := opts.GroupProvider
		mux.HandleFunc("/admin/api/config", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				http.Error(w, "GET required", http.StatusMethodNotAllowed)
				return
			}
			writeJSON(w, gp2.GroupInfos())
		})
	}

	// GET /admin/api/sessions
	if opts.SessionsProvider != nil {
		sesp := opts.SessionsProvider
		mux.HandleFunc("/admin/api/sessions", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				http.Error(w, "GET required", http.StatusMethodNotAllowed)
				return
			}
			writeJSON(w, sesp.ActiveSessions())
		})
	}

	// POST /admin/api/reload — alias for /admin/reload for the new UI.
	if reloader != nil {
		mux.HandleFunc("/admin/api/reload", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "POST required", http.StatusMethodNotAllowed)
				return
			}
			if err := reloader.Reload(); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			writeJSON(w, map[string]string{"status": "reloaded"})
		})
	}

	// POST /admin/api/upstreams/{id}/drain?enabled=true|false
	if opts.DrainController != nil {
		dc := opts.DrainController
		mux.HandleFunc("/admin/api/upstreams/", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "POST required", http.StatusMethodNotAllowed)
				return
			}
			// Path: /admin/api/upstreams/{id}/drain
			tail := strings.TrimPrefix(r.URL.Path, "/admin/api/upstreams/")
			parts := strings.SplitN(tail, "/", 2)
			if len(parts) != 2 || parts[1] != "drain" {
				http.NotFound(w, r)
				return
			}
			id := parts[0]
			if id == "" {
				http.Error(w, "upstream id required", http.StatusBadRequest)
				return
			}
			enabledStr := r.URL.Query().Get("enabled")
			drain, err := strconv.ParseBool(enabledStr)
			if err != nil {
				http.Error(w, "enabled must be true or false", http.StatusBadRequest)
				return
			}
			if !dc.SetUpstreamDrain(id, drain) {
				http.Error(w, fmt.Sprintf("upstream %q not found", id), http.StatusNotFound)
				return
			}
			writeJSON(w, map[string]interface{}{"id": id, "draining": drain})
		})
	}

	// --- Admin UI ---
	mux.HandleFunc("/admin/ui", serveUI)
	mux.HandleFunc("/admin/ui/", serveUI)

	var handler http.Handler = mux
	if opts.BearerToken != "" {
		handler = bearerAuth(opts.BearerToken, mux)
	}

	// Bind the listener synchronously so startup errors are returned to the caller.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("admin listen %s: %w", addr, err)
	}

	return &Server{
		srv: &http.Server{
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
		listener: ln,
		log:      log,
	}, nil
}

// writeJSON marshals v to JSON and writes it with Content-Type: application/json.
func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// bearerAuth wraps a handler requiring Authorization: Bearer <token>.
func bearerAuth(token string, next http.Handler) http.Handler {
	want := "Bearer " + token
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != want {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// isLoopbackListenAddr reports whether addr resolves to a loopback interface.
func isLoopbackListenAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Start runs the admin server in a goroutine using the already-bound listener.
func (a *Server) Start() {
	go func() {
		a.log.Info("admin_listening", "addr", a.listener.Addr().String())
		if err := a.srv.Serve(a.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.log.Error("admin_server_error", "err", err.Error())
		}
	}()
}

// Stop gracefully shuts down the admin server.
func (a *Server) Stop(ctx context.Context) { _ = a.srv.Shutdown(ctx) }
