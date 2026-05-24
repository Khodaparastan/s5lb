package balancer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/khodaparastan/socks5lb/internal/admission"
	"github.com/khodaparastan/socks5lb/internal/config"
	"github.com/khodaparastan/socks5lb/internal/metrics"
	"github.com/khodaparastan/socks5lb/internal/strategy"
	"github.com/khodaparastan/socks5lb/internal/transport"
	"github.com/khodaparastan/socks5lb/internal/upstream"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"
)

// GroupInfo is a read-only snapshot of a single group's state, suitable for
// the admin API. Secrets are never included.
type GroupInfo struct {
	Name     string              `json:"name"`
	Listen   string              `json:"listen"`
	Strategy string              `json:"strategy"`
	Config   PublicConfig        `json:"config"`
	Upstream []upstream.Snapshot `json:"upstreams"`
}

// PublicConfig is a secrets-free projection of config.Config for admin DTOs.
type PublicConfig struct {
	ListenAddr           string                      `json:"listen"`
	MaxPerProxy          int                         `json:"max_per_proxy"`
	MaxClients           int                         `json:"max_clients"`
	HealthInterval       string                      `json:"health_interval"`
	RetryBackoff         string                      `json:"retry_backoff"`
	ConnectTimeout       string                      `json:"connect_timeout"`
	HandshakeTimeout     string                      `json:"handshake_timeout"`
	QueueWaitTimeout     string                      `json:"queue_wait_timeout"`
	FailureThreshold     int                         `json:"failure_threshold"`
	FailureWindow        string                      `json:"failure_window"`
	IdleTimeout          string                      `json:"idle_timeout"`
	TCPKeepAlive         bool                        `json:"tcp_keepalive"`
	Strategy             string                      `json:"strategy"`
	HashKey              config.HashKey              `json:"hash_key"`
	Backpressure         config.BackpressureStrategy `json:"backpressure"`
	AdmissionWaitTimeout string                      `json:"admission_wait_timeout"`
	UDPEnabled           bool                        `json:"udp_enabled"`
	UDPBindAddr          string                      `json:"udp_bind"`
	UDPIdleTimeout       string                      `json:"udp_idle_timeout"`
	Transport            PublicTransportConfig       `json:"transport"`
	Upstreams            []PublicUpstreamSpec        `json:"upstreams"`
}

// PublicTransportConfig is a secrets-free projection of the transport config.
type PublicTransportConfig struct {
	Type       config.TransportType `json:"type"`
	KubeMode   string               `json:"kubernetes_mode,omitempty"`
	Namespace  string               `json:"namespace,omitempty"`
	Pod        string               `json:"pod,omitempty"`
	Container  string               `json:"container,omitempty"`
	HasHeaders bool                 `json:"has_headers,omitempty"`
	HasToken   bool                 `json:"has_token,omitempty"`
}

// PublicUpstreamSpec is a secrets-free projection of an upstream spec.
type PublicUpstreamSpec struct {
	ID             string `json:"id,omitempty"`
	Host           string `json:"host"`
	Port           int    `json:"port"`
	Weight         int    `json:"weight"`
	Priority       int    `json:"priority"`
	AuthConfigured bool   `json:"auth_configured"`
}

// publicConfig converts a config.Config to a PublicConfig, omitting secrets.
func publicConfig(c config.Config) PublicConfig {
	ups := make([]PublicUpstreamSpec, 0, len(c.Upstreams))
	for _, u := range c.Upstreams {
		ups = append(ups, PublicUpstreamSpec{
			ID:             u.ID,
			Host:           u.Host,
			Port:           u.Port,
			Weight:         u.Weight,
			Priority:       u.Priority,
			AuthConfigured: u.Username != "" || u.Password != "",
		})
	}

	k := c.Transport.Kubernetes
	return PublicConfig{
		ListenAddr:           c.ListenAddr,
		MaxPerProxy:          c.MaxPerProxy,
		MaxClients:           c.MaxClients,
		HealthInterval:       c.HealthInterval.String(),
		RetryBackoff:         c.RetryBackoff.String(),
		ConnectTimeout:       c.ConnectTimeout.String(),
		HandshakeTimeout:     c.HandshakeTimeout.String(),
		QueueWaitTimeout:     c.QueueWaitTimeout.String(),
		FailureThreshold:     c.FailureThreshold,
		FailureWindow:        c.FailureWindow.String(),
		IdleTimeout:          c.IdleTimeout.String(),
		TCPKeepAlive:         c.TCPKeepAlive,
		Strategy:             c.Strategy,
		HashKey:              c.HashKey,
		Backpressure:         c.Backpressure,
		AdmissionWaitTimeout: c.AdmissionWaitTimeout.String(),
		UDPEnabled:           c.UDPEnabled,
		UDPBindAddr:          c.UDPBindAddr,
		UDPIdleTimeout:       c.UDPIdleTimeout.String(),
		Transport: PublicTransportConfig{
			Type:       c.Transport.Type,
			KubeMode:   k.Mode,
			Namespace:  k.Namespace,
			Pod:        k.Pod,
			Container:  k.Container,
			HasHeaders: len(k.Headers) > 0,
			HasToken:   k.BearerToken != "" || k.BearerTokenFile != "",
		},
		Upstreams: ups,
	}
}

// serveResult carries the outcome of a group's Serve goroutine back to the
// manager supervisor loop.
type serveResult struct {
	name    string
	err     error
	retired bool
}

// groupEntry holds the runtime state for one named group.
type groupEntry struct {
	name    string
	lb      *LoadBalancer
	met     *metrics.Metrics
	retired atomic.Bool
}

// Manager owns the lifecycle of one LoadBalancer per group.
// It supports hot-reload of individual groups or all groups at once.
type Manager struct {
	mu     sync.RWMutex
	groups []*groupEntry
	byName map[string]*groupEntry

	// supervisor fields: initialized by Start via startOnce.
	// cancelVal stores context.CancelFunc as an atomic.Value so Shutdown can
	// safely read it from a different goroutine without holding startOnce.
	ctx       context.Context
	cancelVal atomic.Value // stores context.CancelFunc
	serveErrs chan serveResult
	startOnce sync.Once

	cfgPath   string
	log       *slog.Logger
	reg       prometheus.Registerer
	tracer    trace.Tracer
	version   string
	commit    string
	buildDate string
}

// NewManager constructs a Manager from a MultiConfig, building one LoadBalancer
// per effective group.
func NewManager(
	mc config.MultiConfig,
	cfgPath string,
	log *slog.Logger,
	reg prometheus.Registerer,
	tracer trace.Tracer,
	version, commit, buildDate string,
) (*Manager, error) {
	if err := mc.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	m := &Manager{
		byName:    make(map[string]*groupEntry),
		cfgPath:   cfgPath,
		log:       log,
		reg:       reg,
		tracer:    tracer,
		version:   version,
		commit:    commit,
		buildDate: buildDate,
	}

	effs := mc.EffectiveGroups()
	for _, cfg := range effs {
		entry, err := m.buildGroup(cfg)
		if err != nil {
			return nil, fmt.Errorf("group %q: %w", cfg.GroupName, err)
		}
		m.groups = append(m.groups, entry)
		m.byName[cfg.GroupName] = entry
	}

	return m, nil
}

// buildGroup constructs a groupEntry (LoadBalancer + supporting objects) from a
// fully-merged Config.
func (m *Manager) buildGroup(cfg config.Config) (*groupEntry, error) {
	name := cfg.GroupName
	if name == "" {
		name = "default"
	}

	ups, err := cfg.BuildUpstreams()
	if err != nil {
		return nil, fmt.Errorf("build upstreams: %w", err)
	}
	if len(ups) == 0 {
		return nil, fmt.Errorf("at least one upstream is required")
	}

	sel, err := strategy.New(cfg.Strategy)
	if err != nil {
		return nil, fmt.Errorf("strategy %q: %w", cfg.Strategy, err)
	}

	met, err := metrics.New(m.reg, name, m.version, m.commit, m.buildDate)
	if err != nil {
		return nil, fmt.Errorf("metrics: %w", err)
	}

	dialer, err := transport.NewFromConfig(cfg)
	if err != nil {
		met.Unregister()
		return nil, fmt.Errorf("transport: %w", err)
	}

	lb, err := New(cfg, m.cfgPath, m.log.With("group", name), met, m.tracer, ups, sel)
	if err != nil {
		met.Unregister()
		return nil, fmt.Errorf("balancer: %w", err)
	}
	lb.SetDialer(dialer)

	return &groupEntry{name: name, lb: lb, met: met}, nil
}

// Start launches all group Serve goroutines and supervises them.
// It blocks until the context is done or any non-retired group exits unexpectedly.
// All groups (initial and those added via Reload) are supervised through a shared
// serveErrs channel. Groups removed via Reload are marked retired so their exit
// does not trigger a global shutdown.
func (m *Manager) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	m.startOnce.Do(func() {
		var cancel context.CancelFunc
		m.ctx, cancel = context.WithCancel(ctx)
		m.cancelVal.Store(cancel)
		// Buffer large enough so no Serve goroutine ever blocks sending.
		m.serveErrs = make(chan serveResult, 64)

		m.mu.Lock()
		for _, g := range m.groups {
			m.startGroupLocked(g)
		}
		m.mu.Unlock()
	})

	var firstErr error
	for {
		select {
		case <-ctx.Done():
			m.Shutdown()
			return ctx.Err()

		case r := <-m.serveErrs:
			if r.retired {
				m.log.Info("group_retired", "group", r.name)
				continue
			}

			if r.err != nil {
				m.log.Error("group_serve_error", "group", r.name, "err", r.err.Error())
				firstErr = r.err
			} else {
				m.log.Warn("group_serve_exited", "group", r.name)
			}

			m.Shutdown()
			return firstErr
		}
	}
}

// startGroupLocked launches the Serve goroutine for a group entry. Must be
// called with m.mu held (or before the manager is shared across goroutines).
// After Start has been called, newly added groups call this to register in the
// supervisor loop.
func (m *Manager) startGroupLocked(g *groupEntry) {
	errs := m.serveErrs
	if errs == nil {
		// Start not yet called; goroutines will be launched by startOnce.
		return
	}
	go func() {
		m.log.Info("group_serving", "group", g.name, "listen", g.lb.Config().ListenAddr)
		err := g.lb.Serve()
		errs <- serveResult{
			name:    g.name,
			err:     err,
			retired: g.retired.Load(),
		}
	}()
}

// Shutdown gracefully drains all groups.
func (m *Manager) Shutdown() {
	if cancel, ok := m.cancelVal.Load().(context.CancelFunc); ok && cancel != nil {
		cancel()
	}

	m.mu.RLock()
	groups := m.groups
	m.mu.RUnlock()

	var wg sync.WaitGroup
	for _, g := range groups {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.lb.Shutdown()
		}()
	}
	wg.Wait()
}

// Reload re-reads the MultiConfig from disk and hot-reloads every group.
// Groups whose names appear in the new config are updated; groups that
// disappear are drained; new groups are added and started.
func (m *Manager) Reload() error {
	if m.cfgPath == "" {
		return fmt.Errorf("reload: no config path was supplied at startup")
	}

	// Load and validate outside the manager lock.
	mc, err := config.LoadMultiFile(m.cfgPath)
	if err != nil {
		return fmt.Errorf("reload: load: %w", err)
	}
	if err := mc.Validate(); err != nil {
		return fmt.Errorf("reload: validate: %w", err)
	}

	effs := mc.EffectiveGroups()
	var lastErr error

	m.mu.Lock()
	defer m.mu.Unlock()

	// Reload or add groups from the new config.
	newByName := make(map[string]*groupEntry, len(effs))
	newGroups := make([]*groupEntry, 0, len(effs))

	for _, cfg := range effs {
		name := cfg.GroupName
		if existing, ok := m.byName[name]; ok {
			if err := existing.lb.reloadFromConfig(cfg); err != nil {
				m.log.Error("reload_group_failed", "group", name, "err", err.Error())
				lastErr = err
			} else {
				m.log.Info("reload_group_applied", "group", name)
			}
			newByName[name] = existing
			newGroups = append(newGroups, existing)
		} else {
			// New group — build and register in supervisor.
			entry, err := m.buildGroup(cfg)
			if err != nil {
				m.log.Error("reload_new_group_failed", "group", name, "err", err.Error())
				lastErr = err
				continue
			}
			newByName[name] = entry
			newGroups = append(newGroups, entry)
			m.startGroupLocked(entry)
		}
	}

	// Mark and drain groups that are no longer in the config. The supervisor
	// loop ignores retired group exits, so this cannot trigger a global shutdown.
	for name, old := range m.byName {
		if _, kept := newByName[name]; !kept {
			m.log.Info("reload_group_removed", "group", name)
			old.retired.Store(true)
			go func(e *groupEntry) {
				e.lb.Shutdown()
				e.met.Unregister()
			}(old)
		}
	}

	m.groups = newGroups
	m.byName = newByName

	return lastErr
}

// ReloadGroup hot-reloads a single named group by re-reading the on-disk
// MultiConfig and applying only the named group's effective config.
func (m *Manager) ReloadGroup(name string) error {
	if m.cfgPath == "" {
		return fmt.Errorf("reload group %q: no config path was supplied at startup", name)
	}

	// Load and validate outside the manager lock.
	mc, err := config.LoadMultiFile(m.cfgPath)
	if err != nil {
		return fmt.Errorf("reload group %q: load: %w", name, err)
	}
	if err := mc.Validate(); err != nil {
		return fmt.Errorf("reload group %q: validate: %w", name, err)
	}

	var selected *config.Config
	for _, cfg := range mc.EffectiveGroups() {
		if cfg.GroupName == name {
			c := cfg
			selected = &c
			break
		}
	}
	if selected == nil {
		return fmt.Errorf("reload group %q: group not present in config", name)
	}

	m.mu.RLock()
	entry, ok := m.byName[name]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("group %q not found", name)
	}

	return entry.lb.reloadFromConfig(*selected)
}

// Group returns the LoadBalancer for the named group, or nil.
func (m *Manager) Group(name string) *LoadBalancer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if e, ok := m.byName[name]; ok {
		return e.lb
	}
	return nil
}

// Groups returns a snapshot of all group entries in order.
func (m *Manager) Groups() []*groupEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*groupEntry, len(m.groups))
	copy(out, m.groups)
	return out
}

// GroupInfos returns a JSON-friendly snapshot of all groups.
// Returns interface{} to satisfy admin.GroupInfoProvider without an import cycle.
func (m *Manager) GroupInfos() interface{} {
	entries := m.Groups()
	out := make([]GroupInfo, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.info())
	}
	return out
}

// GroupInfoByName returns the GroupInfo for a named group, or false if not found.
// Returns interface{} to satisfy admin.GroupInfoProvider without an import cycle.
func (m *Manager) GroupInfoByName(name string) (interface{}, bool) {
	m.mu.RLock()
	e, ok := m.byName[name]
	m.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return e.info(), true
}

// AnyHealthy reports whether at least one upstream in any group is healthy.
func (m *Manager) AnyHealthy() bool {
	for _, e := range m.Groups() {
		if e.lb.AnyHealthy() {
			return true
		}
	}
	return false
}

// info returns a GroupInfo snapshot for this entry. Secrets are redacted.
func (e *groupEntry) info() GroupInfo {
	cfg := e.lb.Config()
	snaps, _ := e.lb.currentPool()
	return GroupInfo{
		Name:     e.name,
		Listen:   cfg.ListenAddr,
		Strategy: cfg.Strategy,
		Config:   publicConfig(cfg),
		Upstream: snaps,
	}
}

// DrainUpstream sets or clears the drain flag on an upstream by ID across all groups.
// Returns false if the upstream was not found in any group.
func (m *Manager) DrainUpstream(id string, drain bool) bool {
	for _, e := range m.Groups() {
		if e.lb.DrainUpstream(id, drain) {
			return true
		}
	}
	return false
}

// Sessions returns a combined snapshot of all active sessions across all groups.
func (m *Manager) Sessions() []admission.SessionSnapshot {
	var out []admission.SessionSnapshot
	for _, e := range m.Groups() {
		out = append(out, e.lb.Sessions()...)
	}
	return out
}

// upstreamStateItem is the JSON shape the admin UI expects per upstream.
type upstreamStateItem struct {
	ID                  string  `json:"id"`
	Addr                string  `json:"addr"`
	Healthy             bool    `json:"healthy"`
	Draining            bool    `json:"draining"`
	Priority            int     `json:"priority"`
	Weight              int     `json:"weight"`
	Active              int     `json:"active"`
	EWMALatencyMS       float64 `json:"ewma_latency_ms"`
	TotalSessions       uint64  `json:"total_sessions"`
	TotalFailures       uint64  `json:"total_failures"`
	ConsecutiveFailures int     `json:"consecutive_failures"`
}

// groupStateItem is the per-group slice of the state response.
type groupStateItem struct {
	Name         string              `json:"name"`
	Strategy     string              `json:"strategy"`
	Backpressure string              `json:"backpressure"`
	ListenAddr   string              `json:"listen_addr"`
	UDPEnabled   bool                `json:"udp_enabled"`
	MaxClients   int                 `json:"max_clients"`
	SessionCount int                 `json:"session_count"`
	Upstreams    []upstreamStateItem `json:"upstreams"`
}

// stateResponse is the JSON payload for GET /admin/api/state.
type stateResponse struct {
	Version      string           `json:"version"`
	Commit       string           `json:"commit"`
	Now          string           `json:"now"`
	SessionCount int              `json:"session_count"`
	Groups       []groupStateItem `json:"groups"`
}

// State returns a JSON-serialisable snapshot for all groups.
// Satisfies admin.StateProvider.
func (m *Manager) State() interface{} {
	entries := m.Groups()
	now := time.Now().UTC()

	totalSessions := 0
	groups := make([]groupStateItem, 0, len(entries))
	for _, e := range entries {
		cfg := e.lb.Config()
		snaps, _ := e.lb.currentPool()
		sc := e.lb.tracker.Count()
		totalSessions += sc

		ups := make([]upstreamStateItem, 0, len(snaps))
		for _, s := range snaps {
			ups = append(ups, upstreamStateItem{
				ID:                  s.ID,
				Addr:                s.Addr,
				Healthy:             s.Healthy,
				Draining:            s.Draining,
				Priority:            s.Priority,
				Weight:              s.Weight,
				Active:              s.Active,
				EWMALatencyMS:       s.EWMALatency * 1000,
				TotalSessions:       s.TotalSessions,
				TotalFailures:       s.TotalFailures,
				ConsecutiveFailures: s.ConsecutiveFailures,
			})
		}

		groups = append(groups, groupStateItem{
			Name:         e.name,
			Strategy:     cfg.Strategy,
			Backpressure: string(cfg.Backpressure),
			ListenAddr:   cfg.ListenAddr,
			UDPEnabled:   cfg.UDPEnabled,
			MaxClients:   cfg.MaxClients,
			SessionCount: sc,
			Upstreams:    ups,
		})
	}

	return stateResponse{
		Version:      m.version,
		Commit:       m.commit,
		Now:          now.Format(time.RFC3339),
		SessionCount: totalSessions,
		Groups:       groups,
	}
}

// sessionItem is the JSON shape the admin UI expects per session.
type sessionItem struct {
	ClientAddr       string  `json:"client_addr"`
	UpstreamID       string  `json:"upstream_id"`
	UpstreamPriority int     `json:"upstream_priority"`
	AdmittedAt       string  `json:"admitted_at"`
	AgeMS            float64 `json:"age_ms"`
}

// sessionsResponse is the JSON payload for GET /admin/api/sessions.
type sessionsResponse struct {
	Sessions []sessionItem `json:"sessions"`
}

// ActiveSessions returns a JSON-serialisable snapshot of active sessions.
// Satisfies admin.SessionsProvider.
func (m *Manager) ActiveSessions() interface{} {
	snaps := m.Sessions()
	now := time.Now()
	items := make([]sessionItem, 0, len(snaps))
	for _, s := range snaps {
		items = append(items, sessionItem{
			ClientAddr:       s.ClientAddr,
			UpstreamID:       s.UpstreamID,
			UpstreamPriority: s.UpstreamPrio,
			AdmittedAt:       s.AdmittedAt.UTC().Format(time.RFC3339Nano),
			AgeMS:            float64(now.Sub(s.AdmittedAt).Milliseconds()),
		})
	}
	return sessionsResponse{Sessions: items}
}

// SetUpstreamDrain sets or clears the drain flag on an upstream by ID.
// Satisfies admin.DrainController.
func (m *Manager) SetUpstreamDrain(id string, drain bool) bool {
	return m.DrainUpstream(id, drain)
}
