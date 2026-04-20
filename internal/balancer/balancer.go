// Package balancer wires upstream selection, admission, queueing, health,
// and session handling together behind a single Serve/Shutdown/Reload surface.
package balancer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/khodaparastan/socks5lb/internal/admission"
	"github.com/khodaparastan/socks5lb/internal/config"
	"github.com/khodaparastan/socks5lb/internal/metrics"
	"github.com/khodaparastan/socks5lb/internal/socks5"
	"github.com/khodaparastan/socks5lb/internal/strategy"
	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// LoadBalancer is the proxy engine.
type LoadBalancer struct {
	log     *slog.Logger
	metrics *metrics.Metrics
	tracer  trace.Tracer

	// Config is atomically swapped on reload. Readers take cfgMu.RLock().
	cfgMu sync.RWMutex
	cfg   config.Config

	selector strategy.Selector

	// Upstream pool is swapped on reload. Readers take poolMu.RLock().
	poolMu    sync.RWMutex
	upstreams []*upstream.Upstream
	byID      map[string]*upstream.Upstream

	// Queue state (FIFO waiters).
	qmu   sync.Mutex
	queue []*waiter

	// Admission gate + session tracker.
	tracker *admission.Tracker
	gate    admission.Gate

	// Active conn tracking for forced shutdown.
	connsMu sync.Mutex
	conns   map[net.Conn]struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// configPath is remembered for SIGHUP reload.
	configPath string
}

// waiter is a queued request waiting for an upstream slot.
type waiter struct {
	ticket chan *upstream.Upstream
	sc     strategy.SelectCtx
}

// New constructs a LoadBalancer.
func New(
	cfg config.Config,
	configPath string,
	log *slog.Logger,
	m *metrics.Metrics,
	tracer trace.Tracer,
	ups []*upstream.Upstream,
	sel strategy.Selector,
) *LoadBalancer {
	ctx, cancel := context.WithCancel(context.Background())

	tracker := admission.NewTracker()
	gate := admission.New(cfg, log, tracker)

	lb := &LoadBalancer{
		log:        log,
		metrics:    m,
		tracer:     tracer,
		cfg:        cfg,
		configPath: configPath,
		selector:   sel,
		tracker:    tracker,
		gate:       gate,
		conns:      make(map[net.Conn]struct{}),
		ctx:        ctx,
		cancel:     cancel,
	}
	lb.setUpstreams(ups)

	m.StrategyInfo.WithLabelValues(sel.Name()).Set(1)
	m.BackpressureInfo.WithLabelValues(string(cfg.Backpressure)).Set(1)
	return lb
}

// setUpstreams atomically replaces the pool. Called from New and Reload.
func (lb *LoadBalancer) setUpstreams(ups []*upstream.Upstream) {
	// Sort by priority ascending for priority-failover compatibility.
	sort.SliceStable(ups, func(i, j int) bool {
		return ups[i].Priority < ups[j].Priority
	})
	idx := make(map[string]*upstream.Upstream, len(ups))
	for _, u := range ups {
		u.NormalizeID()
		idx[u.ID] = u
		lb.metrics.UpActive.WithLabelValues(u.Addr()).Set(float64(u.State.Active()))
		lb.metrics.UpHealthy.WithLabelValues(u.Addr()).Set(boolToFloat(u.State.Healthy()))
	}
	lb.poolMu.Lock()
	lb.upstreams = ups
	lb.byID = idx
	lb.poolMu.Unlock()
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// Config returns a snapshot of the current config.
func (lb *LoadBalancer) Config() config.Config {
	lb.cfgMu.RLock()
	defer lb.cfgMu.RUnlock()
	return lb.cfg
}

// --- admin.ReadinessProber --------------------------------------------------

// AnyHealthy reports whether at least one upstream is currently healthy.
func (lb *LoadBalancer) AnyHealthy() bool {
	lb.poolMu.RLock()
	defer lb.poolMu.RUnlock()
	for _, u := range lb.upstreams {
		if u.State.Healthy() {
			return true
		}
	}
	return false
}

// --- conn tracking ----------------------------------------------------------

func (lb *LoadBalancer) track(c net.Conn) {
	lb.connsMu.Lock()
	lb.conns[c] = struct{}{}
	lb.connsMu.Unlock()
}
func (lb *LoadBalancer) untrack(c net.Conn) {
	lb.connsMu.Lock()
	delete(lb.conns, c)
	lb.connsMu.Unlock()
}
func (lb *LoadBalancer) closeAllConns() int {
	lb.connsMu.Lock()
	defer lb.connsMu.Unlock()
	n := len(lb.conns)
	for c := range lb.conns {
		_ = c.Close()
	}
	return n
}

// --- accept loop ------------------------------------------------------------

// Serve binds the frontend listener and accepts clients until context cancel.
// Returns nil on graceful shutdown, or a bind error if the listener fails.
func (lb *LoadBalancer) Serve() error {
	cfg := lb.Config()

	lc := net.ListenConfig{}
	listener, err := lc.Listen(lb.ctx, "tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("bind %s: %w", cfg.ListenAddr, err)
	}
	lb.log.Info("listening",
		"addr", cfg.ListenAddr,
		"upstreams", len(lb.upstreams),
		"strategy", lb.selector.Name(),
		"backpressure", string(cfg.Backpressure),
		"udp_enabled", cfg.UDPEnabled,
	)

	lb.wg.Add(1)
	go func() { defer lb.wg.Done(); lb.healthLoop() }()

	// Listener closer on ctx cancel.
	go func() {
		<-lb.ctx.Done()
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if lb.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				lb.log.Warn("accept_timeout", "err", err.Error())
				time.Sleep(50 * time.Millisecond)
				continue
			}
			lb.log.Warn("accept_error", "err", err.Error())
			time.Sleep(100 * time.Millisecond)
			continue
		}
		lb.metrics.AcceptedTotal.Inc()

		// Admission gate.
		dec := lb.gate.Acquire(lb.ctx)
		lb.metrics.AdmissionInFly.Set(float64(lb.gate.InFlight()))
		switch dec.Outcome {
		case admission.Admitted, admission.Evicted:
			if dec.Outcome == admission.Evicted {
				lb.metrics.BackpressureEvict.Inc()
			}
			sess := &admission.Session{
				Conn:       conn,
				AdmittedAt: time.Now(),
			}
			lb.tracker.Add(sess)

			lb.wg.Add(1)
			go func() {
				defer lb.wg.Done()
				defer func() {
					lb.tracker.Release(sess)
					lb.gate.Release()
					lb.metrics.AdmissionInFly.Set(float64(lb.gate.InFlight()))
				}()
				lb.handleClient(conn, sess)
			}()

		case admission.Rejected:
			lb.metrics.RejectedTotal.WithLabelValues(dec.Reason).Inc()
			lb.log.Warn("rejected",
				"reason", dec.Reason,
				"remote", conn.RemoteAddr().String(),
			)
			_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
			_, _ = conn.Write(socks5.ReplyBytes(socks5.RepGeneralFailure))
			_ = conn.Close()
		}
	}
}

// --- shutdown ---------------------------------------------------------------

// Shutdown performs a two-phase drain:
//
//  1. Cancel ctx -> listener closes, health loop stops, no new admissions.
//     Wait up to DrainSoftTimeout for in-flight sessions to finish naturally.
//  2. Force-close all tracked conns. Wait up to DrainHardTimeout for
//     goroutines to exit.
func (lb *LoadBalancer) Shutdown() {
	cfg := lb.Config()
	lb.log.Info("shutdown_begin",
		"drain_soft", cfg.DrainSoftTimeout.String(),
		"drain_hard", cfg.DrainHardTimeout.String(),
	)
	lb.cancel()

	done := make(chan struct{})
	go func() { lb.wg.Wait(); close(done) }()

	// Phase 1.
	select {
	case <-done:
		lb.log.Info("shutdown_drained_softly")
		return
	case <-time.After(cfg.DrainSoftTimeout):
	}

	// Phase 2.
	n := lb.closeAllConns()
	lb.log.Warn("shutdown_force_closing", "connections", n)

	select {
	case <-done:
		lb.log.Info("shutdown_complete")
	case <-time.After(cfg.DrainHardTimeout):
		lb.log.Error("shutdown_hard_timeout_exceeded",
			"remaining", lb.tracker.Count(),
		)
	}
}

// --- internal helpers available to session code -----------------------------

// currentPool returns a snapshot slice suitable for selectors.
func (lb *LoadBalancer) currentPool() ([]upstream.Snapshot, map[string]*upstream.Upstream) {
	lb.poolMu.RLock()
	pool := lb.upstreams
	idx := lb.byID
	lb.poolMu.RUnlock()

	snaps := make([]upstream.Snapshot, 0, len(pool))
	for _, u := range pool {
		snaps = append(snaps, u.Snapshot())
	}
	return snaps, idx
}
