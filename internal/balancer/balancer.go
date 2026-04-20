// Package balancer wires upstream selection, queueing, health, and session
// handling together behind a single Serve/Shutdown surface.
package balancer

import (
	"context"
	"fmt"
	"log/slog"
	mathrand "math/rand"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/khodaparastan/socks5lb/internal/config"
	"github.com/khodaparastan/socks5lb/internal/metrics"
	"github.com/khodaparastan/socks5lb/internal/socks5"
	"github.com/khodaparastan/socks5lb/internal/strategy"
	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// LoadBalancer is the core proxy engine.
type LoadBalancer struct {
	cfg      config.Config
	log      *slog.Logger
	metrics  *metrics.Metrics
	selector strategy.Selector

	upstreams []*upstream.Upstream

	mu    sync.Mutex
	queue []*waiter

	admission chan struct{}

	connsMu sync.Mutex
	conns   map[net.Conn]struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	rng *mathrand.Rand
}

// waiter is a queued request awaiting an upstream slot. Carries the
// SelectCtx so dispatch honors affinity for hash-based strategies.
type waiter struct {
	ticket chan *upstream.Upstream
	sc     strategy.SelectCtx
}

// New builds a LoadBalancer. The upstream slice is sorted by Priority so that
// priority-failover works without additional state.
func New(cfg config.Config, log *slog.Logger, m *metrics.Metrics,
	upstreams []*upstream.Upstream, sel strategy.Selector) *LoadBalancer {

	sort.SliceStable(upstreams, func(i, j int) bool {
		return upstreams[i].Priority < upstreams[j].Priority
	})

	ctx, cancel := context.WithCancel(context.Background())
	lb := &LoadBalancer{
		cfg:       cfg,
		log:       log,
		metrics:   m,
		selector:  sel,
		upstreams: upstreams,
		admission: make(chan struct{}, cfg.MaxClients),
		conns:     make(map[net.Conn]struct{}),
		ctx:       ctx,
		cancel:    cancel,
		rng:       mathrand.New(mathrand.NewSource(time.Now().UnixNano())),
	}

	for _, u := range upstreams {
		m.UpActive.WithLabelValues(u.Addr()).Set(0)
		m.UpHealthy.WithLabelValues(u.Addr()).Set(boolToFloat(u.Healthy))
	}
	m.StrategyInfo.WithLabelValues(sel.Name()).Set(1)

	return lb
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// AnyHealthy reports whether at least one upstream is currently healthy.
// Implements admin.ReadinessProber.
func (lb *LoadBalancer) AnyHealthy() bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	for _, u := range lb.upstreams {
		if u.Healthy {
			return true
		}
	}
	return false
}

// --- Conn tracking (for forced shutdown close) ------------------------------

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

// --- Accept loop ------------------------------------------------------------

// Serve binds the listener and begins accepting clients.  Returns nil on
// graceful shutdown.
func (lb *LoadBalancer) Serve() error {
	lc := net.ListenConfig{}
	listener, err := lc.Listen(lb.ctx, "tcp", lb.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("bind %s: %w", lb.cfg.ListenAddr, err)
	}
	lb.log.Info("listening",
		"addr", lb.cfg.ListenAddr,
		"upstreams", len(lb.upstreams),
		"strategy", lb.selector.Name(),
		"max_per_proxy", lb.cfg.MaxPerProxy,
		"max_clients", lb.cfg.MaxClients,
	)

	lb.wg.Add(1)
	go func() { defer lb.wg.Done(); lb.healthLoop() }()

	go func() { <-lb.ctx.Done(); _ = listener.Close() }()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if lb.ctx.Err() != nil {
				return nil
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() { //nolint:staticcheck
				lb.log.Warn("accept_temporary", "err", err.Error())
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return err
		}
		lb.metrics.AcceptedTotal.Inc()

		select {
		case lb.admission <- struct{}{}:
			lb.metrics.AdmissionInFly.Set(float64(len(lb.admission)))
			lb.wg.Add(1)
			go func() {
				defer lb.wg.Done()
				defer func() {
					<-lb.admission
					lb.metrics.AdmissionInFly.Set(float64(len(lb.admission)))
				}()
				lb.handleClient(conn)
			}()
		default:
			lb.metrics.RejectedTotal.WithLabelValues("admission_full").Inc()
			lb.log.Warn("rejected_admission_full",
				"remote", conn.RemoteAddr().String(),
				"cap", lb.cfg.MaxClients)
			_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
			_, _ = conn.Write(socks5.ReplyBytes(socks5.RepGeneralFailure))
			_ = conn.Close()
		}
	}
}

// Shutdown cancels the context, waits for drain up to DrainTimeout, then
// force-closes anything still in flight.
func (lb *LoadBalancer) Shutdown() {
	lb.log.Info("shutdown_begin", "drain_timeout", lb.cfg.DrainTimeout.String())
	lb.cancel()

	done := make(chan struct{})
	go func() { lb.wg.Wait(); close(done) }()

	select {
	case <-done:
		lb.log.Info("shutdown_drained")
	case <-time.After(lb.cfg.DrainTimeout):
		n := lb.closeAllConns()
		lb.log.Warn("shutdown_force_closed", "connections", n)
		<-done
		lb.log.Info("shutdown_complete")
	}
}
