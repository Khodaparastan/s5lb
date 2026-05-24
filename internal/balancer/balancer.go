package balancer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/khodaparastan/socks5lb/internal/admission"
	"github.com/khodaparastan/socks5lb/internal/config"
	"github.com/khodaparastan/socks5lb/internal/metrics"
	"github.com/khodaparastan/socks5lb/internal/socks5"
	"github.com/khodaparastan/socks5lb/internal/strategy"
	"github.com/khodaparastan/socks5lb/internal/transport"
	"github.com/khodaparastan/socks5lb/internal/upstream"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// LoadBalancer is the proxy engine.
type LoadBalancer struct {
	log     *slog.Logger
	metrics *metrics.Metrics
	tracer  trace.Tracer
	dialer  transport.Dialer

	cfgMu sync.RWMutex
	cfg   config.Config

	selector strategy.Selector

	poolMu    sync.RWMutex
	upstreams []*upstream.Upstream
	byID      map[string]*upstream.Upstream

	qmu   sync.Mutex
	queue []*waiter

	tracker *admission.Tracker
	gateVal atomic.Value // stores admission.Gate

	connsMu sync.Mutex
	conns   map[net.Conn]struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	configPath string
}

// waiter is a queued request waiting for an upstream slot.
type waiter struct {
	ticket   chan *upstream.Upstream
	sc       strategy.SelectCtx
	canceled bool
}

// New constructs a LoadBalancer. Returns an error if upstream IDs are invalid.
// Nil log, metrics, or tracer arguments are replaced with safe no-op defaults.
func New(
	cfg config.Config,
	configPath string,
	log *slog.Logger,
	m *metrics.Metrics,
	tracer trace.Tracer,
	ups []*upstream.Upstream,
	sel strategy.Selector,
) (*LoadBalancer, error) {
	if log == nil {
		log = slog.Default()
	}
	if tracer == nil {
		tracer = noop.NewTracerProvider().Tracer("")
	}

	ctx, cancel := context.WithCancel(context.Background())

	tracker := admission.NewTracker()

	lb := &LoadBalancer{
		log: log,
		dialer: transport.TCPDialer{
			Timeout:   cfg.ConnectTimeout,
			KeepAlive: 30 * time.Second,
		},
		metrics:    m,
		tracer:     tracer,
		cfg:        cfg,
		configPath: configPath,
		selector:   sel,
		tracker:    tracker,
		conns:      make(map[net.Conn]struct{}),
		ctx:        ctx,
		cancel:     cancel,
	}

	lb.setGate(admission.New(cfg, log, tracker))
	if err := lb.setUpstreams(ups); err != nil {
		cancel()
		return nil, fmt.Errorf("validate upstreams: %w", err)
	}

	if m != nil {
		m.StrategyInfo.WithLabelValues(sel.Name()).Set(1)
		m.BackpressureInfo.WithLabelValues(string(cfg.Backpressure)).Set(1)
	}

	return lb, nil
}

// SetDialer overrides the upstream transport dialer.
//
// Call this immediately after New and before Serve.
func (lb *LoadBalancer) SetDialer(d transport.Dialer) {
	if d == nil {
		return
	}

	lb.cfgMu.Lock()
	lb.dialer = d
	lb.cfgMu.Unlock()
}

// Dialer returns the active upstream transport dialer.
func (lb *LoadBalancer) Dialer() transport.Dialer {
	lb.cfgMu.RLock()
	defer lb.cfgMu.RUnlock()

	return lb.dialer
}

func (lb *LoadBalancer) currentGate() admission.Gate {
	g, _ := lb.gateVal.Load().(admission.Gate)
	return g
}

func (lb *LoadBalancer) setGate(g admission.Gate) {
	lb.gateVal.Store(g)
}

// setUpstreams atomically replaces the pool and preserves runtime state for
// upstreams with the same stable ID. Returns an error if duplicate IDs are found.
func (lb *LoadBalancer) setUpstreams(ups []*upstream.Upstream) error {
	// Normalize IDs first so we can check for duplicates.
	for _, u := range ups {
		u.NormalizeID()
	}

	// Validate uniqueness.
	seen := make(map[string]struct{}, len(ups))
	for _, u := range ups {
		if _, dup := seen[u.ID]; dup {
			return fmt.Errorf("duplicate upstream ID %q", u.ID)
		}
		seen[u.ID] = struct{}{}
	}

	// Copy before sorting so the caller's slice is not mutated.
	sorted := make([]*upstream.Upstream, len(ups))
	copy(sorted, ups)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Priority < sorted[j].Priority
	})

	lb.poolMu.Lock()
	oldByID := lb.byID

	idx := make(map[string]*upstream.Upstream, len(sorted))
	for _, u := range sorted {
		if old := oldByID[u.ID]; old != nil && old.State != nil {
			u.State = old.State
		}
		if u.State == nil {
			u.State = upstream.NewState()
		}

		idx[u.ID] = u

		lb.metrics.UpActive.WithLabelValues(u.Addr()).Set(float64(u.State.Active()))
		lb.metrics.UpHealthy.WithLabelValues(u.Addr()).Set(boolToFloat(u.State.Healthy()))
	}

	// Delete metrics for upstreams that are no longer present.
	for id, old := range oldByID {
		if _, stillPresent := idx[id]; !stillPresent {
			lb.metrics.UpActive.DeleteLabelValues(old.Addr())
			lb.metrics.UpHealthy.DeleteLabelValues(old.Addr())
			lb.metrics.UpSelected.DeleteLabelValues(old.Addr())
			lb.metrics.UpSessions.DeleteLabelValues(old.Addr())
			lb.metrics.UpFailures.DeleteLabelValues(old.Addr(), "probe")
			lb.metrics.UpFailures.DeleteLabelValues(old.Addr(), "connect")
			lb.metrics.UpFailures.DeleteLabelValues(old.Addr(), "handshake")
		}
	}

	lb.upstreams = sorted
	lb.byID = idx
	lb.poolMu.Unlock()
	return nil
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

// Serve binds the frontend listener and accepts clients until context cancel.
func (lb *LoadBalancer) Serve() error {
	cfg := lb.Config()

	lc := net.ListenConfig{}
	listener, err := lc.Listen(lb.ctx, "tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("bind %s: %w", cfg.ListenAddr, err)
	}

	snaps, _ := lb.currentPool()
	lb.log.Info("listening",
		"addr", cfg.ListenAddr,
		"upstreams", len(snaps),
		"strategy", lb.selector.Name(),
		"backpressure", string(cfg.Backpressure),
		"udp_enabled", cfg.UDPEnabled,
	)

	lb.wg.Add(1)
	go func() {
		defer lb.wg.Done()
		lb.healthLoop()
	}()

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
				t := time.NewTimer(50 * time.Millisecond)
				select {
				case <-t.C:
				case <-lb.ctx.Done():
					t.Stop()
					return nil
				}
				continue
			}

			lb.log.Warn("accept_error", "err", err.Error())
			t := time.NewTimer(100 * time.Millisecond)
			select {
			case <-t.C:
			case <-lb.ctx.Done():
				t.Stop()
				return nil
			}
			continue
		}

		lb.metrics.AcceptedTotal.Inc()

		gate := lb.currentGate()
		if gate == nil {
			_ = conn.Close()
			lb.metrics.RejectedTotal.WithLabelValues("admission_unavailable").Inc()
			continue
		}

		dec := gate.Acquire(lb.ctx)
		lb.metrics.AdmissionInFlight.Set(float64(gate.InFlight()))

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
			go func(g admission.Gate) {
				defer lb.wg.Done()
				defer func() {
					lb.tracker.Release(sess)
					g.Release()
					lb.metrics.AdmissionInFlight.Set(float64(g.InFlight()))
				}()

				lb.handleClient(conn, sess)
			}(gate)

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

// Shutdown performs a two-phase drain.
func (lb *LoadBalancer) Shutdown() {
	cfg := lb.Config()

	lb.log.Info("shutdown_begin",
		"drain_soft", cfg.DrainSoftTimeout.String(),
		"drain_hard", cfg.DrainHardTimeout.String(),
	)

	lb.cancel()

	done := make(chan struct{})
	go func() {
		lb.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		lb.log.Info("shutdown_drained_softly")
		return
	case <-time.After(cfg.DrainSoftTimeout):
	}

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

func (lb *LoadBalancer) currentPool() ([]upstream.Snapshot, map[string]*upstream.Upstream) {
	lb.poolMu.RLock()
	pool := append([]*upstream.Upstream(nil), lb.upstreams...)
	idx := lb.byID
	lb.poolMu.RUnlock()

	snaps := make([]upstream.Snapshot, 0, len(pool))
	for _, u := range pool {
		snaps = append(snaps, u.Snapshot())
	}

	return snaps, idx
}

// DrainUpstream sets or clears the drain flag on the upstream with the given ID.
// Returns false if no upstream with that ID exists.
func (lb *LoadBalancer) DrainUpstream(id string, drain bool) bool {
	lb.poolMu.RLock()
	u, ok := lb.byID[id]
	lb.poolMu.RUnlock()
	if !ok {
		return false
	}
	u.State.SetDrain(drain)
	return true
}

// Sessions returns a snapshot of all active sessions tracked by this balancer.
func (lb *LoadBalancer) Sessions() []admission.SessionSnapshot {
	return lb.tracker.Sessions()
}
