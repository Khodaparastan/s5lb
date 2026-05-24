package balancer

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/khodaparastan/s5lb/internal/socks5"
	"github.com/khodaparastan/s5lb/internal/transport"
	"github.com/khodaparastan/s5lb/internal/upstream"
)

// markHealthy performs the state transition and dispatches any queued waiters.
func (lb *LoadBalancer) markHealthy(u *upstream.Upstream) {
	if u.State.MarkHealthy() {
		lb.metrics.UpHealthy.WithLabelValues(u.Addr()).Set(1)
		lb.log.Info("upstream_healthy", "upstream", u.Addr())
		lb.dispatch()
	}
}

// recordFailure increments failure counters and trips the breaker if needed.
func (lb *LoadBalancer) recordFailure(u *upstream.Upstream, stage, reason string) {
	cfg := lb.Config()
	lb.metrics.UpFailures.WithLabelValues(u.Addr(), stage).Inc()
	consec, tripped := u.State.RecordFailure(cfg.FailureThreshold, cfg.FailureWindow)
	lb.log.Warn("upstream_failure",
		"upstream", u.Addr(),
		"stage", stage,
		"reason", reason,
		"consecutive", consec,
		"threshold", cfg.FailureThreshold,
	)
	if tripped {
		lb.metrics.UpHealthy.WithLabelValues(u.Addr()).Set(0)
		lb.log.Warn("upstream_unhealthy",
			"upstream", u.Addr(),
			"reason", "circuit_breaker_tripped",
		)
	}
}

// recordSuccess clears consecutive-failure state.
func (lb *LoadBalancer) recordSuccess(u *upstream.Upstream) {
	u.State.RecordSuccess()
}

// --- active probing ---------------------------------------------------------

func (lb *LoadBalancer) probe(u *upstream.Upstream) error {
	cfg := lb.Config()
	start := time.Now()
	result := "ok"
	defer func() {
		lb.metrics.UpProbeSec.
			WithLabelValues(u.Addr(), result).
			Observe(time.Since(start).Seconds())
	}()

	ctx, cancel := context.WithTimeout(lb.ctx, cfg.ConnectTimeout)
	defer cancel()

	d := lb.Dialer()
	if d == nil {
		d = transport.TCPDialer{
			Timeout:   cfg.ConnectTimeout,
			KeepAlive: 30 * time.Second,
		}
	}

	conn, err := d.DialContext(ctx, "tcp", u.Addr())
	if err != nil {
		result = "dial_err"
		return err
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(cfg.HandshakeTimeout))
	if err := socks5.ClientHandshake(conn, u.Username, u.Password); err != nil {
		result = "handshake_err"
		return err
	}
	return nil
}

func (lb *LoadBalancer) healthLoop() {
	t := time.NewTicker(lb.Config().HealthInterval)
	defer t.Stop()
	for {
		select {
		case <-lb.ctx.Done():
			return
		case <-t.C:
			lb.runHealthChecks()
			// Pick up interval changes on reload.
			t.Reset(lb.Config().HealthInterval)
		}
	}
}

func (lb *LoadBalancer) runHealthChecks() {
	cfg := lb.Config()
	lb.poolMu.RLock()
	pool := append([]*upstream.Upstream(nil), lb.upstreams...)
	lb.poolMu.RUnlock()

	if len(pool) == 0 {
		return
	}

	// Bounded worker pool: at most min(len(pool), GOMAXPROCS*2) concurrent probes.
	workers := runtime.GOMAXPROCS(0) * 2
	if workers > len(pool) {
		workers = len(pool)
	}
	if workers < 1 {
		workers = 1
	}

	work := make(chan *upstream.Upstream, len(pool))
	for _, u := range pool {
		work <- u
	}
	close(work)

	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for u := range work {
				if !u.State.Healthy() && time.Since(u.State.LastFailure()) < cfg.RetryBackoff {
					continue
				}
				err := lb.probe(u)
				if err == nil {
					u.State.RecordSuccess()
					lb.markHealthy(u)
					lb.log.Debug("probe_ok", "upstream", u.Addr())
				} else {
					lb.recordFailure(u, "probe", err.Error())
				}
				// Refresh EWMA gauge regardless.
				lb.metrics.UpLatencyEWMA.WithLabelValues(u.Addr()).Set(u.EWMALatency())
			}
		}()
	}
	wg.Wait()
}
