package balancer

import (
	"context"
	"net"
	"time"

	"github.com/khodaparastan/socks5lb/internal/socks5"
	"github.com/khodaparastan/socks5lb/internal/upstream"
)

func (lb *LoadBalancer) markUnhealthyLocked(u *upstream.Upstream, reason string) {
	if u.Healthy {
		u.Healthy = false
		u.LastFailureTS = time.Now()
		lb.metrics.UpHealthy.WithLabelValues(u.Addr()).Set(0)
		lb.log.Warn("upstream_unhealthy", "upstream", u.Addr(), "reason", reason)
	}
}

func (lb *LoadBalancer) markHealthyLocked(u *upstream.Upstream) {
	if !u.Healthy {
		u.Healthy = true
		u.ConsecutiveFailures = 0
		u.FirstFailureTS = time.Time{}
		lb.metrics.UpHealthy.WithLabelValues(u.Addr()).Set(1)
		lb.log.Info("upstream_healthy", "upstream", u.Addr())
		lb.dispatchLocked()
	}
}

func (lb *LoadBalancer) recordSuccessLocked(u *upstream.Upstream) {
	if u.ConsecutiveFailures > 0 {
		u.ConsecutiveFailures = 0
		u.FirstFailureTS = time.Time{}
	}
}

func (lb *LoadBalancer) recordFailureLocked(u *upstream.Upstream, stage, reason string) {
	now := time.Now()
	u.TotalFailures.Add(1)
	lb.metrics.UpFailures.WithLabelValues(u.Addr(), stage).Inc()

	if u.ConsecutiveFailures > 0 && now.Sub(u.FirstFailureTS) > lb.cfg.FailureWindow {
		u.ConsecutiveFailures = 0
	}
	if u.ConsecutiveFailures == 0 {
		u.FirstFailureTS = now
	}
	u.ConsecutiveFailures++

	lb.log.Warn("upstream_failure",
		"upstream", u.Addr(),
		"stage", stage,
		"reason", reason,
		"consecutive", u.ConsecutiveFailures,
		"threshold", lb.cfg.FailureThreshold,
	)

	if u.Healthy && u.ConsecutiveFailures >= lb.cfg.FailureThreshold {
		lb.markUnhealthyLocked(u, "circuit breaker tripped")
	}
}

func (lb *LoadBalancer) recordSuccess(u *upstream.Upstream) {
	lb.mu.Lock()
	lb.recordSuccessLocked(u)
	lb.mu.Unlock()
}
func (lb *LoadBalancer) recordFailure(u *upstream.Upstream, stage, reason string) {
	lb.mu.Lock()
	lb.recordFailureLocked(u, stage, reason)
	lb.mu.Unlock()
}

func (lb *LoadBalancer) probe(u *upstream.Upstream) error {
	start := time.Now()
	result := "ok"
	defer func() {
		lb.metrics.UpProbeSec.
			WithLabelValues(u.Addr(), result).
			Observe(time.Since(start).Seconds())
	}()

	ctx, cancel := context.WithTimeout(lb.ctx, lb.cfg.ConnectTimeout)
	defer cancel()

	d := net.Dialer{Timeout: lb.cfg.ConnectTimeout}
	conn, err := d.DialContext(ctx, "tcp", u.Addr())
	if err != nil {
		result = "dial_err"
		return err
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(lb.cfg.HandshakeTimeout))
	if err := socks5.ClientHandshake(conn, u.Username, u.Password); err != nil {
		result = "handshake_err"
		return err
	}
	return nil
}

func (lb *LoadBalancer) healthLoop() {
	t := time.NewTicker(lb.cfg.HealthInterval)
	defer t.Stop()
	for {
		select {
		case <-lb.ctx.Done():
			return
		case <-t.C:
			lb.runHealthChecks()
		}
	}
}

func (lb *LoadBalancer) runHealthChecks() {
	lb.mu.Lock()
	pool := append([]*upstream.Upstream(nil), lb.upstreams...)
	lb.mu.Unlock()

	for _, u := range pool {
		lb.mu.Lock()
		healthy := u.Healthy
		lastFail := u.LastFailureTS
		lb.mu.Unlock()

		if !healthy && time.Since(lastFail) < lb.cfg.RetryBackoff {
			continue
		}

		err := lb.probe(u)

		lb.mu.Lock()
		if err == nil {
			lb.recordSuccessLocked(u)
			lb.markHealthyLocked(u)
			lb.log.Debug("probe_ok", "upstream", u.Addr())
		} else {
			lb.recordFailureLocked(u, "probe", err.Error())
		}
		lb.mu.Unlock()
	}

	for _, u := range pool {
		lb.metrics.UpLatencyEWMA.WithLabelValues(u.Addr()).Set(u.EWMALatency())
	}
}
