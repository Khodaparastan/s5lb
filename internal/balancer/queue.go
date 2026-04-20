package balancer

import (
	"context"
	"log/slog"
	"time"

	"github.com/khodaparastan/socks5lb/internal/strategy"
	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// pickLocked runs the active selector against the current pool.
// Must be called with lb.mu held.
func (lb *LoadBalancer) pickLocked(sc strategy.SelectCtx) *upstream.Upstream {
	return lb.selector.Pick(sc, lb.upstreams, lb.cfg.MaxPerProxy)
}

// assignLocked increments counters after a successful assignment.
// Must be called with lb.mu held.
func (lb *LoadBalancer) assignLocked(u *upstream.Upstream) {
	u.Active++
	u.TotalSessions.Add(1)
	addr := u.Addr()
	lb.metrics.UpActive.WithLabelValues(addr).Inc()
	lb.metrics.UpSelected.WithLabelValues(addr).Inc()
	lb.metrics.UpSessions.WithLabelValues(addr).Inc()
}

// acquireSlot returns a reserved upstream or nil on timeout/shutdown.
// Increments per-upstream Active; caller MUST call releaseSlot.
func (lb *LoadBalancer) acquireSlot(ctx context.Context, sc strategy.SelectCtx, log *slog.Logger) *upstream.Upstream {
	start := time.Now()

	lb.mu.Lock()
	if u := lb.pickLocked(sc); u != nil {
		lb.assignLocked(u)
		lb.mu.Unlock()
		lb.metrics.QueueWaitSec.Observe(0)
		return u
	}
	w := &waiter{ticket: make(chan *upstream.Upstream, 1), sc: sc}
	lb.queue = append(lb.queue, w)
	qd := len(lb.queue)
	lb.mu.Unlock()

	lb.metrics.QueueDepth.Set(float64(qd))
	log.Info("queued", "queue_depth", qd, "strategy", lb.selector.Name())

	var timeout <-chan time.Time
	if lb.cfg.QueueWaitTimeout > 0 {
		t := time.NewTimer(lb.cfg.QueueWaitTimeout)
		defer t.Stop()
		timeout = t.C
	}

	var out *upstream.Upstream
	select {
	case u := <-w.ticket:
		out = u
	case <-ctx.Done():
		lb.dropWaiter(w)
	case <-lb.ctx.Done():
		lb.dropWaiter(w)
	case <-timeout:
		lb.dropWaiter(w)
		lb.metrics.RejectedTotal.WithLabelValues("queue_timeout").Inc()
		log.Warn("queue_timeout", "waited_ms", time.Since(start).Milliseconds())
	}

	lb.mu.Lock()
	lb.metrics.QueueDepth.Set(float64(len(lb.queue)))
	lb.mu.Unlock()
	lb.metrics.QueueWaitSec.Observe(time.Since(start).Seconds())

	if out != nil {
		log.Debug("dispatched",
			"waited_ms", time.Since(start).Milliseconds(),
			"upstream", out.Addr(),
			"strategy", lb.selector.Name())
	}
	return out
}

// dropWaiter removes a waiter that gave up (ctx/timeout).  If a slot was
// dispatched to it between our wake-up and cleanup, return it to the pool.
func (lb *LoadBalancer) dropWaiter(w *waiter) {
	lb.mu.Lock()
	for i, x := range lb.queue {
		if x == w {
			lb.queue = append(lb.queue[:i], lb.queue[i+1:]...)
			lb.mu.Unlock()
			return
		}
	}
	lb.mu.Unlock()

	select {
	case u := <-w.ticket:
		lb.releaseSlot(u)
	default:
	}
}

// releaseSlot returns an upstream slot and dispatches queued waiters.
func (lb *LoadBalancer) releaseSlot(u *upstream.Upstream) {
	lb.mu.Lock()
	if u.Active > 0 {
		u.Active--
		lb.metrics.UpActive.WithLabelValues(u.Addr()).Dec()
	}
	lb.dispatchLocked()
	lb.mu.Unlock()
}

// dispatchLocked hands out slots to queued waiters in FIFO order.  Each
// waiter is dispatched via its own SelectCtx so hash-based affinity is
// honored (best-effort when saturated).
func (lb *LoadBalancer) dispatchLocked() {
	for len(lb.queue) > 0 {
		head := lb.queue[0]
		u := lb.pickLocked(head.sc)
		if u == nil {
			break
		}
		lb.assignLocked(u)
		lb.queue = lb.queue[1:]
		head.ticket <- u
	}
	lb.metrics.QueueDepth.Set(float64(len(lb.queue)))
}
