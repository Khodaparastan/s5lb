package balancer

import (
	"context"
	"log/slog"
	"time"

	"github.com/khodaparastan/socks5lb/internal/strategy"
	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// pick runs the active selector against a snapshot of the pool and returns
// an upstream pointer + success flag. On success the upstream's Active
// counter has been atomically incremented.
func (lb *LoadBalancer) pick(sc strategy.SelectCtx) (*upstream.Upstream, bool) {
	cfg := lb.Config()
	snaps, idx := lb.currentPool()
	id := lb.selector.Pick(sc, snaps, cfg.MaxPerProxy)
	if id == "" {
		return nil, false
	}
	u := idx[id]
	if u == nil {
		return nil, false
	}
	if !u.State.IncActive(cfg.MaxPerProxy) {
		// Racing loser — selector saw headroom but another goroutine took
		// the last slot. Bail out; caller will either retry via the queue
		// or return a failure.
		return nil, false
	}
	addr := u.Addr()
	lb.metrics.UpActive.WithLabelValues(addr).Inc()
	lb.metrics.UpSelected.WithLabelValues(addr).Inc()
	lb.metrics.UpSessions.WithLabelValues(addr).Inc()
	return u, true
}

// acquireSlot returns an upstream or nil on context cancel / queue timeout.
// Increments per-upstream Active; caller MUST call releaseSlot.
func (lb *LoadBalancer) acquireSlot(ctx context.Context, sc strategy.SelectCtx, log *slog.Logger) *upstream.Upstream {
	start := time.Now()
	cfg := lb.Config()

	if u, ok := lb.pick(sc); ok {
		lb.metrics.QueueWaitSec.Observe(0)
		return u
	}
	// Enqueue.
	w := &waiter{ticket: make(chan *upstream.Upstream, 1), sc: sc}
	lb.qmu.Lock()
	lb.queue = append(lb.queue, w)
	qd := len(lb.queue)
	lb.qmu.Unlock()

	lb.metrics.QueueDepth.Set(float64(qd))
	log.Debug("queued", "queue_depth", qd, "strategy", lb.selector.Name())

	var timeout <-chan time.Time
	if cfg.QueueWaitTimeout > 0 {
		t := time.NewTimer(cfg.QueueWaitTimeout)
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

	lb.qmu.Lock()
	lb.metrics.QueueDepth.Set(float64(len(lb.queue)))
	lb.qmu.Unlock()
	lb.metrics.QueueWaitSec.Observe(time.Since(start).Seconds())

	if out != nil {
		log.Debug("dispatched",
			"waited_ms", time.Since(start).Milliseconds(),
			"upstream", out.Addr(),
		)
	}
	return out
}

// dropWaiter removes a waiter that gave up. If it was already dispatched,
// the slot is returned to the pool.
func (lb *LoadBalancer) dropWaiter(w *waiter) {
	lb.qmu.Lock()
	for i, x := range lb.queue {
		if x == w {
			lb.queue = append(lb.queue[:i], lb.queue[i+1:]...)
			lb.qmu.Unlock()
			return
		}
	}
	lb.qmu.Unlock()

	select {
	case u := <-w.ticket:
		lb.releaseSlot(u)
	default:
	}
}

// releaseSlot returns an upstream slot and wakes queued waiters.
func (lb *LoadBalancer) releaseSlot(u *upstream.Upstream) {
	u.State.DecActive()
	lb.metrics.UpActive.WithLabelValues(u.Addr()).Dec()
	lb.dispatch()
}

// dispatch hands out slots to queued waiters in FIFO order.
func (lb *LoadBalancer) dispatch() {
	for {
		lb.qmu.Lock()
		if len(lb.queue) == 0 {
			lb.qmu.Unlock()
			return
		}
		head := lb.queue[0]
		lb.qmu.Unlock()

		u, ok := lb.pick(head.sc)
		if !ok {
			return
		}

		lb.qmu.Lock()
		// Re-verify head hasn't changed (a concurrent dropWaiter may have
		// removed it). If so, put the reservation back.
		if len(lb.queue) == 0 || lb.queue[0] != head {
			lb.qmu.Unlock()
			lb.releaseSlot(u)
			continue
		}
		lb.queue = lb.queue[1:]
		qd := len(lb.queue)
		lb.qmu.Unlock()

		lb.metrics.QueueDepth.Set(float64(qd))
		head.ticket <- u
	}
}
