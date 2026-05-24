package balancer

import (
	"context"
	"log/slog"
	"time"

	"github.com/khodaparastan/socks5lb/internal/strategy"
	"github.com/khodaparastan/socks5lb/internal/upstream"
)

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
		return nil, false
	}

	addr := u.Addr()
	lb.metrics.UpActive.WithLabelValues(addr).Inc()
	lb.metrics.UpSelected.WithLabelValues(addr).Inc()
	lb.metrics.UpSessions.WithLabelValues(addr).Inc()

	return u, true
}

func (lb *LoadBalancer) acquireSlot(
	ctx context.Context,
	sc strategy.SelectCtx,
	log *slog.Logger,
) *upstream.Upstream {
	start := time.Now()
	cfg := lb.Config()

	if u, ok := lb.pick(sc); ok {
		lb.metrics.QueueWaitSec.Observe(0)
		return u
	}

	w := &waiter{
		ticket: make(chan *upstream.Upstream, 1),
		sc:     sc,
	}

	lb.qmu.Lock()
	lb.queue = append(lb.queue, w)
	qd := len(lb.queue)
	lb.metrics.QueueDepth.Set(float64(qd))
	lb.qmu.Unlock()

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
		lb.cancelWaiter(w)

	case <-lb.ctx.Done():
		lb.cancelWaiter(w)

	case <-timeout:
		lb.cancelWaiter(w)
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

func (lb *LoadBalancer) cancelWaiter(w *waiter) {
	var released *upstream.Upstream

	lb.qmu.Lock()
	w.canceled = true

	for i, x := range lb.queue {
		if x == w {
			lb.queue = append(lb.queue[:i], lb.queue[i+1:]...)
			lb.qmu.Unlock()
			return
		}
	}

	select {
	case released = <-w.ticket:
	default:
	}

	lb.qmu.Unlock()

	if released != nil {
		lb.releaseSlot(released)
	}
}

func (lb *LoadBalancer) releaseSlot(u *upstream.Upstream) {
	u.State.DecActive()
	lb.metrics.UpActive.WithLabelValues(u.Addr()).Dec()
	lb.dispatch()
}

func (lb *LoadBalancer) dispatch() {
	for {
		lb.qmu.Lock()
		if len(lb.queue) == 0 {
			lb.qmu.Unlock()
			return
		}

		head := lb.queue[0]
		if head.canceled {
			lb.queue = lb.queue[1:]
			lb.metrics.QueueDepth.Set(float64(len(lb.queue)))
			lb.qmu.Unlock()
			continue
		}
		lb.qmu.Unlock()

		u, ok := lb.pick(head.sc)
		if !ok {
			return
		}

		var deliver bool

		lb.qmu.Lock()
		if len(lb.queue) > 0 && lb.queue[0] == head && !head.canceled {
			lb.queue = lb.queue[1:]
			lb.metrics.QueueDepth.Set(float64(len(lb.queue)))

			select {
			case head.ticket <- u:
				deliver = true
			default:
				deliver = false
			}
		}
		lb.qmu.Unlock()

		if !deliver {
			lb.releaseSlot(u)
			continue
		}
	}
}
