package admission

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/khodaparastan/socks5lb/internal/config"
)

// Outcome describes what the gate did with an admission attempt.
type Outcome int

const (
	Admitted Outcome = iota
	Rejected
	Evicted // admitted, but only after evicting another session
)

// Decision is the result of Acquire.
type Decision struct {
	Outcome Outcome
	Reason  string // populated on rejection
}

// Gate controls admission into the proxy. Implementations are safe for
// concurrent use.
type Gate interface {
	// Acquire attempts to admit a new session. On success, returns Admitted
	// (or Evicted if another session was kicked out). On failure, returns
	// Rejected with a reason label suitable for metrics.
	Acquire(ctx context.Context) Decision
	// Release gives a slot back.
	Release()
	// InFlight returns the current count of holders.
	InFlight() int
	// Capacity returns the configured cap.
	Capacity() int
}

// New constructs a Gate per the configured backpressure strategy.
// `tracker` is required for strategies that need to evict live sessions.
func New(cfg config.Config, log *slog.Logger, tracker *Tracker) Gate {
	base := &tokenGate{
		cap:    cfg.MaxClients,
		slots:  make(chan struct{}, cfg.MaxClients),
		log:    log,
		waitTO: cfg.AdmissionWaitTimeout,
	}
	base.inflight.Store(0)

	switch cfg.Backpressure {
	case config.BackpressureWait:
		return &waitGate{tokenGate: base}
	case config.BackpressureDropOldest:
		return &evictGate{tokenGate: base, tracker: tracker, pickOldest: true}
	case config.BackpressureDropLowestPriority:
		return &evictGate{tokenGate: base, tracker: tracker, pickOldest: false}
	default: // BackpressureReject
		return &rejectGate{tokenGate: base}
	}
}

// --- base ------------------------------------------------------------------

type tokenGate struct {
	cap      int
	slots    chan struct{}
	inflight atomic.Int64
	log      *slog.Logger
	waitTO   time.Duration
}

func (g *tokenGate) tryAcquire() bool {
	select {
	case g.slots <- struct{}{}:
		g.inflight.Add(1)
		return true
	default:
		return false
	}
}

func (g *tokenGate) waitAcquire(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		select {
		case g.slots <- struct{}{}:
			g.inflight.Add(1)
			return true
		case <-ctx.Done():
			return false
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case g.slots <- struct{}{}:
		g.inflight.Add(1)
		return true
	case <-ctx.Done():
		return false
	case <-t.C:
		return false
	}
}

func (g *tokenGate) Release() {
	select {
	case <-g.slots:
		g.inflight.Add(-1)
	default:
		// released more than acquired — programmer error; swallow
	}
}

func (g *tokenGate) InFlight() int { return int(g.inflight.Load()) }
func (g *tokenGate) Capacity() int { return g.cap }

// --- reject ----------------------------------------------------------------

type rejectGate struct{ *tokenGate }

func (g *rejectGate) Acquire(_ context.Context) Decision {
	if g.tryAcquire() {
		return Decision{Outcome: Admitted}
	}
	return Decision{Outcome: Rejected, Reason: "admission_full"}
}

// --- wait ------------------------------------------------------------------

type waitGate struct{ *tokenGate }

func (g *waitGate) Acquire(ctx context.Context) Decision {
	if g.tryAcquire() {
		return Decision{Outcome: Admitted}
	}
	if g.waitAcquire(ctx, g.waitTO) {
		return Decision{Outcome: Admitted}
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return Decision{Outcome: Rejected, Reason: "admission_canceled"}
	}
	return Decision{Outcome: Rejected, Reason: "admission_wait_timeout"}
}

// --- evict (drop-oldest / drop-lowest-priority) ----------------------------

type evictGate struct {
	*tokenGate
	tracker    *Tracker
	pickOldest bool
	evictMu    sync.Mutex // serializes concurrent eviction attempts
}

func (g *evictGate) Acquire(ctx context.Context) Decision {
	if g.tryAcquire() {
		return Decision{Outcome: Admitted}
	}

	// Guard against nil tracker (misconfiguration).
	if g.tracker == nil {
		g.log.Error("evict_gate_nil_tracker")
		if g.waitAcquire(ctx, g.waitTO) {
			return Decision{Outcome: Admitted}
		}
		return Decision{Outcome: Rejected, Reason: "evict_no_tracker"}
	}

	// Serialize victim selection + close so concurrent Acquire calls never
	// pick and close the same session twice.
	g.evictMu.Lock()
	var (
		victim Victim
		ok     bool
	)
	if g.pickOldest {
		victim, ok = g.tracker.PickOldestVictim()
	} else {
		victim, ok = g.tracker.PickLowestPriorityVictim()
	}
	g.evictMu.Unlock()

	if !ok {
		// Tracker is empty but gate is full — means in-flight sessions
		// aren't yet tracked (race during startup). Fall back to wait.
		if g.waitAcquire(ctx, g.waitTO) {
			return Decision{Outcome: Admitted}
		}
		return Decision{Outcome: Rejected, Reason: "evict_no_victim"}
	}
	g.log.Warn("backpressure_evict",
		"victim_upstream", victim.UpstreamID,
		"victim_admitted", victim.AdmittedAt.Format(time.RFC3339),
		"victim_prio", victim.UpstreamPrio,
	)
	// Close the victim outside the lock so it doesn't stall other evictions.
	_ = victim.Conn.Close()

	if g.waitAcquire(ctx, g.waitTO) {
		return Decision{Outcome: Evicted}
	}
	return Decision{Outcome: Rejected, Reason: "evict_wait_timeout"}
}
