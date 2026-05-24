package upstream

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const ewmaAlpha = 0.2

// State holds all mutable per-upstream runtime state.
// Every method is safe for concurrent use.
type State struct {
	mu sync.RWMutex

	active              int
	healthy             bool
	draining            bool
	lastFailureTS       time.Time
	firstFailureTS      time.Time
	consecutiveFailures int

	// Lock-free counters.
	totalSessions atomic.Uint64
	totalFailures atomic.Uint64

	// EWMA of (dial+handshake) latency in seconds, stored as float64 bits.
	ewmaBits atomic.Uint64
}

// NewState returns a State that starts healthy (will be flipped by the
// first health probe if unreachable).
func NewState() *State {
	return &State{healthy: true}
}

// Snapshot is an immutable view of State at a point in time, passed to
// selectors so strategy code is a pure function of snapshots.
type Snapshot struct {
	ID       string
	Addr     string
	Weight   int
	Priority int

	Active              int
	Healthy             bool
	Draining            bool
	ConsecutiveFailures int
	EWMALatency         float64 // seconds
	TotalSessions       uint64
	TotalFailures       uint64
}

// Snapshot captures the current state. Cheap; takes only an RLock.
func (u *Upstream) Snapshot() Snapshot {
	s := u.State
	s.mu.RLock()
	snap := Snapshot{
		ID:                  u.ID,
		Addr:                u.Addr(),
		Weight:              u.Weight,
		Priority:            u.Priority,
		Active:              s.active,
		Healthy:             s.healthy,
		Draining:            s.draining,
		ConsecutiveFailures: s.consecutiveFailures,
	}
	s.mu.RUnlock()
	snap.EWMALatency = u.EWMALatency()
	snap.TotalSessions = s.totalSessions.Load()
	snap.TotalFailures = s.totalFailures.Load()
	return snap
}

// --- Active counter ---------------------------------------------------------

// IncActive attempts to reserve a slot. Returns true on success.
// Atomic with respect to the cap check.
func (s *State) IncActive(max int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.healthy || s.draining {
		return false
	}
	if s.active >= max {
		return false
	}
	s.active++
	s.totalSessions.Add(1)
	return true
}

// DecActive releases a slot. Never drops below zero.
func (s *State) DecActive() {
	s.mu.Lock()
	if s.active > 0 {
		s.active--
	}
	s.mu.Unlock()
}

// Active returns the current active count.
func (s *State) Active() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active
}

// --- Health -----------------------------------------------------------------

// Healthy returns the current health flag.
func (s *State) Healthy() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.healthy
}

// LastFailure returns the timestamp of the last observed failure.
func (s *State) LastFailure() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastFailureTS
}

// MarkHealthy flips the upstream healthy and resets failure counters.
// Returns true if a transition occurred.
func (s *State) MarkHealthy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.healthy {
		return false
	}
	s.healthy = true
	s.consecutiveFailures = 0
	s.firstFailureTS = time.Time{}
	return true
}

// MarkUnhealthy flips the upstream unhealthy.
// Returns true if a transition occurred.
func (s *State) MarkUnhealthy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.healthy {
		return false
	}
	s.healthy = false
	s.lastFailureTS = time.Now()
	return true
}

// RecordSuccess clears consecutive-failure counters.
func (s *State) RecordSuccess() {
	s.mu.Lock()
	if s.consecutiveFailures > 0 {
		s.consecutiveFailures = 0
		s.firstFailureTS = time.Time{}
	}
	s.mu.Unlock()
}

// RecordFailure increments failure counters under a sliding window.
// Returns (newConsecutive, thresholdTripped).
func (s *State) RecordFailure(threshold int, window time.Duration) (int, bool) {
	s.totalFailures.Add(1)
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.consecutiveFailures > 0 && now.Sub(s.firstFailureTS) > window {
		s.consecutiveFailures = 0
	}
	if s.consecutiveFailures == 0 {
		s.firstFailureTS = now
	}
	s.consecutiveFailures++
	s.lastFailureTS = now

	tripped := s.healthy && s.consecutiveFailures >= threshold
	if tripped {
		s.healthy = false
	}
	return s.consecutiveFailures, tripped
}

// --- Drain -----------------------------------------------------------------

// SetDrain sets or clears the draining flag. A draining upstream will not
// accept new sessions but existing ones are unaffected.
func (s *State) SetDrain(drain bool) {
	s.mu.Lock()
	s.draining = drain
	s.mu.Unlock()
}

// Draining returns true when the upstream is in drain mode.
func (s *State) Draining() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.draining
}

// --- Latency EWMA (lock-free) ----------------------------------------------

// EWMALatency returns the current EWMA in seconds. Zero if unmeasured.
func (u *Upstream) EWMALatency() float64 {
	return math.Float64frombits(u.State.ewmaBits.Load())
}

// ObserveLatency merges a new sample into the EWMA.
func (u *Upstream) ObserveLatency(d time.Duration) {
	sample := d.Seconds()
	for {
		prev := u.State.ewmaBits.Load()
		cur := math.Float64frombits(prev)
		var next float64
		if cur == 0 {
			next = sample
		} else {
			next = ewmaAlpha*sample + (1-ewmaAlpha)*cur
		}
		if u.State.ewmaBits.CompareAndSwap(prev, math.Float64bits(next)) {
			return
		}
	}
}
