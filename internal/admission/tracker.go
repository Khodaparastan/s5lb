// Package admission owns the global in-flight gate and backpressure logic.
//
// A Session is anything the balancer wants tracked for potential eviction.
// The tracker maintains a doubly-linked-list ordering by admit time so
// "drop-oldest" is O(1), and an index by upstream-priority so
// "drop-lowest-priority" is O(n_sessions) in the worst case (acceptable for
// the session counts this proxy is designed for).
package admission

import (
	"net"
	"sync"
	"time"
)

// Session is the tracker's handle for a live admitted client session.
// Closing Conn triggers eviction; the session goroutine unwinds via its
// defer chain and calls Release.
type Session struct {
	Conn         net.Conn
	UpstreamID   string // may be empty before assignment
	UpstreamPrio int
	AdmittedAt   time.Time

	// intrusive list pointers; guarded by Tracker.mu
	prev, next *Session

	released bool
}

// Tracker is an ordered set of active sessions.
type Tracker struct {
	mu         sync.Mutex
	head, tail *Session // head=oldest, tail=newest
	count      int
}

// NewTracker returns an empty tracker.
func NewTracker() *Tracker { return &Tracker{} }

// Add appends a session. Caller must hold the returned *Session to release.
func (t *Tracker) Add(s *Session) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Reset released state in case this session is being re-added.
	s.released = false
	s.prev = t.tail
	s.next = nil
	if t.tail != nil {
		t.tail.next = s
	} else {
		t.head = s
	}
	t.tail = s
	t.count++
}

// Release removes the session from the tracker. Idempotent.
func (t *Tracker) Release(s *Session) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s.released {
		return
	}
	// Guard against releasing a session that was never added (count would go negative).
	if t.count <= 0 {
		s.released = true
		return
	}
	s.released = true
	if s.prev != nil {
		s.prev.next = s.next
	} else {
		t.head = s.next
	}
	if s.next != nil {
		s.next.prev = s.prev
	} else {
		t.tail = s.prev
	}
	s.prev, s.next = nil, nil
	t.count--
}

// Update refreshes the session's upstream assignment after acquireSlot.
func (t *Tracker) Update(s *Session, upID string, prio int) {
	t.mu.Lock()
	s.UpstreamID = upID
	s.UpstreamPrio = prio
	t.mu.Unlock()
}

// Count returns the current tracked session count.
func (t *Tracker) Count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.count
}

// SessionSnapshot is a point-in-time view of a tracked session.
type SessionSnapshot struct {
	ClientAddr   string
	UpstreamID   string
	UpstreamPrio int
	AdmittedAt   time.Time
}

// Sessions returns a snapshot of all currently tracked sessions, oldest first.
func (t *Tracker) Sessions() []SessionSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]SessionSnapshot, 0, t.count)
	for s := t.head; s != nil; s = s.next {
		addr := ""
		if s.Conn != nil {
			if ra := s.Conn.RemoteAddr(); ra != nil {
				addr = ra.String()
			}
		}
		out = append(out, SessionSnapshot{
			ClientAddr:   addr,
			UpstreamID:   s.UpstreamID,
			UpstreamPrio: s.UpstreamPrio,
			AdmittedAt:   s.AdmittedAt,
		})
	}
	return out
}

// Victim is an immutable snapshot of a session selected for eviction.
// All fields are copied while holding Tracker.mu, so they are safe to use
// without any lock after the call returns.
type Victim struct {
	Conn         net.Conn
	UpstreamID   string
	UpstreamPrio int
	AdmittedAt   time.Time
}

func victimFromSession(s *Session) Victim {
	return Victim{
		Conn:         s.Conn,
		UpstreamID:   s.UpstreamID,
		UpstreamPrio: s.UpstreamPrio,
		AdmittedAt:   s.AdmittedAt,
	}
}

// PickOldestVictim returns an immutable snapshot of the oldest live session.
// Returns (Victim{}, false) when there are no sessions.
func (t *Tracker) PickOldestVictim() (Victim, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.head == nil {
		return Victim{}, false
	}
	return victimFromSession(t.head), true
}

// PickLowestPriorityVictim returns a snapshot of the session whose UpstreamPrio
// is the numerically highest (== lowest importance). Falls back to oldest when
// no session has an upstream assignment yet.
// Returns (Victim{}, false) when there are no sessions.
func (t *Tracker) PickLowestPriorityVictim() (Victim, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.head == nil {
		return Victim{}, false
	}
	var worst *Session
	for s := t.head; s != nil; s = s.next {
		if s.UpstreamID == "" {
			continue
		}
		if worst == nil || s.UpstreamPrio > worst.UpstreamPrio {
			worst = s
		}
	}
	if worst == nil {
		worst = t.head
	}
	return victimFromSession(worst), true
}
