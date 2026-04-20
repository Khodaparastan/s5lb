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

// PickOldest returns the oldest live session or nil.
// Returned session is still in the list; caller must Close its Conn.
func (t *Tracker) PickOldest() *Session {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.head
}

// PickLowestPriority returns the oldest session whose UpstreamPrio is the
// numerically highest value (== lowest priority, since lower Priority = higher
// importance). Falls back to oldest if no session has an assignment yet.
func (t *Tracker) PickLowestPriority() *Session {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.head == nil {
		return nil
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
		return t.head
	}
	return worst
}
