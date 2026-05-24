package admission

import (
	"net"
	"testing"
	"time"
)

// fakeConn satisfies net.Conn; Close is a no-op.
type fakeConn struct{ net.Conn }

func (fakeConn) Close() error { return nil }
func newFakeConn() net.Conn   { return fakeConn{} }

func TestTracker_AddReleaseOrdering(t *testing.T) {
	tr := NewTracker()
	connA := newFakeConn()
	connB := newFakeConn()
	connC := newFakeConn()
	a := &Session{Conn: connA, AdmittedAt: time.Now()}
	b := &Session{Conn: connB, AdmittedAt: time.Now().Add(1 * time.Millisecond)}
	c := &Session{Conn: connC, AdmittedAt: time.Now().Add(2 * time.Millisecond)}
	tr.Add(a)
	tr.Add(b)
	tr.Add(c)
	if tr.Count() != 3 {
		t.Fatalf("count=%d want 3", tr.Count())
	}
	v, ok := tr.PickOldestVictim()
	if !ok || v.Conn != connA {
		t.Fatal("oldest should be a")
	}
	tr.Release(a)
	v, ok = tr.PickOldestVictim()
	if !ok || v.Conn != connB {
		t.Fatal("oldest should be b after releasing a")
	}
	tr.Release(c)
	v, ok = tr.PickOldestVictim()
	if !ok || v.Conn != connB {
		t.Fatal("oldest should still be b")
	}
	tr.Release(b)
	if tr.Count() != 0 {
		t.Fatal("tracker should be empty")
	}
	_, ok = tr.PickOldestVictim()
	if ok {
		t.Fatal("PickOldestVictim on empty tracker should return false")
	}
}

func TestTracker_ReleaseIdempotent(t *testing.T) {
	tr := NewTracker()
	s := &Session{Conn: newFakeConn()}
	tr.Add(s)
	tr.Release(s)
	tr.Release(s) // must not panic
	if tr.Count() != 0 {
		t.Fail()
	}
}

func TestTracker_ReleaseNeverAdded(t *testing.T) {
	tr := NewTracker()
	s := &Session{Conn: newFakeConn()}
	tr.Release(s) // must not panic or underflow count
	if tr.Count() != 0 {
		t.Fatalf("count=%d want 0 after releasing never-added session", tr.Count())
	}
}

func TestTracker_PickLowestPriorityVictim(t *testing.T) {
	tr := NewTracker()
	connA, connB, connC := newFakeConn(), newFakeConn(), newFakeConn()
	a := &Session{Conn: connA}
	b := &Session{Conn: connB}
	c := &Session{Conn: connC}
	tr.Add(a)
	tr.Add(b)
	tr.Add(c)
	tr.Update(a, "u1", 0)
	tr.Update(b, "u2", 5)
	tr.Update(c, "u3", 2)
	v, ok := tr.PickLowestPriorityVictim()
	if !ok || v.Conn != connB {
		t.Fatal("should pick highest numeric priority (=lowest importance)")
	}
}

func TestTracker_PickOldestVictim_Empty(t *testing.T) {
	tr := NewTracker()
	_, ok := tr.PickOldestVictim()
	if ok {
		t.Fatal("should return false on empty tracker")
	}
}

func TestTracker_PickLowestPriorityVictim_Empty(t *testing.T) {
	tr := NewTracker()
	_, ok := tr.PickLowestPriorityVictim()
	if ok {
		t.Fatal("should return false on empty tracker")
	}
}

func TestTracker_AddResetReleased(t *testing.T) {
	tr := NewTracker()
	s := &Session{Conn: newFakeConn()}
	tr.Add(s)
	tr.Release(s)
	if tr.Count() != 0 {
		t.Fatalf("count=%d want 0 after release", tr.Count())
	}
	// Re-adding a released session should work correctly.
	tr.Add(s)
	if tr.Count() != 1 {
		t.Fatalf("count=%d want 1 after re-add", tr.Count())
	}
	tr.Release(s)
	if tr.Count() != 0 {
		t.Fatalf("count=%d want 0 after second release", tr.Count())
	}
}
