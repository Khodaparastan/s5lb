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
	a := &Session{Conn: newFakeConn(), AdmittedAt: time.Now()}
	b := &Session{Conn: newFakeConn(), AdmittedAt: time.Now().Add(1 * time.Millisecond)}
	c := &Session{Conn: newFakeConn(), AdmittedAt: time.Now().Add(2 * time.Millisecond)}
	tr.Add(a)
	tr.Add(b)
	tr.Add(c)
	if tr.Count() != 3 {
		t.Fatalf("count=%d want 3", tr.Count())
	}
	if tr.PickOldest() != a {
		t.Fatal("oldest should be a")
	}
	tr.Release(a)
	if tr.PickOldest() != b {
		t.Fatal("oldest should be b after releasing a")
	}
	tr.Release(c)
	if tr.PickOldest() != b {
		t.Fatal("oldest should still be b")
	}
	tr.Release(b)
	if tr.Count() != 0 || tr.PickOldest() != nil {
		t.Fatal("tracker should be empty")
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

func TestTracker_PickLowestPriority(t *testing.T) {
	tr := NewTracker()
	a := &Session{Conn: newFakeConn()}
	b := &Session{Conn: newFakeConn()}
	c := &Session{Conn: newFakeConn()}
	tr.Add(a)
	tr.Add(b)
	tr.Add(c)
	tr.Update(a, "u1", 0)
	tr.Update(b, "u2", 5)
	tr.Update(c, "u3", 2)
	if tr.PickLowestPriority() != b {
		t.Fatal("should pick highest numeric priority (=lowest importance)")
	}
}
