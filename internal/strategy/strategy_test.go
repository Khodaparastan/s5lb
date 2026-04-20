package strategy

import (
	"net"
	"testing"

	"github.com/khodaparastan/socks5lb/internal/config"
	"github.com/khodaparastan/socks5lb/internal/upstream"
)

func mkSnap(id string, healthy bool, active, weight, prio int) upstream.Snapshot {
	return upstream.Snapshot{
		ID:       id,
		Addr:     id,
		Weight:   weight,
		Priority: prio,
		Active:   active,
		Healthy:  healthy,
	}
}

func TestLeastActive_BasicAndTieBreak(t *testing.T) {
	pool := []upstream.Snapshot{
		mkSnap("a", true, 5, 1, 0),
		mkSnap("b", true, 2, 1, 0),
		mkSnap("c", true, 2, 1, 0),
	}
	s := NewLeastActive()
	seen := map[string]int{}
	for i := 0; i < 200; i++ {
		id := s.Pick(SelectCtx{}, pool, 100)
		if id != "b" && id != "c" {
			t.Fatalf("expected tie between b/c, got %s", id)
		}
		seen[id]++
	}
	if seen["b"] == 0 || seen["c"] == 0 {
		t.Fatalf("tie-break not random: %+v", seen)
	}
}

func TestLeastActive_SkipsSaturatedAndUnhealthy(t *testing.T) {
	pool := []upstream.Snapshot{
		mkSnap("a", true, 100, 1, 0), // saturated
		mkSnap("b", false, 0, 1, 0),  // unhealthy
		mkSnap("c", true, 0, 1, 0),
	}
	s := NewLeastActive()
	if id := s.Pick(SelectCtx{}, pool, 100); id != "c" {
		t.Fatalf("got=%s want=c", id)
	}
}

func TestLeastActive_EmptyPool(t *testing.T) {
	s := NewLeastActive()
	if id := s.Pick(SelectCtx{}, nil, 10); id != "" {
		t.Fatalf("expected empty id, got %s", id)
	}
}

func TestRoundRobin_Rotates(t *testing.T) {
	pool := []upstream.Snapshot{
		mkSnap("a", true, 0, 1, 0),
		mkSnap("b", true, 0, 1, 0),
		mkSnap("c", true, 0, 1, 0),
	}
	s := NewRoundRobin()
	got := []string{
		s.Pick(SelectCtx{}, pool, 10),
		s.Pick(SelectCtx{}, pool, 10),
		s.Pick(SelectCtx{}, pool, 10),
		s.Pick(SelectCtx{}, pool, 10),
	}
	// Order is "a,b,c,a" starting from idx=0 after first Add.
	if got[3] != got[0] {
		t.Fatalf("RR not cyclic: %v", got)
	}
}

func TestWRR_Distribution(t *testing.T) {
	pool := []upstream.Snapshot{
		mkSnap("a", true, 0, 5, 0),
		mkSnap("b", true, 0, 1, 0),
	}
	s := NewWRR()
	counts := map[string]int{}
	for i := 0; i < 600; i++ {
		counts[s.Pick(SelectCtx{}, pool, 100)]++
	}
	ratio := float64(counts["a"]) / float64(counts["b"])
	if ratio < 4 || ratio > 6 {
		t.Fatalf("expected ~5:1, got %v (%v)", ratio, counts)
	}
}

func TestConsistentHash_Stable(t *testing.T) {
	pool := []upstream.Snapshot{
		mkSnap("a", true, 0, 1, 0),
		mkSnap("b", true, 0, 1, 0),
		mkSnap("c", true, 0, 1, 0),
	}
	s := NewConsistentHash()
	sc := SelectCtx{
		ClientAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 1234},
		HashKey:    config.HashClientIP,
	}
	first := s.Pick(sc, pool, 10)
	for i := 0; i < 50; i++ {
		if got := s.Pick(sc, pool, 10); got != first {
			t.Fatalf("unstable: got %s, first %s", got, first)
		}
	}
}

func TestConsistentHash_Failover(t *testing.T) {
	pool := []upstream.Snapshot{
		mkSnap("a", true, 0, 1, 0),
		mkSnap("b", true, 0, 1, 0),
		mkSnap("c", true, 0, 1, 0),
	}
	s := NewConsistentHash()
	sc := SelectCtx{
		ClientAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 1234},
		HashKey:    config.HashClientIP,
	}
	chosen := s.Pick(sc, pool, 10)
	// Mark chosen unhealthy; pick must fall through deterministically.
	for i := range pool {
		if pool[i].ID == chosen {
			pool[i].Healthy = false
		}
	}
	next := s.Pick(sc, pool, 10)
	if next == "" || next == chosen {
		t.Fatalf("failover broke: chosen=%s next=%s", chosen, next)
	}
	// Stable after failover.
	for i := 0; i < 20; i++ {
		if got := s.Pick(sc, pool, 10); got != next {
			t.Fatalf("unstable failover: %s vs %s", got, next)
		}
	}
}

func TestPriorityFailover(t *testing.T) {
	pool := []upstream.Snapshot{
		mkSnap("a", true, 0, 1, 0),
		mkSnap("b", true, 0, 1, 1),
	}
	s := NewPriorityFailover()
	if id := s.Pick(SelectCtx{}, pool, 10); id != "a" {
		t.Fatalf("got=%s want=a", id)
	}
	// Mark priority-0 unhealthy -> should failover to priority-1.
	pool[0].Healthy = false
	if id := s.Pick(SelectCtx{}, pool, 10); id != "b" {
		t.Fatalf("got=%s want=b", id)
	}
}

func TestP2C_AlwaysSelectsEligible(t *testing.T) {
	pool := []upstream.Snapshot{
		mkSnap("a", true, 0, 1, 0),
		mkSnap("b", true, 0, 1, 0),
		mkSnap("c", false, 0, 1, 0),
	}
	s := NewP2C()
	for i := 0; i < 200; i++ {
		id := s.Pick(SelectCtx{}, pool, 10)
		if id != "a" && id != "b" {
			t.Fatalf("selected ineligible: %s", id)
		}
	}
}
