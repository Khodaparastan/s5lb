package strategy

import (
	"sync/atomic"

	"github.com/khodaparastan/s5lb/internal/upstream"
)

// RoundRobinSelector rotates through the pool, skipping ineligible upstreams.
type RoundRobinSelector struct{ idx atomic.Uint64 }

func NewRoundRobin() *RoundRobinSelector   { return &RoundRobinSelector{} }
func (s *RoundRobinSelector) Name() string { return "round-robin" }

func (s *RoundRobinSelector) Pick(_ SelectCtx, pool []upstream.Snapshot, maxPer int) string {
	n := len(pool)
	if n == 0 {
		return ""
	}
	un := uint64(n)
	for attempts := 0; attempts < n; attempts++ {
		i := (s.idx.Add(1) - 1) % un
		u := pool[i]
		if u.Healthy && u.Active < maxPer {
			return u.ID
		}
	}
	return ""
}
