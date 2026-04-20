package strategy

import (
	"sync/atomic"

	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// RoundRobinSelector rotates strictly through the pool, skipping ineligible
// upstreams.
type RoundRobinSelector struct{ idx atomic.Uint64 }

func NewRoundRobin() *RoundRobinSelector   { return &RoundRobinSelector{} }
func (s *RoundRobinSelector) Name() string { return "round-robin" }

func (s *RoundRobinSelector) Pick(_ SelectCtx, pool []*upstream.Upstream, max int) *upstream.Upstream {
	n := len(pool)
	if n == 0 {
		return nil
	}
	for attempts := 0; attempts < n; attempts++ {
		i := int(s.idx.Add(1)-1) % n
		u := pool[i]
		if u.Healthy && u.Active < max {
			return u
		}
	}
	return nil
}
