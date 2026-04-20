package strategy

import (
	"sync"

	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// WRRSelector implements nginx-style smooth weighted round-robin.
// Produces a well-spread sequence proportional to Weight without bursts.
type WRRSelector struct {
	mu      sync.Mutex
	current []int
}

func NewWRR(n int) *WRRSelector     { return &WRRSelector{current: make([]int, n)} }
func (s *WRRSelector) Name() string { return "weighted-round-robin" }

func (s *WRRSelector) Pick(_ SelectCtx, pool []*upstream.Upstream, max int) *upstream.Upstream {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.current) != len(pool) {
		s.current = make([]int, len(pool))
	}
	total := 0
	bestIdx := -1
	for i, u := range pool {
		if !u.Healthy || u.Active >= max {
			continue
		}
		w := u.Weight
		if w <= 0 {
			w = 1
		}
		s.current[i] += w
		total += w
		if bestIdx == -1 || s.current[i] > s.current[bestIdx] {
			bestIdx = i
		}
	}
	if bestIdx == -1 {
		return nil
	}
	s.current[bestIdx] -= total
	return pool[bestIdx]
}
