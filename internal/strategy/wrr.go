package strategy

import (
	"sync"

	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// WRRSelector implements nginx-style smooth weighted round-robin.
// Per-upstream current weights are keyed by upstream ID so pool reloads
// don't corrupt state.
type WRRSelector struct {
	mu      sync.Mutex
	current map[string]int
}

func NewWRR() *WRRSelector          { return &WRRSelector{current: make(map[string]int)} }
func (s *WRRSelector) Name() string { return "weighted-round-robin" }

func (s *WRRSelector) Pick(_ SelectCtx, pool []upstream.Snapshot, maxPer int) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	total := 0
	bestID := ""
	var bestCur int
	for _, u := range pool {
		if !u.Healthy || u.Active >= maxPer {
			continue
		}
		w := u.Weight
		if w <= 0 {
			w = 1
		}
		cur := s.current[u.ID] + w
		s.current[u.ID] = cur
		total += w
		if bestID == "" || cur > bestCur {
			bestID = u.ID
			bestCur = cur
		}
	}
	if bestID == "" {
		return ""
	}
	s.current[bestID] = bestCur - total
	return bestID
}
