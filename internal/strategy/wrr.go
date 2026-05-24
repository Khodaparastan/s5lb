package strategy

import (
	"sync"

	"github.com/khodaparastan/s5lb/internal/upstream"
)

// WRRSelector implements nginx-style smooth weighted round-robin.
// Per-upstream current weights are keyed by upstream ID so pool reloads
// don't corrupt state. int64 is used for weights to prevent overflow.
type WRRSelector struct {
	mu      sync.Mutex
	current map[string]int64
}

func NewWRR() *WRRSelector          { return &WRRSelector{current: make(map[string]int64)} }
func (s *WRRSelector) Name() string { return "weighted-round-robin" }

func (s *WRRSelector) Pick(_ SelectCtx, pool []upstream.Snapshot, maxPer int) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Prune stale keys not present in the current pool.
	if len(s.current) > 0 {
		active := make(map[string]struct{}, len(pool))
		for _, u := range pool {
			active[u.ID] = struct{}{}
		}
		for id := range s.current {
			if _, ok := active[id]; !ok {
				delete(s.current, id)
			}
		}
	}

	var total int64
	bestID := ""
	var bestCur int64
	for _, u := range pool {
		if !u.Healthy || u.Active >= maxPer {
			continue
		}
		w := int64(u.Weight)
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
