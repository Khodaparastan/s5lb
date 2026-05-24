package strategy

import (
	"math/rand/v2"

	"github.com/khodaparastan/s5lb/internal/upstream"
)

// LeastActiveSelector picks the upstream with fewest active connections,
// with random tie-breaking.
type LeastActiveSelector struct{}

func NewLeastActive() *LeastActiveSelector  { return &LeastActiveSelector{} }
func (s *LeastActiveSelector) Name() string { return "least-active" }

func (s *LeastActiveSelector) Pick(_ SelectCtx, pool []upstream.Snapshot, maxPer int) string {
	bestID := ""
	bestActive := -1
	tieCount := 0

	for _, u := range pool {
		if !u.Healthy || u.Active >= maxPer {
			continue
		}
		if bestID == "" || u.Active < bestActive {
			bestID = u.ID
			bestActive = u.Active
			tieCount = 1
		} else if u.Active == bestActive {
			// Reservoir sampling: replace with probability 1/(tieCount+1).
			tieCount++
			if rand.IntN(tieCount) == 0 {
				bestID = u.ID
			}
		}
	}
	return bestID
}
