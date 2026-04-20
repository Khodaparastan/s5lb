package strategy

import (
	"math/rand/v2"

	"github.com/khodaparastan/socks5lb/internal/upstream"
)

// LeastActiveSelector picks the upstream with fewest active connections,
// with random tie-breaking.
type LeastActiveSelector struct{}

func NewLeastActive() *LeastActiveSelector  { return &LeastActiveSelector{} }
func (s *LeastActiveSelector) Name() string { return "least-active" }

func (s *LeastActiveSelector) Pick(_ SelectCtx, pool []upstream.Snapshot, maxPer int) string {
	cands := eligible(pool, maxPer)
	if len(cands) == 0 {
		return ""
	}
	best := []upstream.Snapshot{cands[0]}
	for _, u := range cands[1:] {
		switch {
		case u.Active < best[0].Active:
			best = best[:0]
			best = append(best, u)
		case u.Active == best[0].Active:
			best = append(best, u)
		}
	}
	if len(best) == 1 {
		return best[0].ID
	}
	return best[rand.IntN(len(best))].ID
}
