package strategy

import (
	"math/rand/v2"

	"github.com/khodaparastan/s5lb/internal/upstream"
)

// WeightedRandomSelector picks eligible upstreams with probability
// proportional to Weight.
type WeightedRandomSelector struct{}

func NewWeightedRandom() *WeightedRandomSelector { return &WeightedRandomSelector{} }
func (s *WeightedRandomSelector) Name() string   { return "weighted-random" }

func (s *WeightedRandomSelector) Pick(_ SelectCtx, pool []upstream.Snapshot, maxPer int) string {
	cands := eligible(pool, maxPer)
	if len(cands) == 0 {
		return ""
	}
	var total int64
	for _, u := range cands {
		w := int64(u.Weight)
		if w <= 0 {
			w = 1
		}
		total += w
	}
	if total <= 0 {
		return cands[rand.IntN(len(cands))].ID
	}
	r := rand.Int64N(total)
	for _, u := range cands {
		w := int64(u.Weight)
		if w <= 0 {
			w = 1
		}
		if r < w {
			return u.ID
		}
		r -= w
	}
	return cands[len(cands)-1].ID
}
