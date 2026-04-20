package strategy

import "github.com/khodaparastan/socks5lb/internal/upstream"

// WeightedRandomSelector picks an eligible upstream with probability
// proportional to its Weight.
type WeightedRandomSelector struct{ rng *lockedRand }

func NewWeightedRandom() *WeightedRandomSelector {
	return &WeightedRandomSelector{rng: newLockedRand()}
}
func (s *WeightedRandomSelector) Name() string { return "weighted-random" }

func (s *WeightedRandomSelector) Pick(_ SelectCtx, pool []*upstream.Upstream, max int) *upstream.Upstream {
	cands := eligible(pool, max)
	if len(cands) == 0 {
		return nil
	}
	total := 0
	for _, u := range cands {
		w := u.Weight
		if w <= 0 {
			w = 1
		}
		total += w
	}
	if total <= 0 {
		return cands[s.rng.Intn(len(cands))]
	}
	r := s.rng.Intn(total)
	for _, u := range cands {
		w := u.Weight
		if w <= 0 {
			w = 1
		}
		if r < w {
			return u
		}
		r -= w
	}
	return cands[len(cands)-1]
}
