package strategy

import "github.com/khodaparastan/socks5lb/internal/upstream"

// LeastActiveSelector picks the upstream with the fewest active connections,
// randomizing ties to avoid hot-spotting the first slice element.
type LeastActiveSelector struct{ rng *lockedRand }

func NewLeastActive() *LeastActiveSelector  { return &LeastActiveSelector{rng: newLockedRand()} }
func (s *LeastActiveSelector) Name() string { return "least-active" }

func (s *LeastActiveSelector) Pick(_ SelectCtx, pool []*upstream.Upstream, max int) *upstream.Upstream {
	cands := eligible(pool, max)
	if len(cands) == 0 {
		return nil
	}
	best := []*upstream.Upstream{cands[0]}
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
		return best[0]
	}
	return best[s.rng.Intn(len(best))]
}
