package strategy

import "github.com/khodaparastan/socks5lb/internal/upstream"

// RandomSelector picks a uniformly-random eligible upstream.
type RandomSelector struct{ rng *lockedRand }

func NewRandom() *RandomSelector       { return &RandomSelector{rng: newLockedRand()} }
func (s *RandomSelector) Name() string { return "random" }

func (s *RandomSelector) Pick(_ SelectCtx, pool []*upstream.Upstream, max int) *upstream.Upstream {
	cands := eligible(pool, max)
	if len(cands) == 0 {
		return nil
	}
	return cands[s.rng.Intn(len(cands))]
}
